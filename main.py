import os
import re
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from urllib.parse import urljoin
from google.oauth2 import service_account
from googleapiclient.discovery import build

TARGET_USERNAME = os.environ["TARGET_USERNAME"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Sheet1")
GOOGLE_CREDENTIALS_FILE = "service_account.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)


def get_existing_post_ids(service):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A2:A"
    ).execute()
    values = result.get("values", [])
    return set(row[0] for row in values if row)


def append_rows(service, rows):
    if not rows:
        print("No new rows to append.")
        return

    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:F",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

    print(f"Appended {len(rows)} rows.")


def fetch_profile_html(username: str) -> str:
    url = f"https://www.threads.com/@{username}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def normalize_text(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    return str(value)


def extract_post_id_from_url(url: str) -> str:
    if not url:
        return ""
    m = re.search(r"/post/([^/?#]+)", url)
    if m:
        return m.group(1)
    return url


def recursive_find_posts(obj, found):
    """
    盡量從頁面內的 JSON 找出貼文。
    只要看到像 Threads post 的 URL，就收集。
    """
    if isinstance(obj, dict):
        url = obj.get("url") or obj.get("permalink") or obj.get("canonicalUrl") or ""
        if isinstance(url, str) and "/post/" in url:
            text = (
                obj.get("articleBody")
                or obj.get("text")
                or obj.get("caption")
                or obj.get("description")
                or obj.get("headline")
                or ""
            )
            timestamp = (
                obj.get("datePublished")
                or obj.get("uploadDate")
                or obj.get("created_at")
                or obj.get("taken_at")
                or ""
            )
            found.append({
                "url": url,
                "text": normalize_text(text),
                "timestamp": normalize_text(timestamp),
            })

        for value in obj.values():
            recursive_find_posts(value, found)

    elif isinstance(obj, list):
        for item in obj:
            recursive_find_posts(item, found)


def extract_posts_from_html(html: str):
    soup = BeautifulSoup(html, "html.parser")
    found = []

    # 先抓 JSON-LD
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
            recursive_find_posts(data, found)
        except Exception:
            pass

    # 再抓一般 script 內可能的 JSON
    for script in soup.find_all("script"):
        raw = script.string or script.get_text()
        if not raw:
            continue

        # 快速過濾，避免每段都硬跑
        if "/post/" not in raw and "datePublished" not in raw and "articleBody" not in raw:
            continue

        # 找 script 裡可能的 JSON 片段
        candidates = []

        # 整段若本身就是 JSON
        stripped = raw.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            candidates.append(stripped)

        # 嘗試抓常見 JSON 物件
        matches = re.findall(r'(\{.*?/post/.*?\})', raw, flags=re.DOTALL)
        candidates.extend(matches)

        for candidate in candidates:
            try:
                data = json.loads(candidate)
                recursive_find_posts(data, found)
            except Exception:
                continue

    # 備援：直接抓頁面上的貼文連結
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/post/" in href:
            full_url = urljoin("https://www.threads.com", href)
            text = normalize_text(a.get_text(" ", strip=True))
            found.append({
                "url": full_url,
                "text": text,
                "timestamp": "",
            })

    # 去重
    dedup = {}
    for item in found:
        url = item.get("url", "")
        if not url or "/post/" not in url:
            continue

        # 統一網址
        if url.startswith("/"):
            url = urljoin("https://www.threads.com", url)
        url = url.split("?")[0]

        post_id = extract_post_id_from_url(url)
        if not post_id:
            continue

        old = dedup.get(post_id, {})
        dedup[post_id] = {
            "post_id": post_id,
            "url": url,
            "text": item.get("text") or old.get("text", ""),
            "timestamp": item.get("timestamp") or old.get("timestamp", ""),
        }

    posts = list(dedup.values())
    posts.sort(key=lambda x: x.get("timestamp", ""))
    return posts


def main():
    service = get_sheets_service()
    existing_ids = get_existing_post_ids(service)

    html = fetch_profile_html(TARGET_USERNAME)
    posts = extract_posts_from_html(html)

    now_str = datetime.now(timezone.utc).astimezone().isoformat()

    new_rows = []
    for post in posts:
        post_id = post["post_id"]
        if post_id in existing_ids:
            continue

        new_rows.append([
            post_id,
            now_str,
            post.get("timestamp", ""),
            TARGET_USERNAME,
            post.get("text", ""),
            post.get("url", ""),
        ])

    append_rows(service, new_rows)


if __name__ == "__main__":
    main()
