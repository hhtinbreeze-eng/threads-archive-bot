import os
import requests
from datetime import datetime, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build

THREADS_ACCESS_TOKEN = os.environ["THREADS_ACCESS_TOKEN"]
TARGET_USERNAME = os.environ["TARGET_USERNAME"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SHEET_NAME = os.environ.get("SHEET_NAME", "Sheet1")
GOOGLE_CREDENTIALS_FILE = "service_account.json"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
BASE_URL = "https://graph.threads.net/v1.0"


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_FILE, scopes=SCOPES
    )
    return build("sheets", "v4", credentials=creds)


def lookup_profile(username):
    url = f"{BASE_URL}/profile_lookup"
    params = {
        "username": username,
        "fields": "id,username",
        "access_token": THREADS_ACCESS_TOKEN,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def get_user_threads(user_id):
    url = f"{BASE_URL}/{user_id}/threads"
    params = {
        "fields": "id,text,timestamp,permalink",
        "access_token": THREADS_ACCESS_TOKEN,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("data", [])


def get_existing_post_ids(service):
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A2:A"
    ).execute()
    values = result.get("values", [])
    return set(row[0] for row in values if row)


def append_rows(service, rows):
    if not rows:
        print("No new rows.")
        return

    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:F",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()


def main():
    service = get_sheets_service()
    profile = lookup_profile(TARGET_USERNAME)

    posts = get_user_threads(profile["id"])
    existing_ids = get_existing_post_ids(service)

    now_str = datetime.now(timezone.utc).astimezone().isoformat()
    username = profile.get("username", TARGET_USERNAME)

    new_rows = []

    for post in posts:

        post_id = post.get("id")

        if not post_id or post_id in existing_ids:
            continue

        new_rows.append([
            post_id,
            now_str,
            post.get("timestamp", ""),
            username,
            post.get("text", ""),
            post.get("permalink", "")
        ])

    new_rows.sort(key=lambda x: x[2])

    append_rows(service, new_rows)


if __name__ == "__main__":
    main()
