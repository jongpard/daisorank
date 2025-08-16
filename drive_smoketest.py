# drive_smoketest.py
import os, io, json, re, logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GoogleRequest

logging.basicConfig(level=logging.INFO, format="%(message)s")

GDRIVE_FOLDER_ID_RAW = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_SA_JSON       = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

def norm_folder_id(raw: str) -> str:
    m = re.search(r"/folders/([A-Za-z0-9_-]{10,})", raw or "")
    return m.group(1) if m else (raw or "").strip()

def get_service():
    # 1) Service Account 우선
    if GOOGLE_SA_JSON:
        info = json.loads(GOOGLE_SA_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/drive"])
        logging.info(f"use=service_account email={info.get('client_email')}")
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    # 2) OAuth refresh token
    if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN:
        creds = Credentials(
            None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        creds.refresh(GoogleRequest())
        logging.info("use=oauth scope=drive")
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    raise SystemExit("NO_DRIVE_CREDENTIALS")

def main():
    folder_id = norm_folder_id(GDRIVE_FOLDER_ID_RAW)
    svc = get_service()
    # 업로드 테스트(덮어쓰기 가능 여부)
    name = "_smoketest_daiso.txt"
    data = io.BytesIO(b"hello drive")
    media = MediaIoBaseUpload(data, mimetype="text/plain", resumable=False)

    # 같은 이름 있으면 update, 없으면 create
    q = f"name='{name}' and trashed=false"
    if folder_id: q += f" and '{folder_id}' in parents"
    res = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute().get("files", [])

    if res:
        fid = res[0]["id"]
        svc.files().update(fileId=fid, media_body=media).execute()
        logging.info(f"upsert=update id={fid} folder_id={folder_id or '(none)'}")
    else:
        meta = {"name": name}
        if folder_id: meta["parents"] = [folder_id]
        f = svc.files().create(body=meta, media_body=media, fields="id").execute()
        logging.info(f"upsert=create id={f['id']} folder_id={folder_id or '(none)'}")

if __name__ == "__main__":
    main()
