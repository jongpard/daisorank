# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (API 안정화 버전)
# - API 직접 호출 (Playwright 제거)
# - 뷰티/위생: largeExhCtgrNo=CTGR_00014
# - 일간: period=D
# - 200개 고정
# - 전일 비교: name 기준
# - CSV: date, rank, name, price, url
# - Slack 포맷 기존 유지

import os, csv, io, sys, traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest


# ========= 설정 =========
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))
TOPN = int(os.getenv("TOP_WINDOW", str(MAX_ITEMS)))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))

API_URL = "https://www.daisomall.co.kr/ssn/search/GoodsBestSale"
CATEGORY_CODE = "CTGR_00014"  # 🔥 뷰티/위생 고정
PERIOD = "D"                  # 🔥 일간 고정


# ========= 유틸 =========
def now_kst(): return datetime.now(KST)
def today_str(): return now_kst().strftime("%Y-%m-%d")
def yday_str(): return (now_kst() - timedelta(days=1)).strftime("%Y-%m-%d")
def log(msg): print(f"[{now_kst().strftime('%H:%M:%S')}] {msg}", flush=True)

def strip_best(name: str) -> str:
    if not name: return ""
    return name.replace("BEST", "").strip()


# ========= API 수집 =========
def fetch_daiso_beauty_daily() -> List[Dict]:

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.daisomall.co.kr/ds/rank/C105",
        "Accept": "application/json, text/plain, */*"
    }

    all_rows = []
    page = 1

    while len(all_rows) < MAX_ITEMS:
        params = {
            "period": PERIOD,
            "pageNum": page,
            "cntPerPage": 100,
            "largeExhCtgrNo": CATEGORY_CODE,
            "isCategory": 0,
            "soldOutYn": "N"
        }

        res = requests.get(API_URL, headers=headers, params=params, timeout=15)
        res.raise_for_status()
        data = res.json()

        items = data.get("data", {}).get("list", [])
        if not items:
            break

        for item in items:
            goods_no = item.get("goodsNo")
            name = strip_best(item.get("goodsNm", "").strip())
            price = item.get("salePrice")

            if not (goods_no and name and price):
                continue

            all_rows.append({
                "name": name,
                "price": int(price),
                "url": f"https://www.daisomall.co.kr/pd/pdr/{goods_no}"
            })

        page += 1

    rows = all_rows[:MAX_ITEMS]
    for i, r in enumerate(rows, 1):
        r["rank"] = i

    return rows


# ========= CSV =========
def save_csv(rows: List[Dict]) -> Tuple[str,str]:
    filename = f"다이소몰_뷰티위생_일간_{today_str()}.csv"
    path = os.path.join("data", filename)
    os.makedirs("data", exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","rank","name","price","url"])
        for r in rows:
            w.writerow([today_str(), r["rank"], r["name"], r["price"], r["url"]])
    return path, filename


# ========= Google Drive =========
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        return None
    creds = UserCredentials(
        None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/drive.file"],
    )
    creds.refresh(GoogleRequest())
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def upload_to_drive(svc, filepath, filename):
    if not svc or not GDRIVE_FOLDER_ID:
        return None
    media = MediaIoBaseUpload(io.FileIO(filepath,'rb'), mimetype="text/csv", resumable=True)
    body = {"name": filename, "parents":[GDRIVE_FOLDER_ID]}
    return svc.files().create(body=body, media_body=media, fields="id,name").execute()

def find_file_in_drive(svc, filename):
    if not svc or not GDRIVE_FOLDER_ID:
        return None
    q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
    res = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
    return (res.get("files") or [None])[0]

def download_from_drive(svc, file_id) -> Optional[str]:
    req = svc.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done=False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read().decode("utf-8")


# ========= 전일 비교 =========
def parse_prev_csv(txt: str) -> List[Dict]:
    items=[]
    rdr=csv.DictReader(io.StringIO(txt))
    for row in rdr:
        try:
            items.append({
                "name": row["name"],
                "rank": int(row["rank"]),
                "url": row["url"]
            })
        except:
            continue
    return items

def analyze_trends(today: List[Dict], prev: List[Dict]):
    prev_map = {p["name"]: p["rank"] for p in prev}
    ups, downs = [], []

    for t in today[:TOPN]:
        nm = t["name"]; tr=t["rank"]; pr=prev_map.get(nm)
        if pr is None: continue
        ch = pr - tr
        d = {"name":nm,"url":t["url"],"rank":tr,"prev_rank":pr,"change":ch}
        if ch>0: ups.append(d)
        elif ch<0: downs.append(d)

    today_keys = {t["name"] for t in today[:TOPN]}
    prev_keys  = {p["name"] for p in prev if 1 <= p["rank"] <= TOPN}

    ins_keys  = today_keys - prev_keys
    outs_keys = prev_keys - today_keys

    chart_ins = [t for t in today if t["name"] in ins_keys]
    rank_outs = [p for p in prev  if p["name"] in outs_keys]

    return ups, downs, chart_ins, rank_outs, len(ins_keys)


# ========= Slack =========
def post_slack(rows, analysis, prev_items):
    if not SLACK_WEBHOOK:
        return

    ups, downs, chart_ins, rank_outs, io_cnt = analysis
    prev_map = {p["name"]: p["rank"] for p in prev_items}

    lines = [f"*다이소몰 뷰티/위생 일간 랭킹 {TOPN}* ({now_kst().strftime('%Y-%m-%d %H:%M KST')})"]

    lines.append("\n*TOP 10*")
    for it in rows[:10]:
        cur=it["rank"]
        pr=prev_map.get(it["name"])
        marker="(new)" if pr is None else (f"(↑{pr-cur})" if pr>cur else (f"(↓{cur-pr})" if pr<cur else "(-)"))
        lines.append(f"{cur}. {marker} <{it['url']}|{it['name']}> — {it['price']:,}원")

    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"{io_cnt}개의 제품이 인&아웃 되었습니다.")

    requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=10)


# ========= main =========
def main():
    log("[시작] API 방식 수집")
    rows = fetch_daiso_beauty_daily()
    log(f"[수집] {len(rows)}개")

    csv_path, csv_name = save_csv(rows)

    svc = build_drive_service()
    prev_items=[]

    if svc:
        upload_to_drive(svc, csv_path, csv_name)
        prev_file = find_file_in_drive(svc, f"다이소몰_뷰티위생_일간_{yday_str()}.csv")
        if prev_file:
            txt = download_from_drive(svc, prev_file["id"])
            if txt:
                prev_items = parse_prev_csv(txt)

    analysis = analyze_trends(rows, prev_items)
    post_slack(rows, analysis, prev_items)

    log("[종료] 완료")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[에러] {e}")
        log(traceback.format_exc())
        sys.exit(1)
