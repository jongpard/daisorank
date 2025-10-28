# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (신 UI 대응 완전판)
# - Vue 기반 신 구조 대응: a.product-link / .product_name / .product_price
# - CSV + Google Drive + Slack 리포트 유지
# - 수집 부족해도 실패 종료 금지
# - 디버그: HTML/스크린샷 자동 저장

import os, re, csv, time, io, json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ====== 설정 ======
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_ITEMS = 200
MIN_OK = 10
TOP_WINDOW = 150
SCROLL_PAUSE = 0.6
SCROLL_MAX_ROUNDS = 90

KST = timezone(timedelta(hours=9))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# ====== 유틸 ======
def today_str(): return datetime.now(KST).strftime("%Y-%m-%d")
def yday_str(): return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def strip_best(name: str) -> str:
    if not name: return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()

def close_overlays(page: Page):
    selectors = [".popup .btn-close", ".modal .btn-close", ".layer-popup .btn-close", ".btn-x", "button[aria-label*='닫기']"]
    for sel in selectors:
        try:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click(timeout=1000)
                page.wait_for_timeout(200)
        except Exception:
            pass

# ====== 디버그 ======
def dump_debug(page: Page, label: str):
    os.makedirs("data/debug", exist_ok=True)
    try:
        with open(f"data/debug/rank_raw_{label}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        page.screenshot(path=f"data/debug/page_{label}.png", full_page=True)
    except Exception:
        pass

# ====== Playwright 기본 동작 ======
def select_beauty_daily(page: Page):
    close_overlays(page)
    try:
        page.wait_for_selector('.prod-category', timeout=8000)
        if page.locator('.cate-btn[value="CTGR_00014"]').count() > 0:
            page.locator('.cate-btn[value="CTGR_00014"]').first.click()
        else:
            btn = page.get_by_role("button", name=re.compile("뷰티|위생"))
            if btn: btn.click()
    except Exception: pass
    time.sleep(0.3)

    try:
        if page.locator('.ipt-sorting input[value="2"]').count() > 0:
            page.locator('.ipt-sorting input[value="2"]').first.click()
        else:
            btn = page.get_by_role("button", name=re.compile("일간"))
            if btn: btn.click()
    except Exception: pass
    time.sleep(0.4)

def infinite_scroll(page: Page):
    prev = 0; stable = 0
    for _ in range(SCROLL_MAX_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        try: page.wait_for_load_state("networkidle", timeout=1500)
        except Exception: pass
        page.wait_for_timeout(int(SCROLL_PAUSE * 1000))
        cnt = page.evaluate("() => document.querySelectorAll('a[href*=\"/pd/pdr/\"]').length")
        if cnt >= MAX_ITEMS: break
        if cnt == prev: stable += 1
        else: stable = 0; prev = cnt
        if stable >= 6: break

# ====== 핵심: 수집기 ======
def collect_items(page: Page) -> List[Dict]:
    # Vue 렌더 완료 대기
    page.wait_for_function("() => document.querySelectorAll('a[href*=\"/pd/pdr/\"]').length > 50", timeout=25000)
    time.sleep(1.0)

    data = page.evaluate("""
        () => {
          const anchors = Array.from(document.querySelectorAll('a[href*="/pd/pdr/"]'));
          const seen = new Set();
          const items = [];
          for (const a of anchors) {
            const url = a.href;
            if (!url || seen.has(url)) continue;
            seen.add(url);
            const nameEl = a.querySelector('.product_name, .goods-name, .goods_name, .tit, .name');
            const priceEl = a.querySelector('.product_price, .price, .num');
            let name = nameEl ? nameEl.textContent.trim() : '';
            let priceTxt = priceEl ? priceEl.textContent.replace(/[^0-9]/g, '') : '';
            const price = parseInt(priceTxt || '0', 10);
            if (name && price > 0) {
              items.push({ name, price, url });
            }
          }
          return items;
        }
    """)

    cleaned = []
    for i, it in enumerate(data, 1):
        cleaned.append({
            "rank": i,
            "name": strip_best(it["name"]),
            "price": it["price"],
            "url": it["url"]
        })
    print(f"[DEBUG] 최종 추출 {len(cleaned)}개 완료")
    return cleaned

def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1360, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60000)

        select_beauty_daily(page)
        dump_debug(page, "before")
        infinite_scroll(page)
        dump_debug(page, "after")

        items = collect_items(page)
        context.close()
        browser.close()
        return items

# ====== CSV 저장 ======
def save_csv(rows: List[Dict]):
    os.makedirs("data", exist_ok=True)
    date_str = today_str()
    filename = f"다이소몰_뷰티위생_일간_{date_str}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "rank", "name", "price", "url"])
        for r in rows:
            w.writerow([date_str, r["rank"], r["name"], r["price"], r["url"]])
    return path, filename

# ====== Google Drive ======
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] OAuth 환경변수 미설정")
        return None
    try:
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
    except Exception as e:
        print("[Drive] 서비스 생성 실패:", e)
        return None

def upload_to_drive(service, filepath: str, filename: str):
    if not service or not GDRIVE_FOLDER_ID:
        print("[Drive] 서비스 또는 폴더 ID 없음")
        return
    try:
        q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id)").execute()
        file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None
        media = MediaIoBaseUpload(io.FileIO(filepath, "rb"), mimetype="text/csv")
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"[Drive] 업데이트 완료: {filename}")
        else:
            meta = {"name": filename, "parents": [GDRIVE_FOLDER_ID]}
            service.files().create(body=meta, media_body=media).execute()
            print(f"[Drive] 업로드 완료: {filename}")
    except Exception as e:
        print("[Drive] 업로드 실패:", e)

# ====== Slack ======
def post_slack(rows: List[Dict]):
    if not SLACK_WEBHOOK:
        print("[Slack] Webhook 미설정")
        return
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    lines = [f"*📊 다이소몰 뷰티/위생 일간 Top200* ({now} KST)\n"]
    for it in rows[:10]:
        lines.append(f"{it['rank']}. <{it['url']}|{it['name']}> — {it['price']:,}원")
    msg = "\n".join(lines)
    try:
        requests.post(SLACK_WEBHOOK, json={"text": msg}, timeout=10)
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ====== main ======
def main():
    print("수집 시작:", RANK_URL)
    t0 = time.time()
    rows = fetch_products()
    print(f"[수집 완료] {len(rows)}개")
    if len(rows) < MIN_OK:
        print(f"[경고] 수집 부족({len(rows)}개), 그래도 진행")

    csv_path, csv_filename = save_csv(rows)
    drive_service = build_drive_service()
    if drive_service:
        upload_to_drive(drive_service, csv_path, csv_filename)
    post_slack(rows)
    print(f"총 {len(rows)}건, 경과 {time.time() - t0:.1f}s")

if __name__ == "__main__":
    main()
