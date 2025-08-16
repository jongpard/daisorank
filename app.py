# -*- coding: utf-8 -*-
"""
다이소몰 뷰티/위생 '일간' 랭킹 크롤러
- 수집: Playwright(우선) + Requests(폴백)
- 탭 강제: '뷰티/위생' 카테고리, '일간' 탭 고정 (검증/재시도)
- 로드: 무한 스크롤 + '더보기' 병행, 최소 TARGET_COUNT까지 강제
- 저장: data/다이소몰_뷰티위생_일간_YYYY-MM-DD.csv (KST)
- 슬랙: 올리브영 포맷 (TOP10 → 급상승 → 뉴랭커 → 급하락(5개) → 랭크 인&아웃)
- 드라이브: refresh token oauth-only 업로드 (ID는 로그에만 남기고 메시지 미노출)
"""
import os
import re
import csv
import sys
import time
import traceback
import pathlib
import datetime as dt
from typing import List, Dict, Optional

import pytz
import requests
from bs4 import BeautifulSoup

# -------------------- 고정값/경로 --------------------
BASE_URL = "https://www.daisomall.co.kr"
RANK_URL = f"{BASE_URL}/ds/rank/C105"  # 뷰티/위생
DATA_DIR = pathlib.Path("data")
DEBUG_DIR = pathlib.Path("data/debug")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)
KST = pytz.timezone("Asia/Seoul")

# 최소 수집 목표 개수 (기본 200; 필요 시 환경변수로 조절: 100/200 등)
TARGET_COUNT = int(os.getenv("DAISO_TARGET_COUNT", "200"))

# -------------------- 환경변수 --------------------
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()

# -------------------- 유틸 --------------------
def today_kst() -> str:
    return dt.datetime.now(KST).strftime("%Y-%m-%d")

def to_int(s: str) -> Optional[int]:
    try:
        return int(re.sub(r"[^\d]", "", s))
    except Exception:
        return None

def fmt_won(n: Optional[int]) -> str:
    if n is None:
        return "0원"
    return f"{n:,}원"

def slack(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[Slack] 미설정")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
        print("[Slack] 전송 완료")
    except Exception as e:
        print("[Slack 실패]", e)

def load_csv(path: Optional[pathlib.Path]) -> List[Dict]:
    if not path or not isinstance(path, pathlib.Path) or not path.exists() or not path.is_file():
        return []
    out = []
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            out.append({
                "date": row.get("date",""),
                "rank": int(row.get("rank","0") or 0),
                "name": row.get("name",""),
                "price": int(row.get("price","0") or 0),
                "url": row.get("url",""),
            })
    return out

def save_csv(path: pathlib.Path, rows: List[Dict]):
    cols = ["date","rank","name","price","url"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k:r.get(k,"") for k in cols})

# -------------------- Google Drive --------------------
def gdrive_service():
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN and GDRIVE_FOLDER_ID):
        raise RuntimeError("Drive 자격/폴더 ID 미설정")
    creds = Credentials(
        None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        # scopes 미지정 → refresh token 권한만 사용 (invalid_scope 회피)
    )
    return build("drive","v3",credentials=creds, cache_discovery=False)

def gdrive_upload(path: pathlib.Path) -> str:
    from googleapiclient.http import MediaInMemoryUpload
    svc = gdrive_service()
    media = MediaInMemoryUpload(path.read_bytes(), mimetype="text/csv", resumable=False)
    meta = {"name": path.name, "parents":[GDRIVE_FOLDER_ID]}
    f = svc.files().create(body=meta, media_body=media, fields="id", supportsAllDrives=True).execute()
    return f["id"]

# -------------------- 파싱 --------------------
def _clean_name(txt: str) -> str:
    # 'BEST |' 접두 제거 + 공백 축소
    t = re.sub(r"^\s*BEST\s*\|\s*", "", (txt or "").strip(), flags=re.I)
    return " ".join(t.split())

def parse_html(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    items: List[Dict] = []
    cards = soup.select(".goods-list.type-card .goods-unit")
    for idx, card in enumerate(cards, start=1):
        # 이름
        name = ""
        for sel in [".goods-name", ".title", ".tit", ".name", ".goods-info .txt"]:
            el = card.select_one(sel)
            if el:
                name = el.get_text(" ", strip=True)
                break
        name = _clean_name(name)

        # 가격
        price_txt = ""
        for sel in [".goods-price .value", ".price .value", ".goods-price .price", ".price"]:
            el = card.select_one(sel)
            if el:
                price_txt = el.get_text(strip=True)
                break
        price = to_int(price_txt) or 0

        # 링크
        href = ""
        a = card.select_one("a")
        if a and a.has_attr("href"):
            href = a["href"]
        url = href if href.startswith("http") else (BASE_URL + href if href else RANK_URL)

        items.append({
            "rank": idx,
            "name": name,
            "price": price,
            "url": url,
        })
    return items

# -------------------- Playwright 수집 --------------------
def fetch_playwright() -> List[Dict]:
    from playwright.sync_api import sync_playwright

    def is_day_active(page) -> bool:
        grp = page.locator(".el-radio-group.ipt-sorting")
        try:
            act = grp.locator("label.is-active, label.active")
            if act.count() and "일간" in act.first.inner_text():
                return True
        except Exception:
            pass
        try:
            day = grp.locator("label:has-text('일간')")
            if day.count():
                pressed = (day.first.get_attribute("aria-pressed") or "") == "true"
                checked = (day.first.locator("input").get_attribute("aria-checked") or "") == "true"
                if pressed or checked:
                    return True
        except Exception:
            pass
        return False

    def ensure_category_and_day(page):
        # 1) 뷰티/위생 on
        try:
            cat = page.locator("button.cate-btn:has-text('뷰티/위생')")
            if cat.count():
                cls = cat.first.get_attribute("class") or ""
                if "on" not in cls:
                    cat.first.click()
                    page.wait_for_timeout(500)
        except Exception:
            pass
        # 2) 일간 탭 강제(주간→일간 연속 클릭 포함, 검증 최대 6회)
        for _ in range(6):
            if is_day_active(page):
                return
            try:
                week = page.locator(".el-radio-group.ipt-sorting label:has-text('주간')")
                if week.count():
                    week.first.click()
            except Exception:
                pass
            try:
                day = page.locator(".el-radio-group.ipt-sorting label:has-text('일간')")
                if day.count():
                    day.first.click()
            except Exception:
                pass
            page.wait_for_timeout(700)
        print("[warn] 일간 탭 고정 확인 실패(현재 탭으로 진행)")

    def force_load(page, target: int) -> int:
        def count_cards():
            return page.locator(".goods-list.type-card .goods-unit").count()

        last = -1
        stall = 0
        for _ in range(80):  # 안전 한도
            if count_cards() >= target:
                break
            # 더보기 클릭
            try:
                more = page.locator("button:has-text('더보기')")
                if more.count() and more.first.is_enabled():
                    more.first.click()
                    page.wait_for_timeout(800)
            except Exception:
                pass
            # 스크롤
            page.mouse.wheel(0, 3600)
            page.wait_for_timeout(700)
            cur = count_cards()
            if cur == last:
                stall += 1
                if stall >= 6:  # 증가 없음 6회 연속 → 종료
                    break
            else:
                stall = 0
                last = cur
        return count_cards()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu"])
        ctx = browser.new_context(locale="ko-KR", viewport={"width":1440,"height":2000})
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)

        ensure_category_and_day(page)
        total = force_load(page, TARGET_COUNT)

        # 디버그 저장
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        (DEBUG_DIR / "page_rank.png").write_bytes(page.screenshot(full_page=True))
        (DEBUG_DIR / "page_rank.html").write_text(page.content(), encoding="utf-8")

        html = page.content()
        ctx.close()
        browser.close()

    items = parse_html(html)
    # TARGET_COUNT로 잘렸을 수 있으니 상위만 취함
    return items[:max(TARGET_COUNT, 1)]

# -------------------- Requests 폴백 --------------------
def fetch_requests() -> List[Dict]:
    r = requests.get(RANK_URL, timeout=20, headers={
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    })
    r.raise_for_status()
    html = r.text
    (DEBUG_DIR / "page_rank.html").write_text(html, encoding="utf-8")
    return parse_html(html)

# -------------------- 전일 비교/변동 --------------------
def normalize_name(s: str) -> str:
    return re.sub(r"\s+"," ",s or "").strip().lower()

def prev_csv_path(today_csv: pathlib.Path) -> Optional[pathlib.Path]:
    stem = "다이소몰_뷰티위생_일간_"
    files = sorted(DATA_DIR.glob(f"{stem}*.csv"))
    prevs = [p for p in files if p.name < today_csv.name]
    return prevs[-1] if prevs else None

def analyze(today: List[Dict], prev: List[Dict]) -> Dict[str, List[Dict]]:
    N = 30
    tmap = {normalize_name(x["name"]): x for x in today}
    pmap = {normalize_name(x["name"]): x for x in prev}
    rising, falling, new_in, out = [], [], [], []

    for k, t in tmap.items():
        if t["rank"]<=N:
            p = pmap.get(k)
            if p:
                d = p["rank"] - t["rank"]
                if d>0:
                    rising.append({"name":t["name"],"prev":p["rank"],"curr":t["rank"],"delta":d,"url":t["url"]})
            else:
                new_in.append({"name":t["name"],"prev":None,"curr":t["rank"],"url":t["url"]})

    for k, p in pmap.items():
        if p["rank"]<=N:
            t = tmap.get(k)
            if not t or t["rank"]>N:
                out.append({"name":p["name"],"prev":p["rank"],"curr":None})
            else:
                d = t["rank"] - p["rank"]
                if d>0:
                    falling.append({"name":t["name"],"prev":p["rank"],"curr":t["rank"],"delta":d,"url":t["url"]})

    rising.sort(key=lambda x:(-x["delta"], x["curr"], x["prev"], normalize_name(x["name"])))
    falling.sort(key=lambda x:(-x["delta"], x["prev"], x["curr"], normalize_name(x["name"])))
    new_in.sort(key=lambda x:(x["curr"], normalize_name(x["name"])))
    out.sort(key=lambda x:(x["prev"], normalize_name(x["name"])))

    return {
        "rising": rising[:3],
        "new_in": new_in[:3],
        "falling": falling[:5],
        "out": out,
        "inout_count": len(new_in)+len(out),
    }

# -------------------- 슬랙 메시지 --------------------
def slack_message(today_rows: List[Dict], change: Dict) -> str:
    lines = []
    lines.append(f"*다이소몰 뷰티/위생 일간 — {today_kst()}*")
    lines.append("")
    lines.append("*TOP 10*")
    for r in [x for x in today_rows if x["rank"]<=10]:
        nm = r["name"].replace("&","&amp;").replace("<","〈").replace(">","〉")
        lines.append(f"{r['rank']}. <{r['url']}|{nm}> — {fmt_won(r['price'])}")
    lines.append("")

    lines.append("🔥 *급상승*")
    if change["rising"]:
        for r in change["rising"]:
            lines.append(f"- {r['name']} {r['prev']}위 → {r['curr']}위 (↑{r['delta']})")
    else:
        lines.append("- 해당 없음")
    lines.append("")

    lines.append("🆕 *뉴랭커*")
    if change["new_in"]:
        for r in change["new_in"]:
            lines.append(f"- {r['name']} NEW → {r['curr']}위")
    else:
        lines.append("- 해당 없음")
    lines.append("")

    lines.append("📉 *급하락*")
    had = False
    for r in change["falling"]:
        lines.append(f"- {r['name']} {r['prev']}위 → {r['curr']}위 (↓{r['delta']})"); had=True
    outs = [o for o in change["out"] if o["prev"]<=30]
    for o in outs:
        lines.append(f"- {o['name']} {o['prev']}위 → OUT"); had=True
    if not had: lines.append("- 해당 없음")
    lines.append("")

    lines.append("🔁 *랭크 인&아웃*")
    lines.append(f"{change['inout_count']}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(lines)

# -------------------- MAIN --------------------
def main():
    t0 = time.time()
    print("수집 시작:", RANK_URL)

    # 1) 수집
    try:
        items = fetch_playwright()
        print("[Playwright] 정상 수집")
    except Exception as e:
        print("[Playwright 실패 → Requests 폴백]", e)
        items = fetch_requests()

    cnt = len([i for i in items if i.get("name")])
    print("수집 완료:", cnt)
    if cnt < 20:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    # 2) 저장
    csv_path = DATA_DIR / f"다이소몰_뷰티위생_일간_{today_kst()}.csv"
    rows = [{"date": today_kst(), **{"rank":i["rank"], "name":i["name"], "price":i["price"], "url":i["url"]}} for i in items]
    save_csv(csv_path, rows)

    # 3) 전일 비교
    prev_path = prev_csv_path(csv_path)
    prev_rows = load_csv(prev_path)
    change = analyze(rows, prev_rows)

    # 4) 드라이브 업로드 (ID는 로그만)
    try:
        file_id = gdrive_upload(csv_path)
        print("Drive 업로드 완료:", file_id)
    except Exception as e:
        print("[Drive 업로드 실패]", e)

    # 5) 슬랙 메시지
    msg = slack_message(rows, change)
    slack(msg)

    print(f"총 {cnt}건, 경과: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
