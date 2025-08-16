# -*- coding: utf-8 -*-
import os, re, csv, sys, time, traceback, pathlib, datetime as dt
from typing import List, Dict, Optional

import pytz
import requests
from bs4 import BeautifulSoup

# -------------------- 설정 --------------------
BASE_URL = "https://www.daisomall.co.kr"
RANK_URL = f"{BASE_URL}/ds/rank/C105"     # 뷰티/위생
DATA_DIR = pathlib.Path("data")
DEBUG_DIR = pathlib.Path("data/debug")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)
KST = pytz.timezone("Asia/Seoul")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()

# -------------------- 유틸 --------------------
def today_kst() -> str:
    return dt.datetime.now(KST).strftime("%Y-%m-%d")

def to_int(s: str) -> Optional[int]:
    try: return int(re.sub(r"[^\d]", "", s))
    except: return None

def fmt_won(n: Optional[int]) -> str:
    if n is None: return "0원"
    return f"{n:,}원"

def slack(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[Slack] 미설정")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
        print("Slack 전송 완료")
    except Exception as e:
        print("[Slack 실패]", e)

def load_csv(path: Optional[pathlib.Path]) -> List[Dict]:
    """파일이 없거나 디렉터리면 빈 리스트 반환"""
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
        for r in rows: w.writerow({k:r.get(k,"") for k in cols})

# -------------------- Drive (scopes 지정 X) --------------------
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
        # 중요: scopes 전달하지 않음 (리프레시 토큰의 기존 scope 사용)
    )
    return build("drive","v3",credentials=creds, cache_discovery=False)

def gdrive_upload(path: pathlib.Path) -> str:
    from googleapiclient.http import MediaInMemoryUpload
    svc = gdrive_service()
    media = MediaInMemoryUpload(path.read_bytes(), mimetype="text/csv", resumable=False)
    meta = {"name": path.name, "parents":[GDRIVE_FOLDER_ID]}
    file = svc.files().create(body=meta, media_body=media, fields="id", supportsAllDrives=True).execute()
    return file["id"]

# -------------------- 파서 --------------------
def parse_html(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    items: List[Dict] = []
    for card in soup.select(".goods-unit"):
        num_el = card.select_one(".ranking-area .rank .num")
        if not num_el:  # 배너 등
            continue
        rank = to_int(num_el.get_text(strip=True)) or 0

        # 이름: BEST 라벨 제거
        tit = card.select_one(".goods-detail .tit")
        name = ""
        if tit:
            for b in tit.select(".best"): b.extract()
            name = " ".join(tit.get_text(" ", strip=True).split())
            name = re.sub(r"^\s*BEST\s*", "", name, flags=re.I)

        price_el = card.select_one(".goods-detail .goods-price .value")
        price = to_int(price_el.get_text(strip=True)) if price_el else 0

        a = card.select_one(".goods-thumb a.goods-link")
        url = BASE_URL + a["href"] if a and a.has_attr("href") else RANK_URL

        items.append({"rank": rank, "name": name, "price": price or 0, "url": url})
    items.sort(key=lambda x: x["rank"])
    return items

# -------------------- 수집 (Playwright 우선) --------------------
def fetch_playwright() -> List[Dict]:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        page = browser.new_page(viewport={"width":1440,"height":1800},
                                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                            "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"))
        page.goto(RANK_URL, wait_until="networkidle", timeout=60_000)

        # 1) 카테고리 '뷰티/위생' 강제 클릭
        def try_click_beauty():
            tries = [
                "role=link[name='뷰티/위생']",
                "role=button[name='뷰티/위생']",
                "a:has-text('뷰티/위생')",
                "button:has-text('뷰티/위생')",
            ]
            for sel in tries:
                try:
                    page.locator(sel).first.click(timeout=1200); return True
                except Exception: pass
            try:
                page.evaluate("""
                () => {
                  const t = (el) => el.textContent && el.textContent.includes('뷰티') && el.textContent.includes('위생');
                  const nodes = Array.from(document.querySelectorAll('a,button,div,span'));
                  const hit = nodes.find(t);
                  if (hit) hit.click();
                }
                """); return True
            except Exception:
                return False

        try_click_beauty()
        page.wait_for_timeout(600)

        # 2) '일간' 강제 클릭
        def try_click_daily():
            tries = [
                "role=button[name='일간']",
                "button:has-text('일간')",
                "a:has-text('일간')",
            ]
            for sel in tries:
                try:
                    page.locator(sel).first.click(timeout=1200); return True
                except Exception: pass
            try:
                page.evaluate("""
                () => {
                  const nodes = Array.from(document.querySelectorAll('button,a,div,span'));
                  const hit = nodes.find(el => (el.textContent||'').includes('일간'));
                  if (hit) hit.click();
                }
                """); return True
            except Exception:
                return False

        try_click_daily()
        page.wait_for_timeout(800)

        # 3) 끝까지 스크롤 (lazy load)
        def scroll_bottom():
            same = 0
            for _ in range(40):
                h = page.evaluate("() => document.body.scrollHeight")
                page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(300)
                hh = page.evaluate("() => document.body.scrollHeight")
                if hh == h:
                    same += 1
                    if same >= 3: break
                else:
                    same = 0

        scroll_bottom()

        # 디버그 저장
        DEBUG_DIR.mkdir(exist_ok=True, parents=True)
        (DEBUG_DIR / "page_rank.png").write_bytes(page.screenshot(full_page=True))
        (DEBUG_DIR / "page_rank.html").write_text(page.content(), encoding="utf-8")

        html = page.content()
        browser.close()
    items = parse_html(html)

    # 카드가 적으면 재시도 1회
    if len([i for i in items if i["rank"]]) < 30:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
            page = browser.new_page()
            page.goto(RANK_URL, wait_until="networkidle", timeout=60_000)
            try:
                page.locator("button:has-text('일간')").first.click(timeout=1500)
            except Exception: pass
            page.wait_for_timeout(700)
            html = page.content()
            browser.close()
        items = parse_html(html)

    return items

def fetch_requests() -> List[Dict]:
    r = requests.get(RANK_URL, timeout=20, headers={
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    })
    r.raise_for_status()
    html = r.text
    (DEBUG_DIR / "page_rank.html").write_text(html, encoding="utf-8")
    return parse_html(html)

# -------------------- 변동 계산 (Top30) --------------------
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

    # 급상승/뉴랭커
    for k, t in tmap.items():
        if t["rank"]<=N:
            p = pmap.get(k)
            if p:
                d = p["rank"] - t["rank"]
                if d>0:
                    rising.append({"name":t["name"],"prev":p["rank"],"curr":t["rank"],"delta":d,"url":t["url"]})
            else:
                new_in.append({"name":t["name"],"prev":None,"curr":t["rank"],"url":t["url"]})

    # 급하락 + OUT
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
def slack_message(today_rows: List[Dict], change: Dict, csv_path: pathlib.Path, drive_id: Optional[str]) -> str:
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
    lines.append("")
    tail = [f"CSV: `{csv_path.name}`"]
    if drive_id: tail.append(f"Drive 파일 ID: `{drive_id}`")
    lines.append("_" + "  •  ".join(tail) + "_")
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

    cnt = len([i for i in items if i.get("rank")])
    print("수집 완료:", cnt)
    if cnt < 10:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    # 2) 저장
    csv_path = DATA_DIR / f"다이소몰_뷰티위생_일간_{today_kst()}.csv"
    rows = [{"date": today_kst(), **i} for i in items]
    save_csv(csv_path, rows)

    # 3) 전일 비교
    prev_path = prev_csv_path(csv_path)
    prev_rows = load_csv(prev_path)
    change = analyze(rows, prev_rows)

    # 4) 드라이브
    drive_id = None
    try:
        drive_id = gdrive_upload(csv_path)
        print("Drive 업로드 완료:", drive_id)
    except Exception as e:
        print("[Drive 업로드 실패]", e)

    # 5) 슬랙
    msg = slack_message(rows, change, csv_path, drive_id)
    slack(msg)

    print(f"총 {cnt}건, 경과: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
