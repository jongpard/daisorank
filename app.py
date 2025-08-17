# -*- coding: utf-8 -*-
"""
다이소몰 뷰티/위생 '일간' 랭킹 크롤러 (안정화/유연 파싱/빈셀 제거)
환경변수:
  SLACK_WEBHOOK_URL, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN, GDRIVE_FOLDER_ID
  DAISO_TARGET_COUNT(선택, 기본 200)
"""

import os, re, csv, sys, time, traceback, pathlib, datetime as dt
from typing import List, Dict, Optional

import pytz
import requests
from bs4 import BeautifulSoup

# -------------------- 상수/경로 --------------------
BASE_URL = "https://www.daisomall.co.kr"
RANK_URL  = f"{BASE_URL}/ds/rank/C105"  # 뷰티/위생
DATA_DIR  = pathlib.Path("data")
DEBUG_DIR = pathlib.Path("data/debug")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)
KST = pytz.timezone("Asia/Seoul")

TARGET_COUNT = int(os.getenv("DAISO_TARGET_COUNT", "200"))

SLACK_WEBHOOK_URL   = os.getenv("SLACK_WEBHOOK_URL", "").strip()
GOOGLE_CLIENT_ID    = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET= os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN= os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()
GDRIVE_FOLDER_ID    = os.getenv("GDRIVE_FOLDER_ID", "").strip()

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
    if not path or not path.exists() or not path.is_file():
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
    )
    return build("drive","v3",credentials=creds, cache_discovery=False)

def gdrive_upload(path: pathlib.Path) -> str:
    from googleapiclient.http import MediaInMemoryUpload
    svc = gdrive_service()
    media = MediaInMemoryUpload(path.read_bytes(), mimetype="text/csv", resumable=False)
    meta = {"name": path.name, "parents":[GDRIVE_FOLDER_ID]}
    f = svc.files().create(body=meta, media_body=media, fields="id", supportsAllDrives=True).execute()
    return f["id"]

# -------------------- 파싱/클린 --------------------
def _clean_name(txt: str) -> str:
    """'BEST |', 'BEST｜', 'BEST ㅣ', 'BEST·', 'BEST-' 등 접두 제거 + 공백 정리"""
    t = (txt or "").strip()
    sep = r"[|\uFF5C\u2502\u3139lI:\u00B7\.\-\u2014\u2013\u30FB]"  # | ｜ │ ㅣ l I : · . - — – ・
    pat = re.compile(rf"^\s*BEST(?:\s+|{sep})\s*", re.I)
    while True:
        new = pat.sub("", t).strip()
        if new == t: break
        t = new
    return " ".join(t.split())

def _pick_price(unit) -> Optional[int]:
    """
    가격 셀렉터 유연 파싱:
      - .price .value
      - [class*=price] .value
      - [class*=price] 내부 텍스트에서 숫자 추출
    """
    cand = unit.select_one(".price .value")
    if cand:
        v = to_int(cand.get_text(strip=True))
        if v: return v
    cand = unit.select_one('[class*="price"] .value')
    if cand:
        v = to_int(cand.get_text(strip=True))
        if v: return v
    block = unit.select_one('[class*="price"]')
    if block:
        m = re.search(r"(\d[\d,]*)", block.get_text(" ", strip=True))
        if m:
            v = to_int(m.group(1))
            if v: return v
    return None

def parse_html_filtered(html: str) -> List[Dict]:
    """
    카드 필터링: 제목(.tit/.name/[class*=tit])과 가격이 모두 있는 노드만 상품.
    BEST 라벨 제거. 유효 카드만 연속 랭킹 부여.
    """
    soup = BeautifulSoup(html, "lxml")
    units = soup.select(".goods-list .goods-unit")
    items: List[Dict] = []
    rank = 1
    for unit in units:
        tit_el = unit.select_one(".tit") or unit.select_one(".name") or unit.select_one('[class*="tit"]')
        if not tit_el:
            continue

        # 제목 내부 BEST 뱃지 제거
        for b in tit_el.select(".best"):
            b.extract()
        name = _clean_name(tit_el.get_text(" ", strip=True))

        price = _pick_price(unit)
        if not name or not price or price <= 0:
            continue

        # 링크
        url = RANK_URL
        a = unit.select_one("a")
        if a and a.has_attr("href"):
            href = a["href"]
            url = href if href.startswith("http") else (BASE_URL + href)

        items.append({
            "rank": rank,
            "name": name,
            "price": price,
            "url": url,
        })
        rank += 1
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
                if pressed or checked: return True
        except Exception:
            pass
        return False

    def ensure_category_and_day(page):
        # 뷰티/위생
        try:
            cat = page.locator("button.cate-btn:has-text('뷰티/위생')")
            if cat.count():
                cls = cat.first.get_attribute("class") or ""
                if "on" not in cls and "is-active" not in cls:
                    cat.first.click()
                    page.wait_for_timeout(600)
        except Exception:
            pass
        # 일간 고정
        for _ in range(8):
            if is_day_active(page):
                return
            try:
                week = page.locator(".el-radio-group.ipt-sorting label:has-text('주간')")
                if week.count(): week.first.click()
            except Exception:
                pass
            try:
                day = page.locator(".el-radio-group.ipt-sorting label:has-text('일간')")
                if day.count(): day.first.click()
            except Exception:
                pass
            page.wait_for_timeout(800)
        print("[warn] 일간 탭 고정 확인 실패(현재 탭으로 진행)")

    def force_load(page, target: int) -> int:
        """무한 스크롤 + '더보기' 병행 로드"""
        def count_cards():
            return page.locator(".goods-list .goods-unit").count()

        last = -1; stall = 0
        for _ in range(120):
            if count_cards() >= target: break
            # 더보기
            try:
                more = page.locator("button:has-text('더보기')")
                if more.count() and more.first.is_enabled():
                    more.first.click()
                    page.wait_for_timeout(900)
            except Exception:
                pass
            # 스크롤
            page.mouse.wheel(0, 5000)
            page.wait_for_timeout(700)

            cur = count_cards()
            if cur == last:
                stall += 1
                if stall >= 8: break
            else:
                stall = 0; last = cur
        return count_cards()

    def wait_stable(page, min_round=3, gap_ms=700) -> None:
        """카드 수가 일정 라운드 동안 변하지 않을 때까지 대기(스켈레톤 방지)"""
        def cnt(): return page.locator(".goods-list .goods-unit").count()
        same = 0; last = -1
        for _ in range(40):
            c = cnt()
            if c == last:
                same += 1
                if same >= min_round: break
            else:
                same = 0; last = c
            page.wait_for_timeout(gap_ms)

        # 마지막 카드 노출 시간 확보(1.2s)
        try:
            if c > 0:
                last_card = page.locator(".goods-list .goods-unit").nth(c-1)
                last_card.scroll_into_view_if_needed()
                page.wait_for_timeout(1200)
        except Exception:
            pass

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-gpu",
                  "--disable-blink-features=AutomationControlled"]
        )
        ctx = browser.new_context(
            locale="ko-KR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
            viewport={"width":1440,"height":2200}
        )
        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)

        # 카테고리/일간 강제 + 카드 가시화 대기
        ensure_category_and_day(page)
        try:
            page.wait_for_selector(".goods-list .goods-unit", state="visible", timeout=20_000)
        except Exception:
            pass

        _ = force_load(page, TARGET_COUNT)
        wait_stable(page)

        # 디버그 저장
        (DEBUG_DIR / "page_rank.png").write_bytes(page.screenshot(full_page=True))
        (DEBUG_DIR / "page_rank.html").write_text(page.content(), encoding="utf-8")

        html = page.content()
        ctx.close(); browser.close()

    items = parse_html_filtered(html)

    # 유효 카드 너무 적으면 예외 발생 → main()에서 Requests 폴백
    if len(items) < 10:
        raise RuntimeError(f"유효 카드 부족(Playwright 파싱 수={len(items)})")
    return items[:max(TARGET_COUNT, 1)]

# -------------------- Requests 폴백 --------------------
def fetch_requests() -> List[Dict]:
    r = requests.get(RANK_URL, timeout=25, headers={
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    })
    r.raise_for_status()
    html = r.text
    (DEBUG_DIR / "page_rank.html").write_text(html, encoding="utf-8")
    items = parse_html_filtered(html)
    return items

# -------------------- 전일 비교/변동 --------------------
def normalize_name(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().lower()

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

# -------------------- Slack 메시지 --------------------
def slack_message(today_rows: List[Dict], change: Dict) -> str:
    L = []
    L.append(f"*다이소몰 뷰티/위생 일간 랭킹 — {today_kst()}*")
    L.append("")
    L.append("*TOP 10*")
    for r in [x for x in today_rows if x["rank"]<=10]:
        name = r["name"].replace("&","&amp;").replace("<","〈").replace(">","〉")
        L.append(f"{r['rank']}. <{r['url']}|{name}> — {fmt_won(r['price'])}")
    L.append("")

    L.append("🔥 *급상승*")
    if change["rising"]:
        for r in change["rising"]:
            L.append(f"- {r['name']} {r['prev']}위 → {r['curr']}위 (↑{r['delta']})")
    else:
        L.append("- 해당 없음")
    L.append("")

    L.append("🆕 *뉴랭커*")
    if change["new_in"]:
        for r in change["new_in"]:
            L.append(f"- {r['name']} NEW → {r['curr']}위")
    else:
        L.append("- 해당 없음")
    L.append("")

    L.append("📉 *급하락*")
    had = False
    for r in change["falling"]:
        L.append(f"- {r['name']} {r['prev']}위 → {r['curr']}위 (↓{r['delta']})"); had = True
    outs = [o for o in change["out"] if o["prev"]<=30]
    for o in outs:
        L.append(f"- {o['name']} {o['prev']}위 → OUT"); had = True
    if not had:
        L.append("- 해당 없음")
    L.append("")

    L.append("🔁 *랭크 인&아웃*")
    L.append(f"{change['inout_count']}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(L)

# -------------------- 메인 --------------------
def main():
    t0 = time.time()
    print("수집 시작:", RANK_URL)

    try:
        items = fetch_playwright()
        print("[Playwright] 정상 수집")
    except Exception as e:
        print("[Playwright 실패 → Requests 폴백]", e)
        items = fetch_requests()

    cnt = len(items)
    print("수집 완료:", cnt)
    if cnt < 20:
        raise RuntimeError("유효 상품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    csv_path = DATA_DIR / f"다이소몰_뷰티위생_일간_{today_kst()}.csv"
    rows = [{"date": today_kst(), "rank":i["rank"], "name":i["name"], "price":i["price"], "url":i["url"]} for i in items]
    save_csv(csv_path, rows)

    prev_path = prev_csv_path(csv_path)
    prev_rows = load_csv(prev_path)
    change = analyze(rows, prev_rows)

    try:
        fid = gdrive_upload(csv_path)
        print("Drive 업로드 완료:", fid)
    except Exception as e:
        print("[Drive 업로드 실패]", e)

    msg = slack_message(rows, change)
    slack(msg)

    print(f"총 {cnt}건, 경과: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
