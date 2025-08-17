# -*- coding: utf-8 -*-
"""
다이소몰 뷰티/위생 '일간' 랭킹 크롤러 (중복/뒤죽박죽 방지·카테고리/일간 강제)
- 카테고리: 뷰티/위생 강제 선택
- 정렬(기간): 일간 강제 선택 + 활성 검증 루프
- 로딩: 무한 스크롤 + '더보기' 병행, 목표 개수(기본 200) 도달 시까지
- 안정화: 카드 수 정체 감지 + 마지막 카드 노출 대기
- 파싱: 스켈레톤/배너 제외(제목·가격 둘 다 있어야 상품으로 간주), 'BEST' 접두 제거
- 중복 방지: URL 기준으로 중복 제거 → 최종적으로 1..N 순위 재부여(뒤죽박죽 방지)
- 저장: data/다이소몰_뷰티위생_일간_YYYY-MM-DD.csv (KST)
- Slack: 올리브영 포맷 (TOP10 → 급상승 → 뉴랭커 → 급하락(5) → 랭크 인&아웃)
- Google Drive: refresh token(OAuth)로 업로드 (ID는 로그만, 메시지 미노출)
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

TARGET_COUNT = int(os.getenv("DAISO_TARGET_COUNT", "200"))  # 100/200 등 조절

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
        return int(re.sub(r"[^\d]", "", s or ""))
    except Exception:
        return None

def fmt_won(n: Optional[int]) -> str:
    if not n:
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
      - .goods-detail .goods-price .value
      - .price .value
      - [class*=price] .value
      - [class*=price] 전체 텍스트에서 숫자 추출
    """
    for sel in [".goods-detail .goods-price .value", ".price .value", '[class*="price"] .value']:
        el = unit.select_one(sel)
        if el:
            v = to_int(el.get_text(strip=True))
            if v:
                return v
    block = unit.select_one('[class*="price"]')
    if block:
        m = re.search(r"(\d[\d,]*)", block.get_text(" ", strip=True))
        if m:
            v = to_int(m.group(1))
            if v:
                return v
    return None

def parse_html_filtered(html: str) -> List[Dict]:
    """
    - 제목(.goods-detail .tit / .tit / .name / [class*=tit])과 가격이 모두 있는 노드만 상품.
    - BEST 라벨 제거.
    - URL 절대경로화 후 dict로 중복 제거(키: url).
    - 최종적으로 rank=1..N 재부여(뒤죽박죽 방지).
    """
    soup = BeautifulSoup(html, "lxml")
    units = soup.select(".goods-list .goods-unit")
    by_url: Dict[str, Dict] = {}
    for unit in units:
        tit_el = (unit.select_one(".goods-detail .tit")
                  or unit.select_one(".tit")
                  or unit.select_one(".name")
                  or unit.select_one('[class*="tit"]'))
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
        a = (unit.select_one(".goods-thumb a.goods-link")
             or unit.select_one(".goods-detail a.goods-link")
             or unit.select_one("a"))
        if a and a.has_attr("href"):
            href = a["href"].strip()
            if href.startswith("//"):
                url = "https:" + href
            elif href.startswith("/"):
                url = BASE_URL + href
            else:
                url = href

        # 중복 제거 (최신 DOM 순서대로 마지막 값으로 유지)
        by_url[url] = {
            "name": name,
            "price": int(price),
            "url": url,
        }

    # DOM 순서를 못 믿을 때를 대비해, 가격/이름을 보조키로 안정 정렬
    items = list(by_url.values())
    items.sort(key=lambda x: (x["name"].lower(), x["price"], x["url"]))
    # 최종 rank 재부여(1..N)
    out = []
    for i, it in enumerate(items, start=1):
        out.append({"rank": i, **it})
    return out

# -------------------- Playwright 수집 --------------------
def fetch_playwright() -> List[Dict]:
    from playwright.sync_api import sync_playwright

    def _is_day_active(page) -> bool:
        grp = page.locator(".el-radio-group.ipt-sorting")
        try:
            act = grp.locator("label.is-active, label.active")
            if act.count() and "일간" in (act.first.inner_text() or ""):
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

    def _click_beauty(page):
        """
        카테고리: 뷰티/위생 강제 클릭
        - value="CTGR_00014" 우선
        - 텍스트 '뷰티/위생' 보조
        - 스와이퍼 next 버튼으로 가시화 시도
        """
        try:
            target = page.locator('button.cate-btn[value="CTGR_00014"]')
            if not target.count():
                target = page.locator("button.cate-btn:has-text('뷰티/위생')")
            if target.count():
                if not target.first.is_visible():
                    nxt = page.locator(".pdcate-swiper .swiper-button-next")
                    for _ in range(10):
                        if target.first.is_visible(): break
                        if nxt.count(): nxt.first.click()
                        page.wait_for_timeout(150)
                target.first.click()
                page.wait_for_timeout(500)
        except Exception:
            pass

    def _click_daily(page):
        """
        정렬: 일간 강제 클릭(주간→일간 토글 포함), 활성 검증 루프
        """
        for _ in range(8):
            if _is_day_active(page):
                return
            # 주간 클릭 후 일간 클릭(토글 유도)
            try:
                wk = page.locator(".el-radio-group.ipt-sorting label:has-text('주간')")
                if wk.count(): wk.first.click()
            except Exception: pass
            try:
                dy = page.locator(".el-radio-group.ipt-sorting label:has-text('일간')")
                if dy.count(): dy.first.click()
            except Exception: pass
            # value=2 라디오 직접 트리거(보조)
            try:
                page.evaluate("""
                    () => {
                        const el = document.querySelector('.ipt-sorting input.el-radio-button__orig-radio[value="2"]');
                        if (el) el.click();
                    }
                """)
            except Exception: pass
            page.wait_for_timeout(700)
        print("[warn] 일간 탭 고정 확인 실패(현재 탭으로 진행)")

    def _force_load(page, target: int) -> int:
        """무한 스크롤 + '더보기' 병행 로드, 정체 감지로 종료"""
        def count_cards():
            return page.locator(".goods-list .goods-unit").count()

        last = -1; stall = 0
        for _ in range(150):
            if count_cards() >= target: break
            # 더보기 버튼 클릭
            try:
                more = page.locator("button:has-text('더보기')")
                if more.count() and more.first.is_enabled():
                    more.first.click()
                    page.wait_for_timeout(900)
            except Exception:
                pass
            # 스크롤 다운
            page.mouse.wheel(0, 5000)
            page.wait_for_timeout(700)
            cur = count_cards()
            if cur == last:
                stall += 1
                if stall >= 8: break
            else:
                stall = 0; last = cur
        return count_cards()

    def _wait_stable(page, rounds=3, gap_ms=700):
        """카드 수 변동이 rounds회 연속 없을 때까지 대기 + 마지막 카드 노출 1.2s"""
        def cnt(): return page.locator(".goods-list .goods-unit").count()
        same = 0; last = -1
        for _ in range(50):
            c = cnt()
            if c == last:
                same += 1
                if same >= rounds: break
            else:
                same = 0; last = c
            page.wait_for_timeout(gap_ms)
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
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/126.0 Safari/537.36"),
            viewport={"width":1440,"height":2200}
        )
        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)

        # 뷰티/위생 + 일간 강제
        _click_beauty(page)
        _click_daily(page)
        try:
            page.wait_for_selector(".goods-list .goods-unit", state="visible", timeout=20_000)
        except Exception:
            pass

        _ = _force_load(page, TARGET_COUNT)
        _wait_stable(page)

        # 디버그 저장
        (DEBUG_DIR / "page_rank.png").write_bytes(page.screenshot(full_page=True))
        (DEBUG_DIR / "page_rank.html").write_text(page.content(), encoding="utf-8")

        html = page.content()
        ctx.close(); browser.close()

    # 파싱 + 중복 제거 + 순위 재부여
    items = parse_html_filtered(html)
    if len(items) < 10:
        raise RuntimeError(f"유효 카드 부족(Playwright 파싱 수={len(items)})")
    # 목표 개수만 잘라서 반환
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
        _ = gdrive_upload(csv_path)
        print("Drive 업로드 완료")
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
