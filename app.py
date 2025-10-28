# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (강화판)
# - 최신 DOM 대응: div.product-info + /pd/pdr/ 링크 기반 추출
# - 스크롤 로더 개선: 200개 도달까지 안정 스크롤
# - 전일 비교/Slack 리포트(요청 포맷) + GDrive 업/다운

import os, re, csv, time, io
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Locator

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ====== 설정 ======
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"  # 뷰티/위생 · 일간
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))
TOP_WINDOW = 150
SCROLL_PAUSE_MS = int(float(os.getenv("SCROLL_PAUSE", "650")))

SCROLL_STABLE_ROUNDS = int(os.getenv("SCROLL_STABLE_ROUNDS", "8"))
SCROLL_MAX_ROUNDS = int(os.getenv("SCROLL_MAX_ROUNDS", "160"))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))

# ====== 유틸 ======
def today_str() -> str: return datetime.now(KST).strftime("%Y-%m-%d")
def yday_str() -> str:  return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def strip_best(name: str) -> str:
    if not name: return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()

def parse_name_price(text: str) -> Tuple[Optional[str], Optional[int]]:
    # "5,000 원 [상품명] 택배배송 ..." 형태에서 가격/이름 뽑기
    text = re.sub(r"\s+", " ", (text or "")).strip()
    m = re.search(r"([0-9,]+)\s*원\s*(.+?)(?:\s*(?:택배배송|매장픽업|오늘배송|별점|리뷰|구매|쿠폰|장바구니|찜|상세))", text)
    if not m: 
        return None, None
    try:
        price = int(m.group(1).replace(",", ""))
    except Exception:
        price = None
    name = strip_best(m.group(2).strip())
    return name or None, price

def _to_locator(page: Page, target) -> Locator:
    return target if isinstance(target, Locator) else page.locator(target)

def close_overlays(page: Page):
    sels = [
        ".layer-popup .btn-close", ".modal .btn-close", ".popup .btn-close",
        ".layer-popup .close", ".modal .close", ".popup .close",
        ".btn-x", ".btn-close, button[aria-label='닫기']"
    ]
    for sel in sels:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=800)
                page.wait_for_timeout(150)
        except Exception: pass

# ====== 페이지 고정(뷰티/위생 · 일간) ======
def ensure_tab(page: Page):
    close_overlays(page)
    # 카테고리 '뷰티/위생'
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count() > 0:
            page.locator('.prod-category .cate-btn[value="CTGR_00014"]').first.click(timeout=1200)
        else:
            page.get_by_role("button", name=re.compile("뷰티\\/?위생")).click(timeout=1200)
    except Exception:
        # 강제 클릭
        page.evaluate("""
            () => {
              const byVal = document.querySelector('.prod-category .cate-btn[value="CTGR_00014"]');
              if (byVal) byVal.click();
              else {
                const btns = [...document.querySelectorAll('.prod-category .cate-btn, .prod-category *')];
                const t = btns.find(b => /뷰티\\/?위생/.test((b.textContent||"").trim()));
                if (t) t.click();
              }
            }
        """)
    page.wait_for_timeout(300)

    # 정렬/기간: 일간
    try:
        page.locator('.ipt-sorting input[value="2"]').first.click(timeout=1200)  # 일간
    except Exception:
        try:
            page.get_by_role("button", name=re.compile("일간")).click(timeout=1200)
        except Exception:
            pass
    page.wait_for_timeout(350)

# ====== 스크롤 로더(200개까지) ======
def load_all(page: Page, target_min: int = MAX_ITEMS) -> int:
    prev = 0
    stable = 0
    for r in range(SCROLL_MAX_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        # 현재 로드된 카드 수(신규 DOM 기준)
        try:
            cnt = page.evaluate("""() => {
              return document.querySelectorAll('div.product-info a[href*="/pd/pdr/"]').length
            }""")
        except Exception:
            cnt = 0
        if cnt >= target_min:
            return cnt
        if cnt == prev:
            stable += 1
            if stable >= SCROLL_STABLE_ROUNDS:
                break
        else:
            stable = 0
            prev = cnt
    return prev

# ====== 추출(브라우저 JS 우선 + 파이썬 파서 보정) ======
def extract_items_js(page: Page) -> List[Dict]:
    data = page.evaluate("""
      () => {
        const cards = [...document.querySelectorAll('div.product-info')];
        const items = [];
        const seen = new Set();
        for (const info of cards) {
          const a = info.querySelector('a[href*="/pd/pdr/"]');
          if (!a) continue;
          let href = a.href || a.getAttribute('href') || '';
          if (!href) continue;
          // 절대경로 보정
          if (!/^https?:/i.test(href)) href = new URL(href, location.origin).href;

          const text = (info.textContent || '').replace(/\\s+/g,' ').trim();
          // 가격/이름 추출은 서버에서 재처리
          items.push({ raw: text, url: href });
        }
        return items;
      }
    """)
    # 후처리(이름/가격 파싱)
    cleaned = []
    for it in data:
        name, price = parse_name_price(it.get("raw",""))
        if not (name and price and price > 0): 
            continue
        cleaned.append({"name": name, "price": price, "url": it["url"]})
    for i, it in enumerate(cleaned, 1):
        it["rank"] = i
    return cleaned

def extract_items_fallback(html_text: str) -> List[Dict]:
    # BeautifulSoup fallback용(워크플로 아티팩트/디버그 HTML 활용)
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_text, "html.parser")
    items = []
    for info in soup.select("div.product-info"):
        a = info.select_one('a[href*="/pd/pdr/"]')
        href = ""
        if a:
            href = a.get("href") or ""
            if href and not href.startswith("http"):
                href = "https://www.daisomall.co.kr" + href
        text = info.get_text(" ", strip=True)
        name, price = parse_name_price(text)
        if not (name and price and href): 
            continue
        items.append({"name": name, "price": price, "url": href})
    for i, it in enumerate(items, 1):
        it["rank"] = i
    return items

def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1380, "height": 940},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36"),
        )
        page = context.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=45_000)
        ensure_tab(page)

        loaded = load_all(page, MAX_ITEMS)
        # 디버그용 HTML 저장(워크플로 아티팩트)
        os.makedirs("data/debug", exist_ok=True)
        page_content = page.content()
        with open(f"data/debug/rank_raw_{today_str()}.html", "w", encoding="utf-8") as f:
            f.write(page_content)

        items = extract_items_js(page)
        context.close(); browser.close()

        return items

# ====== CSV 저장 ======
def save_csv(rows: List[Dict]) -> Tuple[str,str]:
    date_str = today_str()
    os.makedirs("data", exist_ok=True)
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
        print("[Drive] 서비스 또는 폴더 ID 미설정 → 업로드 생략")
        return None
    try:
        media = MediaIoBaseUpload(io.FileIO(filepath, 'rb'), mimetype="text/csv", resumable=True)
        body = {"name": filename, "parents": [GDRIVE_FOLDER_ID]}
        file = service.files().create(body=body, media_body=media, fields="id,name").execute()
        print(f"[Drive] 업로드 성공: {file.get('name')} (ID: {file.get('id')})")
        return file.get("id")
    except Exception as e:
        print("[Drive] 업로드 실패:", e)
        return None

def find_file_in_drive(service, filename: str):
    if not service or not GDRIVE_FOLDER_ID:
        return None
    try:
        q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
        res = service.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        return res.get("files", [])[0] if res.get("files") else None
    except Exception as e:
        print(f"[Drive] 파일 검색 실패 ({filename}):", e)
        return None

def download_from_drive(service, file_id: str) -> Optional[str]:
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read().decode("utf-8")
    except Exception as e:
        print(f"[Drive] 파일 다운로드 실패 (ID: {file_id}):", e)
        return None

# ====== 전일 파싱/분석 ======
def parse_prev_csv(csv_text: str) -> List[Dict]:
    import csv as _csv, io as _io
    items = []
    try:
        reader = _csv.DictReader(_io.StringIO(csv_text))
        for row in reader:
            try:
                items.append({"rank": int(row.get("rank")), "name": row.get("name"), "url": row.get("url")})
            except Exception: pass
    except Exception as e:
        print("[CSV Parse] 전일 데이터 파싱 실패:", e)
    return items

def analyze_trends(today_items: List[Dict], prev_items: List[Dict]):
    prev_map = {p["url"]: p["rank"] for p in prev_items if p.get("url")}
    prev_top_urls = {p["url"] for p in prev_items if p.get("url") and p.get("rank", 999) <= TOP_WINDOW}

    trends = []
    for it in today_items:
        url = it.get("url")
        pr = prev_map.get(url)
        trends.append({"name": it["name"], "url": url, "rank": it["rank"], "prev_rank": pr,
                       "change": (pr - it["rank"]) if pr else None})

    movers = [t for t in trends if t["prev_rank"] is not None]
    ups   = sorted([t for t in movers if (t["change"] or 0) > 0], key=lambda x: x["change"], reverse=True)
    downs = sorted([t for t in movers if (t["change"] or 0) < 0], key=lambda x: x["change"])

    chart_ins = [t for t in trends if t["prev_rank"] is None and t["rank"] <= TOP_WINDOW]

    today_urls = {t["url"] for t in trends if t.get("url")}
    rank_out_urls = prev_top_urls - today_urls
    rank_outs = [p for p in prev_items if p.get("url") in rank_out_urls]

    # 인&아웃(집합 기준) — 항상 IN≡OUT
    return ups, downs, chart_ins, rank_outs, len(chart_ins)

# ====== Slack 리포트 ======
def post_slack(rows: List[Dict], analysis_results, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK: return
    ups, downs, chart_ins, rank_outs, _ = analysis_results

    def _link(name: str, url: Optional[str]) -> str:
        return f"<{url}|{name}>" if url else (name or "")

    # TOP10 변동표시용 맵(키: url 우선, 없으면 name)
    def _key(it: dict) -> str:
        return (it.get("url") or "").strip() or (it.get("name") or "").strip()

    prev_map: Dict[str, int] = {}
    if prev_items:
        for p in prev_items:
            try:
                r = int(p.get("rank") or 0)
            except Exception:
                continue
            k = _key(p)
            if k and r > 0:
                prev_map[k] = r

    now_kst = datetime.now(KST)
    title = f"*다이소몰 뷰티/위생 일간 랭킹 200* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})"
    lines = [title]

    # TOP 10
    lines.append("\n*TOP 10*")
    for it in (rows or [])[:10]:
        ptxt = f"{int(it.get('price') or 0):,}원"
        cur_r = int(it.get("rank") or 0)
        k = _key(it)
        marker = "(new)"
        if k in prev_map:
            prev_r = prev_map[k]
            diff = prev_r - cur_r
            marker = f"(↑{diff})" if diff > 0 else (f"(↓{abs(diff)})" if diff < 0 else "(-)")
        lines.append(f"{cur_r}. {marker} {_link(it.get('name') or '', it.get('url'))} — {ptxt}")

    # 🔥 급상승
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else:
        lines.append("- (해당 없음)")

    # 🆕 뉴랭커
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t['name'], t['url'])} NEW → {t['rank']}위")
    else:
        lines.append("- (해당 없음)")

    # 📉 급하락 + OUT
    lines.append("\n*📉 급하락*")
    if downs:
        downs_sorted = sorted(downs, key=lambda m: (-abs(int(m.get("change") or 0)),
                                                    int(m.get("rank") or 9999),
                                                    int(m.get("prev_rank") or 9999)))
        for m in downs_sorted[:5]:
            drop = abs(int(m.get("change") or 0))
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↓{drop})")
    else:
        lines.append("- (급하락 없음)")

    if rank_outs:
        outs_sorted = sorted(rank_outs, key=lambda x: int(x.get("rank") or 9999))
        for ro in outs_sorted[:5]:
            prev_r = int(ro.get("rank") or 0)
            lines.append(f"- {_link(ro.get('name'), ro.get('url'))} {prev_r}위 → OUT")
    else:
        lines.append("- (OUT 없음)")

    # ↔ 랭크 인&아웃 (집합 기준)
    io_cnt = 0
    if prev_items is not None:
        today_keys = {_key(it) for it in (rows or [])[:200] if _key(it)}
        prev_keys  = {_key(p) for p in (prev_items or []) if _key(p) and 1 <= int(p.get("rank") or 0) <= 200}
        io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2

    lines.append("\n*:+1::양방향_화살표: 랭크 인&아웃*")
    lines.append(f"*{io_cnt}개의 제품이 인&아웃 되었습니다.*")

    try:
        requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=10).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ====== main ======
def main():
    print("수집 시작:", RANK_URL)
    t0 = time.time()
    rows = fetch_products()
    print(f"[수집 완료] 개수: {len(rows)}")

    if len(rows) < MAX_ITEMS:
        print(f"[경고] 목표 {MAX_ITEMS} < 실제 {len(rows)} → 소스 구조/로딩 이슈 가능")

    # CSV 로컬 저장
    csv_path, csv_filename = save_csv(rows)
    print("로컬 저장:", csv_path)

    # 구글 드라이브
    drive_service = build_drive_service()
    prev_items: List[Dict] = []
    if drive_service:
        upload_to_drive(drive_service, csv_path, csv_filename)

        yday_filename = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
        prev_file = find_file_in_drive(drive_service, yday_filename)
        if prev_file:
            print(f"[Drive] 전일 파일 발견: {prev_file['name']} (ID: {prev_file['id']})")
            csv_content = download_from_drive(drive_service, prev_file['id'])
            if csv_content:
                prev_items = parse_prev_csv(csv_content)
                print(f"[분석] 전일 데이터 {len(prev_items)}건 로드")
        else:
            print(f"[Drive] 전일 파일({yday_filename}) 미발견")

        analysis_results = analyze_trends(rows, prev_items)
    else:
        analysis_results = ([], [], [], [], 0)

    # Slack
    post_slack(rows, analysis_results, prev_items)

    print(f"총 경과: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
