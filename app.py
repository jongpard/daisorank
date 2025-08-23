# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집
# - Slack 포맷: Top200, 급상/급하락 ±10, OUT=전일 ≤150 (기존 유지)
# - 크롤링 안정화: DOM 대기/무한스크롤/더보기 혼합, 셀렉터 다중화, 재시도

import os, re, csv, time, io
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Union

import requests
from playwright.sync_api import sync_playwright, Page, Locator
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ====== 설정 ======
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))

# ---- Slack 분석/표시 규칙(다이소 전용) ----
DS_TOTAL_RANGE = 200
DS_OUT_LIMIT = 150
DS_RISING_FALLING_THRESHOLD = 10
DS_TOP_MOVERS_MAX = 5
DS_NEWCOMERS_TOP = 30

SCROLL_PAUSE = 0.7
SCROLL_STABLE_ROUNDS = 5
SCROLL_MAX_ROUNDS = 120
RETRY_IF_FEW = 2                 # 0건/소량일 때 재시도 횟수

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))

# ====== 유틸 ======
def today_str() -> str: return datetime.now(KST).strftime("%Y-%m-%d")
def yday_str() -> str:  return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
def ensure_int(v) -> Optional[int]:
    try: return int(v)
    except Exception: return None
def clean_spaces(s: str) -> str: return re.sub(r"\s+", " ", (s or "").strip())
def parse_price_kr(s: str) -> Optional[int]:
    if not s: return None
    m = re.search(r"(\d[\d,]*)", s)
    if not m: return None
    try: return int(m.group(1).replace(",", ""))
    except Exception: return None
def strip_best(name: str) -> str:
    if not name: return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()

def _to_locator(page: Page, target: Union[str, Locator]) -> Locator:
    return target if isinstance(target, Locator) else page.locator(target)

def close_overlays(page: Page):
    for sel in [
        ".layer-popup .btn-close", ".modal .btn-close", ".popup .btn-close",
        ".layer-popup .close", ".modal .close", ".popup .close",
        ".btn-x", ".btn-close, button[aria-label='닫기']"
    ]:
        try:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click(timeout=800)
                page.wait_for_timeout(150)
        except Exception:
            pass

def click_hard(page: Page, target: Union[str, Locator]):
    loc = _to_locator(page, target)
    try:
        loc.click(timeout=2500)
        page.wait_for_timeout(150)
        return True
    except Exception:
        try:
            page.evaluate("""
                (sel) => {
                  const el = (typeof sel === 'string') ? document.querySelector(sel) : null;
                  if (el) { el.click(); return true; }
                  return false;
                }
            """, target if isinstance(target, str) else None)
            page.wait_for_timeout(150)
            return True
        except Exception:
            return False

# ====== 크롤링(안정화) ======
CARD_SEL = (
    ".goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, "
    ".goods-unit-v2, .rank-list .goods-item, .rank-list li"
)
NAME_SELS = [
    ".goods-detail .tit a", ".goods-detail .tit", ".tit a", ".tit",
    ".name", ".goods-name", ".goods-tit", ".title"
]
PRICE_SELS = [
    ".goods-detail .sale-price .num", ".sale-price .num", ".sale .price",
    ".price .num", ".goods-price .num", ".final-price .num", ".goods-detail .price .num"
]
HREF_SELS = [
    'a[href*="/pd/pdr/"]', 'a[href*="/goods/"]', 'a[href*="/detail/"]', "a[href]"
]

def _wait_list_loaded(page: Page, timeout_ms=15000):
    try:
        page.wait_for_selector(CARD_SEL, timeout=timeout_ms, state="attached")
    except Exception:
        pass
    # 최소 1개 이상 DOM에 생길 때까지
    try:
        page.wait_for_function(
            f"() => document.querySelectorAll('{CARD_SEL}').length > 0",
            timeout=timeout_ms
        )
    except Exception:
        pass

def _load_more(page: Page):
    # 스크롤 + 더보기 혼합
    prev = 0; stable = 0
    for _ in range(SCROLL_MAX_ROUNDS):
        page.mouse.wheel(0, 4000)
        page.wait_for_timeout(int(SCROLL_PAUSE*1000))
        # 더보기류 버튼 클릭 시도
        for sel in ["button:has-text('더보기')", "button:has-text('더 보기')", "button:has-text('More')", ".btn-more", ".more"]:
            try:
                if page.locator(sel).first.is_visible():
                    page.locator(sel).first.click(timeout=800)
                    page.wait_for_timeout(400)
            except Exception:
                pass
        cur = page.locator(CARD_SEL).count()
        if cur >= MAX_ITEMS: break
        if cur == prev: stable += 1
        else: stable = 0
        prev = cur
        if stable >= SCROLL_STABLE_ROUNDS: break

def _collect_items(page: Page) -> List[Dict]:
    data = page.evaluate(
        f"""
() => {{
  const qs = s => Array.from(document.querySelectorAll(s));
  const units = qs("{CARD_SEL}");
  const items = [];
  const seen = new Set();
  const pick = (el, sels) => {{
    for (const s of {NAME_SELS!r}) {{
      const n = el.querySelector(s);
      if (n && (n.textContent||'').trim()) return n.textContent.trim();
    }}
    return null;
  }};
  const pickPrice = (el) => {{
    for (const s of {PRICE_SELS!r}) {{
      const n = el.querySelector(s);
      if (n && (n.textContent||'').trim()) return n.textContent.trim();
    }}
    return null;
  }};
  const pickHref = (el) => {{
    for (const s of {HREF_SELS!r}) {{
      const a = el.querySelector(s);
      if (a && a.getAttribute('href')) return a.href || a.getAttribute('href');
    }}
    return null;
  }};
  for (const el of units) {{
    let name = pick(el, {NAME_SELS!r});
    if (!name) continue;
    let ptxt = pickPrice(el);
    if (!ptxt) continue;
    let href = pickHref(el);
    if (!href) continue;
    if (href.startsWith('/')) href = location.origin + href;
    if (seen.has(href)) continue;
    seen.add(href);
    items.push({{name, ptxt, href}});
  }}
  return items;
}}
        """
    )
    out = []
    for it in data:
        nm = strip_best(it["name"])
        price = parse_price_kr(it["ptxt"])
        if not nm or not price:  # 최소 보정
            continue
        out.append({"name": nm, "price": price, "url": it["href"]})
    for i, r in enumerate(out, start=1):
        r["rank"] = i
    return out[:MAX_ITEMS]

def _select_category_and_tab(page: Page):
    # 카테고리: 뷰티/위생
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').first.is_visible():
            click_hard(page, '.prod-category .cate-btn[value="CTGR_00014"]')
        else:
            click_hard(page, page.get_by_role("button", name=re.compile("뷰티\\/?위생")))
    except Exception:
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
    page.wait_for_timeout(250)
    # 일간 탭
    try:
        click_hard(page, '.ipt-sorting input[value="2"]')
    except Exception:
        click_hard(page, page.get_by_role("button", name=re.compile("일간")))
    page.wait_for_timeout(350)

def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            locale="ko-KR",
        )
        page = ctx.new_page()
        page.set_default_timeout(30000)

        def run_once() -> List[Dict]:
            page.goto(RANK_URL, wait_until="domcontentloaded")
            try: page.wait_for_load_state("networkidle", timeout=15000)
            except Exception: pass
            close_overlays(page)
            _select_category_and_tab(page)
            _wait_list_loaded(page, 15000)
            _load_more(page)
            items = _collect_items(page)
            return items

        items: List[Dict] = []
        for attempt in range(RETRY_IF_FEW + 1):
            items = run_once()
            if len(items) >= 50 or attempt == RETRY_IF_FEW:
                break
            # 재시도: 화면 맨 아래까지 한번 더 강제 로딩
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1200)

        ctx.close(); browser.close()
        # 정렬(혹시 순서 꼬였을 때 대비)
        items = sorted(items, key=lambda r: int(r.get("rank") or 9999))[:MAX_ITEMS]
        return items

# ====== CSV ======
def save_csv(rows: List[Dict]) -> (str, str):
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

# ====== Drive ======
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] OAuth 환경변수 미설정"); return None
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
        print("[Drive] OAuth 세션 생성 실패:", e); return None

def upload_to_drive(service, filepath: str, filename: str) -> Optional[str]:
    if not service or not GDRIVE_FOLDER_ID: return None
    try:
        media = MediaIoBaseUpload(io.FileIO(filepath, 'rb'), mimetype="text/csv", resumable=True)
        body = {"name": filename, "parents": [GDRIVE_FOLDER_ID]}
        file = service.files().create(body=body, media_body=media, fields="id,name").execute()
        print(f"[Drive] 업로드 성공: {file.get('name')} (ID: {file.get('id')})")
        return file.get("id")
    except Exception as e:
        print("[Drive] 업로드 실패:", e); return None

def find_file_in_drive(service, filename: str):
    if not service or not GDRIVE_FOLDER_ID: return None
    try:
        q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
        res = service.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        return res.get("files", [])[0] if res.get("files") else None
    except Exception as e:
        print(f"[Drive] 파일 검색 실패 ({filename}):", e); return None

def download_from_drive(service, file_id: str) -> Optional[str]:
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0); return fh.read().decode("utf-8")
    except Exception as e:
        print(f"[Drive] 파일 다운로드 실패 (ID: {file_id}):", e); return None

# ====== 전일 CSV 파싱 ======
def parse_prev_csv(csv_text: str) -> List[Dict]:
    items: List[Dict] = []
    try:
        rdr = csv.DictReader(io.StringIO(csv_text))
        for row in rdr:
            try:
                items.append({
                    "rank": int(row.get("rank") or 0),
                    "name": row.get("name"),
                    "url": row.get("url"),
                    "price": int(row.get("price") or 0) if row.get("price") else None,
                })
            except (ValueError, TypeError):
                continue
    except Exception as e:
        print("[CSV Parse] 전일 데이터 파싱 실패:", e)
    return items

# ====== 분석(다이소 전용 규칙) ======
def analyze_trends(today_items: List[Dict], prev_items: List[Dict]):
    prev_map: Dict[str, int] = {}
    for p in (prev_items or []):
        u = p.get("url")
        try: r = int(p.get("rank") or 0)
        except Exception: continue
        if u and 1 <= r <= DS_TOTAL_RANGE: prev_map[u] = r

    trends: List[Dict] = []
    today_urls = set()
    for it in (today_items or []):
        u = it.get("url"); 
        if not u: continue
        try: cr = int(it.get("rank") or 0)
        except Exception: continue
        pr = prev_map.get(u)
        trends.append({"name": it.get("name"), "url": u, "rank": cr, "prev_rank": pr, "change": (pr - cr) if pr else None})
        today_urls.add(u)

    movers = [t for t in trends if t["prev_rank"] is not None]
    ups   = [t for t in movers if (t["change"] or 0) >= DS_RISING_FALLING_THRESHOLD]
    downs = [t for t in movers if (t["change"] or 0) <= -DS_RISING_FALLING_THRESHOLD]
    ups.sort(key=lambda x: (-(x["change"] or 0), x.get("rank", 9999), x.get("prev_rank", 9999), x.get("name") or ""))
    downs.sort(key=lambda x: (abs(x["change"] or 0), x.get("rank", 9999), x.get("prev_rank", 9999), x.get("name") or ""))

    chart_ins = [t for t in trends if t["prev_rank"] is None and (t["rank"] or 9999) <= DS_NEWCOMERS_TOP]
    chart_ins.sort(key=lambda x: x.get("rank", 9999))

    prev_out = []
    for p in prev_items or []:
        u = p.get("url"); 
        if not u: continue
        try: r = int(p.get("rank") or 0)
        except Exception: continue
        if r <= DS_OUT_LIMIT and u not in today_urls:
            prev_out.append({"name": p.get("name"), "url": u, "rank": r})
    prev_out.sort(key=lambda x: int(x.get("rank") or 9999))

    in_out_count = len(chart_ins) + len(prev_out)
    return ups, downs, chart_ins, prev_out, in_out_count

# ====== Slack (다이소 전용 포맷) ======
def post_slack(rows: List[Dict], analysis_results, prev_items: List[Dict]):
    if not SLACK_WEBHOOK: return
    ups, downs, chart_ins, rank_outs, in_out_count = analysis_results

    prev_rank_map: Dict[str, int] = {}
    for p in (prev_items or []):
        u = p.get("url")
        try: r = int(p.get("rank") or 0)
        except Exception: continue
        if u and 1 <= r <= DS_TOTAL_RANGE: prev_rank_map[u] = r

    now_kst = datetime.now(KST)
    lines = [f"*다이소몰 뷰티/위생 일간 랭킹 {DS_TOTAL_RANGE}* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})", "\n*TOP 10*"]

    for it in (rows or [])[:10]:
        cur = int(it.get("rank") or 0)
        url = it.get("url"); prev = prev_rank_map.get(url)
        if prev is None: badge = "(new)"
        elif prev > cur: badge = f"(↑{prev - cur})"
        elif prev < cur: badge = f"(↓{cur - prev})"
        else: badge = "(-)"
        price = it.get("price")
        try: price_txt = f"{int(price):,}원"
        except Exception: price_txt = str(price or "")
        name_link = f"<{url}|{it.get('name') or ''}>" if url else (it.get('name') or '')
        lines.append(f"{cur}. {badge} {name_link} — {price_txt}")

    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:DS_TOP_MOVERS_MAX]:
            lines.append(f"- <{m['url']}|{m['name']}> {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else:
        lines.append("- (해당 없음)")

    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:3]:
            lines.append(f"- <{t['url']}|{t['name']}> NEW → {t['rank']}위")
    else:
        lines.append("- (해당 없음)")

    lines.append("\n*📉 급하락*")
    shown = 0
    if downs:
        for m in downs[:DS_TOP_MOVERS_MAX]:
            lines.append(f"- <{m['url']}|{m['name']}> {m['prev_rank']}위 → {m['rank']}위 (↓{abs(m['change'])})")
            shown += 1
    if shown < DS_TOP_MOVERS_MAX and rank_outs:
        for ro in rank_outs[:DS_TOP_MOVERS_MAX - shown]:
            lines.append(f"- {ro['name']} {int(ro['rank'])}위 → OUT")

    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"{in_out_count}개의 제품이 인&아웃 되었습니다.")

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

    csv_path, csv_filename = save_csv(rows)
    print("로컬 저장:", csv_path)

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
                print(f"[분석] 전일 데이터 {len(prev_items)}건 로드 완료")
        else:
            print(f"[Drive] 전일 파일({yday_filename})을 찾을 수 없습니다.")

    analysis_results = analyze_trends(rows, prev_items)
    post_slack(rows, analysis_results, prev_items)
    print(f"총 {len(rows)}건, 경과 시간: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
