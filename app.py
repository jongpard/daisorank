# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (이름기반 비교, 200개 고정, 슬랙 문구 수정본)
# - FIX: URL 쿼리스트링 보존( pdNo=… 유지 ), 해시(#)만 제거
# - 일간/뷰티 고정, 200개 안정 스크롤
# - 가격/이름 파싱 보강
# - 전일 비교: "제품명(name)" 기준 (pdNo 제거)
# - CSV 컬럼: date, rank, name, price, url
# - Slack: 요청한 인&아웃 문구만 출력(불필요한 진단 제거)

import os, re, csv, io, sys, time, random, traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, Page

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ========= 설정 =========
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))   # 모든 산출물은 상위 200개로 고정
TOPN = int(os.getenv("TOP_WINDOW", str(MAX_ITEMS)))
SCROLL_PAUSE_MS = int(float(os.getenv("SCROLL_PAUSE", "650")))
SCROLL_STABLE_ROUNDS = int(os.getenv("SCROLL_STABLE_ROUNDS", "10"))
SCROLL_MAX_ROUNDS = int(os.getenv("SCROLL_MAX_ROUNDS", "220"))
SCROLL_JIGGLE_PX = int(os.getenv("SCROLL_JIGGLE_PX", "600"))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))

# ========= 유틸 =========
def now_kst(): return datetime.now(KST)
def today_str(): return now_kst().strftime("%Y-%m-%d")
def yday_str(): return (now_kst() - timedelta(days=1)).strftime("%Y-%m-%d")
def log(msg): print(f"[{now_kst().strftime('%H:%M:%S')}] {msg}", flush=True)
def ensure_dirs(): os.makedirs("data/debug", exist_ok=True); os.makedirs("data", exist_ok=True)

# ✅ FIX: 쿼리스트링은 살리고, 해시(#)만 제거
def normalize_url_for_key(url: str) -> str:
    return re.sub(r"#.*$", "", (url or "").strip())

def strip_best(name: str) -> str:
    if not name: return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()

PRICE_STOPWORDS = r"(택배배송|매장픽업|오늘배송|별점|리뷰|구매|쿠폰|장바구니|찜|상세|배송비|혜택|적립)"
def parse_name_price(text: str) -> Tuple[Optional[str], Optional[int]]:
    text = re.sub(r"\s+", " ", (text or "")).strip()
    m = re.search(r"([0-9][0-9,]*)\s*원\s*(.+?)(?:\s*(?:%s))" % PRICE_STOPWORDS, text)
    if not m:
        m = re.search(r"([0-9][0-9,]*)\s*원\s*(.+)$", text)
        if not m: return None, None
    try: price = int(m.group(1).replace(",", ""))
    except Exception: price = None
    name = strip_best(m.group(2).strip())
    if name and len(name) < 2: name = None
    return name or None, price

# ========= DOM 조작 =========
def close_overlays(page: Page):
    for sel in [
        ".layer-popup .btn-close", ".modal .btn-close", ".popup .btn-close",
        ".layer-popup .close", ".modal .close", ".popup .close",
        ".btn-x", ".btn-close, button[aria-label='닫기']",
    ]:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=800)
                page.wait_for_timeout(150)
        except Exception: pass

def _click_beauty_chip(page: Page) -> bool:
    close_overlays(page)

    try:
        # 1️⃣ value 기반 직접 클릭 (가장 안정적)
        btn = page.locator('.prod-category .cate-btn[value="CTGR_00014"]').first
        btn.wait_for(state="attached", timeout=5000)
        page.evaluate("(el) => el.click()", btn)

        # 2️⃣ 카테고리 변경 대기
        page.wait_for_timeout(800)

        # 3️⃣ 활성화 확인
        page.wait_for_function("""
            () => {
                const active = document.querySelector('.prod-category .on');
                return active && active.innerText.includes('뷰티');
            }
        """, timeout=5000)

        return True

    except Exception as e:
        log(f"[카테고리 클릭 실패] {e}")
        return False


def _click_daily(page: Page) -> bool:
    ok=False
    try:
        if page.locator('.ipt-sorting input[value="2"]').count()>0:
            page.locator('.ipt-sorting input[value="2"]').first.click(timeout=1500); ok=True
    except Exception: pass
    if not ok:
        try: page.get_by_role("button", name=re.compile("일간")).click(timeout=1500); ok=True
        except Exception:
            try:
                page.evaluate("""
                    () => {
                      const els=[...document.querySelectorAll('button,[role=button],.ipt-sorting *')];
                      const t=els.find(e=>/일간/.test((e.textContent||'').trim())); if (t) t.click();
                    }
                """); ok=True
            except Exception: ok=False
    try:
        page.wait_for_function(
            "()=>document.querySelectorAll('div.product-info a[href*=\"/pd/pdr/\"]').length>0", timeout=5000
        ); ok=True
    except Exception: ok=False
    page.wait_for_timeout(350); return ok

def _count_cards(page: Page) -> int:
    try:
        return int(page.evaluate("()=>document.querySelectorAll('div.product-info a[href*=\"/pd/pdr/\"]').length"))
    except Exception:
        return 0

def _load_all(page: Page, target_min: int = MAX_ITEMS) -> int:
    prev=0; stable=0
    for _ in range(SCROLL_MAX_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        try:
            more = page.locator("button:has-text('더보기'), button:has-text('더 보기'), a:has-text('더보기')")
            if more.count()>0: more.first.click(timeout=800); page.wait_for_timeout(400)
        except Exception: pass
        try:
            jiggle = random.randint(200, SCROLL_JIGGLE_PX)
            page.evaluate(f"window.scrollBy(0, -{jiggle})"); page.wait_for_timeout(120)
            page.evaluate(f"window.scrollBy(0, {jiggle + 200})")
        except Exception: pass
        try:
            page.wait_for_function(
                f"(prev)=>{{const n=document.querySelectorAll('div.product-info a[href*=\"/pd/pdr/\"]').length;return n>prev||n>={target_min};}}",
                timeout=4000, arg=prev
            )
        except Exception: pass
        cnt=_count_cards(page)
        if cnt>=target_min: return cnt
        if cnt==prev: stable+=1
        else: stable=0; prev=cnt
        if stable>=SCROLL_STABLE_ROUNDS: break
    return _count_cards(page)

# ========= 추출 + 200개 고정 =========
def _extract_items(page: Page) -> List[Dict]:
    data = page.evaluate("""
      () => {
        const cards = [...document.querySelectorAll('div.product-info')];
        const items = [];
        for (const info of cards) {
          const a = info.querySelector('a[href*="/pd/pdr/"]');
          if (!a) continue;
          let href = a.getAttribute('href') || a.href || '';
          if (!href) continue;
          if (!/^https?:/i.test(href)) href = new URL(href, location.origin).href; // 절대경로화(쿼리 포함)
          const text = (info.textContent || '').replace(/\\s+/g,' ').trim();
          items.push({ raw: text, url: href });
        }
        return items;
      }
    """)
    cleaned=[]
    for it in data:
        # ✅ 여기서도 쿼리 유지(#만 제거)
        url = normalize_url_for_key(it.get("url",""))
        name, price = parse_name_price(it.get("raw",""))
        if not (url and name and price and price>0): continue
        cleaned.append({"name": name, "price": price, "url": url})
    # 상위 MAX_ITEMS로 컷 + 랭크 재부여
    rows = cleaned[:MAX_ITEMS]
    for i, it in enumerate(rows, 1): it["rank"] = i
    return rows

# ========= CSV =========
def save_csv(rows: List[Dict]) -> Tuple[str,str]:
    ensure_dirs()
    filename = f"다이소몰_뷰티위생_일간_{today_str()}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","rank","name","price","url"])
        for r in rows:
            w.writerow([today_str(), r["rank"], r["name"], r["price"], r["url"]])
    return path, filename

# ========= Google Drive =========
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        log("[Drive] OAuth 환경변수 미설정"); return None
    try:
        creds = UserCredentials(
            None, refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        creds.refresh(GoogleRequest())
        return build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as e:
        log(f"[Drive] 서비스 생성 실패: {e}"); return None

def _retry(fn, tries=3, base=1.2, msg=""):
    for i in range(tries):
        try: return fn()
        except Exception as e:
            wait = base*(2**i) + random.random()*0.2
            log(f"[Retry] {msg} 실패({i+1}/{tries}): {e} → {wait:.1f}s 대기"); time.sleep(wait)
    return None

def upload_to_drive(svc, filepath, filename):
    if not svc or not GDRIVE_FOLDER_ID:
        log("[Drive] 업로드 생략(설정 없음)"); return None
    def _do():
        media = MediaIoBaseUpload(io.FileIO(filepath,'rb'), mimetype="text/csv", resumable=True)
        body = {"name": filename, "parents":[GDRIVE_FOLDER_ID]}
        return svc.files().create(body=body, media_body=media, fields="id,name").execute()
    res = _retry(_do, msg="업로드"); 
    if res: log(f"[Drive] 업로드 성공: {res.get('name')} (ID: {res.get('id')})"); return res.get("id")
    log("[Drive] 업로드 최종 실패"); return None

def find_file_in_drive(svc, filename):
    if not svc or not GDRIVE_FOLDER_ID: return None
    def _do():
        q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
        return svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
    res = _retry(_do, msg="파일 검색")
    return (res.get("files") or [None])[0] if res else None

def download_from_drive(svc, file_id) -> Optional[str]:
    def _do():
        req = svc.files().get_media(fileId=file_id)
        fh = io.BytesIO(); downloader = MediaIoBaseDownload(fh, req)
        done=False
        while not done: _, done = downloader.next_chunk()
        fh.seek(0); return fh.read().decode("utf-8")
    return _retry(_do, msg="다운로드")

# ========= 전일 비교 (제품명 기준) =========
def parse_prev_csv(txt: str) -> List[Dict]:
    items=[]; rdr=csv.DictReader(io.StringIO(txt))
    for row in rdr:
        try:
            name = (row.get("name") or "").strip()
            if not name: continue
            rnk = int(row.get("rank"))
            # ✅ 이전 CSV에서도 쿼리 유지(#만 제거)
            url = normalize_url_for_key(row.get("url",""))
            items.append({"name": name, "rank": rnk, "url": url})
        except Exception: continue
    return items

def analyze_trends(today: List[Dict], prev: List[Dict]):
    # name 기준 비교
    prev_map = {p["name"]: p["rank"] for p in prev}
    ups, downs = [], []
    for t in today[:TOPN]:
        nm = t["name"]; tr=t["rank"]; pr=prev_map.get(nm)
        if pr is None: continue
        ch = pr - tr
        d = {"name":nm,"url":t["url"],"rank":tr,"prev_rank":pr,"change":ch}
        if ch>0: ups.append(d)
        elif ch<0: downs.append(d)
    ups.sort(key=lambda x:(-x["change"], x["rank"]))
    downs.sort(key=lambda x:(x["change"], x["rank"]))

    today_keys = {t["name"] for t in today[:TOPN]}
    prev_keys  = {p["name"] for p in prev if 1 <= p["rank"] <= TOPN}
    ins_keys  = today_keys - prev_keys
    outs_keys = prev_keys - today_keys

    chart_ins = [t for t in today if t["name"] in ins_keys]
    rank_outs = [p for p in prev  if p["name"] in outs_keys]
    chart_ins.sort(key=lambda r:r["rank"]); rank_outs.sort(key=lambda r:r["rank"])

    io_cnt = len(ins_keys)  # IN 개수 == OUT 개수
    return ups, downs, chart_ins, rank_outs, io_cnt

# ========= Slack =========
def post_slack(rows: List[Dict], analysis, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK: return
    ups, downs, chart_ins, rank_outs, io_cnt = analysis
    prev_map = {p["name"]: p["rank"] for p in (prev_items or [])}
    def _link(n,u): return f"<{u}|{n}>" if u else (n or "")

    lines = [f"*다이소몰 뷰티/위생 일간 랭킹 {TOPN}* ({now_kst().strftime('%Y-%m-%d %H:%M KST')})"]

    # TOP 10
    lines.append("\n*TOP 10*")
    for it in rows[:10]:
        cur=it["rank"]; price=f"{int(it['price']):,}원"
        pr=prev_map.get(it["name"])
        marker = "(new)" if pr is None else (f"(↑{pr-cur})" if pr > cur else (f"(↓{cur-pr})" if pr < cur else "(-)"))
        lines.append(f"{cur}. {marker} {_link(it['name'], it['url'])} — {price}")

    # 🔥 급상승
    lines.append("\n*🔥 급상승*")
    if ups: 
        for m in ups[:5]: lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else: lines.append("- (해당 없음)")

    # 🆕 뉴랭커
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]: lines.append(f"- {_link(t['name'], t['url'])} NEW → {t['rank']}위")
    else: lines.append("- (해당 없음)")

    # 📉 급하락 + OUT
    lines.append("\n*📉 급하락*")
    if downs:
        ds = sorted(downs, key=lambda x:(-abs(x["change"]), x["rank"]))
        for m in ds[:5]:
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↓{abs(m['change'])})")
    else: lines.append("- (급하락 없음)")

    if rank_outs:
        os_ = sorted(rank_outs, key=lambda x:x["rank"])
        for ro in os_[:5]:
            lines.append(f"- {_link(ro.get('name'), ro.get('url'))} {int(ro.get('rank') or 0)}위 → OUT")
    else: lines.append("- (OUT 없음)")

    # ↔ 랭크 인&아웃 (요청 포맷)
    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"{io_cnt}개의 제품이 인&아웃 되었습니다.")

    try:
        requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=12).raise_for_status()
        log("[Slack] 전송 성공")
    except Exception as e:
        log(f"[Slack] 전송 실패: {e}")

# ========= main =========
def main():
    ensure_dirs()
    log(f"[시작] {RANK_URL}")
    log(f"[ENV] SLACK={'OK' if SLACK_WEBHOOK else 'NONE'} / GDRIVE={'OK' if (GDRIVE_FOLDER_ID and GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN) else 'NONE'}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = browser.new_context(
            viewport={"width": 1380, "height": 940},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
        )
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)
        ok_cat = _click_beauty_chip(page);  log(f"[검증] 카테고리(뷰티/위생): {ok_cat}")
        ok_day = _click_daily(page);        log(f"[검증] 일간선택: {ok_day}")
        loaded = _load_all(page, MAX_ITEMS); log(f"[로드] 카드 수: {loaded}")
        dbg = f"data/debug/rank_raw_{today_str()}.html"
        with open(dbg, "w", encoding="utf-8") as f: f.write(page.content()); log(f"[디버그] HTML 저장: {dbg}")
        rows = _extract_items(page)
        ctx.close(); browser.close()


   
    # 200개 고정
    rows = rows[:MAX_ITEMS]
    for i, r in enumerate(rows, 1): r["rank"] = i
    log(f"[수집 결과] {len(rows)}개 (MAX={MAX_ITEMS})")

    csv_path, csv_name = save_csv(rows);     log(f"[CSV] 저장: {csv_path}")

    # 전일 CSV 로드(Drive에서)
    prev_items: List[Dict] = []
    yfile = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
    svc = build_drive_service()
    if svc:
        upload_to_drive(svc, csv_path, csv_name)
        prev = find_file_in_drive(svc, yfile)
        if prev:
            txt = download_from_drive(svc, prev["id"])
            if txt: prev_items = parse_prev_csv(txt)
            log(f"[Drive] 전일 로드: {len(prev_items)}건")
        else:
            log("[Drive] 전일 파일 없음")

    analysis = analyze_trends(rows, prev_items)
    post_slack(rows, analysis, prev_items)
    log("[끝] 정상 종료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        ensure_dirs()
        err = f"[예외] {type(e).__name__}: {e}"
        log(err); log(traceback.format_exc())
        try:
            with open(f"data/debug/exception_{today_str()}.txt", "w", encoding="utf-8") as f:
                f.write(err + "\n" + traceback.format_exc())
        except Exception: pass
        sys.exit(1)
