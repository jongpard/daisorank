# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (200개 고정 + 비교 진단 강화)
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
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))   # 수집/CSV/비교 모두 이 값으로 컷
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

def normalize_url_for_key(url: str) -> str:
    return re.sub(r"[?#].*$", "", (url or "").strip())

def extract_item_id(url: str) -> Optional[str]:
    m = re.search(r"/pd/pdr/(\d+)", url or "")
    return m.group(1) if m else None

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
    close_overlays(page); ok=False
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count()>0:
            page.locator('.prod-category .cate-btn[value="CTGR_00014"]').first.click(timeout=1500); ok=True
        else:
            page.get_by_role("button", name=re.compile("뷰티\\/?위생")).click(timeout=1500); ok=True
    except Exception:
        try:
            page.evaluate("""
                () => {
                  const v=document.querySelector('.prod-category .cate-btn[value="CTGR_00014"]');
                  if (v) v.click();
                  else {
                    const btns=[...document.querySelectorAll('.prod-category .cate-btn, .prod-category *')];
                    const t=btns.find(b=>/뷰티\\/?위생/.test((b.textContent||'').trim()));
                    if (t) t.click();
                  }
                }
            """); ok=True
        except Exception: ok=False
    page.wait_for_timeout(300); return ok

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
            import random
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
        if cnt==prev: stable+=1; 
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
          if (!/^https?:/i.test(href)) href = new URL(href, location.origin).href;
          const text = (info.textContent || '').replace(/\\s+/g,' ').trim();
          items.push({ raw: text, url: href });
        }
        return items;
      }
    """)
    cleaned=[]
    for it in data:
        url = normalize_url_for_key(it.get("url",""))
        name, price = parse_name_price(it.get("raw",""))
        if not (url and name and price and price>0): continue
        pdNo = extract_item_id(url) or url
        cleaned.append({"pdNo": pdNo, "name": name, "price": price, "url": url})
    return clip_to_max(cleaned, MAX_ITEMS)

def clip_to_max(rows: List[Dict], max_n: int) -> List[Dict]:
    """상위 max_n으로 자르고 rank 재부여(1..N)."""
    rows = rows[:max_n]
    for i, it in enumerate(rows, 1): it["rank"] = i
    return rows

# ========= CSV =========
def save_csv(rows: List[Dict]) -> Tuple[str,str]:
    ensure_dirs()
    filename = f"다이소몰_뷰티위생_일간_{today_str()}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","rank","name","price","url","pdNo"])
        for r in rows:
            w.writerow([today_str(), r["rank"], r["name"], r["price"], r["url"], r["pdNo"]])
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

# ========= 전일 비교 =========
def parse_prev_csv(txt: str) -> List[Dict]:
    items=[]; rdr=csv.DictReader(io.StringIO(txt))
    for row in rdr:
        try:
            url = normalize_url_for_key(row.get("url",""))
            pd  = row.get("pdNo") or extract_item_id(url) or url
            rnk = int(row.get("rank"))
            items.append({"pdNo": pd, "rank": rnk, "name": row.get("name"), "url": url})
        except Exception: continue
    return items

def analyze_trends(today: List[Dict], prev: List[Dict]):
    prev_map = {p["pdNo"]: p["rank"] for p in prev}
    ups, downs = [], []
    for t in today[:TOPN]:
        pd = t["pdNo"]; tr=t["rank"]; pr=prev_map.get(pd)
        if pr is None: continue
        ch = pr - tr
        d = {"pdNo":pd,"name":t["name"],"url":t["url"],"rank":tr,"prev_rank":pr,"change":ch}
        if ch>0: ups.append(d)
        elif ch<0: downs.append(d)
    ups.sort(key=lambda x:(-x["change"], x["rank"]))
    downs.sort(key=lambda x:(x["change"], x["rank"]))

    today_keys = {t["pdNo"] for t in today[:TOPN]}
    prev_keys  = {p["pdNo"] for p in prev if 1 <= p["rank"] <= TOPN}
    ins_keys  = today_keys - prev_keys
    outs_keys = prev_keys - today_keys

    chart_ins = [t for t in today if t["pdNo"] in ins_keys]
    rank_outs = [p for p in prev  if p["pdNo"] in outs_keys]
    chart_ins.sort(key=lambda r:r["rank"]); rank_outs.sort(key=lambda r:r["rank"])

    io_cnt = len(ins_keys)  # IN 개수 == OUT 개수
    return ups, downs, chart_ins, rank_outs, io_cnt, len(ins_keys), len(outs_keys)

# ========= Slack =========
def post_slack(rows: List[Dict], analysis, prev_items: Optional[List[Dict]] = None, yfile: Optional[str]=None):
    if not SLACK_WEBHOOK: return
    ups, downs, chart_ins, rank_outs, io_cnt, ins_raw, outs_raw = analysis
    prev_map = {p["pdNo"]: p["rank"] for p in (prev_items or [])}
    def _link(n,u): return f"<{u}|{n}>" if u else (n or "")

    lines = [f"*다이소몰 뷰티/위생 일간 랭킹 {TOPN}* ({now_kst().strftime('%Y-%m-%d %H:%M KST')})"]

    lines.append("\n*TOP 10*")
    for it in rows[:10]:
        cur=it["rank"]; price=f"{int(it['price']):,}원"
        pr=prev_map.get(it["pdNo"])
        marker = "(new)" if pr is None else (f"(↑{pr-cur})" if pr > cur else (f"(↓{cur-pr})" if pr < cur else "(-)")) 
        lines.append(f"{cur}. {marker} {_link(it['name'], it['url'])} — {price}")

    lines.append("\n*🔥 급상승*")
    if ups: 
        for m in ups[:5]: lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else: lines.append("- (해당 없음)")

    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]: lines.append(f"- {_link(t['name'], t['url'])} NEW → {t['rank']}위")
    else: lines.append("- (해당 없음)")

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

    lines.append("\n*:양방향_화살표: 랭크 인&아웃*")
    lines.append(f"*{io_cnt}개의 제품이 인&아웃 되었습니다.*")

    # 진단(희미한 회색 톤을 위한 코드블록) — 필요 없으면 주석 처리 가능
    diag = []
    if yfile: diag.append(f"prev: {yfile}")
    diag.append(f"IN_raw={ins_raw}, OUT_raw={outs_raw}, today={len(rows)}, prev={(len(prev_items) if prev_items else 0)}")
    lines.append(f"\n```{', '.join(diag)}```")

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

    # >>> 여기서 200개로 최종 고정 <<<
    rows = clip_to_max(rows, MAX_ITEMS)
    log(f"[수집 결과] {len(rows)}개 (MAX={MAX_ITEMS})")

    csv_path, csv_name = save_csv(rows);     log(f"[CSV] 저장: {csv_path}")

    prev_items: List[Dict] = []
    yfile = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
    svc = build_drive_service()
    if svc:
        upload_to_drive(svc, csv_path, csv_name)
        prev = find_file_in_drive(svc, yfile)
        if prev:
            txt = download_from_drive(svc, prev["id"])
            if txt: prev_items = parse_prev_csv(txt)
            log(f"[Drive] 전일 로드: {len(prev_items)}건 ({yfile})")
        else:
            log(f"[Drive] 전일 파일 없음: {yfile}")

    analysis = analyze_trends(rows, prev_items)
    post_slack(rows, analysis, prev_items, yfile if prev_items else None)
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
