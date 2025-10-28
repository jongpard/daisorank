# app.py — DaisoMall 뷰티/위생 '일간' Top200 수집/비교/리포트 (2025-10 안정화)
# - 0개 수집 가드(재시도→대체파서→페이지네이션 후에도 0이면 종료)
# - 셀렉터 변경 대응: JS-Eval 기반 카드 추출 + BeautifulSoup 보조
# - 스크롤·페이지네이션 동시 지원: 한 페이지에서 충분히 로딩되면 스크롤, 아니면 ?page=
# - 비교 안정키: pdNo(product_code) → NEW/인&아웃 정확
# - 인&아웃 단일 숫자(쌍 일치) + 볼드 포맷
# - GDrive OAuth 업/다운 + 슬랙 리포트

import os, re, io, csv, time
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page

# ===== KST / 날짜 =====
KST = timezone(timedelta(hours=9))
def kst_now() -> datetime: return datetime.now(KST)
def today_str() -> str:     return kst_now().strftime("%Y-%m-%d")
def yday_str() -> str:      return (kst_now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ===== 설정 =====
RANK_URL = os.getenv("DAISO_RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
MAX_ITEMS = 200
SCROLL_ROUNDS = 60
SCROLL_STABLE = 4
PAGE_LIMIT = 8

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# Google Drive (OAuth)
GDRIVE_FOLDER_ID    = os.getenv("GDRIVE_FOLDER_ID", "").strip()
GOOGLE_CLIENT_ID    = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET= os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN= os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()

# ===== 유틸 =====
def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/debug", exist_ok=True)

def save_text(path: str, text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def parse_krw(s: str) -> Optional[int]:
    s = re.sub(r"[^\d]", "", s or "")
    return int(s) if s else None

PDNO_RE = re.compile(r"[?&]pdNo=(\d+)")

# ===== Playwright 공통 =====
def open_page(p, url: str) -> Page:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        viewport={"width": 1360, "height": 900},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"),
    )
    page = ctx.new_page()
    page.goto(url, wait_until="domcontentloaded", timeout=45_000)
    try: page.wait_for_load_state("networkidle", timeout=3000)
    except Exception: pass
    return page

def close_page(page: Page):
    ctx = page.context
    br = ctx.browser
    ctx.close(); br.close()

# ===== JS 기반 카드 추출(주 파서) =====
JS_COLLECT = """
() => {
  const qs = (el, sel) => el.querySelector(sel);
  const qsa = (sel) => Array.from(document.querySelectorAll(sel));

  // 카드 후보: li/div 안에 상품상세 링크가 있는 요소
  let cards = qsa('li, div').filter(el => el.querySelector('a[href*="/pd/pdr/"]'));

  const seen = new Set();
  const items = [];

  for (const el of cards) {
    const a = qs(el, 'a[href*="/pd/pdr/"]');
    if (!a) continue;
    let href = a.getAttribute('href') || '';
    if (href.startsWith('/')) href = location.origin + href;

    // pdNo 안정키
    const m = href.match(/[?&]pdNo=(\\d+)/);
    const code = m ? m[1] : null;
    if (!code || seen.has(code)) continue;
    seen.add(code);

    // 이름
    const nameSel = ['.product_name','.goods_name','.name','.tit','[class*="name"]','[class*="tit"]'];
    let name = '';
    for (const s of nameSel) { const n = qs(el, s); if (n && n.textContent.trim()) { name = n.textContent.trim(); break; } }
    if (!name) continue;

    // 가격
    const priceSel = ['.product_price','.sale_price','.final','.price','[class*="price"]'];
    let priceTxt = '';
    for (const s of priceSel) { const n = qs(el, s); if (n && n.textContent.trim()) { priceTxt = n.textContent.trim(); break; } }
    priceTxt = (priceTxt || '').replace(/[^0-9]/g, '');
    if (!priceTxt) continue;
    const price = parseInt(priceTxt,10)||0;
    if (!price) continue;

    // 랭크
    const rankSel = ['.rank','.num','.badge_rank','[class*="rank"]'];
    let rankTxt = '';
    for (const s of rankSel) { const n = qs(el, s); if (n && n.textContent.trim()) { rankTxt = n.textContent.trim(); break; } }
    let rank = null;
    if (rankTxt) { const m = rankTxt.match(/\\d+/); if (m) rank = parseInt(m[0],10); }

    items.push({ rank, raw_name: name, price, url: href, product_code: code });
  }
  // rank 없으면 문서 순서로 보정
  let num = 1;
  for (const it of items) { if (!it.rank) it.rank = num; num += 1; }
  // 정렬
  items.sort((a,b) => a.rank - b.rank);
  return items;
}
"""

def infinite_scroll(page: Page, target_min: int = 120):
    prev = 0; stable = 0
    for _ in range(SCROLL_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        try: page.wait_for_load_state("networkidle", timeout=1200)
        except Exception: pass
        page.wait_for_timeout(300)
        cnt = page.evaluate("""() => document.querySelectorAll('a[href*="/pd/pdr/"]').length""")
        if cnt >= target_min: break
        if cnt == prev:
            stable += 1
            if stable >= SCROLL_STABLE: break
        else:
            prev = cnt; stable = 0

def collect_cards_js(page: Page) -> List[Dict]:
    try:
        return page.evaluate(JS_COLLECT)
    except Exception:
        return []

# ===== BeautifulSoup 보조 파서 =====
def parse_cards_bs(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    # a[href*="/pd/pdr/"]를 기준으로 카드 탐지
    items = []
    seen = set()
    for a in soup.select('a[href*="/pd/pdr/"]'):
        href = a.get("href","").strip()
        if href.startswith("/"): href = "https://www.daisomall.co.kr" + href
        m = PDNO_RE.search(href)
        code = m.group(1) if m else None
        if not code or code in seen: continue
        seen.add(code)
        card = a
        for _ in range(3):
            if card.name in ("li","div"): break
            if not card.parent: break
            card = card.parent

        # 이름
        name_el = card.select_one(".product_name,.goods_name,.name,.tit,[class*='name'],[class*='tit']")
        name = name_el.get_text(" ", strip=True) if name_el else ""
        # 가격
        price_el = card.select_one(".product_price,.sale_price,.final,.price,[class*='price']")
        price = parse_krw(price_el.get_text("", strip=True) if price_el else "")
        if not name or not price: continue

        # 랭크
        rank_el = card.select_one(".rank,.num,.badge_rank,[class*='rank']")
        rank = None
        if rank_el:
            m2 = re.search(r"\d+", rank_el.get_text("", strip=True))
            if m2: rank = int(m2.group())

        items.append({"rank": rank, "raw_name": name, "price": price, "url": href, "product_code": code})

    # rank 보정/정렬
    n=1
    for it in items:
        if not it["rank"]: it["rank"]=n
        n+=1
    items.sort(key=lambda x: x["rank"])
    return items

# ===== Top200 수집(스크롤 → 0이면 재시도 → 보조파서 → 페이지네이션) =====
def collect_top200() -> List[Dict]:
    print(f"수집 시작: {RANK_URL}")
    results: List[Dict] = []
    seen = set()

    with sync_playwright() as p:
        # 1) 첫 페이지 스크롤 + JS 파서
        page = open_page(p, RANK_URL)
        infinite_scroll(page, target_min=120)
        items = collect_cards_js(page)
        if items:
            save_text("data/debug/rank_raw_page1.html", page.content())
        close_page(page)

        # 2) 0개면: 페이지 리로드 재시도 + 보조 파서
        if not items:
            page = open_page(p, RANK_URL)
            page.reload(wait_until="domcontentloaded")
            try: page.wait_for_load_state("networkidle", timeout=2000)
            except Exception: pass
            infinite_scroll(page, target_min=60)
            items = collect_cards_js(page)
            if not items:
                html = page.content()
                save_text("data/debug/rank_raw_page1.html", html)
                items = parse_cards_bs(html)
            close_page(page)

        # 3) 수집/중복제거
        for it in items:
            code = it["product_code"]
            if code in seen: continue
            seen.add(code); results.append(it)
            if len(results) >= MAX_ITEMS: break

        # 4) 200 미만이면 페이지네이션 ?page=2..N
        if len(results) < MAX_ITEMS:
            # base 만들기
            base = RANK_URL
            if "?" in base and "page=" in base:
                base = re.sub(r"([?&])page=\d+", r"\\1page=%d", base)
            elif "?" in base:
                base += "&page=%d"
            else:
                base += "?page=%d"

            for pg in range(2, PAGE_LIMIT+1):
                if len(results) >= MAX_ITEMS: break
                url = base % pg
                page = open_page(p, url)
                infinite_scroll(page, target_min=60)
                more = collect_cards_js(page)
                if not more:
                    html = page.content()
                    save_text(f"data/debug/rank_raw_page{pg}.html", html)
                    more = parse_cards_bs(html)
                close_page(page)
                if not more: break

                for it in more:
                    code = it["product_code"]
                    if code in seen: continue
                    seen.add(code); results.append(it)
                    if len(results) >= MAX_ITEMS: break

    # rank 정렬, 상위 200 컷
    results.sort(key=lambda x: int(x["rank"]))
    results = results[:MAX_ITEMS]
    print(f"[수집 완료] {len(results)}개")

    # === 강력 가드: 0개면 즉시 종료(업로드/슬랙 금지) ===
    if len(results) == 0:
        raise RuntimeError("랭킹 카드 0개 수집 — 셀렉터/로딩 이슈. data/debug/rank_raw_page1.html 확인 필요.")

    return results

# ===== CSV 저장 =====
def save_csv(rows: List[Dict]) -> Tuple[str, str]:
    ensure_dirs()
    fn = f"다이소몰_뷰티위생_일간_{today_str()}.csv"
    path = os.path.join("data", fn)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","rank","raw_name","price","url","product_code"])
        for r in rows:
            w.writerow([today_str(), r["rank"], r["raw_name"], r["price"], r["url"], r["product_code"]])
    return path, fn

# ===== Google Drive (OAuth) =====
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] OAuth 환경변수 부족 → 업로드 생략"); return None
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
        from google.oauth2.credentials import Credentials as UserCredentials
        from google.auth.transport.requests import Request as GoogleRequest
        creds = UserCredentials(
            None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        creds.refresh(GoogleRequest())
        svc = build("drive","v3",credentials=creds, cache_discovery=False)
        svc._Upload = MediaIoBaseUpload
        svc._Download = MediaIoBaseDownload
        return svc
    except Exception as e:
        print("[Drive] 서비스 생성 실패:", e); return None

def drive_upload(svc, filepath: str, filename: str) -> Optional[str]:
    try:
        media = svc._Upload(io.FileIO(filepath,"rb"), mimetype="text/csv", resumable=True)
        meta = {"name": filename, "parents":[GDRIVE_FOLDER_ID]} if GDRIVE_FOLDER_ID else {"name": filename}
        file = svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        print(f"[Drive] 업로드 성공: {file.get('name')} (id={file.get('id')})")
        return file.get("id")
    except Exception as e:
        print("[Drive] 업로드 실패:", e); return None

def drive_find_csv(svc, filename: str) -> Optional[str]:
    try:
        q = f"name='{filename}' and trashed=false and mimeType='text/csv'"
        if GDRIVE_FOLDER_ID:
            q += f" and '{GDRIVE_FOLDER_ID}' in parents"
        res = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        files = res.get("files",[])
        return files[0]["id"] if files else None
    except Exception as e:
        print("[Drive] 검색 실패:", e); return None

def drive_download_text(svc, file_id: str) -> Optional[str]:
    try:
        req = svc.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        dl = svc._Download(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        buf.seek(0); return buf.read().decode("utf-8")
    except Exception as e:
        print("[Drive] 다운로드 실패:", e); return None

# ===== 전일 비교 / 인&아웃 =====
def parse_prev_csv(text: str) -> List[Dict]:
    out = []
    rd = csv.DictReader(io.StringIO(text))
    for r in rd:
        code = (r.get("product_code") or "").strip()
        if not code: continue
        try:
            out.append({
                "rank": int(r.get("rank") or 0),
                "raw_name": r.get("raw_name") or "",
                "url": r.get("url") or "",
                "product_code": code
            })
        except Exception:
            continue
    return out

def analyze_trends(today: List[Dict], prev: Optional[List[Dict]], topN: int = 200):
    if not prev:
        for t in today: t["prev_rank"]=None; t["is_new"]=False
        return today, [], [], [], 0

    prev_top = {p["product_code"]: p["rank"] for p in prev if p.get("rank",9999) <= topN}
    prev_set = set(prev_top.keys())

    for t in today:
        code = t.get("product_code")
        pr = prev_top.get(code)
        t["prev_rank"] = pr
        t["is_new"] = pr is None

    movers = [t for t in today if t["prev_rank"] is not None]
    ups   = sorted([m for m in movers if m["prev_rank"] > m["rank"]],
                   key=lambda x: (x["prev_rank"]-x["rank"]), reverse=True)
    downs = sorted([m for m in movers if m["prev_rank"] < m["rank"]],
                   key=lambda x: (x["rank"]-x["prev_rank"]), reverse=True)

    today_set = {t["product_code"] for t in today[:topN] if t.get("product_code")}
    ins_set  = today_set - prev_set
    outs_set = prev_set - today_set
    io_cnt = min(len(ins_set), len(outs_set))  # 쌍 일치 강제

    chart_ins = [t for t in today if (t["product_code"] in ins_set and t["rank"]<=topN)]
    rank_outs = [p for p in prev if (p["product_code"] in outs_set and p["rank"]<=topN)]

    return today, ups, downs, chart_ins, rank_outs, io_cnt

# ===== Slack =====
def _link(name: str, url: Optional[str]) -> str:
    return f"<{url}|{name}>" if (name and url) else (name or (url or ""))

def post_slack(today: List[Dict], ups, downs, chart_ins, rank_outs, io_cnt: int):
    if not SLACK_WEBHOOK: return
    lines = []
    lines.append(f"*다이소몰 뷰티/위생 일간 랭킹 200* ({kst_now().strftime('%Y-%m-%d %H:%M KST')})")

    lines.append("\n*TOP 10*")
    for it in today[:10]:
        cur = it["rank"]; name=it["raw_name"]; url=it["url"]; price=it["price"]
        pr  = it.get("prev_rank")
        if pr is None: marker = "(new)"
        else:
            diff = pr - cur
            marker = f"(↑{diff})" if diff>0 else (f"(↓{abs(diff)})" if diff<0 else "(-)")
        lines.append(f"{cur}. {marker} {_link(name,url)} — {price:,}원")

    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m['raw_name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↑{m['prev_rank']-m['rank']})")
    else:
        lines.append("- (해당 없음)")

    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for x in chart_ins[:5]:
            lines.append(f"- {_link(x['raw_name'], x['url'])} NEW → {x['rank']}위")
    else:
        lines.append("- (해당 없음)")

    lines.append("\n*📉 급하락*")
    if downs:
        for m in downs[:5]:
            lines.append(f"- {_link(m['raw_name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↓{m['rank']-m['prev_rank']})")
    else:
        lines.append("- (급하락 없음)")

    if rank_outs:
        rank_outs_sorted = sorted(rank_outs, key=lambda x: int(x.get("rank") or 9999))
        for ro in rank_outs_sorted[:5]:
            lines.append(f"- {_link(ro.get('raw_name') or '', ro.get('url'))} {ro.get('rank')}위 → OUT")
    else:
        lines.append("- (OUT 없음)")

    lines.append("\n:left_right_arrow: **랭크 인&아웃**")
    if io_cnt==0:
        lines.append("변동 없음.")
    else:
        lines.append(f"**{io_cnt}개의 제품이 인&아웃 되었습니다.**")

    try:
        requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=12).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ===== main =====
def main():
    t0 = time.time()
    ensure_dirs()

    rows = collect_top200()  # 0개면 여기서 예외로 중단
    path, fname = save_csv(rows)

    # Drive 업로드 + 전일 파일 로드
    svc = build_drive_service()
    prev_list: Optional[List[Dict]] = None
    if svc:
        drive_upload(svc, path, fname)
        yname = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
        fid = drive_find_csv(svc, yname)
        if fid:
            txt = drive_download_text(svc, fid)
            if txt:
                prev_list = parse_prev_csv(txt)
                print(f"[분석] 전일 {len(prev_list)}건")

    # 비교/리포트
    today_aug, ups, downs, chart_ins, rank_outs, io_cnt = analyze_trends(rows, prev_list, topN=200)
    post_slack(today_aug, ups, downs, chart_ins, rank_outs, io_cnt)

    print(f"총 {len(rows)}건, 경과 시간: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
