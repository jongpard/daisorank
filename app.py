# app.py — 다이소몰 뷰티/위생 '일간' 랭킹 수집/분석 (강제 디버깅/가드 추가판)

import os, re, csv, io, sys, traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import requests
from playwright.sync_api import sync_playwright, Page

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ===== 설정 =====
RANK_URL = os.getenv("RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
TOPN = int(os.getenv("TOPN", "200"))
SCROLL_MAX_ROUNDS = int(os.getenv("SCROLL_MAX_ROUNDS", "260"))
SCROLL_PAUSE_MS = int(os.getenv("SCROLL_PAUSE_MS", "700"))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Drive OAuth
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))
def now_kst(): return datetime.now(KST)
def today_str(): return now_kst().strftime("%Y-%m-%d")
def yday_str():  return (now_kst() - timedelta(days=1)).strftime("%Y-%m-%d")

# ===== 공통 유틸 =====
def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/debug", exist_ok=True)
    # artifact 업로드 보조 마커
    with open("data/debug/run_marker.txt", "w", encoding="utf-8") as f:
        f.write(now_kst().isoformat())

def log(msg: str):
    print(msg, flush=True)
    try:
        with open("data/debug/run_log.txt", "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def strip_best(s: str) -> str:
    if not s: return ""
    s = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", s, flags=re.I)
    s = re.sub(r"\s*\bBEST\b\s*", " ", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip()

def extract_pdno(url: str) -> Optional[str]:
    if not url: return None
    m = re.search(r"[?&]pdNo=(\d+)", url)
    if m: return m.group(1)
    m = re.search(r"/pd/(?:pdr|detail)/(\d+)", url)
    if m: return m.group(1)
    return None

# ===== DOM 카운트 =====
def _count_cards(page: Page) -> int:
    try:
        return page.evaluate("""
          () => document.querySelectorAll(
            'a[href*="pdNo="], a[href*="/pd/pdr/"], a[href*="/product/detail/"]'
          ).length
        """)
    except Exception:
        return 0

def _click_via_js(page: Page, text: str):
    page.evaluate("""
      (txt) => {
        const nodes = [...document.querySelectorAll('button, a, .cate-btn, .chip, .tab *, .category *')];
        const t = nodes.find(n => (n.textContent||'').trim().includes(txt));
        if (t) {
          t.scrollIntoView({block:'center'});
          t.dispatchEvent(new MouseEvent('mouseover', {bubbles:true}));
          t.dispatchEvent(new MouseEvent('mousedown', {bubbles:true}));
          t.click();
          t.dispatchEvent(new MouseEvent('mouseup', {bubbles:true}));
        }
      }
    """, text)

def _beauty_chip_active(page: Page) -> bool:
    try:
        return page.evaluate("""
          () => {
            const all = [...document.querySelectorAll('.prod-category * , .category, .cate, .chips, .tab, .filter *')];
            const isActive = (el) => {
              const c = (el.className||'') + ' ' + (el.parentElement?.className||'');
              return /\\bis-active\\b|\\bon\\b|\\bactive\\b|\\bselected\\b/i.test(c)
                  || el.getAttribute('aria-selected')==='true'
                  || el.getAttribute('aria-pressed')==='true';
            };
            const t = all.find(n => /뷰티\\/?위생/.test((n.textContent||'').trim()));
            return !!(t && isActive(t));
          }
        """)
    except Exception:
        return False

def _period_is_daily(page: Page) -> bool:
    try:
        return page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp && (inp.checked || inp.getAttribute('checked')==='true')) return true;
            const nodes = [...document.querySelectorAll('*')];
            const isActive = (el) => {
              const c = (el.className||'') + ' ' + (el.parentElement?.className||'');
              return /\\bis-active\\b|\\bon\\b|\\bactive\\b|\\bselected\\b/i.test(c)
                  || el.getAttribute('aria-selected')==='true'
                  || el.getAttribute('aria-pressed')==='true'
            };
            const t = nodes.filter(n => /일간/.test((n.textContent||'').trim()));
            return t.some(isActive);
          }
        """)
    except Exception:
        return False

def _click_beauty_chip(page: Page) -> bool:
    try:
        page.evaluate("() => document.querySelector('.prod-category')?.scrollIntoView({block:'center'})")
        page.wait_for_timeout(150)
    except Exception:
        pass
    before = _count_cards(page)
    for attempt in range(8):
        clicked = False
        for sel in [
            '.prod-category .cate-btn[value="CTGR_00014"]',
            "button:has-text('뷰티/위생')",
            "a:has-text('뷰티/위생')",
        ]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    loc.first.scroll_into_view_if_needed(); page.wait_for_timeout(100)
                    loc.first.click(timeout=800); clicked = True; break
            except Exception:
                continue
        if not clicked: _click_via_js(page, "뷰티/위생")
        page.wait_for_timeout(350)
        if _beauty_chip_active(page) or _count_cards(page) != before: return True
    return _beauty_chip_active(page)

def _click_daily(page: Page) -> bool:
    for _ in range(10):
        try: page.evaluate("window.scrollTo(0,0)")
        except Exception: pass
        try:
            inp = page.locator('.ipt-sorting input[value="2"]')
            if inp.count() > 0: inp.first.click(timeout=800)
        except Exception: pass
        page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp && inp.id) {
              const lb = document.querySelector('label[for="'+inp.id+'"]');
              if (lb) lb.click();
            }
          }
        """)
        try: page.get_by_role("button", name=re.compile("일간")).first.click(timeout=800)
        except Exception: _click_via_js(page, "일간")
        # 최후수단
        page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp) {
              inp.checked = true; inp.setAttribute('checked','true');
              ['input','change','click'].forEach(ev => inp.dispatchEvent(new Event(ev,{bubbles:true})));
            }
          }
        """)
        page.wait_for_timeout(350)
        try: page.wait_for_load_state("networkidle", timeout=1200)
        except Exception: pass
        if _period_is_daily(page): return True
    return _period_is_daily(page)

def _try_more_button(page: Page) -> bool:
    try:
        btn = page.locator("button:has-text('더보기'), a:has-text('더보기')")
        if btn.count() > 0:
            btn.first.scroll_into_view_if_needed(); btn.first.click(timeout=700)
            page.wait_for_timeout(300); return True
    except Exception:
        pass
    return False

def _load_all(page: Page, want: int):
    def js_scroll_once():
        page.evaluate("""
          () => {
            const list = document.querySelector('.goods-list') || document.querySelector('.list') ||
                         document.scrollingElement || document.body;
            list.scrollTop = list.scrollHeight;
            window.scrollTo(0, document.body.scrollHeight);
            const cards = document.querySelectorAll('a[href*="pdNo="], a[href*="/pd/pdr/"], a[href*="/product/detail/"]');
            if (cards.length) {
              const last = cards[cards.length-1].closest('.product-info, .goods-unit, .goods-item, .goods-unit-v2, li') || cards[cards.length-1];
              last.scrollIntoView({block:'end'});
            }
            window.dispatchEvent(new Event('scroll'));
          }
        """)
    def js_wiggle():
        page.evaluate("() => { window.scrollBy(0, -600); window.scrollBy(0, 5000); }")

    prev = 0; same = 0
    for round_idx in range(SCROLL_MAX_ROUNDS):
        js_scroll_once(); page.wait_for_timeout(SCROLL_PAUSE_MS)
        _try_more_button(page)
        try: page.wait_for_load_state("networkidle", timeout=1200)
        except Exception: pass
        cnt = _count_cards(page)
        log(f"[스크롤] 라운드 {round_idx+1} → {cnt}개")
        if cnt >= want: break
        if cnt == prev:
            same += 1; js_wiggle(); page.wait_for_timeout(int(SCROLL_PAUSE_MS*0.8))
        else:
            same = 0; prev = cnt
        if same >= 4:
            _try_more_button(page); js_scroll_once(); same = 0; page.wait_for_timeout(SCROLL_PAUSE_MS)

    for extra in range(3):
        cur = _count_cards(page)
        if cur >= want: break
        log(f"[스크롤-추가] pass {extra+1} 시작 (현재 {cur})")
        for _ in range(8):
            js_scroll_once(); page.wait_for_timeout(int(SCROLL_PAUSE_MS*0.9))
        _try_more_button(page); page.wait_for_timeout(600)
        log(f"[스크롤-추가] pass {extra+1} 종료 (현재 {_count_cards(page)})")

def _extract_items(page: Page) -> List[Dict]:
    data = page.evaluate("""
      () => {
        const anchors = [...document.querySelectorAll('a[href*="pdNo="], a[href*="/pd/pdr/"], a[href*="/product/detail/"]')];
        const seenCard = new Set();
        const rows = [];
        const cleanName = (s) => {
          if (!s) return '';
          s = s.trim();
          s = s.replace(/(택배배송|오늘배송|매장픽업|별점\\s*\\d+[.,\\d]*점|\\d+[.,\\d]*\\s*건\\s*작성).*$/g, '').trim();
          return s;
        };
        const cardOf = (a) => a.closest('.product-info, .goods-unit, .goods-item, .goods-unit-v2, li') || a.parentElement;

        for (const a of anchors) {
          const card = cardOf(a);
          if (!card || seenCard.has(card)) continue;
          seenCard.add(card);

          const nameEl = card.querySelector('.goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .goods-name, .name') || a;
          let name = cleanName(nameEl?.textContent || '');
          if (!name) continue;

          const priceEl = card.querySelector('.goods-detail .goods-price .value, .price .num, .sale-price .num, .sale .price, .goods-price .num, .price');
          let priceText = (priceEl?.textContent || '').replace(/[^0-9]/g, '');
          let price = parseInt(priceText || '0', 10);
          if (!price || price <= 0) {
            const t = (card.textContent || '').replace(/\\s+/g, ' ');
            const matches = [...t.matchAll(/([0-9][0-9,]{2,})\\s*원/g)];
            if (matches.length) {
              const last = matches[matches.length - 1][1];
              price = parseInt(last.replace(/,/g, ''), 10);
            }
          }
          if (!price || price <= 0) continue;

          let href = a.href || a.getAttribute('href') || '';
          if (!/^https?:/i.test(href)) href = new URL(href, location.origin).href;

          rows.push({ name, price, url: href });
        }
        return rows;
      }
    """)

    out = []; seen_pd = set()
    for it in data:
        pd = extract_pdno(it.get("url","") or ""); 
        if not pd or pd in seen_pd: continue
        seen_pd.add(pd)
        nm = strip_best(it["name"])
        if not nm: continue
        out.append({"pdNo": pd, "name": nm, "price": int(it["price"]), "url": it["url"]})
    for i, r in enumerate(out, 1): r["rank"] = i
    return out[:TOPN]

# ===== CSV =====
def save_csv(rows: List[Dict]):
    base_dir = os.path.join(os.getcwd(), "data")
    os.makedirs(base_dir, exist_ok=True)
    name = f"다이소몰_뷰티위생_일간_{today_str()}.csv"
    path = os.path.join(base_dir, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["date","pdNo","rank","name","price","url"])
        for r in rows[:TOPN]:
            w.writerow([today_str(), r["pdNo"], r["rank"], r["name"], r["price"], r["url"]])
    return path, name

# ===== Drive =====
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        log(f"[Drive] 비활성화 — ENV 미설정")
        return None
    try:
        creds = UserCredentials(
            None, refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive.file"]
        )
        creds.refresh(GoogleRequest())
        return build("drive","v3",credentials=creds,cache_discovery=False)
    except Exception as e:
        log(f"[Drive] 서비스 생성 실패: {e}")
        return None

def upload_to_drive(svc, path: str, name: str):
    if not svc or not GDRIVE_FOLDER_ID:
        log("[Drive] 업로드 건너뜀 — 서비스 없음 또는 폴더 ID 없음")
        return None
    try:
        media = MediaIoBaseUpload(io.FileIO(path,'rb'), mimetype="text/csv", resumable=True)
        meta  = {"name": name, "parents":[GDRIVE_FOLDER_ID]}
        f = svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        log(f"[Drive] 업로드 성공: {f.get('name')} (ID: {f.get('id')})")
        return f.get("id")
    except Exception as e:
        log(f"[Drive] 업로드 실패: {e}")
        return None

def find_file_in_drive(svc, name: str):
    if not svc or not GDRIVE_FOLDER_ID: return None
    try:
        q = f"name='{name}' and '{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
        r = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        return r.get("files", [])[0] if r.get("files") else None
    except Exception as e:
        log(f"[Drive] 파일 검색 실패({name}): {e}"); return None

def download_from_drive(svc, file_id: str) -> Optional[str]:
    try:
        req = svc.files().get_media(fileId=file_id)
        buf = io.BytesIO(); dl = MediaIoBaseDownload(buf, req); done=False
        while not done: _, done = dl.next_chunk()
        buf.seek(0); return buf.read().decode("utf-8")
    except Exception as e:
        log(f"[Drive] 파일 다운로드 실패(ID:{file_id}): {e}"); return None

# ===== 비교/분석 (pdNo) =====
def parse_prev_csv(txt: str) -> List[Dict]:
    items=[]; rdr=csv.DictReader(io.StringIO(txt))
    for row in rdr:
        try:
            pdno = row.get("pdNo") or extract_pdno(row.get("url","") or "")
            if not pdno: continue
            items.append({"pdNo": pdno, "rank": int(row.get("rank")), "name": row.get("name"), "url": row.get("url")})
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
    chart_ins = [t for t in today if t["pdNo"] in (today_keys - prev_keys)]
    rank_outs = [p for p in prev  if p["pdNo"] in (prev_keys - today_keys)]
    io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2
    chart_ins.sort(key=lambda r:r["rank"]); rank_outs.sort(key=lambda r:r["rank"])
    return ups, downs, chart_ins, rank_outs, io_cnt

# ===== Slack =====
def post_slack(rows: List[Dict], analysis, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK:
        log("[Slack] 비활성화 — SLACK_WEBHOOK_URL 없음")
        return
    ups, downs, chart_ins, rank_outs, io_cnt = analysis
    prev_map = {p["pdNo"]: p["rank"] for p in (prev_items or [])}
    def _link(n,u): return f"<{u}|{n}>" if u else (n or "")
    lines = [f"*다이소몰 뷰티/위생 일간 랭킹 {TOPN}* ({now_kst().strftime('%Y-%m-%d %H:%M KST')})"]
    lines.append("\n*TOP 10*")
    for it in rows[:10]:
        cur=it["rank"]; price=f"{int(it['price']):,}원"
        pr=prev_map.get(it["pdNo"])
        marker="(new)" if pr is None else (f"(↑{pr-cur})" if pr>cur else (f"(↓{cur-pr})" if pr<cur else "(-)"))
        lines.append(f"{cur}. {marker} {_link(it['name'], it['url'])} — {price}")
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
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
    lines.append("\n*↔ 랭크 인&아웃*"); lines.append(f"{io_cnt}개의 제품이 인&아웃 되었습니다.")
    try:
        requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=12).raise_for_status()
        log("[Slack] 전송 성공")
    except Exception as e:
        log(f"[Slack] 전송 실패: {e}")

# ===== main =====
def main():
    ensure_dirs()
    log(f"[시작] {RANK_URL}")
    log(f"[ENV] SLACK={'OK' if SLACK_WEBHOOK else 'NONE'} / GDRIVE={'OK' if (GDRIVE_FOLDER_ID and GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN) else 'NONE'}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = browser.new_context(viewport={"width": 1380, "height": 940},
                                  user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                                              "Chrome/123.0.0.0 Safari/537.36"))
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)

        ok_cat = _click_beauty_chip(page);  log(f"[검증] 카테고리(뷰티/위생): {ok_cat}")
        ok_day = _click_daily(page);        log(f"[검증] 일간선택: {ok_day}")

        _load_all(page, TOPN)
        html_path = f"data/debug/rank_raw_{today_str()}.html"
        with open(html_path, "w", encoding="utf-8") as f: f.write(page.content())
        log(f"[디버그] HTML 저장: {html_path} (카드 {_count_cards(page)}개)")

        rows = _extract_items(page)
        ctx.close(); browser.close()

    log(f"[수집 결과] {len(rows)}개")
    if len(rows) == 0:
        log("[치명] 0개 수집 — 실패로 종료")
        sys.exit(2)

    # CSV 저장
    csv_path, csv_name = save_csv(rows)
    log(f"[CSV] 저장: {csv_path}")

    # 전일 비교 및 Drive 업로드
    prev_items: List[Dict] = []
    svc = build_drive_service()
    if svc:
        upload_to_drive(svc, csv_path, csv_name)
        prev = find_file_in_drive(svc, f"다이소몰_뷰티위생_일간_{yday_str()}.csv")
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
        # 실패 시에도 디버그 아티팩트 남기기
        try:
            with open(f"data/debug/exception_{today_str()}.txt", "w", encoding="utf-8") as f:
                f.write(err + "\n" + traceback.format_exc())
        except Exception:
            pass
        sys.exit(1)
