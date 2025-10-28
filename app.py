# app.py — 다이소몰 뷰티/위생 '일간' Top200 수집·분석 (카테고리/일간/Top200 강제 + 카드기준 스크롤 + ID보강)

import os, re, csv, io, sys, traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import requests
from playwright.sync_api import sync_playwright, Page

# Google Drive
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

# ===== 유틸/로그 =====
def ensure_dirs():
    os.makedirs("data/debug", exist_ok=True)
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

# ---- ID 추출: 다양한 파라미터/패스 + URL 폴백 ----
ID_PARAMS = r"(?:pdNo|prdNo|productNo|goodsNo|itemNo|prdCd|prdId)"
def extract_item_id(url: str) -> Optional[str]:
    if not url: return None
    m = re.search(rf"[?&]{ID_PARAMS}=(\d+)", url, re.I)
    if m: return m.group(1)
    m = re.search(r"/(?:product|pd)/(?:detail|pdr)/(\d+)", url, re.I)
    if m: return m.group(1)
    return None

def normalize_url_for_key(url: str) -> str:
    u = re.sub(r"[?#].*$", "", (url or ""))
    return u.rstrip("/")

# ===== 카드/상태 검출 =====
CARD_SEL = (
    ".goods-list .goods-unit, .goods-unit-v2, .product-info, li.goods"
)

def _count_cards(page: Page) -> int:
    try:
        return page.evaluate(f"() => document.querySelectorAll('{CARD_SEL}').length")
    except Exception:
        return 0

def _is_beauty_active(page: Page) -> bool:
    try:
        return page.evaluate("""
          () => {
            const nodes = [...document.querySelectorAll('.prod-category * , .chips * , .tab * , .category *')];
            const t = nodes.find(n => /뷰티\\/?위생/.test((n.textContent||'').trim()));
            if (!t) return false;
            const c = (t.className||'') + ' ' + (t.parentElement?.className||'');
            return /\bis-active\b|\bon\b|\bactive\b|\bselected\b/i.test(c)
                   || t.getAttribute('aria-selected')==='true'
                   || t.getAttribute('aria-pressed')==='true';
          }
        """)
    except Exception:
        return False

def _is_daily_active(page: Page) -> bool:
    try:
        return page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp && (inp.checked || inp.getAttribute('checked')==='true')) return true;
            const nodes = [...document.querySelectorAll('*')];
            const t = nodes.filter(n => /일간/.test((n.textContent||'').trim()));
            const isAct = (el) => {
              const c = (el.className||'') + ' ' + (el.parentElement?.className||'');
              return /\bis-active\b|\bon\b|\bactive\b|\bselected\b/i.test(c)
                     || el.getAttribute('aria-selected')==='true'
                     || el.getAttribute('aria-pressed')==='true';
            };
            return t.some(isAct);
          }
        """)
    except Exception:
        return False

def _ensure_top200(page: Page) -> bool:
    """Top200 토글/버튼이 있으면 200으로 맞추고, 없으면 통과."""
    for _ in range(6):
        try:
            # 자주 보이는 패턴들 시도
            for qs in [
                "button:has-text('Top200')",
                "a:has-text('Top200')",
                "button:has-text('200')",
                "[data-top='200']",
            ]:
                loc = page.locator(qs)
                if loc.count() > 0:
                    loc.first.scroll_into_view_if_needed()
                    loc.first.click(timeout=800)
                    page.wait_for_timeout(250)
            # 일부 사이트는 select/라디오로 있을 수도 있음
            page.evaluate("""
              () => {
                const sel = document.querySelector('select[name*="top"]');
                if (sel) { sel.value = '200'; sel.dispatchEvent(new Event('change',{bubbles:true})); }
                const r = [...document.querySelectorAll('input[type=radio][value="200"]')][0];
                if (r) { r.checked = true; ['input','change','click'].forEach(ev=>r.dispatchEvent(new Event(ev,{bubbles:true}))); }
              }
            """)
            page.wait_for_timeout(300)
            # 검증은 카드 개수 ≥ 200이 목표
            if _count_cards(page) >= TOPN: return True
        except Exception:
            pass
    return _count_cards(page) >= TOPN

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

def _click_beauty(page: Page) -> bool:
    try:
        page.evaluate("() => document.querySelector('.prod-category')?.scrollIntoView({block:'center'})")
        page.wait_for_timeout(120)
    except Exception:
        pass
    for _ in range(8):
        clicked = False
        for sel in [
            '.prod-category .cate-btn[value="CTGR_00014"]',
            "button:has-text('뷰티/위생')",
            "a:has-text('뷰티/위생')",
            "text=뷰티/위생",
        ]:
            try:
                if sel == "text=뷰티/위생":
                    page.get_by_text("뷰티/위생", exact=False).first.click(timeout=800)
                    clicked = True; break
                loc = page.locator(sel)
                if loc.count() > 0:
                    loc.first.scroll_into_view_if_needed(); page.wait_for_timeout(80)
                    loc.first.click(timeout=800); clicked = True; break
            except Exception:
                continue
        if not clicked:
            _click_via_js(page, "뷰티/위생")
        page.wait_for_timeout(350)
        try: page.wait_for_load_state("networkidle", timeout=1200)
        except Exception: pass
        if _is_beauty_active(page): return True
    # 마지막 수단(가능하면): URL 파라미터로 카테고리 고정 — 사이트 구조에 따라 불가할 수 있음.
    return _is_beauty_active(page)

def _click_daily(page: Page) -> bool:
    for _ in range(10):
        try:
            loc = page.locator('.ipt-sorting input[value="2"]')
            if loc.count() > 0:
                loc.first.click(timeout=800)
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
        page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp) {
              inp.checked = true; inp.setAttribute('checked','true');
              ['input','change','click'].forEach(ev => inp.dispatchEvent(new Event(ev,{bubbles:true})));
            }
          }
        """)
        page.wait_for_timeout(320)
        try: page.wait_for_load_state("networkidle", timeout=1200)
        except Exception: pass
        if _is_daily_active(page): return True
    return _is_daily_active(page)

def _try_more_button(page: Page) -> bool:
    try:
        btn = page.locator("button:has-text('더보기'), a:has-text('더보기')")
        if btn.count() > 0:
            btn.first.scroll_into_view_if_needed()
            btn.first.click(timeout=700)
            page.wait_for_timeout(280)
            return True
    except Exception:
        pass
    return False

def _load_all(page: Page, want: int):
    def js_scroll_once():
        page.evaluate(f"""
          () => {{
            const list = document.querySelector('.goods-list') || document.scrollingElement || document.body;
            list.scrollTop = list.scrollHeight;
            window.scrollTo(0, document.body.scrollHeight);
            const cards = document.querySelectorAll('{CARD_SEL}');
            if (cards.length) {{
              const last = cards[cards.length-1];
              last.scrollIntoView({{block:'end'}});
            }}
            window.dispatchEvent(new Event('scroll'));
          }}
        """)
    def js_wiggle():
        page.evaluate("() => { window.scrollBy(0, -800); window.scrollBy(0, 6000); }")

    prev = 0; same = 0
    for round_idx in range(SCROLL_MAX_ROUNDS):
        js_scroll_once(); page.wait_for_timeout(SCROLL_PAUSE_MS)
        _try_more_button(page)
        try: page.wait_for_load_state("networkidle", timeout=1200)
        except Exception: pass
        cnt = _count_cards(page)
        log(f"[스크롤] 라운드 {round_idx+1} → {cnt}개(카드)")
        if cnt >= want: break
        if cnt == prev:
            same += 1; js_wiggle()
        else:
            same = 0; prev = cnt
        if same >= 4:
            _try_more_button(page); same = 0

    # 추가 3패스
    for extra in range(3):
        cur = _count_cards(page)
        if cur >= want: break
        log(f"[스크롤-추가] pass {extra+1} 시작 (현재 {cur})")
        for _ in range(8):
            js_scroll_once(); page.wait_for_timeout(int(SCROLL_PAUSE_MS*0.9))
        _try_more_button(page); page.wait_for_timeout(600)
        log(f"[스크롤-추가] pass {extra+1} 종료 (현재 {_count_cards(page)})")

def _extract_items(page: Page) -> List[Dict]:
    data = page.evaluate(f"""
      () => {{
        const cards = [...document.querySelectorAll('{CARD_SEL}')];
        const rows = [];
        const cleanName = (s) => {{
          if (!s) return '';
          s = s.trim();
          s = s.replace(/(택배배송|오늘배송|매장픽업|별점\\s*\\d+[.,\\d]*점|\\d+[.,\\d]*\\s*건\\s*작성).*$/g, '').trim();
          return s;
        }};
        for (const el of cards) {{
          const link = el.querySelector('a[href*="pdNo="], a[href*="prdNo="], a[href*="/pd/pdr/"], a[href*="/product/detail/"]');
          const nameEl = el.querySelector('.goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .goods-name, .name') || link;
          const priceEl = el.querySelector('.goods-detail .goods-price .value, .price .num, .sale-price .num, .sale .price, .goods-price .num, .price');

          const href = link?.href || link?.getAttribute('href') || '';
          let name = cleanName(nameEl?.textContent || '');
          if (!href || !name) continue;

          let priceText = (priceEl?.textContent || '').replace(/[^0-9]/g, '');
          let price = parseInt(priceText || '0', 10);
          if (!price || price <= 0) {{
            const t = (el.textContent || '').replace(/\\s+/g, ' ');
            const m = [...t.matchAll(/([0-9][0-9,]{2,})\\s*원/g)];
            if (m.length) price = parseInt(m[m.length-1][1].replace(/,/g,''),10);
          }}
          if (!price || price <= 0) continue;

          rows.push({{ name, price, url: href }});
        }}
        return rows;
      }}
    """)

    out = []; seen = set(); shown = 0
    for it in data:
        url = it.get("url","") or ""
        item_id = extract_item_id(url)
        key = item_id or normalize_url_for_key(url)
        if key in seen: continue
        seen.add(key)
        nm = strip_best(it["name"])
        if not nm: continue
        out.append({"pdNo": key, "name": nm, "price": int(it["price"]), "url": url})
        if shown < 3:
            log(f"[추출] 샘플 URL={url} → KEY={key}")
            shown += 1

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
        log("[Drive] 비활성화 — ENV 미설정"); return None
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
        log(f"[Drive] 서비스 생성 실패: {e}"); return None

def upload_to_drive(svc, path: str, name: str):
    if not svc or not GDRIVE_FOLDER_ID:
        log("[Drive] 업로드 건너뜀 — 서비스 없음 또는 폴더 ID 없음"); return None
    try:
        media = MediaIoBaseUpload(io.FileIO(path,'rb'), mimetype="text/csv", resumable=True)
        meta  = {"name": name, "parents":[GDRIVE_FOLDER_ID]}
        f = svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        log(f"[Drive] 업로드 성공: {f.get('name')} (ID: {f.get('id')})")
        return f.get("id")
    except Exception as e:
        log(f"[Drive] 업로드 실패: {e}"); return None

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

# ===== 비교/분석 =====
def parse_prev_csv(txt: str) -> List[Dict]:
    items=[]; rdr=csv.DictReader(io.StringIO(txt))
    for row in rdr:
        try:
            url = row.get("url","") or ""
            id0 = row.get("pdNo") or extract_item_id(url) or normalize_url_for_key(url)
            items.append({"pdNo": id0, "rank": int(row.get("rank")), "name": row.get("name"), "url": url})
        except Exception:
            continue
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
        log("[Slack] 비활성화 — SLACK_WEBHOOK_URL 없음"); return
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
        for m in ups[:5]: lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else: lines.append("- (해당 없음)")
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]: lines.append(f"- {_link(t['name'], t['url'])} NEW → {t['rank']}위")
    else: lines.append("- (해당 없음)")
    lines.append("\n*📉 급하락*")
    if downs:
        ds = sorted(downs, key=lambda x:(-abs(x["change"]), x["rank"]))
        for m in ds[:5]: lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↓{abs(m['change'])})")
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
                                  user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"))
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)

        ok_cat  = _click_beauty(page); log(f"[검증] 카테고리(뷰티/위생): {ok_cat}")
        ok_day  = _click_daily(page);  log(f"[검증] 일간선택: {ok_day}")
        _ensure_top200(page)          # 있으면 200으로
        _load_all(page, TOPN)

        html_path = f"data/debug/rank_raw_{today_str()}.html"
        with open(html_path, "w", encoding="utf-8") as f: f.write(page.content())
        log(f"[디버그] HTML 저장: {html_path} (카드 {_count_cards(page)}개)")

        rows = _extract_items(page)
        ctx.close(); browser.close()

    log(f"[수집 결과] {len(rows)}개")
    if len(rows) == 0:
        log("[치명] 0개 수집 — 실패로 종료"); sys.exit(2)

    csv_path, csv_name = save_csv(rows)
    log(f"[CSV] 저장: {csv_path}")

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
        try:
            with open(f"data/debug/exception_{today_str()}.txt", "w", encoding="utf-8") as f:
                f.write(err + "\n" + traceback.format_exc())
        except Exception:
            pass
        sys.exit(1)
