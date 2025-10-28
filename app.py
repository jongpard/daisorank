# app.py — 다이소몰 뷰티/위생 '일간' 랭킹 수집/분석
# - 카테고리/일간 강제 검증 (라디오/라벨/버튼/JS 최후수단)
# - Top200 스크롤 확로드(End/scrollTo/wheel/더보기)
# - 견고한 추출(제품명/가격/링크), pdNo 기준 비교
# - Slack 포맷: 인&아웃 1줄 고정

import os, re, csv, io, time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

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
SCROLL_PAUSE_MS = int(os.getenv("SCROLL_PAUSE_MS", "750"))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Drive OAuth
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))
def today_str(): return datetime.now(KST).strftime("%Y-%m-%d")
def yday_str():  return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

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

# ===== 공통 =====
def _count_cards(page: Page) -> int:
    try:
        return page.evaluate("""
          () => document.querySelectorAll(
            'a[href*="pdNo="], a[href*="/pd/pdr/"], a[href*="/product/detail/"]'
          ).length
        """)
    except Exception:
        return 0

def _scroll_to_chipbar(page: Page):
    try:
        page.evaluate("""
          () => {
            const bars = [...document.querySelectorAll('.prod-category, .chips, .tab, .category')];
            if (bars.length) bars[0].scrollIntoView({block:'center'});
          }
        """)
        page.wait_for_timeout(200)
    except Exception:
        pass

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

def _is_active_like(page: Page, el) -> bool:
    # python에서 안씀. js 안으로 넣어 호출
    return False

# ===== 카테고리/일간 =====
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
            const dailyInput = document.querySelector('.ipt-sorting input[value="2"]');
            if (dailyInput && (dailyInput.checked || dailyInput.getAttribute('checked')==='true')) return true;

            const nodes = [...document.querySelectorAll('*')];
            const isActive = (el) => {
              const c = (el.className||'') + ' ' + (el.parentElement?.className||'');
              return /\\bis-active\\b|\\bon\\b|\\bactive\\b|\\bselected\\b/i.test(c)
                  || el.getAttribute('aria-selected')==='true'
                  || el.getAttribute('aria-pressed')==='true';
            };
            const candid = nodes.filter(n => /일간/.test((n.textContent||'').trim()));
            return candid.some(isActive);
          }
        """)
    except Exception:
        return False

def _click_beauty_chip(page: Page) -> bool:
    _scroll_to_chipbar(page)
    before = _count_cards(page)
    candidates = [
        '.prod-category .cate-btn[value="CTGR_00014"]',
        "button:has-text('뷰티/위생')",
        "a:has-text('뷰티/위생')",
        "text=뷰티/위생",
    ]
    for attempt in range(8):
        clicked = False
        for sel in candidates:
            try:
                if sel == "text=뷰티/위생":
                    page.get_by_text("뷰티/위생", exact=False).first.scroll_into_view_if_needed()
                    page.get_by_text("뷰티/위생", exact=False).first.click(timeout=900)
                    clicked = True; break
                loc = page.locator(sel)
                if loc.count() > 0:
                    loc.first.scroll_into_view_if_needed()
                    page.wait_for_timeout(120)
                    loc.first.hover(timeout=600)
                    loc.first.click(timeout=900)
                    clicked = True; break
            except Exception:
                continue
        if not clicked:
            _click_via_js(page, "뷰티/위생")
        page.wait_for_timeout(450)
        if _beauty_chip_active(page):
            return True
        if _count_cards(page) != before:
            return True
    return _beauty_chip_active(page)

def _click_daily(page: Page) -> bool:
    # 최대로 집요하게 '일간'을 활성화
    for attempt in range(10):
        try:
            page.evaluate("window.scrollTo(0,0)")
            page.wait_for_timeout(200)
        except Exception:
            pass

        try:
            # 1) 라디오 직접 클릭
            inp = page.locator('.ipt-sorting input[value="2"]')
            if inp.count() > 0:
                inp.first.click(timeout=800)
        except Exception:
            pass

        try:
            # 2) 라벨 클릭
            page.evaluate("""
              () => {
                const inp = document.querySelector('.ipt-sorting input[value="2"]');
                if (inp && inp.id) {
                  const lb = document.querySelector('label[for="'+inp.id+'"]');
                  if (lb) lb.click();
                }
              }
            """)
        except Exception:
            pass

        # 3) 버튼/탭 텍스트
        try:
            page.get_by_role("button", name=re.compile("일간")).first.click(timeout=800)
        except Exception:
            try:
                _click_via_js(page, "일간")
            except Exception:
                pass

        # 4) 최후수단: JS로 checked + 이벤트
        page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp) {
              inp.checked = true;
              inp.setAttribute('checked','true');
              ['input','change','click'].forEach(ev => inp.dispatchEvent(new Event(ev, {bubbles:true})));
              const form = inp.closest('form');
              if (form) form.dispatchEvent(new Event('change', {bubbles:true}));
            }
          }
        """)

        page.wait_for_timeout(400)
        # 네트워크 안정 대기
        try:
            page.wait_for_load_state("networkidle", timeout=1200)
        except Exception:
            pass

        if _period_is_daily(page):
            return True

    return _period_is_daily(page)

# ===== 스크롤 로딩 =====
def _try_more_button(page: Page) -> bool:
    try:
        btn = page.locator("button:has-text('더보기'), a:has-text('더보기')")
        if btn.count() > 0:
            btn.first.scroll_into_view_if_needed()
            btn.first.click(timeout=800)
            page.wait_for_timeout(400)
            return True
    except Exception:
        pass
    return False

def _load_all(page: Page, want: int):
    prev = 0; stable = 0
    for _ in range(SCROLL_MAX_ROUNDS):
        acted = _try_more_button(page)
        if not acted:
            page.keyboard.press("End")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try: page.mouse.wheel(0, 16000)
            except Exception: pass
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        cnt = _count_cards(page)
        if cnt >= want: break
        if cnt == prev:
            stable += 1
            if stable >= 10: break
        else:
            stable = 0; prev = cnt
    # 부족하면 2라운드 더
    for _ in range(2):
        if _count_cards(page) >= want: break
        for __ in range(6):
            page.keyboard.press("End")
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(int(SCROLL_PAUSE_MS*0.8))
        _try_more_button(page)
        page.wait_for_timeout(600)

# ===== 추출 =====
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

    out = []
    seen_pd = set()
    for it in data:
        pd = extract_pdno(it.get("url","") or "")
        if not pd: continue
        if pd in seen_pd: continue
        seen_pd.add(pd)

        nm = strip_best(it["name"])
        if not nm: continue

        out.append({
            "pdNo": pd,
            "name": nm,
            "price": int(it["price"]),
            "url": it["url"],
        })

    for i, r in enumerate(out, 1):
        r["rank"] = i
    return out[:TOPN]

# ===== 수집 =====
def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = browser.new_context(
            viewport={"width": 1380, "height": 940},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
        )
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)

        if not _click_beauty_chip(page):
            print("[경고] 뷰티/위생 칩 활성화 검증 실패")
        if not _click_daily(page):
            print("[경고] '일간' 활성화 검증 실패")

        _load_all(page, TOPN)

        os.makedirs("data/debug", exist_ok=True)
        with open(f"data/debug/rank_raw_{today_str()}.html", "w", encoding="utf-8") as f:
            f.write(page.content())

        rows = _extract_items(page)

        ctx.close(); browser.close()
        return rows

# ===== CSV =====
def save_csv(rows: List[Dict]):
    date = today_str()
    os.makedirs("data", exist_ok=True)
    name = f"다이소몰_뷰티위생_일간_{date}.csv"
    path = os.path.join("data", name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["date","pdNo","rank","name","price","url"])
        for r in rows[:TOPN]:
            w.writerow([date, r["pdNo"], r["rank"], r["name"], r["price"], r["url"]])
    return path, name

# ===== Drive =====
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] OAuth env 미설정 → 비활성화"); return None
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
        print("[Drive] 서비스 생성 실패:", e); return None

def upload_to_drive(svc, path: str, name: str):
    if not svc or not GDRIVE_FOLDER_ID: return None
    try:
        media = MediaIoBaseUpload(io.FileIO(path,'rb'), mimetype="text/csv", resumable=True)
        meta  = {"name": name, "parents":[GDRIVE_FOLDER_ID]}
        f = svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        print(f"[Drive] 업로드 성공: {f.get('name')} (ID: {f.get('id')})"); return f.get("id")
    except Exception as e:
        print("[Drive] 업로드 실패:", e); return None

def find_file_in_drive(svc, name: str):
    if not svc or not GDRIVE_FOLDER_ID: return None
    try:
        q = f"name='{name}' and '{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
        r = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        return r.get("files", [])[0] if r.get("files") else None
    except Exception as e:
        print(f"[Drive] 파일 검색 실패({name}):", e); return None

def download_from_drive(svc, file_id: str) -> Optional[str]:
    try:
        req = svc.files().get_media(fileId=file_id)
        buf = io.BytesIO(); dl = MediaIoBaseDownload(buf, req); done=False
        while not done: _, done = dl.next_chunk()
        buf.seek(0); return buf.read().decode("utf-8")
    except Exception as e:
        print(f"[Drive] 파일 다운로드 실패(ID:{file_id}):", e); return None

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
    chart_ins.sort(key=lambda r:r["rank"])
    rank_outs.sort(key=lambda r:r["rank"])
    return ups, downs, chart_ins, rank_outs, io_cnt

# ===== Slack =====
def post_slack(rows: List[Dict], analysis, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK: return
    ups, downs, chart_ins, rank_outs, io_cnt = analysis
    prev_map = {p["pdNo"]: p["rank"] for p in (prev_items or [])}
    def _link(n,u): return f"<{u}|{n}>" if u else (n or "")

    now = datetime.now(KST)
    lines = [f"*다이소몰 뷰티/위생 일간 랭킹 {TOPN}* ({now.strftime('%Y-%m-%d %H:%M KST')})"]

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
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t['name'], t['url'])} NEW → {t['rank']}위")
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

    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"{io_cnt}개의 제품이 인&아웃 되었습니다.")

    try:
        requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=12).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ===== main =====
def main():
    print("수집 시작:", RANK_URL)
    rows = fetch_products()
    print(f"[수집 완료] {len(rows)}개 → Top{TOPN}로 사용")
    rows = rows[:TOPN]

    csv_path, csv_name = save_csv(rows)
    print("로컬 저장:", csv_path)

    prev_items: List[Dict] = []
    svc = build_drive_service()
    if svc:
        upload_to_drive(svc, csv_path, csv_name)
        yname = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
        prev = find_file_in_drive(svc, yname)
        if prev:
            txt = download_from_drive(svc, prev["id"])
            if txt: prev_items = parse_prev_csv(txt)

    analysis = analyze_trends(rows, prev_items)
    post_slack(rows, analysis, prev_items)

if __name__ == "__main__":
    main()
