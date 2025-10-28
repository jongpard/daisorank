# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집/분석 (뷰티/위생 강제, Top200 고정, pdNo 비교, Slack 포맷 고정)
import os, re, csv, io, time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, Page

# Google Drive (사용자 OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ===== 설정 =====
RANK_URL = os.getenv("RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
TOPN = int(os.getenv("TOPN", "200"))  # 정확히 이 개수만 저장/분석
SCROLL_MAX_ROUNDS = int(os.getenv("SCROLL_MAX_ROUNDS", "220"))
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

def parse_name_price(text: str) -> Tuple[Optional[str], Optional[int]]:
    t = re.sub(r"\s+", " ", (text or "")).strip()
    m = re.search(r"([0-9,]+)\s*원\s*(.+)", t)
    if m:
        price = int(m.group(1).replace(",", ""))
        name = strip_best(m.group(2))
        return (name or None), price
    m2 = re.search(r"(.+?)\s*([0-9,]+)\s*원", t)
    if m2:
        name = strip_best(m2.group(1))
        price = int(m2.group(2).replace(",", ""))
        return (name or None), price
    return None, None

# ===== Playwright =====
def _count_cards(page: Page) -> int:
    try:
        return page.evaluate("""() => document.querySelectorAll('div.product-info a[href*="/pd/pdr/"]').length""")
    except Exception:
        return 0

def _click_beauty_chip(page: Page) -> bool:
    """
    '뷰티/위생' 카테고리 칩을 다양한 방식으로 클릭하고,
    목록 개수 변화가 생기면 True.
    """
    before = _count_cards(page)
    selectors = [
        '.prod-category .cate-btn[value="CTGR_00014"]',
        "button:has-text('뷰티/위생')",
        "a:has-text('뷰티/위생')",
        "*:text('뷰티/위생')"
    ]
    clicked = False
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=1200)
                clicked = True
                break
        except Exception:
            continue
    if not clicked:
        # JS 강제
        page.evaluate("""
          () => {
            const byVal = document.querySelector('.prod-category .cate-btn[value="CTGR_00014"]');
            if (byVal) { byVal.click(); return; }
            const all = [...document.querySelectorAll('.prod-category *')];
            const t = all.find(n => /뷰티\\/?위생/.test((n.textContent||'').trim()));
            if (t) (t.closest('button') || t).click();
          }
        """)
        clicked = True
    # 클릭 후 대기: 개수 변화를 최대 2.5초(5회) 확인
    if clicked:
        for _ in range(5):
            page.wait_for_timeout(500)
            now = _count_cards(page)
            if now != before:
                return True
    return False

def _click_daily(page: Page):
    try:
        page.locator('.ipt-sorting input[value="2"]').first.click(timeout=1200)  # 일간
    except Exception:
        try:
            page.get_by_role("button", name=re.compile("일간")).click(timeout=1200)
        except Exception:
            pass
    page.wait_for_timeout(300)

def _load_all(page: Page, want: int):
    prev = 0; stable = 0
    for _ in range(SCROLL_MAX_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        cnt = _count_cards(page)
        if cnt >= want: break
        if cnt == prev:
            stable += 1
            if stable >= 10: break
        else:
            stable = 0; prev = cnt

def _extract_items(page: Page) -> List[Dict]:
    data = page.evaluate("""
      () => {
        const cards = [...document.querySelectorAll('div.product-info')];
        const out = [];
        for (const info of cards) {
          const a = info.querySelector('a[href*="/pd/pdr/"]');
          if (!a) continue;
          let href = a.href || a.getAttribute('href') || '';
          if (href && !/^https?:/i.test(href)) href = new URL(href, location.origin).href;
          const text = (info.textContent || '').replace(/\\s+/g, ' ').trim();
          out.push({ url: href, raw: text });
        }
        return out;
      }
    """)
    rows=[]
    for it in data:
        name, price = parse_name_price(it["raw"])
        pd = extract_pdno(it["url"])
        if not (name and price and pd): continue
        rows.append({"pdNo": pd, "name": name, "price": price, "url": it["url"]})
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows[:TOPN]  # TopN 고정

def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = browser.new_context(
            viewport={"width": 1380, "height": 940},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
        )
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)

        # 1) 카테고리/기간 고정
        _click_beauty_chip(page)
        _click_daily(page)

        # 2) 스크롤 로드
        _load_all(page, TOPN)

        # 3) 디버그 HTML 저장
        os.makedirs("data/debug", exist_ok=True)
        with open(f"data/debug/rank_raw_{today_str()}.html", "w", encoding="utf-8") as f:
            f.write(page.content())

        # 4) 추출
        rows = _extract_items(page)

        ctx.close(); browser.close()
        return rows

# ===== CSV =====
def save_csv(rows: List[Dict]) -> Tuple[str,str]:
    date = today_str()
    os.makedirs("data", exist_ok=True)
    name = f"다이소몰_뷰티위생_일간_{date}.csv"
    path = os.path.join("data", name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","pdNo","rank","name","price","url"])
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
        meta = {"name": name, "parents":[GDRIVE_FOLDER_ID]}
        f = svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        print(f"[Drive] 업로드 성공: {f.get('name')} (ID: {f.get('id')})")
        return f.get("id")
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

# ===== 전일 비교/분석 (pdNo 기준) =====
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
        pd = t["pdNo"]; tr=t["rank"]; pr = prev_map.get(pd)
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
        pr = prev_map.get(it["pdNo"])
        if pr is None: marker="(new)"
        else:
            diff=pr-cur; marker=f"(↑{diff})" if diff>0 else (f"(↓{abs(diff)})" if diff<0 else "(-)")
        lines.append(f"{cur}. {marker} {_link(it['name'], it['url'])} — {price}")

    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else:
        lines.append("- (해당 없음)")

    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t['name'], t['url'])} NEW → {t['rank']}위")
    else:
        lines.append("- (해당 없음)")

    lines.append("\n*📉 급하락*")
    if downs:
        ds = sorted(downs, key=lambda x:(-abs(x["change"]), x["rank"]))
        for m in ds[:5]:
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↓{abs(m['change'])})")
    else:
        lines.append("- (급하락 없음)")
    if rank_outs:
        os_ = sorted(rank_outs, key=lambda x:x["rank"])
        for ro in os_[:5]:
            lines.append(f"- {_link(ro.get('name'), ro.get('url'))} {int(ro.get('rank') or 0)}위 → OUT")
    else:
        lines.append("- (OUT 없음)")

    # 요청 포맷 그대로
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
