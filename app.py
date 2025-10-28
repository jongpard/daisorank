# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집/분석 (pdNo 비교, Top200 고정, Slack 아이콘 수정)
# - 비교키 통일: pdNo (URL에서 추출) → 전일/금일 정확 매칭
# - 정확히 Top200만 저장/분석
# - IN&OUT: Top200 집합 대칭차//2 (항상 IN≡OUT)
# - Slack 인&아웃 섹션: :양방향_화살표: + 굵게 카운트

import os, re, csv, time, io
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, Page

# Google Drive (사용자 OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ===== 파라미터 =====
RANK_URL = os.getenv("RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
TOPN = int(os.getenv("TOPN", "200"))        # 정확히 이 개수로 저장/분석
SCROLL_MAX_ROUNDS = int(os.getenv("SCROLL_MAX_ROUNDS", "200"))
SCROLL_PAUSE_MS = int(os.getenv("SCROLL_PAUSE_MS", "700"))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Drive OAuth
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))

# ===== 유틸 =====
def today_str(): return datetime.now(KST).strftime("%Y-%m-%d")
def yday_str():  return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def strip_best(name: str) -> str:
    if not name: return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()

def extract_pdno(url: str) -> Optional[str]:
    if not url: return None
    # ?pdNo=123456
    m = re.search(r"[?&]pdNo=(\d+)", url)
    if m: return m.group(1)
    # /pd/pdr/123456 or /pd/detail/123456
    m = re.search(r"/pd/(?:pdr|detail)/(\d+)", url)
    if m: return m.group(1)
    return None

def parse_name_price(text: str) -> Tuple[Optional[str], Optional[int]]:
    text = re.sub(r"\s+", " ", (text or "")).strip()
    # “5,000원 상품명 …” 혹은 “상품명 … 5,000원”
    m = re.search(r"([0-9,]+)\s*원\s*(.+)", text)
    if not m:
        m2 = re.search(r"(.+?)\s*([0-9,]+)\s*원", text)
        if not m2: return None, None
        name = strip_best(m2.group(1))
        price = int(m2.group(2).replace(",", ""))
        return (name or None), price
    price = int(m.group(1).replace(",", ""))
    name = strip_best(m.group(2))
    return (name or None), price

# ===== Playwright =====
def ensure_tab(page: Page):
    # 뷰티/위생 + 일간 고정 (실패해도 진행)
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count() > 0:
            page.locator('.prod-category .cate-btn[value="CTGR_00014"]').first.click(timeout=1500)
        else:
            page.get_by_role("button", name=re.compile("뷰티\\/?위생")).click(timeout=1500)
    except Exception: pass
    try:
        page.locator('.ipt-sorting input[value="2"]').first.click(timeout=1500)  # 일간
    except Exception:
        try: page.get_by_role("button", name=re.compile("일간")).click(timeout=1500)
        except Exception: pass

def load_all(page: Page, want: int = TOPN):
    prev = 0; stable = 0
    for _ in range(SCROLL_MAX_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        cnt = page.evaluate("""() => document.querySelectorAll('div.product-info a[href*="/pd/pdr/"]').length""")
        if cnt >= want: break
        if cnt == prev:
            stable += 1
            if stable >= 8: break
        else:
            stable = 0; prev = cnt

def extract_items_js(page: Page) -> List[Dict]:
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
    res = []
    for it in data:
        name, price = parse_name_price(it["raw"])
        pdno = extract_pdno(it["url"])
        if not (name and price and pdno): continue
        res.append({"pdNo": pdno, "name": name, "price": price, "url": it["url"]})
    # 순위 부여 & Top200 슬라이스
    for i, r in enumerate(res, 1): r["rank"] = i
    return res[:TOPN]  # <<< 정확히 200개만 사용

def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        ctx = browser.new_context(viewport={"width": 1380, "height": 940},
                                  user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                                              "Chrome/123.0.0.0 Safari/537.36"))
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)
        ensure_tab(page)
        load_all(page, TOPN)
        # 디버그 HTML 저장
        os.makedirs("data/debug", exist_ok=True)
        with open(f"data/debug/rank_raw_{today_str()}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        items = extract_items_js(page)
        ctx.close(); browser.close()
        return items

# ===== CSV =====
def save_csv(rows: List[Dict]) -> Tuple[str, str]:
    date = today_str()
    os.makedirs("data", exist_ok=True)
    fn = f"다이소몰_뷰티위생_일간_{date}.csv"
    path = os.path.join("data", fn)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","pdNo","rank","name","price","url"])
        for r in rows[:TOPN]:
            w.writerow([date, r["pdNo"], r["rank"], r["name"], r["price"], r["url"]])
    return path, fn

# ===== Drive =====
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] OAuth 환경변수 미설정 → 비활성화"); return None
    try:
        creds = UserCredentials(
            None, refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive.file"])
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

# ===== 비교/분석(pdNo 기반) =====
def parse_prev_csv(csv_text: str) -> List[Dict]:
    items=[]; rdr=csv.DictReader(io.StringIO(csv_text))
    for row in rdr:
        try:
            pdno = row.get("pdNo") or extract_pdno(row.get("url","") or "")
            if not pdno: continue
            items.append({"pdNo": pdno, "rank": int(row.get("rank")), "name": row.get("name"), "url": row.get("url")})
        except Exception: continue
    return items

def analyze_trends(today: List[Dict], prev: List[Dict]):
    # 맵: pdNo → rank
    prev_map = {p["pdNo"]: p["rank"] for p in prev}
    # 상승/하락
    ups, downs = [], []
    for t in today[:TOPN]:
        pd = t["pdNo"]; tr = t["rank"]; pr = prev_map.get(pd)
        if pr is None: continue
        change = pr - tr
        d = {"pdNo": pd, "name": t["name"], "url": t["url"], "rank": tr, "prev_rank": pr, "change": change}
        if change > 0: ups.append(d)
        elif change < 0: downs.append(d)
    ups.sort(key=lambda x: (-x["change"], x["rank"]))
    downs.sort(key=lambda x: (x["change"], x["rank"]))
    # IN/OUT (Top200 기준 집합)
    today_keys = {t["pdNo"] for t in today[:TOPN]}
    prev_keys  = {p["pdNo"] for p in prev if 1 <= p["rank"] <= TOPN}
    chart_ins_keys = list(today_keys - prev_keys)
    rank_out_keys  = list(prev_keys - today_keys)
    chart_ins = [t for t in today if t["pdNo"] in chart_ins_keys]
    rank_outs = [p for p in prev  if p["pdNo"] in rank_out_keys]
    # 집합 기준 대칭차/2 → 항상 IN≡OUT
    io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2
    # 정렬
    chart_ins.sort(key=lambda r: r["rank"])
    rank_outs.sort(key=lambda r: r["rank"])
    return ups, downs, chart_ins, rank_outs, io_cnt

# ===== Slack =====
def post_slack(rows: List[Dict], analysis_results, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK: return
    ups, downs, chart_ins, rank_outs, io_cnt = analysis_results

    def _link(n,u): return f"<{u}|{n}>" if u else (n or "")
    prev_map = {p["pdNo"]: p["rank"] for p in (prev_items or [])}

    now = datetime.now(KST)
    lines = [f"*다이소몰 뷰티/위생 일간 랭킹 {TOPN}* ({now.strftime('%Y-%m-%d %H:%M KST')})"]

    # TOP10 (변동표시)
    lines.append("\n*TOP 10*")
    for it in rows[:10]:
        cur = it["rank"]; price = f"{int(it['price']):,}원"
        pr = prev_map.get(it["pdNo"])
        if pr is None: marker="(new)"
        else:
            diff = pr - cur
            marker = f"(↑{diff})" if diff>0 else (f"(↓{abs(diff)})" if diff<0 else "(-)")
        lines.append(f"{cur}. {marker} {_link(it['name'], it['url'])} — {price}")

    # 급상승
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else:
        lines.append("- (해당 없음)")

    # 뉴랭커
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t['name'], t['url'])} NEW → {t['rank']}위")
    else:
        lines.append("- (해당 없음)")

    # 급하락 + OUT
    lines.append("\n*📉 급하락*")
    if downs:
        downs_sorted = sorted(downs, key=lambda x: (-abs(x["change"]), x["rank"]))
        for m in downs_sorted[:5]:
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↓{abs(m['change'])})")
    else:
        lines.append("- (급하락 없음)")
    if rank_outs:
        outs_sorted = sorted(rank_outs, key=lambda x: x["rank"])
        for ro in outs_sorted[:5]:
            lines.append(f"- {_link(ro.get('name'), ro.get('url'))} {int(ro.get('rank') or 0)}위 → OUT")
    else:
        lines.append("- (OUT 없음)")

    # 인&아웃 (요청 포맷/아이콘)
    lines.append("\n:양방향_화살표: 랭크 인&아웃")
    lines.append(f"**{io_cnt}개의 제품이 인&아웃 되었습니다.**")

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

    # 정확히 200개로 고정
    rows = rows[:TOPN]

    # CSV 저장
    csv_path, csv_name = save_csv(rows)
    print("로컬 저장:", csv_path)

    # Drive 업/전일 로딩
    prev_items: List[Dict] = []
    svc = build_drive_service()
    if svc:
        upload_to_drive(svc, csv_path, csv_name)
        yname = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
        prev_file = find_file_in_drive(svc, yname)
        if prev_file:
            txt = download_from_drive(svc, prev_file["id"])
            if txt: prev_items = parse_prev_csv(txt)

    # 분석(pdNo 기반)
    analysis = analyze_trends(rows, prev_items)

    # 슬랙
    post_slack(rows, analysis, prev_items)

if __name__ == "__main__":
    main()
