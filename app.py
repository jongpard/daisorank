# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (기능 강화/포맷 고정본)
# - Drive: 사용자 OAuth(클라ID/시크릿/리프레시 토큰)만 사용, ADC 미사용
# - 전일 파일: Drive 우선, 실패 시 로컬 data/ 폴백
# - Slack: 예전 포맷 유지(섹션 제목, TOP10 변동표시, 급상승/뉴랭커/급하락+OUT, 랭크 인&아웃)
#          인&아웃 카운트는 굵게(**42개의 제품이…**) 표기
# - 수집: Playwright 무한 스크롤, 최소 200개 확보 시 종료

import os, re, csv, time, io, json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Union

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Locator

# ====== 설정 ======
RANK_URL = os.getenv("RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))   # 최소 200 보장 목표
TOP_WINDOW = int(os.getenv("TOP_WINDOW", "150")) # 뉴랭커/OUT 판단 상위 구간
KST = timezone(timedelta(hours=9))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Google Drive (OAuth 사용자 계정 — 반드시 설정)
GDRIVE_FOLDER_ID   = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID   = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# ====== Google API (사용자 OAuth) ======
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ================= 공용 유틸 =================
def today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")

def yday_str() -> str:
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def strip_best(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()

# ================= Playwright 보조 =================
def _to_locator(page: Page, target: Union[str, Locator]) -> Locator:
    return target if isinstance(target, Locator) else page.locator(target)

def close_overlays(page: Page):
    sels = [
        ".layer-popup .btn-close", ".modal .btn-close", ".popup .btn-close",
        ".layer-popup .close", ".modal .close", ".popup .close",
        ".btn-x", "button[aria-label='닫기']"
    ]
    for s in sels:
        try:
            if page.locator(s).count() > 0:
                page.locator(s).first.click(timeout=600)
                page.wait_for_timeout(150)
        except Exception:
            pass

def click_hard(page: Page, target: Union[str, Locator], name_for_log: str = ""):
    loc = _to_locator(page, target)
    try:
        loc.first.wait_for(state="attached", timeout=2500)
    except Exception:
        raise RuntimeError(f"[click_hard] 대상 미존재: {name_for_log}")
    for _ in range(3):
        try:
            loc.first.click(timeout=900)
            return
        except Exception:
            try:
                loc.first.scroll_into_view_if_needed(timeout=600)
                page.wait_for_timeout(120)
            except Exception:
                pass
    try:
        loc.first.evaluate("(el)=>el.click()")
    except Exception:
        raise RuntimeError(f"[click_hard] 클릭 실패: {name_for_log}")

# ================= 수집: 카테고리/일간 고정 + 무한 스크롤 =================
def select_beauty_daily(page: Page):
    close_overlays(page)
    # 카테고리: 뷰티/위생
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count() > 0:
            click_hard(page, '.prod-category .cate-btn[value="CTGR_00014"]', "뷰티/위생(value)")
        else:
            click_hard(page, page.get_by_role("button", name=re.compile("뷰티\\/?위생")), "뷰티/위생(text)")
    except Exception:
        page.evaluate("""
            () => {
              const byVal = document.querySelector('.prod-category .cate-btn[value="CTGR_00014"]');
              if (byVal) byVal.click();
              else {
                const nodes = [...document.querySelectorAll('.prod-category *')];
                const t = nodes.find(n => /뷰티\\/?위생/.test((n.textContent||'').trim()));
                if (t) (t.closest('button') || t).click();
              }
            }
        """)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(250)

    # 정렬: 일간
    try:
        click_hard(page, '.ipt-sorting input[value="2"]', "일간(value)")
    except Exception:
        try:
            click_hard(page, page.get_by_role("button", name=re.compile("일간")), "일간(text)")
        except Exception:
            pass
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(300)

def infinite_scroll(page: Page, want: int):
    prev = 0
    stable = 0
    for _ in range(120):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(600)
        cnt = page.evaluate("""
            () => document.querySelectorAll('.goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2').length
        """)
        if cnt >= want:
            break
        if cnt == prev:
            stable += 1
            if stable >= 4:
                break
        else:
            stable = 0
            prev = cnt

def collect_items(page: Page) -> List[Dict]:
    data = page.evaluate("""
        () => {
          const q = s => [...document.querySelectorAll(s)];
          const units = q('.goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2');
          const seen = new Set();
          const items = [];
          for (const el of units) {
            const nameEl = el.querySelector('.goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .name, .goods-name');
            let name = (nameEl?.textContent || '').trim();
            if (!name) continue;
            const priceEl = el.querySelector('.goods-detail .goods-price .value, .price .num, .sale-price .num, .sale .price, .goods-price .num');
            let priceTxt = (priceEl?.textContent || '').replace(/[^0-9]/g, '');
            if (!priceTxt) continue;
            let price = parseInt(priceTxt, 10);
            if (!price) continue;
            let href = null;
            const a = el.querySelector('a[href*="/pd/pdr/"]');
            if (a && a.href) href = a.href;
            if (!href) continue;
            if (seen.has(href)) continue;
            seen.add(href);
            items.push({ name, price, url: href });
          }
          return items;
        }
    """)
    cleaned = []
    for it in data:
        nm = strip_best(it["name"])
        if nm:
            cleaned.append({"name": nm, "price": it["price"], "url": it["url"]})
    for i, it in enumerate(cleaned, 1):
        it["rank"] = i
    return cleaned

def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        context = browser.new_context(
            viewport={"width": 1360, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36"),
        )
        page = context.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=45_000)
        try:
            page.wait_for_selector(".prod-category", timeout=15_000)
        except PWTimeout:
            pass
        select_beauty_daily(page)
        try:
            page.wait_for_selector(".goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2", timeout=20_000)
        except PWTimeout:
            pass
        infinite_scroll(page, MAX_ITEMS)
        items = collect_items(page)
        context.close()
        browser.close()
        return items

# ================= CSV =================
def save_csv(rows: List[Dict]) -> (str, str):
    date_str = today_str()
    os.makedirs("data", exist_ok=True)
    filename = f"다이소몰_뷰티위생_일간_{date_str}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","rank","name","price","url"])
        for r in rows:
            w.writerow([date_str, r["rank"], r["name"], r["price"], r["url"]])
    return path, filename

# ================= Drive (사용자 OAuth만) =================
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] 사용자 OAuth 환경변수 미설정 → Drive 기능 비활성화")
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
        print("[Drive] 서비스/폴더 미설정 → 업로드 생략")
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
        req = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        dl = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        fh.seek(0)
        return fh.read().decode("utf-8")
    except Exception as e:
        print(f"[Drive] 파일 다운로드 실패 (ID: {file_id}):", e)
        return None

# 로컬 폴백: 전일 CSV 읽기
def read_local_yday_csv() -> Optional[str]:
    path = os.path.join("data", f"다이소몰_뷰티위생_일간_{yday_str()}.csv")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None

# ================= 분석 =================
def parse_prev_csv(csv_text: str) -> List[Dict]:
    items = []
    try:
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            try:
                items.append({
                    "rank": int(row.get("rank")),
                    "name": row.get("name"),
                    "url": row.get("url"),
                })
            except Exception:
                continue
    except Exception as e:
        print("[CSV Parse] 전일 데이터 파싱 실패:", e)
    return items

def analyze_trends(today_items: List[Dict], prev_items: List[Dict]):
    prev_map = {p["url"]: p["rank"] for p in prev_items if p.get("url")}
    prev_top_urls = {p["url"] for p in prev_items if p.get("url") and p.get("rank", 9999) <= TOP_WINDOW}

    trends = []
    for it in today_items:
        url = it.get("url")
        if not url: 
            continue
        pr = prev_map.get(url)
        trends.append({
            "name": it["name"],
            "url": url,
            "rank": it["rank"],
            "prev_rank": pr,
            "change": (pr - it["rank"]) if pr else None
        })

    movers = [t for t in trends if t["prev_rank"] is not None]
    ups   = sorted([t for t in movers if t["change"] > 0], key=lambda x: x["change"], reverse=True)
    downs = sorted([t for t in movers if t["change"] < 0], key=lambda x: x["change"])

    chart_ins = [t for t in trends if t["prev_rank"] is None and t["rank"] <= TOP_WINDOW]

    today_urls = {t["url"] for t in trends}
    rank_out_urls = prev_top_urls - today_urls
    rank_outs = [p for p in prev_items if p.get("url") in rank_out_urls]

    # IN≡OUT 카운트(전일 데이터가 있는 경우 집합 대칭차 기반)
    if prev_items:
        today_keys = {t["url"] for t in trends if t.get("url") and t.get("rank", 9999) <= TOP_WINDOW}
        prev_keys  = {p["url"] for p in prev_items if p.get("url") and p.get("rank", 9999) <= TOP_WINDOW}
        io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2
    else:
        io_cnt = min(len(chart_ins), len(rank_outs))

    return ups, downs, chart_ins, rank_outs, io_cnt

# ================= Slack (예전 포맷 유지 + 인&아웃 볼드) =================
def post_slack(rows: List[Dict], analysis_results, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK:
        print("[Slack] Webhook 미설정 → 전송 생략")
        return

    ups, downs, chart_ins, rank_outs, _io_cnt = analysis_results

    def _link(name: str, url: Optional[str]) -> str:
        return f"<{url}|{name}>" if url else (name or "")

    def _key(it: dict) -> str:
        return (it.get("url") or "").strip() or (it.get("name") or "").strip()

    # 전일 맵 (TOP10 변동표시)
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

    # TOP10 (변동표시 ↑n/↓n/-/new)
    lines.append("\n*TOP 10*")
    for it in (rows or [])[:10]:
        cur_r = int(it.get("rank") or 0)
        price_txt = f"{int(it.get('price') or 0):,}원"
        k = _key(it)
        marker = "(new)"
        if k in prev_map:
            prev_r = prev_map[k]
            diff = prev_r - cur_r
            marker = f"(↑{diff})" if diff > 0 else (f"(↓{abs(diff)})" if diff < 0 else "(-)")
        lines.append(f"{cur_r}. {marker} {_link(it.get('name') or '', it.get('url'))} — {price_txt}")

    # 🔥 급상승
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m.get('name'), m.get('url'))} {m.get('prev_rank')}위 → {m.get('rank')}위 (↑{m.get('change')})")
    else:
        lines.append("- (해당 없음)")

    # 🆕 뉴랭커
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t.get('name'), t.get('url'))} NEW → {t.get('rank')}위")
    else:
        lines.append("- (해당 없음)")

    # 📉 급하락 + OUT
    lines.append("\n*📉 급하락*")
    if downs:
        downs_sorted = sorted(
            downs,
            key=lambda m: (-abs(int(m.get("change") or 0)), int(m.get("rank") or 9999))
        )
        for m in downs_sorted[:5]:
            drop = abs(int(m.get("change") or 0))
            lines.append(f"- {_link(m.get('name'), m.get('url'))} {m.get('prev_rank')}위 → {m.get('rank')}위 (↓{drop})")
    else:
        lines.append("- (급하락 없음)")

    if rank_outs:
        outs_sorted = sorted(rank_outs, key=lambda x: int(x.get("rank") or 9999))
        for ro in outs_sorted[:5]:
            lines.append(f"- {_link(ro.get('name'), ro.get('url'))} {int(ro.get('rank') or 0)}위 → OUT")
    else:
        lines.append("- (OUT 없음)")

    # ↔ 랭크 인&아웃 (볼드)
    # prev_items가 있을 때 대칭차//2, 없을 땐 min(new, out)
    if prev_items:
        today_keys = {_key(it) for it in (rows or [])[:200] if _key(it)}
        prev_keys  = {_key(p) for p in (prev_items or []) if _key(p) and 1 <= int(p.get("rank") or 0) <= 200}
        io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2
    else:
        io_cnt = min(len(chart_ins or []), len(rank_outs or []))
    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"**{io_cnt}개의 제품이 인&아웃 되었습니다.**")

    try:
        requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=12).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ================= main =================
def main():
    print("수집 시작:", RANK_URL)
    t0 = time.time()
    rows = fetch_products()
    print(f"[수집 완료] 개수: {len(rows)}")

    if len(rows) < MAX_ITEMS:
        print(f"[경고] 목표 {MAX_ITEMS} < 실제 {len(rows)} — 소스 구조/로딩 이슈 가능")

    # CSV 저장
    csv_path, csv_filename = save_csv(rows)
    print("로컬 저장:", csv_path)

    # Drive 서비스 (사용자 OAuth만)
    drive_service = build_drive_service()

    # 오늘 업로드
    if drive_service:
        upload_to_drive(drive_service, csv_path, csv_filename)

    # 전일 파일 로딩: Drive → 로컬 data/ 폴백
    prev_items: List[Dict] = []
    yname = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
    csv_content = None
    if drive_service:
        prev_file = find_file_in_drive(drive_service, yname)
        if prev_file:
            print(f"[Drive] 전일 파일 발견: {prev_file['name']} (ID: {prev_file['id']})")
            csv_content = download_from_drive(drive_service, prev_file['id'])
        else:
            print(f"[Drive] 전일 파일 미발견 → 로컬 폴백 시도: data/{yname}")
    if csv_content is None:
        csv_content = read_local_yday_csv()
        if csv_content:
            print("[Local] 전일 CSV 로컬에서 로드 성공")
        else:
            print("[Local] 전일 CSV 없음")

    if csv_content:
        prev_items = parse_prev_csv(csv_content)
        print(f"[분석] 전일 데이터 {len(prev_items)}건 로드")

    # 분석
    analysis_results = analyze_trends(rows, prev_items)

    # 슬랙 전송(예전 포맷 + 인&아웃 볼드)
    post_slack(rows, analysis_results, prev_items)

    print(f"총 {len(rows)}건, 경과 {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
