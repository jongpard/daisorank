# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (2025-10-28 구조 대응판)
# - DOM 해시 클래스(dDeMca 등) 무시: .product-list > div 컨테이너의 텍스트/링크 기반 파싱
# - 무한 스크롤 안정화(앵커 개수 기반), 최소 카드수 가드
# - Google Drive(OAuth) 업로드/전일 파일 다운로드 후 비교 분석
# - Slack 요약 리포트(Top10 변동, 급상승/뉴랭커/급하락/OUT, 인&아웃 합)
# - 디버그 산출물 저장(data/debug/)

import os, re, csv, io, time, json
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page

# ===== 기본 설정 =====
KST = timezone(timedelta(hours=9))
RANK_URL = os.getenv("DAISO_RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))          # 목표 수집 개수
MIN_VALID = int(os.getenv("MIN_VALID", "10"))           # 최소 유효 카드수 (미만이면 가드)
SCROLL_MAX = int(os.getenv("SCROLL_MAX", "120"))        # 스크롤 라운드 상한
SCROLL_PAUSE = float(os.getenv("SCROLL_PAUSE", "0.5"))  # 라운드 간 대기(초)
SCROLL_STABLE = int(os.getenv("SCROLL_STABLE", "6"))    # 정체 라운드 수

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# Google Drive OAuth (User)
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()

# ===== 날짜 유틸 =====
def today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")

def yday_str() -> str:
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

# ===== 파일/디버그 =====
def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/debug", exist_ok=True)

def save_text(path: str, text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

# ===== Playwright 유틸 =====
def wait_net_idle(page: Page, ms: int = 400):
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(ms)

def select_category_daily(page: Page):
    """
    페이지가 다른 카테고리로 열리거나 초기값이 '주간/월간'일 수 있어 방어적 클릭.
    (실패해도 치명적이지 않도록 try 블록 구성)
    """
    try:
        # 카테고리(뷰티/위생)
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count() > 0:
            page.locator('.prod-category .cate-btn[value="CTGR_00014"]').first.click(timeout=1500)
        else:
            btn = page.get_by_role("button", name=re.compile("뷰티\\/?위생"))
            if btn.count() > 0:
                btn.first.click(timeout=1500)
    except Exception:
        pass
    wait_net_idle(page)

    try:
        # 정렬(일간)
        if page.locator('.ipt-sorting input[value="2"]').count() > 0:
            page.locator('.ipt-sorting input[value="2"]').first.click(timeout=1500)
        else:
            btn = page.get_by_role("button", name=re.compile("일간"))
            if btn.count() > 0:
                btn.first.click(timeout=1500)
    except Exception:
        pass
    wait_net_idle(page, 600)

def count_cards_by_anchor(page: Page) -> int:
    """ /pd/pdr/ 링크 개수를 카드 수로 간주 (해시 클래스 회피) """
    return page.locator('a[href*="/pd/pdr/"]').count()

def infinite_scroll(page: Page, target: int = MAX_ITEMS):
    prev = 0
    stable = 0
    for _ in range(SCROLL_MAX):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        wait_net_idle(page, int(SCROLL_PAUSE * 1000))
        cur = count_cards_by_anchor(page)
        if cur >= target:
            break
        if cur == prev:
            stable += 1
            if stable >= SCROLL_STABLE:
                break
        else:
            stable = 0
            prev = cur

def extract_items_from_html(html: str) -> List[Dict]:
    """
    .product-list > div 하위 블록의 텍스트에서 '... 5,000 원 상품명 ...' 패턴을 파싱.
    링크는 /pd/pdr/ 앵커 사용.
    """
    items: List[Dict] = []

    # product-list 블록 단위로 쪼갠 뒤, 각 블록에서 텍스트/링크 파싱
    # (정규표현식: price → \d[,\d]* 원, name → '원 ' 이후 ~ '택배배송|별점|리뷰|매장픽업|오늘배송' 전까지)
    # 링크: /pd/pdr/ ?pdNo=xxxxx
    # 다수의 해시 클래스(dDeMca 등)로 분리되어 있으므로 product-list 다음 깊이의 <div>를 카드로 취급
    # 성능을 위해 간단한 문자열 기반 파싱을 사용
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    plist = soup.select_one(".product-list")
    if not plist:
        return items

    cards = plist.select("> div")
    for rank, card in enumerate(cards, start=1):
        t = card.get_text(" ", strip=True)
        # 가격
        m_price = re.search(r'([0-9][0-9,\.]*)\s*원', t)
        # 명칭(가격 다음 등장하는 키워드 전까지)
        m_name = re.search(r'원\s+(.+?)\s+(택배배송|별점|리뷰|매장픽업|오늘배송)', t)
        # 링크
        a = card.select_one('a[href*="/pd/pdr/"]')
        href = a["href"] if a and a.has_attr("href") else None

        if not (m_price and m_name and href):
            continue

        price_txt = m_price.group(1).replace(",", "")
        try:
            price = int(float(price_txt))
        except Exception:
            price = None

        name = m_name.group(1).strip()
        url = href if href.startswith("http") else f"https://www.daisomall.co.kr{href}"

        items.append(
            {
                "rank": rank,
                "name": name,
                "price": price,
                "url": url,
            }
        )

    return items

def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1360, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=45_000)

        # 초기 로딩 안정화 & 일간/카테고리 고정
        try:
            page.wait_for_selector(".product-list, .prod-category", timeout=15_000)
        except PWTimeout:
            pass
        select_category_daily(page)

        # 무한 스크롤
        infinite_scroll(page, MAX_ITEMS)

        # 디버그(전/후 셀렉터 카운트 + 스크린샷 + 원본HTML)
        try:
            before = page.locator('a[href*="/pd/pdr/"]').count()
            page.screenshot(path="data/debug/page_after.png", full_page=True)
            html = page.content()
            after = len(re.findall(r'href="[^"]*/pd/pdr/', html))
            save_text("data/debug/rank_raw_after.html", html)
            save_text("data/debug/selector_counts.txt", f"anchors_before={before}, anchors_after={after}\n")
            print("[DEBUG] 최종 추출 앵커", after, "개")
        except Exception:
            pass

        html = page.content()
        items = extract_items_from_html(html)

        ctx.close()
        browser.close()
        return items

# ===== CSV =====
def save_csv(rows: List[Dict]) -> Tuple[str, str]:
    ensure_dirs()
    date_str = today_str()
    filename = f"다이소몰_뷰티위생_일간_{date_str}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "rank", "name", "price", "url"])
        for r in rows:
            w.writerow([date_str, r.get("rank"), r.get("name"), r.get("price"), r.get("url")])
    return path, filename

# ===== Google Drive (OAuth) =====
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] OAuth 환경변수 부족 → 업로드 건너뜀")
        return None
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
        svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        # 반환과 함께 헬퍼 붙이기
        svc._MediaIoBaseUpload = MediaIoBaseUpload
        svc._MediaIoBaseDownload = MediaIoBaseDownload
        return svc
    except Exception as e:
        print("[Drive] 서비스 생성 실패:", e)
        return None

def drive_upload_csv(svc, filepath: str, filename: str) -> Optional[str]:
    try:
        media = svc._MediaIoBaseUpload(io.FileIO(filepath, "rb"), mimetype="text/csv", resumable=True)
        meta = {"name": filename, "parents": [GDRIVE_FOLDER_ID]} if GDRIVE_FOLDER_ID else {"name": filename}
        f = svc.files().create(body=meta, media_body=media, fields="id,name").execute()
        print(f"[Drive] 업로드 성공: {f.get('name')} (id={f.get('id')})")
        return f.get("id")
    except Exception as e:
        print("[Drive] 업로드 실패:", e)
        return None

def drive_find(svc, filename: str) -> Optional[Dict]:
    try:
        if GDRIVE_FOLDER_ID:
            q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
        else:
            q = f"name='{filename}' and mimeType='text/csv' and trashed=false"
        res = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        files = res.get("files", [])
        return files[0] if files else None
    except Exception as e:
        print("[Drive] 검색 실패:", e)
        return None

def drive_download_text(svc, file_id: str) -> Optional[str]:
    try:
        req = svc.files().get_media(fileId=file_id)
        buf = io.BytesIO()
        dl = svc._MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            _, done = dl.next_chunk()
        buf.seek(0)
        return buf.read().decode("utf-8", errors="replace")
    except Exception as e:
        print("[Drive] 다운로드 실패:", e)
        return None

# ===== 전일 비교 =====
def parse_prev_csv(csv_text: str) -> List[Dict]:
    arr: List[Dict] = []
    try:
        rd = csv.DictReader(io.StringIO(csv_text))
        for r in rd:
            try:
                rank = int(r.get("rank", "") or "0")
            except Exception:
                rank = None
            arr.append({"rank": rank, "name": r.get("name"), "url": r.get("url")})
    except Exception as e:
        print("[CSV Parse] 실패:", e)
    return arr

def analyze_trends(today: List[Dict], prev: List[Dict], top_window: int = 200):
    prev_map = {p.get("url"): p.get("rank") for p in prev if p.get("url")}
    prev_top = {p.get("url") for p in prev if p.get("url") and (p.get("rank") or 9999) <= top_window}

    trends = []
    for t in today:
        u = t.get("url")
        pr = prev_map.get(u)
        change = (pr - t["rank"]) if pr else None
        trends.append({"name": t["name"], "url": u, "rank": t["rank"], "prev_rank": pr, "change": change})

    movers = [x for x in trends if x["prev_rank"] is not None]
    ups = sorted([x for x in movers if (x["change"] or 0) > 0], key=lambda x: x["change"], reverse=True)
    downs = sorted([x for x in movers if (x["change"] or 0) < 0], key=lambda x: x["change"])

    today_urls = {x["url"] for x in trends if x.get("url")}
    chart_ins = [x for x in trends if x["prev_rank"] is None and x["rank"] <= top_window]
    rank_out_urls = prev_top - today_urls
    rank_outs = [p for p in prev if p.get("url") in rank_out_urls]

    return ups, downs, chart_ins, rank_outs

# ===== Slack =====
def _link(name: str, url: Optional[str]) -> str:
    return f"<{url}|{name}>" if (url and name) else (name or (url or ""))

def post_slack(rows: List[Dict], analysis, prev: Optional[List[Dict]]):
    if not SLACK_WEBHOOK:
        return
    ups, downs, chart_ins, rank_outs = analysis

    # prev map for Top10 diff 표시
    prev_map = {}
    if prev:
        for p in prev:
            k = (p.get("url") or "").strip() or (p.get("name") or "").strip()
            if not k:
                continue
            try:
                prev_map[k] = int(p.get("rank") or 0)
            except Exception:
                pass

    def _key(d: Dict) -> str:
        return (d.get("url") or "").strip() or (d.get("name") or "").strip()

    lines = []
    lines.append(f"*다이소몰 뷰티/위생 일간 랭킹 200* ({datetime.now(KST).strftime('%Y-%m-%d %H:%M KST')})")

    # Top10
    lines.append("\n*TOP 10*")
    for it in rows[:10]:
        cur = int(it["rank"])
        name = it["name"]
        url = it["url"]
        price = it.get("price")
        marker = "(new)"
        pk = _key(it)
        if pk in prev_map:
            diff = prev_map[pk] - cur
            marker = f"(↑{diff})" if diff > 0 else ("(↓%d)" % (-diff) if diff < 0 else "(-)")
        lines.append(f"{cur}. {marker} {_link(name, url)} — {price:,}원" if price else f"{cur}. {marker} {_link(name, url)}")

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
        for x in chart_ins[:5]:
            lines.append(f"- {_link(x['name'], x['url'])} NEW → {x['rank']}위")
    else:
        lines.append("- (해당 없음)")

    # 급하락 + OUT
    lines.append("\n*📉 급하락*")
    if downs:
        d_sorted = sorted(downs, key=lambda m: (-abs(int(m.get("change") or 0)), int(m.get("rank") or 9999)))
        for m in d_sorted[:5]:
            drop = abs(int(m.get("change") or 0))
            lines.append(f"- {_link(m['name'], m['url'])} {m['prev_rank']}위 → {m['rank']}위 (↓{drop})")
    else:
        lines.append("- (급하락 없음)")

    if rank_outs:
        outs_sorted = sorted(rank_outs, key=lambda x: int(x.get("rank") or 9999))
        for ro in outs_sorted[:5]:
            lines.append(f"- {_link(ro.get('name'), ro.get('url'))} {int(ro.get('rank') or 0)}위 → OUT")
    else:
        lines.append("- (OUT 없음)")

    # 인&아웃 합(Top200 기준)
    if prev is not None:
        today_keys = {_key(it) for it in rows[:200] if _key(it)}
        prev_keys = {_key(p) for p in prev if _key(p) and 1 <= int(p.get("rank") or 0) <= 200}
        io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2
        lines.append("\n*↔ 랭크 인&아웃*")
        lines.append(f"IN/OUT 합: {io_cnt}")
    try:
        requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=12).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ===== main =====
def main():
    print("수집 시작:", RANK_URL)
    t0 = time.time()

    ensure_dirs()
    rows = fetch_products()
    print(f"[수집 완료] 개수: {len(rows)}")

    # 최소 카드수 가드 (사이트 변동 등)
    if len(rows) < MIN_VALID:
        print(f"[경고] 유효 상품 카드가 {len(rows)}개 — 기준({MIN_VALID}) 미달 → 그래도 CSV/드라이브/슬랙 진행")
    # CSV 저장
    csv_path, csv_filename = save_csv(rows)
    print("로컬 저장:", csv_path)

    # 드라이브 업로드 & 전일 비교
    prev_items: List[Dict] = []
    svc = build_drive_service()
    if svc:
        _id = drive_upload_csv(svc, csv_path, csv_filename)
        yname = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
        prev_file = drive_find(svc, yname)
        if prev_file:
            txt = drive_download_text(svc, prev_file["id"])
            if txt:
                prev_items = parse_prev_csv(txt)
                print(f"[분석] 전일 데이터 {len(prev_items)}건 로드")
        else:
            print(f"[Drive] 전일 파일 없음: {yname}")

    # 비교/슬랙
    analysis = analyze_trends(rows, prev_items) if prev_items else ([], [], [], [])
    post_slack(rows, analysis, prev_items if prev_items else None)

    print(f"총 {len(rows)}건, 경과 시간: {time.time() - t0:.1f}s")

if __name__ == "__main__":
    main()
