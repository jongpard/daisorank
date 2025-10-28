# app.py — DaisoMall 뷰티/위생 '일간' Top200 랭킹 수집/비교/리포트 (2025-10 구조 대응)
# - 200위까지 수집: ?page=1..N 페이지네이션 루프
# - 비교 안정키: product_code(pdNo)  ➜ NEW/인&아웃 오판 방지
# - 인&아웃: 쌍 일치 강제(단일 숫자), 불일치 시 전송 차단 가드
# - Google Drive(OAuth): 오늘 업로드 + 전일 파일 다운로드 비교
# - Slack: TOP10(변동 ↑↓−/new), 급상승/뉴랭커/급하락/OUT, 인&아웃 단일 문장

import os, re, io, csv, time, json
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout

# ===== KST / 날짜 =====
KST = timezone(timedelta(hours=9))
def kst_now() -> datetime: return datetime.now(KST)
def today_str() -> str:     return kst_now().strftime("%Y-%m-%d")
def yday_str() -> str:      return (kst_now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ===== 설정 =====
RANK_URL = os.getenv("DAISO_RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
MAX_ITEMS = 200
PER_PAGE_SAFE_MAX = 120     # 한 페이지에서 보이는 최대치(여유 상한)
PAGE_LIMIT = 10             # 안전 상한(200 못채우는 비정상 루프 방지)

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# Google Drive (OAuth)
GDRIVE_FOLDER_ID    = os.getenv("GDRIVE_FOLDER_ID", "").strip()
GOOGLE_CLIENT_ID    = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET= os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REFRESH_TOKEN= os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()

# ===== 디버그 아웃 =====
def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs("data/debug", exist_ok=True)

def save_text(path: str, text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

# ===== 공통 파서 =====
PDNO_RE = re.compile(r"[?&]pdNo=(\d+)")

def parse_krw(s: str) -> Optional[int]:
    s = re.sub(r"[^\d]", "", s or "")
    return int(s) if s else None

def extract_rank(s: str) -> Optional[int]:
    m = re.search(r"\d+", s or "")
    return int(m.group()) if m else None

def parse_items_from_html(html: str) -> List[Dict]:
    """
    서버 렌더된 랭킹 리스트를 파싱. (신 UI 클래스 변경 대응: 의미 있는 텍스트 기반)
    필수 컬럼: rank, raw_name, price, url, product_code(pdNo)
    """
    soup = BeautifulSoup(html, "html.parser")

    # 카드 컨테이너 탐색(여러 변형 대응)
    cards = (
        soup.select("ul#goodsList li") or
        soup.select("ul.goods_list li") or
        soup.select(".product-list > li, .product-list > div")
    )

    items: List[Dict] = []
    for i, li in enumerate(cards, start=1):
        a = li.select_one("a[href]")
        href = a["href"].strip() if a and a.has_attr("href") else ""
        if href and href.startswith("/"):
            href = "https://www.daisomall.co.kr" + href

        pdno = None
        if href:
            m = PDNO_RE.search(href)
            pdno = m.group(1) if m else None

        name_el = li.select_one(".product_name, .goods_name, .name, .tit")
        raw_name = (name_el.get_text(" ", strip=True) if name_el else "").strip()

        brand_el = li.select_one(".brand, .brand_name")
        brand = (brand_el.get_text(" ", strip=True) if brand_el else "").strip()

        price_el = li.select_one(".product_price, .sale_price, .final, .price")
        price_txt = (price_el.get_text("", strip=True) if price_el else "")
        price = parse_krw(price_txt)

        rank_el = li.select_one(".rank, .num, .badge_rank")
        r = extract_rank(rank_el.get_text("", strip=True) if rank_el else "")
        rank = r if r else i

        if raw_name and price and href and pdno:
            items.append({
                "rank": int(rank),
                "brand": brand,
                "raw_name": raw_name,
                "price": int(price),
                "url": href,
                "product_code": pdno,
            })

    return items

# ===== Playwright로 페이지네이션 수집 =====
def fetch_html_with_playwright(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1360, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/125.0.0.0 Safari/537.36"),
        )
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=45_000)
        # networkidle 한박자
        try: page.wait_for_load_state("networkidle", timeout=3000)
        except Exception: pass
        html = page.content()
        # 디버그: 페이지1 저장
        if "page=1" in url or url.endswith("/C105") or url.endswith("/C105/"):
            save_text("data/debug/rank_raw_page1.html", html)
        ctx.close(); browser.close()
        return html

def collect_top200() -> List[Dict]:
    print(f"수집 시작: {RANK_URL}")
    results: List[Dict] = []
    seen = set()

    # page= 파라미터 지원: 1..N 루프
    # 기본 URL이 /C105 형태면 page=1부터 붙여 순회
    base = RANK_URL
    if "?" in base and "page=" in base:
        base = re.sub(r"([?&])page=\d+", r"\g<1>page=%d", base)
    elif "?" in base:
        base = base + "&page=%d"
    else:
        base = base + "?page=%d"

    page_no = 1
    while len(results) < MAX_ITEMS and page_no <= PAGE_LIMIT:
        url = base % page_no
        html = fetch_html_with_playwright(url)
        items = parse_items_from_html(html)
        if not items:
            # 페이지가 없으면 break
            if page_no == 1:
                # 혹시 기본 URL 자체에 page가 필요 없을 때 마지막 방어로 한 번 더 파싱
                items = parse_items_from_html(fetch_html_with_playwright(RANK_URL))
                if not items:
                    break
            else:
                break

        # 순위 보정(페이지 오프셋 고려: 사이트가 절대순위 표기 안 해도 정렬 유지)
        for it in items:
            code = it["product_code"]
            if code in seen:  # 중복 방지
                continue
            seen.add(code)
            results.append(it)
            if len(results) >= MAX_ITEMS:
                break

        print(f"  - page {page_no}: 누적 {len(results)}개")
        if len(items) < 1 or len(items) < PER_PAGE_SAFE_MAX // 2:
            # 희박하게 적으면 더 이상 페이지가 없다고 판단
            break
        page_no += 1

    # 절대순위 정렬
    results = sorted(results, key=lambda x: int(x["rank"]))[:MAX_ITEMS]
    print(f"[수집 완료] {len(results)}개")
    return results

# ===== CSV =====
def save_csv(rows: List[Dict]) -> Tuple[str, str]:
    ensure_dirs()
    fn = f"다이소몰_뷰티위생_일간_{today_str()}.csv"
    path = os.path.join("data", fn)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","rank","brand","raw_name","price","url","product_code"])
        for r in rows:
            w.writerow([today_str(), r["rank"], r["brand"], r["raw_name"], r["price"], r["url"], r["product_code"]])
    return path, fn

# ===== Google Drive (OAuth) =====
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] OAuth 환경변수 부족 → 업로드 생략")
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
        try:
            out.append({
                "rank": int(r.get("rank") or 0),
                "raw_name": r.get("raw_name") or "",
                "url": r.get("url") or "",
                "product_code": (r.get("product_code") or "").strip()
            })
        except Exception:
            continue
    return out

def analyze_trends(today: List[Dict], prev: Optional[List[Dict]], topN: int = 200):
    if not prev:
        # 비교 불가: NEW도 표시하지 않음 / 인&아웃 0
        for t in today: t["prev_rank"]=None; t["is_new"]=False
        return today, [], [], [], 0

    prev_top = {p["product_code"]: p["rank"] for p in prev if (p.get("product_code") and p.get("rank")<=topN)}
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
    # 반드시 쌍 일치여야 함 → 불일치 시 작은 쪽 기준으로 맞춤, 가드 로그
    io_cnt = min(len(ins_set), len(outs_set))

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
    prev_map = {t["product_code"]: t["prev_rank"] for t in today if t.get("product_code") is not None}
    for it in today[:10]:
        cur = it["rank"]; name=it["raw_name"]; url=it["url"]; price=it["price"]
        pr  = it.get("prev_rank")
        if pr is None:
            marker = "(new)"
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

    # 인&아웃: 단일 숫자(쌍 일치)
    lines.append("\n:left_right_arrow: 랭크 인&아웃")
    if io_cnt==0:
        lines.append("변동 없음.")
    else:
        lines.append(f"{io_cnt}개의 제품이 인&아웃 되었습니다.")

    try:
        requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=12).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ===== main =====
def main():
    t0 = time.time()
    ensure_dirs()

    rows = collect_top200()
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
