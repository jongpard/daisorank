# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (기능 강화판)
# - GDrive 연동: 전일 데이터 다운로드 및 분석 기능 추가
# - 순위 변동 분석: 급상승, 뉴랭커, 급하락, 랭크아웃
# - Slack 포맷 개선: 올리브영 버전과 동일한 리포트 형식 적용
# - 기존 기능 유지: Playwright 기반 크롤링, CSV 저장 및 업로드

import os, re, csv, time, json, io
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Union

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Locator

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ====== 설정 ======
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))
TOP_WINDOW = 150  # 뉴랭커, 랭크아웃 등을 판단하는 기준 순위
DS_RISING_FALLING_THRESHOLD = 10
DS_TOP_MOVERS_MAX = 5
DS_NEWCOMERS_TOP = 150

SCROLL_PAUSE = 0.6
SCROLL_STABLE_ROUNDS = 4
SCROLL_MAX_ROUNDS = 80

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
# Google Drive (OAuth 사용자 계정 정보)
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))


# ====== 유틸 ======
def today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")

def yday_str() -> str:
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def strip_best(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()


def _to_locator(page: Page, target: Union[str, Locator]) -> Locator:
    return target if isinstance(target, Locator) else page.locator(target)


def close_overlays(page: Page):
    candidates = [
        ".layer-popup .btn-close", ".modal .btn-close", ".popup .btn-close",
        ".layer-popup .close", ".modal .close", ".popup .close",
        ".btn-x", ".btn-close, button[aria-label='닫기']"
    ]
    for sel in candidates:
        try:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click(timeout=1000)
                page.wait_for_timeout(200)
        except Exception:
            pass


def click_hard(page: Page, target: Union[str, Locator], name_for_log: str = ""):
    loc = _to_locator(page, target)
    try:
        loc.first.wait_for(state="attached", timeout=3000)
    except Exception:
        raise RuntimeError(f"[click_hard] 대상 미존재: {name_for_log}")
    try:
        loc.first.click(timeout=1200)
        return
    except Exception: pass
    try:
        loc.first.scroll_into_view_if_needed(timeout=1000)
        page.wait_for_timeout(150)
        loc.first.click(timeout=1200)
        return
    except Exception: pass
    try:
        loc.first.evaluate("(el) => { el.click(); }")
        return
    except Exception: pass
    raise RuntimeError(f"[click_hard] 클릭 실패: {name_for_log}")


# ====== Playwright (카테고리/정렬 고정 + 스크롤 + 추출) ======
def select_beauty_daily(page: Page):
    close_overlays(page)
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
                const btns = [...document.querySelectorAll('.prod-category .cate-btn, .prod-category *')];
                const t = btns.find(b => /뷰티\\/?위생/.test((b.textContent||"").trim()));
                if (t) t.click();
              }
            }
        """)
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(300)

    try:
        click_hard(page, '.ipt-sorting input[value="2"]', "일간(value)")
    except Exception:
        click_hard(page, page.get_by_role("button", name=re.compile("일간")), "일간(text)")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(400)


def infinite_scroll(page: Page):
    prev = 0
    stable = 0
    for _ in range(SCROLL_MAX_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(int(SCROLL_PAUSE * 1000))
        cnt = page.evaluate("""
            () => document.querySelectorAll('.goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2').length
        """)
        if cnt >= MAX_ITEMS:
            break
        if cnt == prev:
            stable += 1
            if stable >= SCROLL_STABLE_ROUNDS:
                break
        else:
            stable = 0
            prev = cnt


def collect_items(page: Page) -> List[Dict]:
    data = page.evaluate(
        """
        () => {
          const qs = sel => [...document.querySelectorAll(sel)];
          const units = qs('.goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2');
          const seen = new Set();
          const items = [];
          for (const el of units) {
            const nameEl = el.querySelector('.goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .name, .goods-name');
            let name = (nameEl?.textContent || '').trim();
            if (!name) continue;
            const priceEl = el.querySelector('.goods-detail .goods-price .value, .price .num, .sale-price .num, .sale .price, .goods-price .num');
            let priceTxt = (priceEl?.textContent || '').replace(/[^0-9]/g, '');
            if (!priceTxt) continue;
            const price = parseInt(priceTxt, 10);
            if (!price || price <= 0) continue;
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
        """
    )
    cleaned = []
    for it in data:
        nm = strip_best(it["name"])
        if not nm:
            continue
        cleaned.append({"name": nm, "price": it["price"], "url": it["url"]})
    for i, it in enumerate(cleaned, 1):
        it["rank"] = i
    return cleaned


def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
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
        infinite_scroll(page)
        items = collect_items(page)
        context.close()
        browser.close()
        return items


# ====== CSV 저장 ======
def save_csv(rows: List[Dict]) -> str:
    date_str = today_str()
    os.makedirs("data", exist_ok=True)
    filename = f"다이소몰_뷰티위생_일간_{date_str}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "rank", "name", "price", "url"])
        for r in rows:
            w.writerow([date_str, r["rank"], r["name"], r["price"], r["url"]])
    return path, filename

# ====== Google Drive (신규 추가 및 수정) ======
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] OAuth 환경변수 미설정")
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
        print("[Drive] 서비스 또는 폴더 ID가 없어 업로드를 건너뜁니다.")
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
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read().decode("utf-8")
    except Exception as e:
        print(f"[Drive] 파일 다운로드 실패 (ID: {file_id}):", e)
        return None


# ====== 변화 감지 및 분석 (신규) ======
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
            except (ValueError, TypeError):
                continue
    except Exception as e:
        print("[CSV Parse] 전일 데이터 파싱 실패:", e)
    return items

def analyze_trends(today_items: List[Dict], prev_items: List[Dict]):
    prev_map = {p["url"]: p["rank"] for p in prev_items if p.get("url")}
    prev_top_urls = {p["url"] for p in prev_items if p.get("url") and p.get("rank", 999) <= TOP_WINDOW}

    trends = []
    for it in today_items:
        url = it.get("url")
        if not url: continue
        prev_rank = prev_map.get(url)
        trends.append({
            "name": it["name"],
            "url": url,
            "rank": it["rank"],
            "prev_rank": prev_rank,
            "change": (prev_rank - it["rank"]) if prev_rank else None
        })

    movers = [t for t in trends if t["prev_rank"] is not None]
    ups = sorted([t for t in movers if t["change"] > 0], key=lambda x: x["change"], reverse=True)
    downs = sorted([t for t in movers if t["change"] < 0], key=lambda x: x["change"])

    chart_ins = [t for t in trends if t["prev_rank"] is None and t["rank"] <= TOP_WINDOW]
    
    today_urls = {t["url"] for t in trends}
    rank_out_urls = prev_top_urls - today_urls
    rank_outs = [p for p in prev_items if p.get("url") in rank_out_urls]

    in_out_count = len(chart_ins) + len(rank_outs)

    return ups, downs, chart_ins, rank_outs, in_out_count


# ====== Slack (급하락 5 + OUT 5, 링크 포함, 인&아웃 수 보정 / TOP10 변동표시 추가) ======
def post_slack(rows: List[Dict], analysis_results, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK:
        return

    ups, downs, chart_ins, rank_outs, _inout_unused = analysis_results

    def _link(name: str, url: Optional[str]) -> str:
        return f"<{url}|{name}>" if url else (name or "")

    def _key(it: dict) -> str:
        # url 우선, 없으면 name
        return (it.get("url") or "").strip() or (it.get("name") or "").strip()

    # ----- 전일 랭크 맵 (TOP10 변동표시에 사용)
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

    # =========================
    # TOP 10 (변동표시: ↑n, ↓n, -, new)
    # =========================
    lines.append("\n*TOP 10*")
    for it in (rows or [])[:10]:
        try:
            ptxt = f"{int(it.get('price') or 0):,}원"
        except Exception:
            ptxt = str(it.get("price") or "")

        cur_r = int(it.get("rank") or 0)
        k = _key(it)
        marker = "(new)"
        if k in prev_map:
            prev_r = prev_map[k]
            diff = prev_r - cur_r
            if diff > 0:
                marker = f"(↑{diff})"
            elif diff < 0:
                marker = f"(↓{abs(diff)})"
            else:
                marker = "(-)"

        lines.append(f"{cur_r}. {marker} {_link(it.get('name') or '', it.get('url'))} — {ptxt}")

    # 🔥 급상승 (최대 5개, 링크)
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m.get('name'), m.get('url'))} {m.get('prev_rank')}위 → {m.get('rank')}위 (↑{m.get('change')})")
    else:
        lines.append("- (해당 없음)")

    # 🆕 뉴랭커 (최대 5개, 링크)
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t.get('name'), t.get('url'))} NEW → {t.get('rank')}위")
    else:
        lines.append("- (해당 없음)")

    # 📉 급하락 (일반 급하락 5개 + OUT 5개, 링크 / OUT은 변동폭 미표기)
    lines.append("\n*📉 급하락*")
    if downs:
        downs_sorted = sorted(
            downs,
            key=lambda m: (
                -abs(int(m.get("change") or 0)),
                int(m.get("rank") or 9999),
                int(m.get("prev_rank") or 9999),
            ),
        )
        for m in downs_sorted[:5]:
            drop = abs(int(m.get("change") or 0))
            lines.append(f"- {_link(m.get('name'), m.get('url'))} {m.get('prev_rank')}위 → {m.get('rank')}위 (↓{drop})")
    else:
        lines.append("- (급하락 없음)")

    if rank_outs:
        outs_sorted = sorted(rank_outs, key=lambda x: int(x.get("rank") or 9999))
        for ro in outs_sorted[:5]:
            prev_r = int(ro.get("rank") or 0)
            lines.append(f"- {_link(ro.get('name'), ro.get('url'))} {prev_r}위 → OUT")
    else:
        lines.append("- (OUT 없음)")

    # ↔ 랭크 인&아웃 (Top200 교체 수 = NEW 수 = OUT 수)
    new_cnt = len(chart_ins or [])
    out_cnt = len(rank_outs or [])
    if prev_items is not None:
        today_keys = {_key(it) for it in (rows or [])[:200] if _key(it)}
        prev_keys = {_key(p) for p in (prev_items or []) if _key(p) and 1 <= int(p.get("rank") or 0) <= 200}
        io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2
    else:
        io_cnt = min(new_cnt, out_cnt)

    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"{io_cnt}개의 제품이 인&아웃 되었습니다.")

    try:
        requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=10).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ====== main (수정) ======
def main():
    print("수집 시작:", RANK_URL)
    t0 = time.time()
    rows = fetch_products()
    print(f"[수집 완료] 개수: {len(rows)}")

    if len(rows) < 10:
        raise RuntimeError("유효 상품 카드가 너무 적게 수집되었습니다.")

    # CSV 로컬 저장
    csv_path, csv_filename = save_csv(rows)
    print("로컬 저장:", csv_path)

    # 구글 드라이브 연동
    drive_service = build_drive_service()
    prev_items: List[Dict] = []   # ← 추가: 기본값
    if drive_service:
        # 오늘 데이터 업로드
        upload_to_drive(drive_service, csv_path, csv_filename)

        # 어제 데이터 다운로드 및 분석
        yday_filename = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
        prev_file = find_file_in_drive(drive_service, yday_filename)
        if prev_file:
            print(f"[Drive] 전일 파일 발견: {prev_file['name']} (ID: {prev_file['id']})")
            csv_content = download_from_drive(drive_service, prev_file['id'])
            if csv_content:
                prev_items = parse_prev_csv(csv_content)   # ← 전일 리스트 확보
                print(f"[분석] 전일 데이터 {len(prev_items)}건 로드 완료")
        else:
            print(f"[Drive] 전일 파일({yday_filename})을 찾을 수 없습니다.")
        
        analysis_results = analyze_trends(rows, prev_items)
    else:
        # 드라이브 연동 실패 시 빈 분석 결과로 전달
        analysis_results = ([], [], [], [], 0)

    # 슬랙 알림 (prev_items 전달)
    post_slack(rows, analysis_results, prev_items)

    print(f"총 {len(rows)}건, 경과 시간: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()

