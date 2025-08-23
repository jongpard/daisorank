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
TOP_WINDOW = 30  # 뉴랭커, 랭크아웃 등을 판단하는 기준 순위

# ---- Slack 분석/표시 규칙(다이소 전용) ----
DS_TOTAL_RANGE = 200             # 비교 범위: Top200
DS_OUT_LIMIT = 150               # OUT 기준: 전일 150위 이내만
DS_RISING_FALLING_THRESHOLD = 10 # 급상승/급하락 최소 변동 계단
DS_TOP_MOVERS_MAX = 5            # 급상승/급하락 최대 노출 개수
DS_NEWCOMERS_TOP = 30            # 뉴랭커 진입 한계(Top30)

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

def ensure_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def parse_price_kr(s: str) -> Optional[int]:
    if not s: return None
    m = re.search(r"(\d[\d,]*)", s)
    if not m: return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None

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
        loc.click(timeout=3000)
        page.wait_for_timeout(200)
        return True
    except Exception:
        try:
            page.evaluate("""
                (sel) => {
                    const el = (typeof sel === 'string') ? document.querySelector(sel) : null;
                    if (el) { el.click(); return true; }
                    return false;
                }
            """, target if isinstance(target, str) else None)
            page.wait_for_timeout(200)
            return True
        except Exception:
            return False


# ====== 크롤링 ======
def fetch_products() -> List[Dict]:
    out: List[Dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            locale="ko-KR"
        )
        page = context.new_page()
        page.set_default_timeout(30000)

        # 진입
        page.goto(RANK_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        close_overlays(page)

        # 카테고리 선택: 뷰티/위생
        try:
            if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').first.is_visible():
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
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(int(SCROLL_PAUSE * 1000))
                cur = page.locator(".goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2").count()
                if cur >= MAX_ITEMS:  # 충분히 로드되면 종료
                    break
                if cur == prev:
                    stable += 1
                else:
                    stable = 0
                prev = cur
                if stable >= SCROLL_STABLE_ROUNDS:
                    break

        infinite_scroll(page)

        def collect_items(page: Page) -> List[Dict]:
            data = page.evaluate(
                """
        () => {
          const qs = sel => Array.from(document.querySelectorAll(sel));
          const units = qs('.goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2, .rank-list .goods-item');
          const seen = new Set();
          const items = [];
          for (const el of units) {
            const nameEl = el.querySelector('.goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .name, .goods-name, .goods-tit, .title');
            let name = (nameEl && nameEl.textContent || '').trim();
            if (!name) continue;
            const priceEl = el.querySelector('.goods-detail .sale-price .num, .sale-price .num, .sale .price, .price .num, .goods-price .num, .final-price .num, .goods-detail .price .num');
            let priceTxt = (priceEl && priceEl.textContent || '').replace(/[^0-9]/g, '');
            if (!priceTxt) continue;
            const price = parseInt(priceTxt, 10);
            if (!price || price <= 0) continue;
            let href = null;
            const a = el.querySelector('a[href*="/pd/pdr/"], a[href*="/goods/"], a[href*="/detail/"], a[href]');
            if (a && a.getAttribute('href')) {
              href = a.href || a.getAttribute('href');
            }
            if (!href) continue;
            if (href.startsWith('/')) href = location.origin + href;
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
                cleaned.append({
                    "name": nm,
                    "price": int(it.get("price") or 0),
                    "url": it.get("url"),
                })
            # 랭크 부여/정렬
            for i, r in enumerate(cleaned, start=1):
                r["rank"] = i
            return cleaned[:MAX_ITEMS]

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
        print("[Drive] OAuth 세션 생성 실패:", e)
        return None

def upload_to_drive(service, filepath: str, filename: str) -> Optional[str]:
    if not service or not GDRIVE_FOLDER_ID:
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


# ====== 전일 CSV 파싱 ======
def parse_prev_csv(csv_text: str) -> List[Dict]:
    items: List[Dict] = []
    try:
        rdr = csv.DictReader(io.StringIO(csv_text))
        for row in rdr:
            try:
                items.append({
                    "rank": int(row.get("rank") or 0),
                    "name": row.get("name"),
                    "url": row.get("url"),
                    "price": int(row.get("price") or 0) if row.get("price") else None,
                })
            except (ValueError, TypeError):
                continue
    except Exception as e:
        print("[CSV Parse] 전일 데이터 파싱 실패:", e)
    return items


# ====== 분석(다이소 전용 규칙 적용) ======
def analyze_trends(today_items: List[Dict], prev_items: List[Dict]):
    """다이소 전용 규칙:
    - 상승/하락: Top200 전체, 변동 절대값 >= 10
    - 뉴랭커: Top30 신규 진입
    - OUT: 전일 150위 이내였고 오늘 목록에 없음
    """
    # prev: url -> rank
    prev_map: Dict[str, int] = {}
    for p in (prev_items or []):
        u = p.get("url")
        try:
            r = int(p.get("rank") or 0)
        except Exception:
            continue
        if u and 1 <= r <= DS_TOTAL_RANGE:
            prev_map[u] = r

    # 오늘 목록
    trends: List[Dict] = []
    today_urls = set()
    for it in (today_items or []):
        u = it.get("url")
        if not u:
            continue
        try:
            cr = int(it.get("rank") or 0)
        except Exception:
            continue
        pr = prev_map.get(u)
        trends.append({
            "name": it.get("name"),
            "url": u,
            "rank": cr,
            "prev_rank": pr,
            "change": (pr - cr) if pr else None,  # +: 상승, -: 하락
        })
        today_urls.add(u)

    movers = [t for t in trends if t["prev_rank"] is not None]
    ups = [t for t in movers if (t["change"] or 0) >= DS_RISING_FALLING_THRESHOLD]
    downs = [t for t in movers if (t["change"] or 0) <= -DS_RISING_FALLING_THRESHOLD]

    ups.sort(key=lambda x: (-(x["change"] or 0), x.get("rank", 9999), x.get("prev_rank", 9999), x.get("name") or ""))
    downs.sort(key=lambda x: (abs(x["change"] or 0), x.get("rank", 9999), x.get("prev_rank", 9999), x.get("name") or ""))

    chart_ins = [t for t in trends if t["prev_rank"] is None and (t["rank"] or 9999) <= DS_NEWCOMERS_TOP]
    chart_ins.sort(key=lambda x: x.get("rank", 9999))

    # OUT: prev <= 150 and not in today
    prev_out = []
    for p in prev_items or []:
        u = p.get("url")
        if not u: 
            continue
        try:
            r = int(p.get("rank") or 0)
        except Exception:
            continue
        if r <= DS_OUT_LIMIT and u not in today_urls:
            prev_out.append({"name": p.get("name"), "url": u, "rank": r})
    prev_out.sort(key=lambda x: int(x.get("rank") or 9999))
    in_out_count = len(chart_ins) + len(prev_out)

    return ups, downs, chart_ins, prev_out, in_out_count


# ====== Slack (다이소 전용 포맷) ======
def post_slack(rows: List[Dict], analysis_results, prev_items: List[Dict]):
    if not SLACK_WEBHOOK:
        return

    ups, downs, chart_ins, rank_outs, in_out_count = analysis_results

    # prev map for TOP10 badge
    prev_rank_map: Dict[str, int] = {}
    for p in (prev_items or []):
        u = p.get("url")
        try:
            r = int(p.get("rank") or 0)
        except Exception:
            continue
        if u and 1 <= r <= DS_TOTAL_RANGE:
            prev_rank_map[u] = r

    now_kst = datetime.now(KST)
    title = f"*다이소몰 뷰티/위생 일간 랭킹 {DS_TOTAL_RANGE}* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})"
    lines = [title, "\n*TOP 10*"]

    # TOP10 with badge
    for it in (rows or [])[:10]:
        cur = int(it.get("rank") or 0)
        url = it.get("url")
        prev = prev_rank_map.get(url)
        if prev is None:
            badge = "(new)"
        elif prev > cur:
            badge = f"(↑{prev - cur})"
        elif prev < cur:
            badge = f"(↓{cur - prev})"
        else:
            badge = "(-)"
        price = it.get("price")
        try:
            price_txt = f"{int(price):,}원"
        except Exception:
            price_txt = str(price or "")
        name_link = f"<{url}|{it.get('name') or ''}>" if url else (it.get('name') or '')
        lines.append(f"{cur}. {badge} {name_link} — {price_txt}")

    # 🔥 급상승
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:DS_TOP_MOVERS_MAX]:
            lines.append(f"- <{m['url']}|{m['name']}> {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else:
        lines.append("- (해당 없음)")

    # 🆕 뉴랭커
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:3]:
            lines.append(f"- <{t['url']}|{t['name']}> NEW → {t['rank']}위")
    else:
        lines.append("- (해당 없음)")

    # 📉 급하락 (우선 급하락, 부족하면 OUT로 보강)
    lines.append("\n*📉 급하락*")
    shown = 0
    if downs:
        for m in downs[:DS_TOP_MOVERS_MAX]:
            lines.append(f"- <{m['url']}|{m['name']}> {m['prev_rank']}위 → {m['rank']}위 (↓{abs(m['change'])})")
            shown += 1
    if shown < DS_TOP_MOVERS_MAX and rank_outs:
        for ro in rank_outs[:DS_TOP_MOVERS_MAX - shown]:
            lines.append(f"- {ro['name']} {int(ro['rank'])}위 → OUT")

    # ↔ 인&아웃
    lines.append(f"\n*↔ 랭크 인&아웃*")
    lines.append(f"{in_out_count}개의 제품이 인&아웃 되었습니다.")

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
    if drive_service:
        # 오늘 데이터 업로드
        upload_to_drive(drive_service, csv_path, csv_filename)

        # 어제 데이터 다운로드 및 분석
        yday_filename = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
        prev_file = find_file_in_drive(drive_service, yday_filename)
        prev_items = []
        if prev_file:
            print(f"[Drive] 전일 파일 발견: {prev_file['name']} (ID: {prev_file['id']})")
            csv_content = download_from_drive(drive_service, prev_file['id'])
            if csv_content:
                prev_items = parse_prev_csv(csv_content)
                print(f"[분석] 전일 데이터 {len(prev_items)}건 로드 완료")
        else:
            print(f"[Drive] 전일 파일({yday_filename})을 찾을 수 없습니다.")
        
        analysis_results = analyze_trends(rows, prev_items)
    else:
        # 드라이브 연동 실패 시 빈 분석 결과로 전달
        prev_items = []
        analysis_results = ([], [], [], [], 0)

    # 슬랙 알림
    post_slack(rows, analysis_results, prev_items)

    print(f"총 {len(rows)}건, 경과 시간: {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
