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
DS_NEWCOMERS_TOP = 30

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


def post_slack(rows: List[Dict], analysis_results, prev_items: List[Dict]):
    if not SLACK_WEBHOOK:
        return

    # 분석 결과(기존 analyze_trends 출력 그대로 사용)
    ups, downs, chart_ins, rank_outs, in_out_count = analysis_results

    # 전일 rank 맵 (url 우선, 없으면 name), 비교 범위는 Top200
    TOTAL_RANGE = 200
    prev_rank_map: Dict[str, int] = {}
    for p in (prev_items or []):
        key = (p.get("url") or "").strip() or (p.get("name") or "").strip()
        if not key:
            continue
        try:
            r = int(p.get("rank") or 0)
            if 1 <= r <= TOTAL_RANGE:
                prev_rank_map[key] = r
        except Exception:
            pass

    def _key(it: dict) -> str:
        return (it.get("url") or "").strip() or (it.get("name") or "").strip()

    def _fmt_price(v) -> str:
        try:
            return f"{int(v):,}원"
        except Exception:
            return str(v or "")

    def _link(name: str, url: str | None) -> str:
        return f"<{url}|{name}>" if url else (name or "")

    # 메시지 타이틀
    now_kst = datetime.now(KST)
    title = f"*다이소몰 뷰티/위생 일간 랭킹 200* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})"
    lines = [title]

    # TOP10 (전일 대비 배지)
    lines.append("\n*TOP 10*")
    for it in (rows or [])[:10]:
        cur = int(it.get("rank") or 0)
        k   = _key(it)
        prev= prev_rank_map.get(k)
        if prev is None:
            badge = "(new)"
        elif prev > cur:
            badge = f"(↑{prev - cur})"
        elif prev < cur:
            badge = f"(↓{cur - prev})"
        else:
            badge = "(-)"
        lines.append(f"{cur}. {badge} {_link(it.get('name') or '', it.get('url'))} — {_fmt_price(it.get('price'))}")

    # 🔥 급상승 (±10 이상, 5개)
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m['name'], m.get('url'))} {m['prev_rank']}위 → {m['rank']}위 (↑{m['change']})")
    else:
        lines.append("- (해당 없음)")

    # 🆕 뉴랭커 (상위 진입 5개 노출)
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t['name'], t.get('url'))} NEW → {t['rank']}위")
    else:
        lines.append("- (해당 없음)")

    # 📉 급하락 (±10 이상 5개, 부족분은 OUT로 보강)
    lines.append("\n*📉 급하락*")
    shown = 0
    if downs:
        for m in downs[:5]:
            drop = abs(int(m.get("change") or 0))
            lines.append(f"- {_link(m['name'], m.get('url'))} {m['prev_rank']}위 → {m['rank']}위 (↓{drop})")
            shown += 1
    if shown < 5 and rank_outs:
        for ro in rank_outs[: 5 - shown]:
            lines.append(f"- {ro['name']} {int(ro['rank'])}위 → OUT")
            shown += 1
    if shown == 0:
        lines.append("- (해당 없음)")

    # ↔ 인&아웃 카운트
    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"{in_out_count}개의 제품이 인&아웃 되었습니다.")

    try:
        requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=10).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)
