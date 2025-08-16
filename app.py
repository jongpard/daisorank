import os, re, time, json, traceback, asyncio, datetime
from urllib.parse import urljoin
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# -------------------- 설정 --------------------
BASE_URL = "https://www.daisomall.co.kr"
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"  # 뷰티/위생 랭킹
TODAY = datetime.datetime.now().strftime("%Y-%m-%d")

DATA_DIR = Path("data")
DEBUG_DIR = Path("data/debug")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

CSV_NAME = f"다이소몰_뷰티위생_일간_{TODAY}.csv"
CSV_PATH = DATA_DIR / CSV_NAME
MIN_ITEMS = 20  # 너무 적게 나오면 경고

# Slack & Drive env
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# -------------------- 공통 유틸 --------------------
def clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def clean_title(title: str) -> str:
    """
    카드 제목에서 맨 앞 'BEST' 라벨 제거 (+ 여분 공백 정리)
    예: 'BEST본샘 ...' / 'BEST CNP ...' / 'BEST 헬로...' 등 모두 제거
    """
    t = title or ""
    t = re.sub(r"^\s*BEST\b[\s-]*", "", t, flags=re.I)   # 앞쪽 BEST 라벨만 제거
    return clean_spaces(t)

def fmt_price(v) -> str:
    try:
        return f"{int(v):,}원"
    except Exception:
        return str(v)

# -------------------- Slack --------------------
def slack_post(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[Slack] 웹훅 미설정 — 메시지 생략")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
    except Exception as e:
        print("[Slack 실패]", e)

# -------------------- Google Drive --------------------
def normalize_folder_id(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{8,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{8,})", s)
    return m.group(1) if m else s

def build_drive_service():
    """
    invalid_scope 방지를 위해 refresh_token 사용 시 scopes 를 넘기지 않음.
    (리프레시 토큰이 가진 기존 scope로만 갱신)
    """
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        raise RuntimeError("Google Drive OAuth 비밀키/토큰이 없습니다.")

    creds = Credentials(
        None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        # scopes=None  # 중요: 지정하지 않음
    )
    service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return service

def upload_to_gdrive(filepath: str) -> str:
    from googleapiclient.http import MediaFileUpload
    try:
        service = build_drive_service()
        fname = os.path.basename(filepath)
        folder_id = normalize_folder_id(GDRIVE_FOLDER_ID)

        meta = {"name": fname}
        if folder_id:
            meta["parents"] = [folder_id]

        media = MediaFileUpload(filepath, mimetype="text/csv", resumable=False)
        created = service.files().create(
            body=meta, media_body=media, fields="id", supportsAllDrives=True
        ).execute()
        return created.get("id", "")
    except Exception as e:
        print("[Drive 업로드 실패]", repr(e))
        return ""

# -------------------- 파싱 --------------------
def parse_cards_from_html(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    # 배너 제외
    cards = soup.select(".goods-list.type-card .goods-unit:not(.banner), .goods-list.type-card.div-5 .goods-unit:not(.banner)")
    items = []
    for i, card in enumerate(cards, start=1):
        title_el = card.select_one(".goods-detail .tit")
        title = clean_title(title_el.get_text(strip=True) if title_el else "")

        price_val = card.select_one(".goods-detail .goods-price .value")
        price_txt = clean_spaces(price_val.get_text(strip=True) if price_val else "")
        num = re.sub(r"[^0-9]", "", price_txt)
        price = int(num) if num else None

        link_a = card.select_one(".goods-thumb a, .goods-detail .tit a")
        href = link_a.get("href") if link_a and link_a.has_attr("href") else ""
        url = urljoin(BASE_URL, href)

        rank_el = card.select_one(".ranking-area .num")
        if rank_el:
            try:
                rank = int(re.sub(r"[^0-9]", "", rank_el.get_text(strip=True)))
            except Exception:
                rank = i
        else:
            rank = i

        if title:
            items.append({"rank": rank, "title": title, "price": price, "url": url})
    items.sort(key=lambda x: x["rank"])
    return items

# -------------------- Playwright 수집 --------------------
async def click_if_exists(page, selector_or_text: str, *, is_text=False, timeout=4000):
    try:
        if is_text:
            await page.get_by_text(selector_or_text, exact=True).first.click(timeout=timeout)
        else:
            await page.locator(selector_or_text).first.click(timeout=timeout)
        return True
    except Exception:
        return False

async def scroll_to_bottom(page, pause_ms=250, max_steps=40):
    last_height = await page.evaluate("() => document.body.scrollHeight")
    for _ in range(max_steps):
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(pause_ms)
        new_height = await page.evaluate("() => document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

async def fetch_with_playwright() -> list:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
            locale="ko-KR",
            viewport={"width": 1440, "height": 900},
        )
        page = await ctx.new_page()
        try:
            await page.goto(RANK_URL, wait_until="networkidle", timeout=30000)

            # 혹시 로그인 유도되면 재진입
            if "login" in (page.url or "").lower():
                await page.goto(RANK_URL, wait_until="networkidle", timeout=30000)

            # 카테고리: 뷰티/위생
            clicked = (await click_if_exists(page, "role=link[name='뷰티/위생']")) \
                   or (await click_if_exists(page, "a:has-text('뷰티/위생')")) \
                   or (await click_if_exists(page, "뷰티/위생", is_text=True))
            if clicked:
                await page.wait_for_timeout(600)

            # 정렬: 일간
            daily = (await click_if_exists(page, "role=button[name='일간']")) \
                 or (await click_if_exists(page, "button:has-text('일간')")) \
                 or (await click_if_exists(page, "일간", is_text=True))
            if daily:
                await page.wait_for_timeout(800)

            # lazy-load
            await scroll_to_bottom(page, pause_ms=300, max_steps=40)

            # 디버그 저장
            png_path = DEBUG_DIR / "page_rank.png"
            html_path = DEBUG_DIR / "page_rank.html"
            await page.screenshot(path=str(png_path), full_page=True)
            html = await page.content()
            html_path.write_text(html, encoding="utf-8")
            print("[debug] saved", png_path, ",", html_path)

            return parse_cards_from_html(html)
        finally:
            await ctx.close()
            await browser.close()

def fetch_with_http_fallback() -> list:
    try:
        headers = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
            "Accept-Language": "ko-KR,ko;q=0.9",
            "Referer": "https://www.daisomall.co.kr/",
        }
        r = requests.get(RANK_URL, headers=headers, timeout=15)
        r.raise_for_status()
        html = r.text
        (DEBUG_DIR / "page_rank_http.html").write_text(html, encoding="utf-8")
        return parse_cards_from_html(html)
    except Exception as e:
        print("[HTTP Fallback 실패]", e)
        return []

# -------------------- CSV/Slack --------------------
def save_csv(items: list, path: Path):
    df = pd.DataFrame(items, columns=["rank", "title", "price", "url"])
    df.sort_values("rank", inplace=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df

def build_slack_top10(df: pd.DataFrame) -> str:
    lines = ["*TOP 10*"]
    for _, r in df.head(10).iterrows():
        lines.append(f"{int(r['rank'])}. {r['title']} — {fmt_price(r['price'])}")
    return "\n".join(lines)

# -------------------- main --------------------
def main():
    t0 = time.time()
    slack_post(f":daiso: 다이소몰 랭킹 수집 시작\nURL: {RANK_URL}")

    items = []
    try:
        items = asyncio.run(fetch_with_playwright())
        print(f"[Playwright] 수집: {len(items)}")
    except Exception as e:
        print("[Playwright 예외]", e)

    if len(items) < MIN_ITEMS:
        http_items = fetch_with_http_fallback()
        if len(http_items) > len(items):
            items = http_items

    if not items:
        slack_post(":warning: 다이소몰 랭킹 수집 실패 — 카드 0건. `data/debug` 확인")
        raise RuntimeError("제품 카드가 너무 적음")

    df = save_csv(items, CSV_PATH)

    # Drive 업로드
    drive_id = ""
    if GDRIVE_FOLDER_ID and GOOGLE_REFRESH_TOKEN:
        drive_id = upload_to_gdrive(str(CSV_PATH))

    # Slack 메시지 (예전 포맷)
    head = f"*다이소몰 뷰티/위생 일간 — {TODAY}*"
    top10 = build_slack_top10(df)
    meta = [
        f"- 총 {len(df)}건",
        f"- CSV: `{CSV_NAME}`",
        f"- 경과: {time.time()-t0:.1f}s"
    ]
    if drive_id:
        meta.append(f"- Drive 파일 ID: `{drive_id}`")

    slack_post("\n".join([head, "", *meta, "", top10]))
    print("\n".join([head, "", *meta, "", top10]))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        traceback.print_exc()
        raise
