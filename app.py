import os, sys, time, json, re, csv, traceback, asyncio, datetime
from urllib.parse import urljoin
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ===== 슬랙 =====
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

def slack_post(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[Slack] 웹훅 미설정. 메시지 생략")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
    except Exception as e:
        print("[Slack] 실패:", e)

# ===== 구글 드라이브 업로드 =====
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

def upload_to_gdrive(filepath: str) -> str:
    """
    Refresh Token 방식으로 Drive 업로드.
    성공 시 파일 ID 반환. 실패 시 빈 문자열.
    """
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        creds = Credentials(
            None,
            refresh_token=GOOGLE_REFRESH_TOKEN,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=GOOGLE_CLIENT_ID,
            client_secret=GOOGLE_CLIENT_SECRET,
            scopes=["https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=creds)
        fname = os.path.basename(filepath)
        file_metadata = {"name": fname}
        if GDRIVE_FOLDER_ID:
            file_metadata["parents"] = [GDRIVE_FOLDER_ID]

        media = MediaFileUpload(filepath, mimetype="text/csv", resumable=True)
        f = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        return f.get("id", "")
    except Exception as e:
        print("[Drive] 업로드 실패:", e)
        return ""

# ===== 유틸 =====
BASE_URL = "https://www.daisomall.co.kr"
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"  # 뷰티/위생 카테고리 랭킹 진입점

TODAY = datetime.datetime.now().strftime("%Y-%m-%d")
DATA_DIR = Path("data")
DEBUG_DIR = Path("data/debug")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

CSV_NAME = f"다이소몰_뷰티위생_일간_{TODAY}.csv"
CSV_PATH = DATA_DIR / CSV_NAME

# 수집 최소 개수(안전장치 – 너무 적으면 알림)
MIN_ITEMS = 20

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
    steps = 0
    while steps < max_steps:
        await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(pause_ms)
        new_height = await page.evaluate("() => document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        steps += 1

def parse_cards_from_html(html: str) -> list:
    """
    HTML에서 카드 파싱 (백업 파서: requests/html 저장본에도 동작)
    """
    soup = BeautifulSoup(html, "html.parser")

    # 카드 셀렉터: 배너 제외
    cards = soup.select(".goods-list.type-card .goods-unit:not(.banner), .goods-list.type-card.div-5 .goods-unit:not(.banner)")
    items = []
    for i, card in enumerate(cards, start=1):
        # 제목
        title = card.select_one(".goods-detail .tit")
        title_txt = title.get_text(strip=True) if title else ""

        # 가격
        price_val = card.select_one(".goods-detail .goods-price .value")
        price_txt = price_val.get_text(strip=True) if price_val else ""
        price_txt = price_txt.replace(",", "").replace("원", "")
        try:
            price = int(re.sub(r"[^0-9]", "", price_txt)) if price_txt else None
        except Exception:
            price = None

        # 링크
        link_a = card.select_one(".goods-thumb a, .goods-detail .tit a")
        href = link_a.get("href") if link_a and link_a.has_attr("href") else ""
        url = urljoin(BASE_URL, href)

        # 이미지(선택)
        img = card.select_one(".goods-thumb img")
        img_url = img.get("src") if img and img.has_attr("src") else ""

        # 순위(있으면 사용, 없으면 enumerate)
        r = card.select_one(".ranking-area .num")
        if r:
            try:
                rank = int(re.sub(r"[^0-9]", "", r.get_text(strip=True)))
            except Exception:
                rank = i
        else:
            rank = i

        if title_txt:
            items.append({
                "rank": rank,
                "title": title_txt,
                "price": price,
                "url": url,
                "img": img_url
            })
    # 순위 정렬 보정
    items.sort(key=lambda x: x["rank"])
    return items

async def fetch_with_playwright() -> list:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ])
        ctx = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
            locale="ko-KR",
            viewport={"width": 1440, "height": 900},
            java_script_enabled=True,
        )
        page = await ctx.new_page()

        try:
            print("수집 시작:", RANK_URL)
            await page.route("**/*", lambda route: route.continue_())
            await page.goto(RANK_URL, wait_until="networkidle", timeout=30000)

            # 혹시 로그인 리다이렉트 걸리면 한번 더 진입
            if "login" in (page.url or "").lower():
                print("[경고] 로그인 페이지로 유도됨 — 재진입 시도")
                await page.goto(RANK_URL, wait_until="networkidle", timeout=30000)

            # 상단 카테고리: '뷰티/위생' 클릭
            # (링크/버튼 두 경우 모두 대비)
            clicked = (await click_if_exists(page, "role=link[name='뷰티/위생']")) \
                      or (await click_if_exists(page, "뷰티/위생", is_text=True)) \
                      or (await click_if_exists(page, "a:has-text('뷰티/위생')"))

            if clicked:
                await page.wait_for_timeout(600)
            
            # 우측 정렬 탭: '일간' 클릭
            daily_clicked = (await click_if_exists(page, "role=button[name='일간']")) \
                            or (await click_if_exists(page, "일간", is_text=True)) \
                            or (await click_if_exists(page, "button:has-text('일간')"))
            if daily_clicked:
                await page.wait_for_timeout(800)

            # 스크롤 다운으로 lazy load 유도
            await scroll_to_bottom(page, pause_ms=300, max_steps=40)

            # 카드가 보일 때까지 대기
            cards_selector = ".goods-list.type-card .goods-unit:not(.banner)"
            try:
                await page.wait_for_selector(cards_selector, timeout=12000)
            except PlaywrightTimeout:
                pass  # 아래에서 실제 개수 확인

            # 디버그 저장
            png_path = DEBUG_DIR / "page_rank.png"
            html_path = DEBUG_DIR / "page_rank.html"
            await page.screenshot(path=str(png_path), full_page=True)
            html = await page.content()
            html_path.write_text(html, encoding="utf-8")
            print("[debug] saved", png_path, ",", html_path)

            items = parse_cards_from_html(html)
            return items
        finally:
            await ctx.close()
            await browser.close()

def fetch_with_http_fallback() -> list:
    """
    JS 비의존 백업(필요 시 사용). 다이소몰은 SPA 성격이라 일반 HTML은 빈 페이지일 수 있음.
    """
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
        # 디버그 저장
        (DEBUG_DIR / "page_rank_http.html").write_text(html, encoding="utf-8")
        return parse_cards_from_html(html)
    except Exception as e:
        print("[HTTP] Fallback 실패:", e)
        return []

def build_sections(df_today: pd.DataFrame) -> dict:
    total = len(df_today)
    top5 = df_today.head(5).copy()
    top5["표시가"] = top5["price"].fillna("").astype(str)
    s = {
        "총개수": total,
        "TOP5": top5[["rank","title","표시가","url"]].to_dict(orient="records"),
    }
    return s

def save_csv(items: list, path: Path):
    df = pd.DataFrame(items, columns=["rank","title","price","url","img"])
    df.sort_values("rank", inplace=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return df

def main():
    start = time.time()
    slack_post(f":daiso: 다이소몰 랭킹 수집 시작\nURL: {RANK_URL}")

    items = []
    try:
        print("[Playwright 풀백 진입]")
        items = asyncio.run(fetch_with_playwright())
        print(f"[Playwright] 수집: {len(items)}")
    except Exception as e:
        print("[Playwright] 예외:", e)

    if len(items) < MIN_ITEMS:
        print("[경고] 제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")
        print("[HTTP] 수집 시도")
        http_items = fetch_with_http_fallback()
        print(f"[HTTP] 수집: {len(http_items)}")
        if len(http_items) > len(items):
            items = http_items

    if not items:
        slack_post(":warning: 다이소몰 랭킹 수집 실패 — 카드 0건. `data/debug` 산출물 확인 필요")
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    df_today = save_csv(items, CSV_PATH)
    secs = build_sections(df_today)

    # 드라이브 업로드
    file_id = upload_to_gdrive(str(CSV_PATH)) if GDRIVE_FOLDER_ID and GOOGLE_REFRESH_TOKEN else ""
    end = time.time()

    msg = [
        f":white_check_mark: 다이소몰 뷰티/위생 일간 수집 완료",
        f"- 총 {secs['총개수']}건",
        f"- CSV: `{CSV_NAME}`",
    ]
    if file_id:
        msg.append(f"- Drive 파일 ID: `{file_id}`")
    msg.append(f"- 경과: {end - start:.1f}s")

    # TOP5 요약
    if secs["TOP5"]:
        lines = ["\n*TOP 5*"]
        for r in secs["TOP5"]:
            price = f"{int(r['표시가']):,}원" if r["표시가"].isdigit() else r["표시가"]
            lines.append(f"{r['rank']}. {r['title']} — {price}")
        msg.extend(lines)

    slack_post("\n".join(msg))
    print("\n".join(msg))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Traceback (most recent call last):")
        traceback.print_exc()
        sys.exit(1)
