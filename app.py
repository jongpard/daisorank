import asyncio
from playwright.async_api import async_playwright
import pandas as pd

URL = "https://www.daisomall.co.kr/ds/rank/C105"
OUTPUT_FILE = "daiso_beauty_daily_top200.csv"


async def scroll_until_200(page):
    last_height = 0

    while True:
        cards = await page.query_selector_all("div.prdItem")
        if len(cards) >= 200:
            break

        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1500)

        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


async def scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        print("[시작] 페이지 접속")
        await page.goto(URL)
        await page.wait_for_timeout(3000)

        # 혹시 탭 클릭이 필요할 경우 (일간 강제 클릭)
        try:
            await page.click("text=일간")
            await page.wait_for_timeout(2000)
        except:
            pass

        print("[스크롤] 200위까지 로딩 중...")
        await scroll_until_200(page)

        print("[수집] 데이터 추출 중...")
        items = await page.query_selector_all("div.prdItem")

        data = []

        for idx, item in enumerate(items[:200], start=1):
            try:
                name = await item.query_selector_eval(
                    ".prdName", "el => el.innerText.trim()"
                )
            except:
                name = ""

            try:
                price = await item.query_selector_eval(
                    ".price", "el => el.innerText.trim()"
                )
            except:
                price = ""

            try:
                link = await item.query_selector_eval(
                    "a", "el => el.href"
                )
            except:
                link = ""

            data.append({
                "rank": idx,
                "product_name": name,
                "price": price,
                "url": link
            })

        df = pd.DataFrame(data)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

        print(f"[완료] {len(df)}개 저장 완료 → {OUTPUT_FILE}")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(scrape())
