# app.py  — DaisoMall 랭킹(뷰티/위생 · 일간) 수집기
# - Playwright로 '뷰티/위생' 카테고리와 '일간' 탭을 강제 선택
# - 스크롤로 최대 200위까지 로드(부하시 자동 조기종료)
# - 상품명 앞 'BEST' 제거, 빈 카드/광고 제거, 중복 URL 제거
# - 결과 CSV 저장 + (선택) 구글 드라이브 업로드 + 슬랙 알림
# --------------------------------------------------------------

import os, re, csv, time, json, math
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ========= 설정 =========
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))  # 100~200 권장
SCROLL_PAUSE = 0.6
SCROLL_STABLE_ROUNDS = 3        # 새 카드가 더 이상 안 늘면 멈춤
SCROLL_MAX_ROUNDS = 60

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
# 구글 드라이브(선택)
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))

# ========= 공통 유틸 =========
def today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")

def yday_str() -> str:
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def num(s: str) -> Optional[int]:
    s = re.sub(r"[^\d]", "", s or "")
    return int(s) if s else None

def clean_name(name: str) -> str:
    if not name: return ""
    # 'BEST' 접두/중간 표기 제거
    name = re.sub(r"^\s*BEST\s*[|:\-\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    # 다중 공백 정리
    name = re.sub(r"\s+", " ", name).strip()
    return name

# ========= Playwright 수집 =========
def fetch_html() -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36")
        )
        page = ctx.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=45_000)

        # 카테고리 버튼: value="CTGR_00014" (뷰티/위생)
        try:
            page.wait_for_selector(".prod-category .cate-btn", timeout=15_000)
        except PWTimeout:
            pass

        # 뷰티/위생 버튼 확보 (value 우선, 없으면 텍스트로)
        cate = page.locator('.prod-category .cate-btn[value="CTGR_00014"]')
        if cate.count() == 0:
            cate = page.get_by_role("button", name=re.compile("뷰티/?위생"))

        try:
            cate.first.scroll_into_view_if_needed()
            cate.first.click()
        except Exception:
            # 가끔 스와이퍼 뷰 밖이면 강제 클릭
            page.evaluate("""
                (sel) => {
                  const el = document.querySelector(sel);
                  if (el) el.click();
                }
            """, '.prod-category .cate-btn[value="CTGR_00014"]')

        page.wait_for_load_state("networkidle")

        # 정렬 탭: '일간' (라디오 value="2")
        # 버튼 레이블이거나 라디오 모두 대응
        daily = page.locator('.ipt-sorting input[value="2"]')
        if daily.count() == 0:
            daily = page.get_by_role("button", name=re.compile("일간"))
        try:
            daily.first.scroll_into_view_if_needed()
            daily.first.click()
        except Exception:
            page.evaluate("""
                () => {
                  const byVal = document.querySelector('.ipt-sorting input[value="2"]');
                  if (byVal) { byVal.click(); return; }
                  const btns = [...document.querySelectorAll('.ipt-sorting *')];
                  const t = btns.find(b => /일간/.test(b.textContent||""));
                  if (t) t.click();
                }
            """)
        page.wait_for_load_state("networkidle")

        # 첫 카드가 보일 때까지 대기
        page.wait_for_selector(".goods-list .goods-unit", timeout=20_000)

        # 무한 스크롤
        prev = 0
        stable = 0
        for _ in range(SCROLL_MAX_ROUNDS):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_load_state("networkidle")
            time.sleep(SCROLL_PAUSE)
            cnt = page.locator(".goods-list .goods-unit").count()
            if cnt >= MAX_ITEMS:
                break
            if cnt == prev:
                stable += 1
                if stable >= SCROLL_STABLE_ROUNDS:
                    break
            else:
                stable = 0
                prev = cnt

        html = page.content()
        ctx.close()
        browser.close()
        return html

# ========= 파서 =========
def parse(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    units = soup.select(".goods-list .goods-unit")
    items: List[Dict] = []
    seen_urls = set()

    for u in units:
        # 상품명
        name_el = u.select_one(".goods-detail .tit a") or u.select_one(".goods-detail .tit")
        if not name_el: 
            continue
        name = clean_name(name_el.get_text(strip=True))
        if not name:
            continue

        # 가격
        price_el = u.select_one(".goods-detail .goods-price .value")
        price = num(price_el.get_text()) if price_el else None
        if not price:
            # 가격 없는 카드(광고/품절)는 제외
            continue

        # URL
        link_el = u.select_one(".goods-detail .tit a") or u.select_one(".goods-detail .goods-link")
        url = None
        if link_el and link_el.has_attr("href"):
            href = link_el["href"]
            url = urljoin(RANK_URL, href)
        if not url or url in seen_urls:
            # URL 없거나 중복(같은 상품 페이지) 제외
            continue

        seen_urls.add(url)
        items.append({
            "name": name,
            "price": price,
            "url": url,
        })

        if len(items) >= MAX_ITEMS:
            break

    # 순위 재부여(빈카드/광고 제거로 생긴 구멍 방지)
    for i, it in enumerate(items, start=1):
        it["rank"] = i
    return items

# ========= CSV =========
def save_csv(rows: List[Dict]) -> str:
    date_str = today_str()
    fname = f"다이소몰_뷰티위생_일간_{date_str}.csv"
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", fname)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "rank", "name", "price", "url"])
        for r in rows:
            w.writerow([date_str, r["rank"], r["name"], r["price"], r["url"]])
    return path

# ========= 변화 감지(급상승/뉴랭커/급하락/인&아웃) =========
def load_prev() -> Dict[str, int]:
    """어제 CSV를 열어 url->rank 매핑(없으면 빈값)."""
    y = yday_str()
    p = os.path.join("data", f"다이소몰_뷰티위생_일간_{y}.csv")
    if not os.path.exists(p):
        return {}
    ranks = {}
    with open(p, newline="", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f)):
            ranks[row["url"]] = int(row["rank"])
    return ranks

def diff_sections(cur: List[Dict], prev_map: Dict[str, int]):
    ups, downs, new, out = [], [], [], []
    cur_urls = [x["url"] for x in cur]
    for it in cur:
        url, r = it["url"], it["rank"]
        pr = prev_map.get(url)
        if pr is None:
            if r <= 20:  # 상위권 신규만 노출
                new.append((r, it["name"]))
        else:
            delta = pr - r
            if delta >= 20:
                ups.append((r, it["name"], f"{pr}→{r}"))
            elif delta <= -20:
                downs.append((r, it["name"], f"{pr}→{r}"))
    for url, pr in prev_map.items():
        if url not in cur_urls and pr <= 20:
            out.append(pr)
    return ups[:5], new[:5], downs[:5], out

# ========= Slack =========
def post_slack(cur: List[Dict]):
    if not SLACK_WEBHOOK:
        return
    top10 = cur[:10]
    prev_map = load_prev()
    ups, new, downs, out = diff_sections(cur, prev_map)

    lines = []
    lines.append(f"*다이소몰 뷰티/위생 일간 랭킹 — {today_str()}*")
    lines.append("")
    lines.append("*TOP 10*")
    for it in top10:
        lines.append(f"{it['rank']}. <{it['url']}|{it['name']}> — {it['price']:,}원")
    lines.append("")
    # 급상승
    lines.append("🔥 *급상승*")
    if ups:
        for r, name, movement in ups:
            lines.append(f"- {name}  ({movement})")
    else:
        lines.append("- 해당 없음")
    # 뉴랭커
    lines.append("\n🆕 *뉴랭커*")
    if new:
        for r, name in new:
            lines.append(f"- {name}  NEW → {r}위")
    else:
        lines.append("- 해당 없음")
    # 급하락
    lines.append("\n📉 *급하락*")
    if downs:
        for r, name, movement in downs:
            lines.append(f"- {name}  ({movement})")
    else:
        lines.append("- 해당 없음")
    # 링크 인&아웃
    lines.append("\n🔗 *링크 인&아웃*")
    if out:
        lines.append(f"{len(out)}개의 제품이 이탈/유입이 있었습니다.")
    else:
        lines.append("0개의 제품이 인&아웃 되었습니다.")

    msg = {"text": "\n".join(lines)}
    try:
        requests.post(SLACK_WEBHOOK, json=msg, timeout=10).raise_for_status()
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ========= Google Drive(선택) =========
def upload_to_drive(path: str) -> Optional[str]:
    if not (GDRIVE_FOLDER_ID and GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        return None
    try:
        # 토큰
        token_res = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "refresh_token": GOOGLE_REFRESH_TOKEN,
                "grant_type": "refresh_token",
            },
            timeout=15,
        )
        token_res.raise_for_status()
        access_token = token_res.json()["access_token"]

        meta = {"name": os.path.basename(path), "parents": [GDRIVE_FOLDER_ID]}
        files = {
            "metadata": ("metadata", json.dumps(meta), "application/json"),
            "file": (os.path.basename(path), open(path, "rb"), "text/csv"),
        }
        up = requests.post(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
            headers={"Authorization": f"Bearer {access_token}"},
            files=files,
            timeout=60,
        )
        up.raise_for_status()
        file_id = up.json().get("id")
        return file_id
    except Exception as e:
        print("[Drive] 업로드 실패:", e)
        return None

# ========= 메인 =========
def main():
    print("수집 시작:", RANK_URL)
    t0 = time.time()
    html = fetch_html()
    cards = parse(html)
    print("[수집 완료] 개수:", len(cards))

    if len(cards) < 20:
        raise RuntimeError("유효 상품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    csv_path = save_csv(cards)
    file_id = upload_to_drive(csv_path)
    post_slack(cards)

    print("로컬 저장:", csv_path)
    if file_id:
        print("Drive 업로드 완료:", file_id)
    print("총", len(cards), "건, 경과:", f"{time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
