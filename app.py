# app.py — DaisoMall 뷰티/위생 일간 랭킹 수집기 (강화판)
# - 카테고리/정렬 강제 고정 + 확인
# - 무한 스크롤 안정화
# - 브라우저 컨텍스트에서 직접 추출 (여러 셀렉터 동시 대응)
# - BEST 제거, 광고/빈카드/중복 제거
# - CSV 저장, (선택) 구글 드라이브 업로드, 슬랙 알림(올영 포맷)

import os, re, csv, time, json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ====== 설정 ======
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))  # 100~200 권장
SCROLL_PAUSE = 0.6
SCROLL_STABLE_ROUNDS = 4
SCROLL_MAX_ROUNDS = 80

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
# Google Drive (선택)
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
    # 'BEST' 접두/중간 제거
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()


# ====== Playwright (카테고리/정렬 고정 + 스크롤 + 추출) ======
def select_beauty_daily(page):
    # 1) '뷰티/위생' 카테고리 강제 선택
    ok = False

    # 1-1) value 기반
    cate_by_val = page.locator('.prod-category .cate-btn[value="CTGR_00014"]')
    if cate_by_val.count() > 0:
        cate_by_val.first.scroll_into_view_if_needed()
        cate_by_val.first.click()
        ok = True
        page.wait_for_load_state("networkidle")

    # 1-2) 텍스트 기반 (뷰티/위생)
    if not ok:
        try:
            page.get_by_role("button", name=re.compile("뷰티\\/?위생")).first.click()
            ok = True
            page.wait_for_load_state("networkidle")
        except Exception:
            pass

    # 1-3) JS 강제 클릭 (마지막 수단)
    if not ok:
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

    # 실제 선택됐는지 확인(활성화/값)
    page.wait_for_timeout(300)
    selected_val = page.evaluate("""
        () => {
          const act = document.querySelector('.prod-category .cate-btn.is-active')
                     || document.querySelector('.prod-category .cate-btn.active');
          return act ? (act.value || act.getAttribute('value') || act.dataset?.value || "") : "";
        }
    """)
    # 그래도 값이 비면, 현재 탭 텍스트로 보정
    if not selected_val:
        selected_txt = page.evaluate("""
          () => {
            const act = document.querySelector('.prod-category .cate-btn.is-active, .prod-category .cate-btn.active');
            return (act?.textContent || "").trim();
          }
        """)
        if selected_txt and "뷰티" not in selected_txt and "위생" not in selected_txt:
            raise RuntimeError("뷰티/위생 카테고리 선택 실패")

    # 2) '일간' 정렬 강제 선택
    chosen = False
    daily_radio = page.locator('.ipt-sorting input[value="2"]')
    if daily_radio.count() > 0:
        daily_radio.first.scroll_into_view_if_needed()
        daily_radio.first.click()
        chosen = True
        page.wait_for_load_state("networkidle")

    if not chosen:
        try:
            page.get_by_role("button", name=re.compile("일간")).first.click()
            chosen = True
            page.wait_for_load_state("networkidle")
        except Exception:
            pass

    if not chosen:
        page.evaluate("""
           () => {
             const r = document.querySelector('.ipt-sorting input[value="2"]');
             if (r) r.click();
             const btns = [...document.querySelectorAll('.ipt-sorting *')];
             const t = btns.find(b => /일간/.test((b.textContent||"")));
             if (t) t.click();
           }
        """)
        page.wait_for_load_state("networkidle")

    page.wait_for_timeout(400)


def infinite_scroll(page):
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


def collect_items(page) -> List[Dict]:
    # 브라우저 컨텍스트에서 직접 추출 (다양한 DOM 변형 대응)
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
            const a = el.querySelector('a[href*="/goods/"], a[href*="/product/"], a.goods-link, .goods-detail a');
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
    # BEST 제거, 정리
    cleaned = []
    for it in data:
        nm = strip_best(it["name"])
        if not nm:
            continue
        cleaned.append({"name": nm, "price": it["price"], "url": it["url"]})

    # 순위 재부여
    for i, it in enumerate(cleaned, 1):
        it["rank"] = i
    return cleaned


def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36")
        )
        page = context.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=45_000)

        # 로딩 안정화
        try:
            page.wait_for_selector(".prod-category", timeout=15_000)
        except PWTimeout:
            pass

        # 카테고리/정렬 고정
        select_beauty_daily(page)

        # 첫 카드 노출 대기
        try:
            page.wait_for_selector(".goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2", timeout=20_000)
        except PWTimeout:
            pass

        # 무한 스크롤
        infinite_scroll(page)

        items = collect_items(page)

        context.close()
        browser.close()
        return items


# ====== CSV 저장 ======
def save_csv(rows: List[Dict]) -> str:
    date_str = today_str()
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", f"다이소몰_뷰티위생_일간_{date_str}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "rank", "name", "price", "url"])
        for r in rows:
            w.writerow([date_str, r["rank"], r["name"], r["price"], r["url"]])
    return path


# ====== 변화 감지 ======
def load_prev_map() -> Dict[str, int]:
    prev = {}
    fn = os.path.join("data", f"다이소몰_뷰티위생_일간_{yday_str()}.csv")
    if not os.path.exists(fn):
        return prev
    with open(fn, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                prev[row["url"]] = int(row["rank"])
            except Exception:
                pass
    return prev


def build_diff(cur: List[Dict], prev_map: Dict[str, int]):
    ups, downs, newin, out = [], [], [], []
    cur_urls = [x["url"] for x in cur]
    for it in cur:
        u, r = it["url"], it["rank"]
        pr = prev_map.get(u)
        if pr is None:
            if r <= 20:
                newin.append((r, it["name"]))
        else:
            d = pr - r
            if d >= 20:
                ups.append((r, it["name"], f"{pr}→{r}"))
            elif d <= -20:
                downs.append((r, it["name"], f"{pr}→{r}"))
    for u, pr in prev_map.items():
        if u not in cur_urls and pr <= 20:
            out.append(pr)
    return ups[:5], newin[:5], downs[:5], out


# ====== Slack ======
def post_slack(rows: List[Dict]):
    if not SLACK_WEBHOOK:
        return
    lines = []
    lines.append(f"*다이소몰 뷰티/위생 일간 랭킹 — {today_str()}*")
    lines.append("")
    lines.append("*TOP 10*")
    for it in rows[:10]:
        lines.append(f"{it['rank']}. <{it['url']}|{it['name']}> — {it['price']:,}원")

    prev_map = load_prev_map()
    ups, newin, downs, out = build_diff(rows, prev_map)

    lines.append("\n🔥 *급상승*")
    if ups:
        for r, name, mv in ups:
            lines.append(f"- {name} ({mv})")
    else:
        lines.append("- 해당 없음")

    lines.append("\n🆕 *뉴랭커*")
    if newin:
        for r, name in newin:
            lines.append(f"- {name} NEW → {r}위")
    else:
        lines.append("- 해당 없음")

    lines.append("\n📉 *급하락*")
    if downs:
        for r, name, mv in downs:
            lines.append(f"- {name} ({mv})")
    else:
        lines.append("- 해당 없음")

    lines.append("\n🔗 *링크 인&아웃*")
    if out:
        lines.append(f"{len(out)}개의 제품이 인&아웃 되었습니다.")
    else:
        lines.append("0개의 제품이 인&아웃 되었습니다.")

    try:
        requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=10).raise_for_status()
    except Exception as e:
        print("[Slack] 전송 실패:", e)


# ====== Google Drive (선택) ======
def upload_to_drive(path: str) -> Optional[str]:
    if not (GDRIVE_FOLDER_ID and GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        return None
    try:
        tok = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "refresh_token": GOOGLE_REFRESH_TOKEN,
                "grant_type": "refresh_token",
            },
            timeout=15,
        )
        tok.raise_for_status()
        access_token = tok.json()["access_token"]

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
        return up.json().get("id")
    except Exception as e:
        print("[Drive] 업로드 실패:", e)
        return None


# ====== main ======
def main():
    print("수집 시작:", RANK_URL)
    t0 = time.time()
    rows = fetch_products()
    print("[수집 완료] 개수:", len(rows))
    if len(rows) < 10:
        raise RuntimeError("유효 상품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    csv_path = save_csv(rows)
    upload_to_drive(csv_path)   # 성공/실패 메시지는 내부에서 출력
    post_slack(rows)

    print("로컬 저장:", csv_path)
    print("총", len(rows), "건, 경과:", f"{time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
