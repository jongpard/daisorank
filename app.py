# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (안정화판)
# - 카테고리/정렬 강제 + 가시성/오버레이 이슈 해결용 하드 클릭
# - 무한 스크롤 안정화
# - JS 컨텍스트 추출 (여러 셀렉터 동시 대응)
# - BEST 제거, 광고/빈카드/중복 제거
# - CSV 저장, (선택) 구글 드라이브 업로드, 슬랙 알림(올영 포맷)

import os, re, csv, time, json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Union
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Locator

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
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()


def _to_locator(page: Page, target: Union[str, Locator]) -> Locator:
    return target if isinstance(target, Locator) else page.locator(target)


def close_overlays(page: Page):
    # 흔한 레이어/배너 닫기 (있을 때만 시도)
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
    """가시성/오버레이 이슈를 뚫는 다단계 클릭"""
    loc = _to_locator(page, target)
    # 단계 0: 존재 대기
    try:
        loc.first.wait_for(state="attached", timeout=3000)
    except Exception:
        raise RuntimeError(f"[click_hard] 대상 미존재: {name_for_log}")

    # 단계 1: 정상 클릭
    for _ in range(2):
        try:
            loc.first.click(timeout=1200)
            return
        except Exception:
            pass

    # 단계 2: 스크롤 중앙 -> 클릭
    try:
        loc.first.scroll_into_view_if_needed(timeout=1000)
        page.wait_for_timeout(150)
        loc.first.click(timeout=1200)
        return
    except Exception:
        pass

    # 단계 3: JS 강제 클릭 (el.click())
    try:
        loc.first.evaluate("(el) => { el.click(); }")
        return
    except Exception:
        pass

    # 단계 4: PointerEvent 디스패치
    try:
        loc.first.evaluate("""(el) => {
            const ev = new PointerEvent('click', {bubbles:true, cancelable:true});
            el.dispatchEvent(ev);
        }""")
        return
    except Exception:
        pass

    # 단계 5: 마우스 좌표 클릭
    try:
        box = loc.first.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
            page.mouse.click(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
            return
    except Exception:
        pass

    # 단계 6: 상단 고정 헤더/배너 무력화 후 재시도
    try:
        page.evaluate("""
            () => {
              const hide = sel => {
                const el = document.querySelector(sel);
                if (el) { el.style.pointerEvents = 'none'; el.style.zIndex = '0'; }
              };
              hide('header'); hide('.header'); hide('#header');
              hide('.fixed-top'); hide('.floating'); hide('.top-banner');
            }
        """)
        loc.first.scroll_into_view_if_needed(timeout=800)
        page.wait_for_timeout(120)
        loc.first.click(timeout=1200)
        return
    except Exception:
        pass

    raise RuntimeError(f"[click_hard] 클릭 실패: {name_for_log}")


# ====== Playwright (카테고리/정렬 고정 + 스크롤 + 추출) ======
def select_beauty_daily(page: Page):
    close_overlays(page)

    # 1) 카테고리 '뷰티/위생'
    # value 시도 → 텍스트 시도 → JS 강제
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count() > 0:
            click_hard(page, '.prod-category .cate-btn[value="CTGR_00014"]', "뷰티/위생(value)")
        else:
            click_hard(page, page.get_by_role("button", name=re.compile("뷰티\\/?위생")), "뷰티/위생(text)")
    except Exception:
        # JS 최후 수단
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

    # 실제 선택 검증
    selected_txt = page.evaluate("""
      () => {
        const act = document.querySelector('.prod-category .cate-btn.is-active, .prod-category .cate-btn.active');
        return (act?.textContent || '').trim();
      }
    """)
    if not (selected_txt and ("뷰티" in selected_txt or "위생" in selected_txt)):
        # 한 번 더 시도
        try:
            click_hard(page, '.prod-category .cate-btn[value="CTGR_00014"]', "뷰티/위생 재시도")
            page.wait_for_timeout(200)
        except Exception:
            pass

    # 2) 정렬 '일간' -> input[value=2] / 텍스트 '일간'
    chosen = False
    if page.locator('.ipt-sorting input[value="2"]').count() > 0:
        try:
            click_hard(page, '.ipt-sorting input[value="2"]', "일간(value)")
            chosen = True
        except Exception:
            pass
    if not chosen:
        try:
            click_hard(page, page.get_by_role("button", name=re.compile("일간")), "일간(text)")
            chosen = True
        except Exception:
            # JS 최후 수단
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
            // 이름
            const nameEl = el.querySelector('.goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .name, .goods-name');
            let name = (nameEl?.textContent || '').trim();
            if (!name) continue;
            // 가격
            const priceEl = el.querySelector('.goods-detail .goods-price .value, .price .num, .sale-price .num, .sale .price, .goods-price .num');
            let priceTxt = (priceEl?.textContent || '').replace(/[^0-9]/g, '');
            if (!priceTxt) continue;
            const price = parseInt(priceTxt, 10);
            if (!price || price <= 0) continue;
            // URL
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

        # 주요 블록 대기
        try:
            page.wait_for_selector(".prod-category", timeout=15_000)
        except PWTimeout:
            pass

        select_beauty_daily(page)

        # 첫 카드 대기
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
            if r <= 30:
                newin.append((r, it["name"]))
        else:
            d = pr - r
            if d >= 20:
                ups.append((r, it["name"], f"{pr}→{r}"))
            elif d <= -20:
                downs.append((r, it["name"], f"{pr}→{r}"))
    for u, pr in prev_map.items():
        if u not in cur_urls and pr <= 30:
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
    upload_to_drive(csv_path)
    post_slack(rows)

    print("로컬 저장:", csv_path)
    print("총", len(rows), "건, 경과:", f"{time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
