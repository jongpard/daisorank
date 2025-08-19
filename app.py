# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (전일 CSV를 Google Drive에서 자동 다운로드)
import os, re, csv, time, json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Union
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Locator

# ====== 설정 ======
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))
SCROLL_PAUSE = 0.6
SCROLL_STABLE_ROUNDS = 4
SCROLL_MAX_ROUNDS = 80

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Google Drive
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

    for _ in range(2):
        try:
            loc.first.click(timeout=1200)
            return
        except Exception:
            pass
    try:
        loc.first.scroll_into_view_if_needed(timeout=1000)
        page.wait_for_timeout(150)
        loc.first.click(timeout=1200)
        return
    except Exception:
        pass
    try:
        loc.first.evaluate("(el) => { el.click(); }")
        return
    except Exception:
        pass
    try:
        loc.first.evaluate("""(el) => {
            const ev = new PointerEvent('click', {bubbles:true, cancelable:true});
            el.dispatchEvent(ev);
        }""")
        return
    except Exception:
        pass
    try:
        box = loc.first.bounding_box()
        if box:
            page.mouse.move(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
            page.mouse.click(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
            return
    except Exception:
        pass
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

# ====== 카테고리/정렬 고정 + 스크롤 + 추출 ======
def select_beauty_daily(page: Page):
    close_overlays(page)

    # 카테고리: 뷰티/위생
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

    # 정렬: 일간 (value=2)
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
        except Exception:
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
            const nameEl = el.querySelector('.goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .name, .goods-name');
            let name = (nameEl?.textContent || '').trim();
            if (!name) continue;

            const priceEl = el.querySelector('.goods-detail .goods-price .value, .price .num, .sale-price .num, .sale .price, .goods-price .num');
            let priceTxt = (priceEl?.textContent || '').replace(/[^0-9]/g, '');
            if (!priceTxt) continue;
            const price = parseInt(priceTxt, 10);
            if (!price or price <= 0) continue;

            let href = None;
            const a = el.querySelector('a[href*="/goods/"], a[href*="/product/"], a.goods-link, .goods-detail a');
            if (a and a.href) href = a.href;
            if (!href) continue;

            if (seen.has(href)) continue;
            seen.add(href);

            items.push({ name, price, url: href });
          }
          return items;
        }
        """.replace("None","null")  # tiny fix for literal
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
    path = os.path.join("data", f"다이소몰_뷰티위생_일간_{date_str}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "rank", "name", "price", "url"])
        for r in rows:
            w.writerow([date_str, r["rank"], r["name"], r["price"], r["url"]])
    return path

# ====== Google Drive: 전일 CSV 자동 다운로드 ======
def drive_access_token() -> Optional[str]:
    if not (GDRIVE_FOLDER_ID and GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        return None
    try:
        r = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "refresh_token": GOOGLE_REFRESH_TOKEN,
                "grant_type": "refresh_token",
            },
            timeout=15,
        )
        r.raise_for_status()
        return r.json()["access_token"]
    except Exception as e:
        print("[Drive] 토큰 발급 실패:", e)
        return None

def ensure_prev_csv_from_drive() -> Optional[str]:
    """전일 CSV가 로컬에 없으면 Drive에서 찾아 data/로 내려받는다."""
    os.makedirs("data", exist_ok=True)
    fname = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
    local_path = os.path.join("data", fname)
    if os.path.exists(local_path):
        return local_path

    token = drive_access_token()
    if not token:
        return None

    try:
        q = f"name = '{fname}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        params = {"q": q, "fields": "files(id,name,modifiedTime)", "pageSize": 1}
        res = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=20,
        )
        res.raise_for_status()
        files = res.json().get("files", [])
        if not files:
            print("[Drive] 전일 파일을 찾지 못했습니다:", fname)
            return None

        file_id = files[0]["id"]
        dl = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        dl.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(dl.content)
        print("[Drive] 전일 CSV 다운로드 완료:", local_path)
        return local_path
    except Exception as e:
        print("[Drive] 전일 CSV 다운로드 실패:", e)
        return None

# ====== 변화 감지 ======
def load_prev_map() -> Dict[str, int]:
    prev = {}
    # 1) 로컬 확인, 2) 없으면 드라이브에서 자동 다운로드
    path = os.path.join("data", f"다이소몰_뷰티위생_일간_{yday_str()}.csv")
    if not os.path.exists(path):
        ensure_prev_csv_from_drive()
    if not os.path.exists(path):
        return prev  # 전일 없음

    with open(path, newline="", encoding="utf-8") as f:
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
    lines += ([f"- {name} ({mv})" for r, name, mv in ups] or ["- 해당 없음"])

    lines.append("\n🆕 *뉴랭커*")
    lines += ([f"- {name} NEW → {r}위" for r, name in newin] or ["- 해당 없음"])

    lines.append("\n📉 *급하락*")
    lines += ([f"- {name} ({mv})" for r, name, mv in downs] or ["- 해당 없음"])

    lines.append("\n🔗 *링크 인&아웃*")
    lines.append(f"{len(out)}개의 제품이 인&아웃 되었습니다." if out else "0개의 제품이 인&아웃 되었습니다.")

    try:
        requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=10).raise_for_status()
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ====== Drive 업로드(변경 없음) ======
def upload_to_drive(path: str) -> Optional[str]:
    token = drive_access_token()
    if not token:
        return None
    try:
        meta = {"name": os.path.basename(path), "parents": [GDRIVE_FOLDER_ID]}
        files = {
            "metadata": ("metadata", json.dumps(meta), "application/json"),
            "file": (os.path.basename(path), open(path, "rb"), "text/csv"),
        }
        up = requests.post(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart",
            headers={"Authorization": f"Bearer {token}"},
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
