# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (신 UI 완전 대응 · 3중 폴백)
# - Vue/Nuxt 동적 렌더 대응: 앵커 기반 + 카드 컨테이너 기반 + 네트워크 JSON 폴백
# - 수집 부족이어도 실패 종료 금지: CSV/Drive/Slack 진행
# - 디버그: HTML/스크린샷/셀렉터 카운트 저장(data/debug)
# - 전일 비교 분석 유지

import os, re, csv, time, io, json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Union

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Locator, Response

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ====== 설정 ======
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"

MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))
TOP_WINDOW = 150
SCROLL_PAUSE = float(os.getenv("SCROLL_PAUSE", "0.6"))
SCROLL_STABLE_ROUNDS = int(os.getenv("SCROLL_STABLE_ROUNDS", "6"))
SCROLL_MAX_ROUNDS = int(os.getenv("SCROLL_MAX_ROUNDS", "90"))
MIN_OK = int(os.getenv("MIN_OK", "10"))  # 미달이어도 파이프라인 진행

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

def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _to_locator(page: Page, target: Union[str, Locator]) -> Locator:
    return target if isinstance(target, Locator) else page.locator(target)

def close_overlays(page: Page):
    candidates = [
        ".layer-popup .btn-close", ".modal .btn-close", ".popup .btn-close",
        ".layer-popup .close", ".modal .close", ".popup .close",
        ".btn-x", ".btn-close, button[aria-label='닫기']",
        "button[aria-label*='닫기']", "button[title*='닫기']",
        ".notice-popup .close", ".cookie, .cookie .close"
    ]
    for sel in candidates:
        try:
            if page.locator(sel).count() > 0:
                page.locator(sel).first.click(timeout=800)
                page.wait_for_timeout(200)
        except Exception:
            pass

def click_hard(page: Page, target: Union[str, Locator], name_for_log: str = ""):
    loc = _to_locator(page, target)
    try:
        loc.first.wait_for(state="attached", timeout=3000)
    except Exception:
        raise RuntimeError(f"[click_hard] 대상 미존재: {name_for_log}")
    for _ in range(3):
        try:
            loc.first.click(timeout=1200); return
        except Exception:
            try:
                loc.first.scroll_into_view_if_needed(timeout=800)
                page.wait_for_timeout(120)
            except Exception:
                pass
            try:
                loc.first.evaluate("(el)=>el.click()"); return
            except Exception:
                pass
    raise RuntimeError(f"[click_hard] 클릭 실패: {name_for_log}")

# ====== DEBUG DUMP ======
DUMP_DEBUG = os.getenv("DUMP_DEBUG", "1") == "1"

def _ensure_dbg_dir():
    os.makedirs("data/debug", exist_ok=True)

def dump_selector_counts(page: Page, label: str):
    if not DUMP_DEBUG: return
    _ensure_dbg_dir()
    selectors = [
        "a[href*='/pd/pdr/']",
        ".rank_list_wrap", ".rank_list_item", ".goods-card", "li.goods-item",
    ]
    counts = page.evaluate("""(sels) => {
        const out = {}; for (const s of sels) out[s] = document.querySelectorAll(s).length; return out;
    }""", selectors)
    with open("data/debug/selector_counts.txt", "a", encoding="utf-8") as f:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"\n## {label} @ {ts}\n")
        for s, c in counts.items(): f.write(f"{c:4d}  {s}\n")
    print(f"[DEBUG] selector_counts ({label}) 저장")

def dump_html_and_cards(page: Page, label: str, sample_n: int = 5):
    if not DUMP_DEBUG: return
    _ensure_dbg_dir()
    with open(f"data/debug/rank_raw_{label}.html", "w", encoding="utf-8") as f:
        f.write(page.content())
    # 컨테이너
    try:
        container_html = page.evaluate("""
            () => {
              const el = document.querySelector('.rank_list_wrap, .goods-list, .product-list, .list-wrap');
              return el ? el.outerHTML : '(container not found)';
            }
        """)
    except Exception:
        container_html = "(container read error)"
    with open(f"data/debug/goods_container_{label}.html", "w", encoding="utf-8") as f:
        f.write(container_html)
    # 스크린샷
    try:
        page.screenshot(path=f"data/debug/page_{label}.png", full_page=True)
    except Exception:
        pass
    print(f"[DEBUG] HTML/screenshot ({label}) 저장 완료")

# ====== Playwright (카테고리/정렬 고정 + 스크롤 + 추출) ======
def select_beauty_daily(page: Page):
    close_overlays(page)
    # 카테고리
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count() > 0:
            click_hard(page, '.prod-category .cate-btn[value="CTGR_00014"]', "뷰티/위생(value)")
        else:
            btn = page.get_by_role("button", name=re.compile("뷰티\\/?위생|뷰티|위생"))
            click_hard(page, btn, "뷰티/위생(text)")
    except Exception:
        page.evaluate("""
            () => {
              const byVal = document.querySelector('.prod-category .cate-btn[value="CTGR_00014"]');
              if (byVal) { byVal.click(); return; }
              const cand = Array.from(document.querySelectorAll('.prod-category .cate-btn, .prod-category *'))
                .find(el => /뷰티\\/?위생|뷰티|위생/.test((el.textContent||'').trim()));
              if (cand) cand.click();
            }
        """)
    try: page.wait_for_load_state("networkidle", timeout=4000)
    except Exception: pass
    page.wait_for_timeout(300)
    # 정렬: 일간
    try:
        if page.locator('.ipt-sorting input[value="2"]').count() > 0:
            click_hard(page, '.ipt-sorting input[value="2"]', "일간(value)")
        else:
            click_hard(page, page.get_by_role("button", name=re.compile("일간")), "일간(text)")
    except Exception:
        page.evaluate("""
            () => {
              const byVal = document.querySelector('.ipt-sorting input[value="2"]');
              if (byVal) { byVal.click(); return; }
              const btns = Array.from(document.querySelectorAll('button, a, [role=button], label'));
              const t = btns.find(b => /일간/.test((b.textContent||'').trim()));
              if (t) t.click();
            }
        """)
    try: page.wait_for_load_state("networkidle", timeout=4000)
    except Exception: pass
    page.wait_for_timeout(400)

def infinite_scroll(page: Page):
    prev = 0
    stable = 0
    for _ in range(SCROLL_MAX_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        try: page.wait_for_load_state("networkidle", timeout=2000)
        except Exception: pass
        page.wait_for_timeout(int(SCROLL_PAUSE * 1000))
        cnt = page.evaluate("() => document.querySelectorAll('a[href*=\"/pd/pdr/\"]').length")
        if cnt >= MAX_ITEMS:
            break
        if cnt == prev:
            stable += 1
            if stable >= SCROLL_STABLE_ROUNDS:
                break
        else:
            stable = 0; prev = cnt

# ---------- 핵심: 수집기 3종 ----------
def _parse_name_price_from_text(txt: str) -> (str, int):
    txt = norm_ws(txt)
    # 가격 추출
    price = 0
    m = re.search(r"(\d{1,3}(?:,\d{3})+)\s*원", txt)
    if not m:
        m = re.search(r"(\d{4,})\s*원", txt)
    if not m:
        m = re.search(r"(\d{1,3}(?:,\d{3})+)", txt)
    if m:
        try: price = int(m.group(1).replace(",", ""))
        except: price = 0
    # 이름 추출: '원/적립/리뷰/담기/쿠폰/배송' 등 제외한 긴 조각
    parts = re.split(r"\s{2,}|·|\||/|>", txt)
    parts = [p.strip() for p in parts if p and not re.search(r"원|적립|리뷰|담기|쿠폰|배송|일간|주간|급상승|포인트|혜택", p)]
    parts.sort(key=len, reverse=True)
    name = parts[0] if parts else ""
    name = strip_best(name)
    return name, price

def collect_items_by_anchors(page: Page) -> List[Dict]:
    # Vue 렌더 대기
    page.wait_for_function("() => document.querySelectorAll('a[href*=\"/pd/pdr/\"]').length > 30", timeout=25_000)
    time.sleep(0.5)
    data = page.evaluate("""
        () => {
          const anchors = Array.from(document.querySelectorAll('a[href*="/pd/pdr/"]'));
          const seen = new Set(); const arr = [];
          for (const a of anchors) {
            const url = a.href;
            if (!url || seen.has(url)) continue; seen.add(url);
            // 루트 후보: 8단계까지 상향
            const roots = [];
            let cur = a; for (let i=0;i<8 && cur; i++) { roots.push(cur); cur = cur.parentElement; }
            arr.push({ url, roots });
          }
          // 루트는 client 측에서 텍스트 처리
          return arr.map(x => ({ url: x.url, path: x.roots.map(n => n.outerHTML ? n.outerHTML.slice(0, 0) : '') })) // placeholder
        }
    """)
    # 위 JS는 roots DOM을 넘기기 어렵기 때문에, 아래에서 다시 텍스트를 가져온다(성능 충분).
    anchors = page.query_selector_all('a[href*="/pd/pdr/"]')
    seen = set(); items = []
    for a in anchors:
        try:
            url = a.get_attribute("href")
            if not url: continue
            if url.startswith("/"): url = "https://www.daisomall.co.kr" + url
            if url in seen: continue
            seen.add(url)
            # 8단계 상향하면서 텍스트 검사
            root = a
            name, price = "", 0
            for _ in range(8):
                if not root: break
                txt = norm_ws(root.text_content())
                n2, p2 = _parse_name_price_from_text(txt)
                if p2 > 0 and (n2 and len(n2) >= 4):
                    name, price = n2, p2
                    break
                root = root.parent_element()
            if name and price > 0:
                items.append({"name": name, "price": price, "url": url})
        except Exception:
            continue
    for i, it in enumerate(items, 1): it["rank"] = i
    return items

def collect_items_by_container(page: Page) -> List[Dict]:
    # 카드 컨테이너 직접 순회
    cards = page.query_selector_all(".rank_list_wrap li, .rank_list_item, li.goods-item, .goods-card")
    items, seen = [], set()
    for el in cards:
        try:
            txt = norm_ws(el.text_content())
            name, price = _parse_name_price_from_text(txt)
            a = el.query_selector('a[href*="/pd/pdr/"]')
            url = a.get_attribute("href") if a else None
            if url and url.startswith("/"): url = "https://www.daisomall.co.kr" + url
            if name and price > 0 and url and url not in seen:
                seen.add(url)
                items.append({"name": name, "price": price, "url": url})
        except Exception:
            pass
    for i, it in enumerate(items, 1): it["rank"] = i
    return items

def collect_items_from_json(responses: List[Response]) -> List[Dict]:
    items, seen = [], set()
    # 네트워크에서 받은 JSON 응답들 스캔
    for r in responses:
        try:
            ctype = (r.headers or {}).get("content-type", "")
            if "application/json" not in ctype: continue
            body = r.json()
        except Exception:
            continue
        txt = json.dumps(body, ensure_ascii=False)
        # URL & 이름 & 가격 키가 근접해 있는 객체들 탐색 (폭넓은 패턴)
        # common keys: name, goodsNm, productName, title / salePrice, price, saleAmt, sellPrice / url, linkUrl, prdtUrl
        pattern = re.compile(r'("(?:(?:goods|product)?(?:N|n)ame|name|title)"\s*:\s*"(.*?)").{0,200}?("(?:(?:sale)?Price|saleAmt|sellPrice)"\s*:\s*"?([0-9,]{3,})"?).{0,200}?("(?:(?:link|prdt|product|detail)Url|url)"\s*:\s*"(\/pd\/pdr\/[^"]+)")', re.S)
        for m in pattern.finditer(txt):
            name = strip_best(m.group(2))
            price = int(re.sub(r"[^0-9]", "", m.group(4)))
            url = m.group(6)
            if url.startswith("/"): url = "https://www.daisomall.co.kr" + url
            if name and price > 0 and url not in seen:
                seen.add(url); items.append({"name": name, "price": price, "url": url})
        # 또다른 URL 패턴
        if not items:
            pattern2 = re.compile(r'"url"\s*:\s*"(\/pd\/pdr\/[^"]+)"')
            for m in pattern2.finditer(txt):
                url = m.group(1)
                if url.startswith("/"): url = "https://www.daisomall.co.kr" + url
                if url in seen: continue
                # 이름/가격은 근처에서 찾아본다(보수적)
                name_m = re.search(r'"(?:goodsNm|name|title)"\s*:\s*"(.*?)"', txt)
                price_m = re.search(r'"(?:salePrice|price|sellPrice|saleAmt)"\s*:\s*"?(\\d{1,3}(?:,\\d{3})+|\\d{4,})"?', txt)
                name = strip_best(name_m.group(1)) if name_m else ""
                price = int(re.sub(r"[^0-9]", "", price_m.group(1))) if price_m else 0
                if name and price > 0:
                    seen.add(url); items.append({"name": name, "price": price, "url": url})
    for i, it in enumerate(items, 1): it["rank"] = i
    return items

# ---------------------------------------
def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            viewport={"width": 1360, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"),
        )
        json_responses: List[Response] = []
        def _log_json(r: Response):
            try:
                if "application/json" in (r.headers or {}).get("content-type", ""):
                    json_responses.append(r)
            except Exception:
                pass
        context.on("response", _log_json)

        page = context.new_page()
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)
        try: page.wait_for_selector(".prod-category", timeout=12_000)
        except PWTimeout: pass

        # 선택 및 스크롤
        select_beauty_daily(page)
        dump_selector_counts(page, "before")
        dump_html_and_cards(page, "before")

        try:
            infinite_scroll(page)
        except Exception:
            pass

        dump_selector_counts(page, "after")
        dump_html_and_cards(page, "after")

        # 1) 앵커 기반
        items = collect_items_by_anchors(page)

        # 2) 컨테이너 기반 보강
        if len(items) < MIN_OK:
            more = collect_items_by_container(page)
            # URL 기준 병합
            url_set = {it["url"] for it in items}
            for m in more:
                if m["url"] not in url_set:
                    items.append(m); url_set.add(m["url"])
            # 순위 재부여
            items = sorted(items, key=lambda x: x["price"], reverse=False)
            for i, it in enumerate(items, 1): it["rank"] = i

        # 3) JSON 폴백
        if len(items) < MIN_OK and json_responses:
            parsed = collect_items_from_json(json_responses)
            url_set = {it["url"] for it in items}
            for m in parsed:
                if m["url"] not in url_set:
                    items.append(m); url_set.add(m["url"])
            items = sorted(items, key=lambda x: x["price"], reverse=False)
            for i, it in enumerate(items, 1): it["rank"] = i

        print(f"[DEBUG] 렌더 감지 완료, 추출 {len(items)}개")
        context.close()
        browser.close()
        return items

# ====== CSV 저장 ======
def save_csv(rows: List[Dict]):
    date_str = today_str()
    os.makedirs("data", exist_ok=True)
    filename = f"다이소몰_뷰티위생_일간_{date_str}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "rank", "name", "price", "url"])
        for r in rows:
            w.writerow([date_str, r.get("rank"), r.get("name"), r.get("price"), r.get("url")])
    return path, filename

# ====== Google Drive ======
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
        svc = build("drive", "v3", credentials=creds, cache_discovery=False)
        try:
            about = svc.about().get(fields="user(displayName,emailAddress)").execute()
            u = about.get("user", {})
            print(f"[Drive] user={u.get('displayName')} <{u.get('emailAddress')}>")
        except Exception as e:
            print("[Drive] whoami 실패:", e)
        return svc
    except Exception as e:
        print("[Drive] 서비스 생성 실패:", e)
        return None

def upload_to_drive(service, filepath: str, filename: str):
    if not service or not GDRIVE_FOLDER_ID:
        print("[Drive] 서비스 또는 폴더 ID가 없어 업로드를 건너뜁니다.")
        return None
    try:
        # 동일 파일명 있으면 업데이트, 없으면 생성
        q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed=false"
        res = service.files().list(q=q, fields="files(id,name)").execute()
        file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None

        media = MediaIoBaseUpload(io.FileIO(filepath, 'rb'), mimetype="text/csv", resumable=False)
        if file_id:
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"[Drive] 업데이트 완료: {filename} (id={file_id})")
            return file_id
        else:
            meta = {"name": filename, "parents": [GDRIVE_FOLDER_ID], "mimeType": "text/csv"}
            created = service.files().create(body=meta, media_body=media, fields="id").execute()
            print(f"[Drive] 업로드 성공: {filename} (id={created.get('id')})")
            return created.get("id")
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

# ====== 전일 비교 분석 ======
def parse_prev_csv(csv_text: str) -> List[Dict]:
    items = []
    try:
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            try:
                items.append({"rank": int(row.get("rank")), "name": row.get("name"), "url": row.get("url")})
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
        url = it.get("url"); 
        if not url: continue
        prev_rank = prev_map.get(url)
        trends.append({
            "name": it["name"], "url": url, "rank": it["rank"], "prev_rank": prev_rank,
            "change": (prev_rank - it["rank"]) if prev_rank else None
        })

    movers = [t for t in trends if t["prev_rank"] is not None]
    ups   = sorted([t for t in movers if (t["change"] or 0) > 0], key=lambda x: x["change"], reverse=True)
    downs = sorted([t for t in movers if (t["change"] or 0) < 0], key=lambda x: x["change"])
    chart_ins = [t for t in trends if t["prev_rank"] is None and t["rank"] <= TOP_WINDOW]

    today_urls = {t["url"] for t in trends}
    rank_out_urls = prev_top_urls - today_urls
    rank_outs = [p for p in prev_items if p.get("url") in rank_out_urls]

    in_out_count = len(chart_ins) + len(rank_outs)
    return ups, downs, chart_ins, rank_outs, in_out_count

# ====== Slack ======
def post_slack(rows: List[Dict], analysis_results, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK:
        return
    ups, downs, chart_ins, rank_outs, _ = analysis_results

    def _link(name: str, url: Optional[str]) -> str:
        return f"<{url}|{name}>" if url else (name or "")

    def _key(it: dict) -> str:
        return (it.get("url") or "").strip() or (it.get("name") or "").strip()

    prev_map: Dict[str, int] = {}
    if prev_items:
        for p in prev_items:
            try: r = int(p.get("rank") or 0)
            except Exception: continue
            k = _key(p)
            if k and r > 0: prev_map[k] = r

    now_kst = datetime.now(KST)
    title = f"*다이소몰 뷰티/위생 일간 랭킹 200* ({now_kst.strftime('%Y-%m-%d %H:%M KST')})"
    lines = [title]

    # TOP 10
    lines.append("\n*TOP 10*")
    for it in (rows or [])[:10]:
        ptxt = f"{int(it.get('price') or 0):,}원" if (it.get("price") or 0) else ""
        cur_r = int(it.get("rank") or 0)
        k = _key(it); marker = "(new)"
        if k in prev_map:
            prev_r = prev_map[k]; diff = prev_r - cur_r
            marker = f"(↑{diff})" if diff > 0 else f"(↓{abs(diff)})" if diff < 0 else "(-)"
        lines.append(f"{cur_r}. {marker} {_link(it.get('name') or '', it.get('url'))} — {ptxt}")

    # 급상승
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m.get('name'), m.get('url'))} {m.get('prev_rank')}위 → {m.get('rank')}위 (↑{m.get('change')})")
    else:
        lines.append("- (해당 없음)")

    # 뉴랭커
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t.get('name'), t.get('url'))} NEW → {t.get('rank')}위")
    else:
        lines.append("- (해당 없음)")

    # 급하락 + OUT
    lines.append("\n*📉 급하락*")
    if downs:
        downs_sorted = sorted(downs, key=lambda m: (-abs(int(m.get("change") or 0)), int(m.get("rank") or 9999)))
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

    # 인&아웃
    today_keys = { _key(it) for it in (rows or [])[:200] if _key(it) }
    prev_keys  = { _key(p)  for p in (prev_items or []) if _key(p) and 1 <= int(p.get("rank") or 0) <= 200 }
    io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2 if prev_items is not None else min(len(chart_ins or []), len(rank_outs or []))
    lines.append("\n*↔ 랭크 인&아웃*"); lines.append(f"{io_cnt}개의 제품이 인&아웃 되었습니다.")

    try:
        requests.post(SLACK_WEBHOOK, json={"text": "\n".join(lines)}, timeout=10).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ====== main ======
def main():
    print("수집 시작:", RANK_URL)
    t0 = time.time()
    rows = fetch_products()
    print(f"[수집 완료] 개수: {len(rows)}")
    if len(rows) < MIN_OK:
        print(f"[경고] 유효 상품 카드가 {len(rows)}개로 기준({MIN_OK}) 미달 — 그래도 CSV/드라이브/슬랙 진행")

    # CSV 저장
    csv_path, csv_filename = save_csv(rows)
    print("로컬 저장:", csv_path)

    # Google Drive
    drive_service = build_drive_service()
    prev_items: List[Dict] = []
    if drive_service:
        upload_to_drive(drive_service, csv_path, csv_filename)

        # 전일 파일 다운로드
        yday_filename = f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
        prev_file = find_file_in_drive(drive_service, yday_filename)
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
        analysis_results = ([], [], [], [], 0)

    # Slack 알림
    post_slack(rows, analysis_results, prev_items)
    print(f"총 {len(rows)}건, 경과 시간: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
