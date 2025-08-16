# -*- coding: utf-8 -*-
"""
DaisoMall C105 랭킹 크롤러
- 1차: HTTP 정적 수집 (부족/실패 시 Playwright 폴백)
- 폴백: 스크롤/탭클릭/다양한 셀렉터 시도 + XHR JSON 복원 + 디버그 덤프
- CSV: 다이소몰_랭킹_YYYY-MM-DD.csv (KST)
- 비교 키: product_code 우선, 없으면 url
- Slack: 국내 포맷(Top10 → 급상승 → 뉴랭커 → 급하락(5) → 인&아웃)
- Google Drive: OAuth(개인 계정 refresh token) 업로드 + 전일 CSV 다운로드
"""

import os
import re
import io
import math
import pytz
import json
import traceback
import datetime as dt
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ================== 기본 설정 ==================
KST = pytz.timezone("Asia/Seoul")
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_RANK = int(os.getenv("DAISO_MAX_RANK", "200"))

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

# ================== 유틸/공통 ==================
def now_kst() -> dt.datetime:
    return dt.datetime.now(KST)

def today_kst_str() -> str:
    return now_kst().strftime("%Y-%m-%d")

def yesterday_kst_str() -> str:
    return (now_kst() - dt.timedelta(days=1)).strftime("%Y-%m-%d")

def build_filename(date_str: str) -> str:
    return f"다이소몰_랭킹_{date_str}.csv"

def clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def slack_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def debug_dump_page(page, prefix="page"):
    """Playwright 디버그: 전체 스크린샷 + HTML 저장"""
    try:
        os.makedirs("data/debug", exist_ok=True)
        png = f"data/debug/{prefix}.png"
        html = f"data/debug/{prefix}.html"
        page.screenshot(path=png, full_page=True)
        content = page.content()
        with open(html, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[debug] saved {png}, {html}")
    except Exception as e:
        print("[debug] dump failed:", e)

# 금액/퍼센트/상품코드
KRW_RE = re.compile(r"(?:₩|)\s*([\d,]+)\s*원")
PCT_RE = re.compile(r"(\d+)\s*%")
PC_PATTERNS = [
    re.compile(r"[?&](?:goodsNo|itemNo|prodNo|productNo|goods_id|no)=(\d+)", re.I),
    re.compile(r"/(?:product|goods)/(\d+)(?:[/?#]|$)", re.I),
    re.compile(r"/p/(\d+)(?:[/?#]|$)", re.I),
]

def extract_product_code(url: str, block_text: str = "") -> str:
    if not url:
        return ""
    for pat in PC_PATTERNS:
        m = pat.search(url)
        if m:
            return m.group(1)
    m2 = re.search(r"상품번호\s*[:：]\s*(\d+)", block_text)
    return m2.group(1) if m2 else ""

def parse_prices(block_text: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """최솟값=판매가, 최댓값=정가, 퍼센트는 텍스트 우선, 없으면 계산(버림)"""
    amounts = [int(x.replace(",", "")) for x in KRW_RE.findall(block_text or "")]
    sale = orig = pct = None
    if amounts:
        sale = min(amounts)
        if len(amounts) >= 2:
            orig = max(amounts)
            if orig == sale:
                orig = None
    m = PCT_RE.search(block_text or "")
    if m:
        pct = int(m.group(1))
    elif orig and sale and orig > 0:
        pct = max(0, int(math.floor((1 - sale / orig) * 100)))
    return sale, orig, pct

# ================== 모델 ==================
@dataclass
class Product:
    rank: Optional[int]
    brand: str
    name: str
    price: Optional[int]
    orig_price: Optional[int]
    discount_percent: Optional[int]
    url: str
    product_code: str

# ================== 파서(HTTP) ==================
def parse_http(html: str) -> List[Product]:
    soup = BeautifulSoup(html, "lxml")
    items: List[Product] = []
    seen = set()

    anchors = soup.select(
        "a[href*='/product/'], a[href*='/goods/'], a[href*='goodsNo='], "
        "ul.goods_list a[href], div.goods_list a[href]"
    )
    for a in anchors:
        href = a.get("href") or ""
        if not href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = "https://www.daisomall.co.kr" + href

        card = a.find_parent("li") or a.find_parent("div")
        if not card:
            continue

        name = clean(a.get_text(" ", strip=True))
        brand = ""

        brand_el = None
        for sel in [".brand", ".brand-name", ".prd-brand", ".ds-brand", ".goods-brand", ".txt_brand", ".brand_name"]:
            brand_el = card.select_one(sel)
            if brand_el:
                break
        if brand_el:
            brand = clean(brand_el.get_text(" ", strip=True))
        else:
            for sub_a in card.select("a"):
                h = (sub_a.get("href") or "").lower()
                if ("/product/" in h) or ("/goods/" in h) or ("goodsno=" in h):
                    continue
                t = clean(sub_a.get_text(" ", strip=True))
                if 1 <= len(t) <= 40:
                    brand = t
                    break

        block_text = clean(card.get_text(" ", strip=True))
        code = extract_product_code(href, block_text)
        key = code or href
        if key in seen:
            continue
        seen.add(key)

        sale, orig, pct = parse_prices(block_text)
        items.append(Product(
            rank=len(items) + 1,
            brand=brand,
            name=name,
            price=sale,
            orig_price=orig,
            discount_percent=pct,
            url=href,
            product_code=code
        ))
        if len(items) >= MAX_RANK:
            break

    return items

# ================== Playwright 폴백(강화) ==================
def fetch_by_playwright() -> List[Product]:
    """
    - C105 탭 클릭 시도
    - 충분히 스크롤·대기
    - 다양한 카드 셀렉터 시도
    - 그래도 부족하면 XHR JSON 응답에서 복원
    - 디버그 덤프 저장
    """
    from playwright.sync_api import sync_playwright

    products: List[Product] = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = browser.new_context(
            viewport={"width": 1440, "height": 1100},
            user_agent=HEADERS["User-Agent"],
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            extra_http_headers={"Accept-Language": HEADERS["Accept-Language"]},
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = context.new_page()

        # XHR JSON 수집
        xhr_rows: List[dict] = []
        def on_response(res):
            try:
                url = res.url
                ctype = (res.headers or {}).get("content-type", "")
                if ("rank" in url.lower() or "best" in url.lower()) and "json" in ctype.lower():
                    data = res.json()
                    if isinstance(data, (dict, list)):
                        xhr_rows.append({"url": url, "data": data})
            except Exception:
                pass
        page.on("response", on_response)

        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60_000)

        # 탭 클릭 시도
        try:
            page.locator('a[href="/ds/rank/C105"]').first.click(timeout=3_000)
        except Exception:
            pass

        # 네트워크 안정화
        try:
            page.wait_for_load_state("networkidle", timeout=5_000)
        except Exception:
            pass

        # 충분히 스크롤 (증가 없으면 종료)
        last = 0
        idle = 0
        for _ in range(20):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try:
                page.wait_for_load_state("networkidle", timeout=1_500)
            except Exception:
                pass
            cnt = page.eval_on_selector_all(
                "a[href*='/product/'], a[href*='/goods/'], a[href*='goodsNo=']",
                "els => els.length"
            )
            if cnt == last:
                idle += 1
            else:
                idle = 0
            last = cnt
            if cnt >= MAX_RANK or idle >= 3:
                break

        debug_dump_page(page, "page_rank")

        # 1차: 다양한 카드 셀렉터에서 수집
        rows = page.evaluate("""
            () => {
              const SELS = [
                'ul li[data-goods-no]',
                'ul.goods_list li',
                'div.goods_list li',
                'li.goods-item',
                'li.rank_list_item',
                'li.prd_item',
                'div.prd_list li',
                'div.item, li.item'
              ];
              let cards = [];
              for (const s of SELS) {
                cards = Array.from(document.querySelectorAll(s));
                if (cards.length >= 10) break;
              }
              const res = [];
              const seen = new Set();
              for (const card of cards) {
                let a = card.querySelector("a[href*='/product/'], a[href*='/goods/'], a[href*='goodsNo=']");
                if (!a) continue;
                let href = a.getAttribute('href') || '';
                if (!href) continue;
                if (href.startsWith('//')) href = 'https:' + href;
                if (href.startsWith('/')) href = location.origin + href;

                let name = (a.textContent || '').replace(/\\s+/g,' ').trim();
                let brand = '';
                const brandSels = ['.brand', '.brand-name', '.prd-brand', '.ds-brand', '.goods-brand', '.txt_brand', '.brand_name'];
                for (const s of brandSels) {
                  const el = card.querySelector(s);
                  if (el) { brand = (el.textContent||'').replace(/\\s+/g,' ').trim(); break; }
                }
                if (!brand) {
                  const subAs = Array.from(card.querySelectorAll('a'));
                  for (const b of subAs) {
                    const h = (b.getAttribute('href') || '').toLowerCase();
                    if (h.includes('/product/') || h.includes('/goods/') || h.includes('goodsno=')) continue;
                    const t = (b.textContent || '').replace(/\\s+/g,' ').trim();
                    if (t.length >= 1 && t.length <= 40) { brand = t; break; }
                  }
                }
                const block = (card.innerText || '').replace(/\\s+/g,' ').trim();

                const key = href + '|' + name;
                if (seen.has(key)) continue;
                seen.add(key);
                res.push({href, name, brand, block});
              }
              return res;
            }
        """)

        # 1차 조립
        for r in rows:
            href = r.get("href") or ""
            name = clean(r.get("name"))
            brand = clean(r.get("brand"))
            block = clean(r.get("block"))
            code = extract_product_code(href, block)
            key = code or href
            if key in seen:
                continue
            seen.add(key)
            sale, orig, pct = parse_prices(block)
            products.append(Product(
                rank=len(products)+1, brand=brand, name=name,
                price=sale, orig_price=orig, discount_percent=pct,
                url=href, product_code=code
            ))
            if len(products) >= MAX_RANK:
                break

        # 2차: 카드로 부족하면 XHR(JSON) 복원
        if len(products) < 10 and xhr_rows:
            for pack in xhr_rows:
                data = pack.get("data")
                stack = [data]
                while stack:
                    cur = stack.pop()
                    if isinstance(cur, dict):
                        if {"goodsNo","goodsNm"} <= set(cur.keys()):
                            try:
                                href = cur.get("url") or f"https://www.daisomall.co.kr/goods/{cur.get('goodsNo')}"
                                name = clean(cur.get("goodsNm") or "")
                                brand = clean(cur.get("brandNm") or "")
                                block = f"{name} {brand} {cur}"
                                code = str(cur.get("goodsNo") or "") or extract_product_code(href, block)
                                key = code or href
                                if key in seen:
                                    continue
                                sale = None
                                if cur.get("sellPrice") is not None:
                                    try: sale = int(str(cur.get("sellPrice")).replace(",",""))
                                    except: pass
                                orig = None
                                if cur.get("originPrice") is not None:
                                    try: orig = int(str(cur.get("originPrice")).replace(",",""))
                                    except: pass
                                pct = None
                                if orig and sale:
                                    pct = max(0, int(math.floor((1 - sale/orig) * 100)))
                                products.append(Product(
                                    rank=len(products)+1, brand=brand, name=name,
                                    price=sale, orig_price=orig, discount_percent=pct,
                                    url=href, product_code=code
                                ))
                                if len(products) >= MAX_RANK:
                                    break
                            except Exception:
                                pass
                        else:
                            for v in cur.values():
                                stack.append(v)
                    elif isinstance(cur, list):
                        for v in cur:
                            stack.append(v)
                if len(products) >= MAX_RANK:
                    break

        context.close()
        browser.close()

    return products

# ================== 수집 ==================
def fetch_products() -> List[Product]:
    print("수집 시작:", RANK_URL)
    # HTTP
    try:
        r = requests.get(RANK_URL, headers=HEADERS, timeout=20)
        r.raise_for_status()
        items = parse_http(r.text)
        print("[HTTP] 수집:", len(items))
        if len(items) >= 30:
            return items[:MAX_RANK]
    except Exception as e:
        print("[HTTP 오류]", e)

    # Playwright 폴백
    print("[Playwright 폴백 진입]")
    items = fetch_by_playwright()
    print("[Playwright] 수집:", len(items))
    return items[:MAX_RANK]

# ================== 데이터프레임/저장 ==================
def to_df(products: List[Product], date_str: str) -> pd.DataFrame:
    rows = []
    for p in products:
        rows.append({
            "date": date_str,
            "rank": p.rank,
            "brand": p.brand,
            "product_name": p.name,
            "price": p.price,
            "orig_price": p.orig_price,
            "discount_percent": p.discount_percent,
            "url": p.url,
            "product_code": p.product_code,
        })
    return pd.DataFrame(rows)

# ================== Google Drive ==================
def normalize_folder_id(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()
    m = re.search(r"/folders/([a-zA-Z0-9_-]{8,})", s) or re.search(r"[?&]id=([a-zA-Z0-9_-]{8,})", s)
    return (m.group(1) if m else s)

def build_drive_service():
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials

    cid = os.getenv("GOOGLE_CLIENT_ID")
    csc = os.getenv("GOOGLE_CLIENT_SECRET")
    rtk = os.getenv("GOOGLE_REFRESH_TOKEN")
    if not (cid and csc and rtk):
        raise RuntimeError("Google Drive 폴더 접근 불가: Client/Secret/Refresh 토큰 확인 필요")

    creds = Credentials(
        None,
        refresh_token=rtk,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cid,
        client_secret=csc,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    svc = build("drive", "v3", credentials=creds, cache_discovery=False)
    try:
        about = svc.about().get(fields="user(displayName,emailAddress)").execute()
        u = about.get("user", {})
        print(f"[Drive] user={u.get('displayName')} <{u.get('emailAddress')}>")
    except Exception as e:
        print("[Drive] whoami 실패:", e)
    return svc

def drive_upload_csv(service, folder_id: str, filename: str, df: pd.DataFrame) -> str:
    from googleapiclient.http import MediaIoBaseUpload

    q = f"name = '{filename}' and '{folder_id}' in parents and trashed = false"
    res = service.files().list(
        q=q,
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    file_id = res.get("files", [{}])[0].get("id") if res.get("files") else None

    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    buf.seek(0)
    media = MediaIoBaseUpload(buf, mimetype="text/csv", resumable=False)

    if file_id:
        service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        return file_id

    meta = {"name": filename, "parents": [folder_id], "mimeType": "text/csv"}
    created = service.files().create(
        body=meta,
        media_body=media,
        fields="id",
        supportsAllDrives=True
    ).execute()
    return created["id"]

def drive_download_csv(service, folder_id: str, filename: str) -> Optional[pd.DataFrame]:
    from googleapiclient.http import MediaIoBaseDownload

    res = service.files().list(
        q=f"name = '{filename}' and '{folder_id}' in parents and trashed = false",
        fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = res.get("files", [])
    if not files:
        return None

    fid = files[0]["id"]
    req = service.files().get_media(fileId=fid, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh)

# ================== Slack ==================
def fmt_krw(v) -> str:
    try:
        return f"₩{int(round(float(v))):,}"
    except:
        return "₩0"

def slack_post(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 콘솔 출력\n", text)
        return
    try:
        r = requests.post(url, json={"text": text}, timeout=20)
        if r.status_code >= 300:
            print("[Slack 실패]", r.status_code, r.text)
    except Exception as e:
        print("[Slack 오류]", e)

# ================== 비교/섹션 구성 ==================
def build_sections(df_today: pd.DataFrame, df_prev: Optional[pd.DataFrame]) -> Dict[str, List[str]]:
    S = {"top10": [], "rising": [], "newcomers": [], "falling": [], "outs": [], "inout_count": 0}

    def display_name(row) -> str:
        br = clean(row.get("brand", ""))
        nm = clean(row.get("product_name", ""))
        return f"{br} {nm}" if br else nm

    # Top10
    top10 = df_today.dropna(subset=["rank"]).sort_values("rank").head(10)
    lines = []
    for _, r in top10.iterrows():
        disp = slack_escape(display_name(r))
        link = f"<{r['url']}|{disp}>"
        tail = f" (↓{int(r['discount_percent'])}%)" if pd.notnull(r.get("discount_percent")) else ""
        lines.append(f"{int(r['rank'])}. {link} — {fmt_krw(r['price'])}{tail}")
    S["top10"] = lines

    if df_prev is None or not len(df_prev):
        return S

    # Key 정의
    def keyify(df):
        df = df.copy()
        df["key"] = df.apply(
            lambda x: x["product_code"] if (pd.notnull(x.get("product_code")) and str(x.get("product_code")).strip())
            else x["url"], axis=1)
        df.set_index("key", inplace=True)
        return df

    df_t = keyify(df_today)
    df_p = keyify(df_prev)

    t30 = df_t[(df_t["rank"].notna()) & (df_t["rank"] <= 30)].copy()
    p30 = df_p[(df_p["rank"].notna()) & (df_p["rank"] <= 30)].copy()

    common = set(t30.index) & set(p30.index)
    new = set(t30.index) - set(p30.index)
    out = set(p30.index) - set(t30.index)

    def link_of(row):
        return f"<{row['url']}|{slack_escape(display_name(row))}>"

    # 급상승
    rising = []
    for k in common:
        pr = int(p30.loc[k, "rank"])
        cr = int(t30.loc[k, "rank"])
        imp = pr - cr
        if imp > 0:
            rising.append((imp, cr, pr, link_of(t30.loc[k])))
    rising.sort(key=lambda x: (-x[0], x[1], x[2]))
    S["rising"] = [f"- {lnk} {pr}위 → {cr}위 (↑{imp})" for imp, cr, pr, lnk in rising[:3]] or ["- 해당 없음"]

    # 뉴랭커
    newcomers = [(int(t30.loc[k, "rank"]), link_of(t30.loc[k])) for k in new]
    newcomers.sort(key=lambda x: x[0])
    S["newcomers"] = [f"- {lnk} NEW → {rk}위" for rk, lnk in newcomers[:3]] or ["- 해당 없음"]

    # 급하락
    falling = []
    for k in common:
        pr = int(p30.loc[k, "rank"])
        cr = int(t30.loc[k, "rank"])
        drop = cr - pr
        if drop > 0:
            falling.append((drop, cr, pr, link_of(t30.loc[k])))
    falling.sort(key=lambda x: (-x[0], x[1], x[2]))
    S["falling"] = [f"- {lnk} {pr}위 → {cr}위 (↓{drop})" for drop, cr, pr, lnk in falling[:5]] or ["- 해당 없음"]

    # OUT
    outs = [(int(p30.loc[k, "rank"]), link_of(p30.loc[k])) for k in out]
    outs.sort(key=lambda x: x[0])
    S["outs"] = [f"- {lnk} {rk}위 → OUT" for rk, lnk in outs]

    S["inout_count"] = len(new) + len(out)
    return S

def build_slack_message(date_str: str, S: Dict[str, List[str]]) -> str:
    parts: List[str] = []
    parts.append(f"*다이소몰 랭킹 — {date_str}*")
    parts.append("")
    parts.append("*TOP 10*")
    parts.extend(S.get("top10") or ["- 데이터 없음"])
    parts.append("")
    parts.append("*🔥 급상승*")
    parts.extend(S.get("rising") or ["- 해당 없음"])
    parts.append("")
    parts.append("*🆕 뉴랭커*")
    parts.extend(S.get("newcomers") or ["- 해당 없음"])
    parts.append("")
    parts.append("*📉 급하락*")
    parts.extend(S.get("falling") or ["- 해당 없음"])
    if S.get("outs"):
        parts.extend(S["outs"])
    parts.append("")
    parts.append("*🔄 랭크 인&아웃*")
    parts.append(f"{S.get('inout_count', 0)}개의 제품이 인&아웃 되었습니다.")
    return "\n".join(parts)

# ================== 메인 ==================
def main():
    date_str = today_kst_str()
    yday_str = yesterday_kst_str()
    file_today = build_filename(date_str)
    file_prev = build_filename(yday_str)

    products = fetch_products()
    print("수집 완료:", len(products))
    if len(products) < 10:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    df_today = to_df(products, date_str)

    os.makedirs("data", exist_ok=True)
    local_path = os.path.join("data", file_today)
    df_today.to_csv(local_path, index=False, encoding="utf-8-sig")
    print("로컬 저장:", local_path)

    # Google Drive 업로드 + 전일 CSV 받기
    df_prev = None
    try:
        folder = normalize_folder_id(os.getenv("GDRIVE_FOLDER_ID", ""))
        if not folder:
            raise RuntimeError("GDRIVE_FOLDER_ID 미설정")
        svc = build_drive_service()
        drive_upload_csv(svc, folder, file_today, df_today)
        print("Google Drive 업로드 완료:", file_today)
        df_prev = drive_download_csv(svc, folder, file_prev)
        print("전일 CSV", "미발견" if df_prev is None else "다운로드 성공")
    except Exception as e:
        print("Google Drive 처리 오류:", e)

    S = build_sections(df_today, df_prev)
    msg = build_slack_message(date_str, S)
    slack_post(msg)
    print("Slack 전송 완료")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[오류 발생]", e)
        traceback.print_exc()
        try:
            slack_post(f"*다이소몰 랭킹 자동화 실패*\n```\n{e}\n```")
        except:
            pass
        raise
