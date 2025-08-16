# -*- coding: utf-8 -*-
"""
다이소몰 뷰티/화장품 랭킹 크롤러 (HTTP → Playwright 폴백)
- 대상 URL: https://www.daisomall.co.kr/ds/rank/C105
- 수집 상한: 기본 500위 (env: DAISO_MAX_RANK)
- 저장: data/다이소몰_랭킹_YYYY-MM-DD.csv (KST)
- Google Drive: 같은 이름 있으면 update, 없으면 create
  · OAuth Refresh Token (scope=https://www.googleapis.com/auth/drive)
  · 또는 Service Account JSON (GOOGLE_SERVICE_ACCOUNT_JSON)
- Slack(Webhook): TOP10 → 급상승 → 뉴랭커 → 급하락(OUT 포함) → 랭크 인&아웃(개수)
"""

import asyncio, os, re, sys, math, json, logging
from io import BytesIO
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup
from pytz import timezone
from playwright.async_api import async_playwright

# ---- Google Drive ----
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request as GoogleRequest
from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2 import service_account

# =========================
# 설정
# =========================
KST = timezone("Asia/Seoul")
BASE_URL = "https://www.daisomall.co.kr/ds/rank/C105"
DATA_DIR = os.path.join(os.getcwd(), "data"); os.makedirs(DATA_DIR, exist_ok=True)

DAISO_MAX_RANK = int(os.getenv("DAISO_MAX_RANK", "500"))
HTTP_MIN_CARDS = 30

HEADLESS = True
NAV_TIMEOUT = 40_000
SEL_TIMEOUT = 12_000
SCROLL_ROUNDS = 100
SCROLL_WAIT_MS = 1000

# Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# Drive (OAuth)
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
# Drive (Service Account JSON string)
GOOGLE_SA_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GDRIVE_FOLDER_ID_RAW = os.getenv("GDRIVE_FOLDER_ID", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# =========================
# 공통 유틸
# =========================
def kst_date_str(dt: Optional[datetime] = None) -> str:
    return (dt or datetime.now(KST)).strftime("%Y-%m-%d")

def csv_name(dt: Optional[datetime] = None) -> str:
    return f"다이소몰_랭킹_{kst_date_str(dt)}.csv"

def csv_path(dt: Optional[datetime] = None) -> str:
    return os.path.join(DATA_DIR, csv_name(dt))

def fmt_won(v: Optional[int]) -> str:
    return "" if v is None or (isinstance(v, float) and math.isnan(v)) else f"₩{int(v):,}"

def normalize_folder_id(raw: Optional[str]) -> Optional[str]:
    if not raw: return None
    m = re.search(r"/folders/([A-Za-z0-9_-]{10,})", raw)
    return (m.group(1) if m else raw.strip())

def slack_post(blocks: List[Dict], fallback: str):
    if not SLACK_WEBHOOK_URL:
        logging.warning("[Slack] SLACK_WEBHOOK_URL 미설정(전송 생략)")
        return
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": fallback, "blocks": blocks}, timeout=15)
        if r.status_code >= 300:
            logging.error("[Slack] Webhook 실패: %s %s", r.status_code, r.text)
    except Exception as e:
        logging.warning("[Slack] 전송 예외: %s", e)

def slack_post_failure(msg: str):
    blocks = [
        {"type":"header","text":{"type":"plain_text","text":"다이소몰 랭킹 수집 실패"}},
        {"type":"section","text":{"type":"mrkdwn","text":f"*사유*: {msg}"}},
        {"type":"context","elements":[{"type":"mrkdwn","text":f"{kst_date_str()} (KST)"}]},
    ]
    slack_post(blocks, f"다이소몰 랭킹 수집 실패: {msg}")

# =========================
# Drive: OAuth or Service Account
# =========================
def build_drive_service():
    # 1) Service Account 우선 (다른 레포 OK였다면 이게 가장 안정적)
    if GOOGLE_SA_JSON:
        try:
            info = json.loads(GOOGLE_SA_JSON)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/drive"]
            )
            return build("drive", "v3", credentials=creds, cache_discovery=False)
        except Exception as e:
            logging.warning("[Drive] 서비스계정 실패: %s", e)

    # 2) OAuth Refresh Token
    if GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN:
        try:
            creds = UserCredentials(
                None,
                refresh_token=GOOGLE_REFRESH_TOKEN,
                client_id=GOOGLE_CLIENT_ID,
                client_secret=GOOGLE_CLIENT_SECRET,
                token_uri="https://oauth2.googleapis.com/token",
                scopes=["https://www.googleapis.com/auth/drive"],
            )
            creds.refresh(GoogleRequest())
            return build("drive", "v3", credentials=creds, cache_discovery=False)
        except Exception as e:
            logging.warning("[Drive] OAuth 실패(업로드 생략): %s", e)

    logging.warning("[Drive] 자격증명 미구성(업/다운로드 생략)")
    return None

def drive_find_by_name(svc, folder_id: Optional[str], name: str) -> Optional[Dict]:
    try:
        q = f"name = '{name}' and trashed = false"
        if folder_id: q += f" and '{folder_id}' in parents"
        r = svc.files().list(q=q, pageSize=1, fields="files(id,name)").execute()
        arr = r.get("files", [])
        return arr[0] if arr else None
    except Exception as e:
        logging.warning("[Drive] 파일 검색 실패: %s", e)
        return None

def drive_upsert_csv(svc, folder_id: Optional[str], filename: str, csv_bytes: bytes):
    try:
        media = MediaIoBaseUpload(BytesIO(csv_bytes), mimetype="text/csv", resumable=False)
        found = drive_find_by_name(svc, folder_id, filename)
        if found:
            svc.files().update(fileId=found["id"], media_body=media).execute()
            logging.info("[Drive] Updated: %s (%s)", filename, found["id"])
        else:
            meta = {"name": filename}
            if folder_id: meta["parents"] = [folder_id]
            f = svc.files().create(body=meta, media_body=media, fields="id").execute()
            logging.info("[Drive] Created: %s (%s)", filename, f.get("id"))
    except Exception as e:
        logging.warning("[Drive] 업서트 실패: %s", e)

def drive_download_prev(svc, folder_id: Optional[str], today: datetime) -> Optional[str]:
    try:
        name = csv_name(today - timedelta(days=1))
        found = drive_find_by_name(svc, folder_id, name)
        if not found:
            logging.info("[Drive] 전일 파일 없음: %s", name)
            return None
        req = svc.files().get_media(fileId=found["id"])
        local = os.path.join(DATA_DIR, name)
        with open(local, "wb") as f:
            downloader = MediaIoBaseDownload(f, req)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        logging.info("[Drive] 전일 CSV 다운로드: %s", local)
        return local
    except Exception as e:
        logging.warning("[Drive] 전일 다운로드 실패: %s", e)
        return None

# =========================
# 파싱 공통
# =========================
PRODUCT_URL_PAT = re.compile(r"(?:/product/|/goods/|[?&](?:goodsNo|itemNo|prodNo|productNo|goods_id|no)=)\d+")
CODE_PATS = [
    re.compile(r"[?&](?:goodsNo|itemNo|prodNo|productNo|goods_id|no)=(\d+)"),
    re.compile(r"/(?:product|goods)/(\d+)(?:[/?#]|$)"),
]

def clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def parse_prices(text: str) -> Tuple[Optional[int], Optional[int]]:
    nums = []
    for m in re.finditer(r"(?:₩\s*|)([\d,]+)\s*원|₩\s*([\d,]+)", text):
        g = m.group(1) or m.group(2)
        if g: nums.append(int(g.replace(",", "")))
    if not nums: return None, None
    nums.sort()
    sale = nums[0]
    orig = nums[-1] if len(nums) >= 2 and nums[-1] != sale else None
    return sale, orig

def calc_discount(price: Optional[int], orig: Optional[int], page_pct: Optional[int]=None) -> Optional[int]:
    if page_pct is not None: return int(page_pct)
    if price and orig and orig > 0 and price < orig:
        return int(math.floor((1 - (price/orig))*100))
    return None

def extract_code(url: str) -> str:
    for p in CODE_PATS:
        m = p.search(url)
        if m: return m.group(1)
    return ""

def extract_cards_from_html(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict] = []
    # “상품 URL 패턴”을 가진 앵커만 후보로 수집
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("/"): href = "https://www.daisomall.co.kr" + href
        if not PRODUCT_URL_PAT.search(href):  # 상품이 아니면 스킵
            continue
        card = a
        # 카드 루트 추정(가격/브랜드가 같이 보이는 상위 노드)
        for _ in range(5):
            if card.parent: card = card.parent
        text = clean(card.get_text(" "))
        price, orig = parse_prices(text)
        if price is None and orig is None:
            continue  # 가격 없는 링크(메뉴/배너) 제외

        # 표시 할인율
        pct = None
        m_pct = re.search(r"(\d{1,3})\s*%\s*할인|↓\s*(\d{1,3})\s*%", text)
        if m_pct: pct = int(m_pct.group(1) or m_pct.group(2))

        # 상품명
        name = clean(a.get_text()) or clean(card.get_text())
        # 브랜드(있으면)
        brand = ""
        for sel in [".brand", ".goods__brand", "[data-brand]", ".prd_brand", ".goods-brand"]:
            el = card.select_one(sel)
            if el:
                brand = clean(el.get_text()); break

        # 순위(텍스트 존재시)
        rank = None
        m = re.search(r"현재\s*순위\s*(\d+)", text) or re.search(r"\b(\d{1,3})\s*위\b", text)
        if m: rank = int(m.group(1))

        rows.append({
            "rank": rank,  # 나중에 정렬/보정
            "brand": brand,
            "product_name": name,
            "price": price,
            "orig_price": orig,
            "discount_percent": calc_discount(price, orig, pct),
            "url": href,
            "product_code": extract_code(href),
        })
        if len(rows) >= DAISO_MAX_RANK: break

    # 랭킹 보정: 텍스트 순위 없는 항목은 등장 순서로 부여
    # 중복 제거(key: product_code or url)
    uniq, out = set(), []
    order = 1
    for r in rows:
        key = r["product_code"] or r["url"]
        if key in uniq: continue
        uniq.add(key)
        if r["rank"] is None:
            r["rank"] = order
        order += 1
        out.append(r)
    out.sort(key=lambda x: int(x["rank"]))
    return out

# =========================
# HTTP → Playwright
# =========================
def http_fetch() -> List[Dict]:
    logging.info(f"[HTTP] GET {BASE_URL}")
    r = requests.get(BASE_URL, headers={
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/121.0.0.0 Safari/537.36"),
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7",
    }, timeout=20)
    r.raise_for_status()
    rows = extract_cards_from_html(r.text)
    logging.info(f"[HTTP] 파싱 개수: {len(rows)}")
    return rows

async def playwright_fetch() -> List[Dict]:
    logging.info("[PW] Playwright 폴백 진입")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox","--disable-dev-shm-usage","--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(
            viewport={"width": 1440, "height": 1100},
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"),
        )
        page = await ctx.new_page()
        await page.goto(BASE_URL, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)

        # 판매 탭으로 (혹시 다른 탭일 수도 있으니)
        for sel in ['button:has-text("판매")','[role="tab"]:has-text("판매")','a:has-text("판매")']:
            try:
                if await page.locator(sel).first.is_visible():
                    await page.locator(sel).first.click(timeout=SEL_TIMEOUT)
                    await page.wait_for_timeout(400); break
            except: pass

        # 뷰티/위생 선택
        clicked = False
        for sel in ['button:has-text("뷰티/위생")','li:has-text("뷰티/위생")','a:has-text("뷰티/위생")','[data-ga-label*="뷰티"]']:
            try:
                loc = page.locator(sel).first
                if await loc.is_visible():
                    await loc.click(timeout=SEL_TIMEOUT); clicked=True; break
            except: pass
        if not clicked:
            try: await page.get_by_text("뷰티/위생", exact=False).first.click(timeout=SEL_TIMEOUT)
            except: logging.info("[PW] 뷰티/위생 클릭 실패(이미 적용일 수 있음)")
        await page.wait_for_timeout(800)

        # 일간 탭
        clicked = False
        for sel in ['button:has-text("일간")','[role="tab"]:has-text("일간")','a:has-text("일간")','li:has-text("일간")']:
            try:
                loc = page.locator(sel).first
                if await loc.is_visible():
                    await loc.click(timeout=SEL_TIMEOUT); clicked=True; break
            except: pass
        if not clicked:
            try: await page.get_by_text("일간", exact=False).first.click(timeout=SEL_TIMEOUT)
            except: logging.info("[PW] 일간 클릭 실패(이미 일간일 수 있음)")
        await page.wait_for_timeout(1200)

        # 무한스크롤 (2회 연속 정지 검증)
        same, last = 0, 0
        for _ in range(SCROLL_ROUNDS):
            await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(SCROLL_WAIT_MS)
            h = await page.evaluate("() => document.body.scrollHeight")
            if h == last:
                same += 1
                if same >= 2: break
            else:
                same = 0
            last = h

        html = await page.content()
        await ctx.close(); await browser.close()

    rows = extract_cards_from_html(html)
    logging.info(f"[PW] 파싱 개수: {len(rows)}")
    return rows

# =========================
# 비교/Slack 포맷
# =========================
def load_prev_csv(path: Optional[str]) -> Optional[pd.DataFrame]:
    if not path or not os.path.exists(path): return None
    try: return pd.read_csv(path, dtype={"product_code": str})
    except Exception as e:
        logging.warning("[CSV] 전일 파일 로드 실패: %s", e); return None

def compute_comparison(df_cur: pd.DataFrame, df_prev: Optional[pd.DataFrame]):
    df = df_cur.copy()
    df["key"] = df["product_code"].fillna(""); df.loc[df["key"]=="","key"]=df["url"]

    if df_prev is None or df_prev.empty:
        df["prev_rank"] = None
    else:
        p = df_prev.copy()
        p["key"] = p["product_code"].fillna(""); p.loc[p["key"]=="","key"]=p["url"]
        mp = p.set_index("key")["rank"].to_dict()
        df["prev_rank"] = df["key"].map(mp)

    def _delta(r):
        if pd.isna(r["prev_rank"]): return None
        try: return int(r["prev_rank"]) - int(r["rank"])
        except: return None
    df["delta"] = df.apply(_delta, axis=1)

    top10 = df.nsmallest(10, "rank")

    up = df.dropna(subset=["prev_rank"]); up = up[up["delta"]>0]
    up = up.sort_values(["delta","rank","prev_rank","product_name"], ascending=[False,True,True,True]).head(3)

    new_in = df[(df["rank"]<=30) & ((df["prev_rank"].isna()) | (df["prev_rank"]>30))]
    new_in = new_in.sort_values(["rank"]).head(3)

    down = df.dropna(subset=["prev_rank"]).copy()
    down["drop"] = down["rank"] - down["prev_rank"]
    down = down[down["drop"]>0]
    down = down.sort_values(["drop","rank","prev_rank","product_name"], ascending=[False,True,True,True]).head(5)

    out_prev_top = df.dropna(subset=["prev_rank"])
    out_prev_top = out_prev_top[(out_prev_top["prev_rank"]<=30) & (out_prev_top["rank"]>30)]

    if df_prev is not None and not df_prev.empty:
        cur_keys = set(df["key"].tolist())
        prev_top = df_prev.copy()
        prev_top["key"]=prev_top["product_code"].fillna(""); prev_top.loc[prev_top["key"]=="","key"]=prev_top["url"]
        prev_top = prev_top[prev_top["rank"]<=30]
        missing = prev_top[~prev_top["key"].isin(cur_keys)]
        if not missing.empty:
            missing = missing.assign(rank=10_000, prev_rank=missing["rank"])
            out_prev_top = pd.concat([out_prev_top, missing], ignore_index=True)

    return (top10, up, new_in, down, out_prev_top,
            len(new_in), len(out_prev_top))

def build_slack_blocks(df, top10, up, new_in, down, out_prev_top, in_cnt, out_cnt):
    def price_line(r):
        price = fmt_won(r.get("price"))
        dp = r.get("discount_percent")
        return f"{price} (↓{int(dp)}%)" if (dp is not None and not pd.isna(dp)) else price

    def link_txt(r):
        bn = str(r.get("brand") or "").strip()
        nm = str(r.get("product_name") or "").strip()
        title = f"{bn} {nm}".strip() if bn else nm
        url = r.get("url") or ""
        return f"<{url}|{title}>" if url else title

    top_lines = [f"{int(r['rank'])}. {link_txt(r)} — {price_line(r)}"
                 for _, r in top10.sort_values("rank").iterrows()]
    up_lines  = [f"- {link_txt(r)} {int(r['prev_rank'])}위 → {int(r['rank'])}위 (↑{int(r['delta'])})"
                 for _, r in up.iterrows()]
    new_lines = []
    for _, r in new_in.iterrows():
        prev = r.get("prev_rank")
        new_lines.append(f"- {link_txt(r)} {'NEW' if pd.isna(prev) else str(int(prev))+'위'} → {int(r['rank'])}위")
    down_lines = [f"- {link_txt(r)} {int(r['prev_rank'])}위 → {int(r['rank'])}위 (↓{int(r['drop'])})"
                  for _, r in down.iterrows()]
    if not out_prev_top.empty:
        if down_lines: down_lines.append("")
        for _, r in out_prev_top.sort_values("prev_rank").iterrows():
            down_lines.append(f"- {link_txt(r)} {int(r['prev_rank'])}위 → OUT")

    blocks = [
        {"type":"header","text":{"type":"plain_text","text":"다이소몰 뷰티/화장품 랭킹 (일간)"}},
        {"type":"context","elements":[{"type":"mrkdwn","text":f"*수집일*: {kst_date_str()} (KST) • *출처*: 다이소몰"}]},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*TOP 10*"}},
        {"type":"section","text":{"type":"mrkdwn","text":"\n".join(top_lines) or "_데이터 없음_"}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*급상승*"}},
        {"type":"section","text":{"type":"mrkdwn","text":"\n".join(up_lines) or "_없음_"}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*뉴랭커*"}},
        {"type":"section","text":{"type":"mrkdwn","text":"\n".join(new_lines) or "_없음_"}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*급하락 (OUT 포함)*"}},
        {"type":"section","text":{"type":"mrkdwn","text":"\n".join(down_lines) or "_없음_"}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":f"*랭크 인&아웃*: {in_cnt + out_cnt}개의 제품이 인&아웃 되었습니다."}},
    ]
    return blocks

# =========================
# 메인 파이프라인
# =========================
def run_pipeline():
    logging.info(f"[시작] 수집 시작 URL: {BASE_URL}")

    # 1) HTTP
    rows: List[Dict] = []
    try:
        rows = http_fetch()
    except Exception as e:
        logging.warning("[HTTP] 오류: %s", e)

    use_fallback = len(rows) < HTTP_MIN_CARDS
    logging.info(f"[판단] [HTTP] 수집 개수={len(rows)} / 폴백필요={use_fallback}")

    # 2) Playwright 폴백
    if use_fallback:
        try:
            rows = asyncio.run(playwright_fetch())
        except Exception as e:
            logging.exception("[PW] 폴백 실패")
            slack_post_failure(f"Playwright 폴백 실패: {e}")
            raise

    if len(rows) < 100:
        logging.warning(f"[검증] 수집 개수 {len(rows)} (<100). 계속 진행하지만 확인 필요.")

    # 3) DataFrame + 저장
    today = datetime.now(KST)
    for r in rows: r["date"] = kst_date_str(today)

    df = pd.DataFrame(rows, columns=[
        "date","rank","brand","product_name","price","orig_price",
        "discount_percent","url","product_code"
    ])
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["orig_price"] = pd.to_numeric(df["orig_price"], errors="coerce")
    df["discount_percent"] = pd.to_numeric(df["discount_percent"], errors="coerce")

    path = csv_path(today); df.to_csv(path, index=False, encoding="utf-8-sig")
    logging.info(f"[저장] 로컬 저장: {path}")

    # 4) Drive 업/다운
    folder_id = normalize_folder_id(GDRIVE_FOLDER_ID_RAW)
    svc = build_drive_service()
    prev_path = None
    if svc:
        try: prev_path = drive_download_prev(svc, folder_id, today)
        except Exception as e: logging.warning("[Drive] 전일 다운로드 실패: %s", e)
        try:
            with open(path,"rb") as f: csv_bytes = f.read()
            drive_upsert_csv(svc, folder_id, os.path.basename(path), csv_bytes)
        except Exception as e: logging.warning("[Drive] 업로드 실패: %s", e)
    else:
        logging.info("[Drive] 서비스 미구성 → 업/다운로드 생략")

    # 5) 비교 + Slack
    df_prev = load_prev_csv(prev_path) if prev_path else None
    top10, up, new_in, down, out_prev_top, in_cnt, out_cnt = compute_comparison(df, df_prev)
    blocks = build_slack_blocks(df, top10, up, new_in, down, out_prev_top, in_cnt, out_cnt)
    slack_post(blocks, "다이소몰 랭킹 업데이트")

    logging.info("[완료] 슬랙 전송 완료")

if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logging.exception("[Fatal] 파이프라인 실패")
        try: slack_post_failure(str(e))
        except: pass
        sys.exit(1)
