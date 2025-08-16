# -*- coding: utf-8 -*-
import asyncio
import os
import re
import sys
import math
import json
import time
import logging
from io import BytesIO
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup
from pytz import timezone

# ---- Google Drive ----
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request as GARequest
from google.oauth2.credentials import Credentials

# ---- Playwright (fallback) ----
from playwright.async_api import async_playwright

# =========================
# 설정
# =========================
KST = timezone("Asia/Seoul")
BASE_URL = "https://www.daisomall.co.kr/ds/rank/C105"  # 뷰티/화장품 랭킹
DATA_DIR = os.path.join(os.getcwd(), "data")
os.makedirs(DATA_DIR, exist_ok=True)

DAISO_MAX_RANK = int(os.getenv("DAISO_MAX_RANK", "200"))
HTTP_MIN_CARDS = 30  # HTTP 성공 기준

HEADLESS = True
NAV_TIMEOUT = 30_000
SEL_TIMEOUT = 10_000
SCROLL_ROUNDS = 20
SCROLL_WAIT_MS = 800

# Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# Google Drive
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")
GDRIVE_FOLDER_ID_RAW = os.getenv("GDRIVE_FOLDER_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# =========================
# 유틸
# =========================
def kst_today() -> datetime:
    return datetime.now(KST)

def date_str(d: Optional[datetime] = None) -> str:
    d = d or kst_today()
    return d.strftime("%Y-%m-%d")

def csv_name_for(d: Optional[datetime] = None) -> str:
    return f"다이소몰_랭킹_{date_str(d)}.csv"

def local_csv_path(d: Optional[datetime] = None) -> str:
    return os.path.join(DATA_DIR, csv_name_for(d))

def fmt_won(v: Optional[int]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return f"₩{int(v):,}"

def extract_gdrive_folder_id(raw: str) -> Optional[str]:
    if not raw:
        return None
    # https://drive.google.com/drive/folders/<ID>
    m = re.search(r"/folders/([A-Za-z0-9_\-]{10,})", raw)
    if m:
        return m.group(1)
    # ID 만 넘어온 경우
    return raw

def build_drive_service() -> Optional[any]:
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        logging.warning("[Drive] 자격 증명 환경변수가 없어 업로드/다운로드를 건너뜁니다.")
        return None
    creds = Credentials(
        None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    creds.refresh(GARequest())
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def drive_find_file(service, folder_id: str, name: str) -> Optional[Dict]:
    q = f"name = '{name}' and '{folder_id}' in parents and trashed = false"
    resp = service.files().list(q=q, fields="files(id,name,modifiedTime,size)").execute()
    files = resp.get("files", [])
    return files[0] if files else None

def drive_upload(service, folder_id: str, local_path: str, name: str):
    found = drive_find_file(service, folder_id, name)
    media = MediaFileUpload(local_path, mimetype="text/csv", resumable=False)
    if found:
        file_id = found["id"]
        service.files().update(fileId=file_id, media_body=media).execute()
        logging.info(f"[Drive] Updated: {name} ({file_id})")
        return file_id, "update"
    else:
        meta = {"name": name, "parents": [folder_id]}
        file = service.files().create(body=meta, media_body=media, fields="id").execute()
        logging.info(f"[Drive] Created: {name} ({file['id']})")
        return file["id"], "create"

def drive_download_prev(service, folder_id: str, target_date: datetime) -> Optional[str]:
    # 전일 파일 우선
    yest = target_date - timedelta(days=1)
    y_name = csv_name_for(yest)
    found = drive_find_file(service, folder_id, y_name)
    if not found:
        logging.info("[Drive] 전일 파일이 없어 비교는 건너뜁니다.")
        return None
    file_id = found["id"]
    req = service.files().get_media(fileId=file_id)
    local_path = os.path.join(DATA_DIR, y_name)
    with open(local_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    logging.info(f"[Drive] 전일 CSV 다운로드: {local_path}")
    return local_path

def slack_post(text_blocks: List[Dict], fallback_text: str):
    if not SLACK_WEBHOOK_URL:
        logging.warning("[Slack] SLACK_WEBHOOK_URL 미설정으로 전송 건너뜀.")
        return
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": fallback_text, "blocks": text_blocks}, timeout=15)
        if resp.status_code >= 300:
            logging.error(f"[Slack] Webhook 실패: {resp.status_code} {resp.text}")
    except Exception as e:
        logging.exception(f"[Slack] 전송 예외: {e}")

def slack_post_failure(msg: str):
    blocks = [
        {"type":"header","text":{"type":"plain_text","text":"다이소몰 랭킹 수집 실패"}},
        {"type":"section","text":{"type":"mrkdwn","text":f"*사유*: {msg}"}},
        {"type":"context","elements":[{"type":"mrkdwn","text":f"{date_str()} KST"}]},
    ]
    slack_post(blocks, f"다이소몰 랭킹 수집 실패: {msg}")

# =========================
# 파싱 로직 (공통)
# =========================
PRICE_PAT = re.compile(r"(?:₩\s*|\s*)([\d,]+)\s*원|₩\s*([\d,]+)")
CODE_PATS = [
    re.compile(r"[?&](?:goodsNo|itemNo|prodNo|productNo|goods_id|no)=(\d+)"),
    re.compile(r"/(?:product|goods)/(\d+)(?:[/?#]|$)"),
]

def parse_prices(text: str) -> Tuple[Optional[int], Optional[int]]:
    nums = []
    for m in re.finditer(r"(?:₩\s*|)([\d,]+)\s*원|₩\s*([\d,]+)", text):
        g = m.group(1) or m.group(2)
        if g:
            nums.append(int(g.replace(",", "")))
    if not nums:
        return None, None
    nums = sorted(nums)
    sale = nums[0]
    orig = nums[-1] if len(nums) >= 2 and nums[-1] != sale else None
    return sale, orig

def calc_discount(price: Optional[int], orig: Optional[int], page_pct: Optional[int]=None) -> Optional[int]:
    if page_pct is not None:
        return int(page_pct)
    if price and orig and orig > 0 and price < orig:
        return int(math.floor((1 - (price / orig)) * 100))
    return None

def extract_code(url: str) -> str:
    if not url:
        return ""
    for pat in CODE_PATS:
        m = pat.search(url)
        if m:
            return m.group(1)
    return ""

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def pick_brand_name(card: BeautifulSoup) -> Tuple[str, str]:
    # 브랜드: 전용 클래스 우선
    for sel in [".brand", ".goods__brand", "[data-brand]", ".prd_brand", ".goods-brand"]:
        el = card.select_one(sel)
        if el:
            return clean_text(el.get_text()), ""
    # 텍스트 앵커 중 상품 링크가 아닌 첫 앵커를 브랜드 추정
    for a in card.select("a"):
        txt = clean_text(a.get_text())
        if txt and not re.search(r"(상세|구매|장바구니|리뷰|쿠폰|옵션|바로가기)", txt):
            # 상품 링크일 수 있으니 product_name 후보로도 반환
            return "", txt
    return "", ""

def extract_cards_from_html(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    # 카드 후보
    cards = soup.select("li, div")
    rows = []
    rank_counter = 1
    for c in cards:
        # 랭킹 힌트
        text = clean_text(c.get_text(" "))
        if not text:
            continue

        # URL & 이름
        prod_a = None
        for a in c.select("a[href]"):
            a_txt = clean_text(a.get_text())
            if a_txt and not re.search(r"(장바구니|리뷰|구매|쿠폰|옵션|보기|픽업|배송|상세)", a_txt):
                prod_a = a
                break
        if not prod_a:
            continue
        href = prod_a.get("href", "")
        if href.startswith("/"):
            href = "https://www.daisomall.co.kr" + href
        product_name = clean_text(prod_a.get_text())

        # 가격
        price, orig = parse_prices(text)
        # 카드 표기 할인율(있으면 우선)
        pct = None
        m_pct = re.search(r"(\d{1,2,3})\s*%\s*할인|↓\s*(\d{1,3})\s*%", text)
        if m_pct:
            pct = int(m_pct.group(1) or m_pct.group(2))

        discount = calc_discount(price, orig, pct)

        # 브랜드/보조명 추정
        brand, name2 = pick_brand_name(c)
        if not brand and name2 and name2 != product_name and len(name2) <= 25:
            # name2가 짧으면 브랜드로 채택
            brand = name2

        # rank 추출(배지, 숫자, '현재 순위')
        rank = None
        m = re.search(r"현재\s*순위\s*(\d+)", text)
        if not m:
            m = re.search(r"\b(\d{1,3})\s*위\b", text)
        if m:
            rank = int(m.group(1))
        # 없으면 카드 순서로 할당
        if rank is None:
            rank = rank_counter
        rank_counter += 1

        rows.append({
            "rank": rank,
            "brand": brand,
            "product_name": product_name,
            "price": price,
            "orig_price": orig,
            "discount_percent": discount,
            "url": href,
            "product_code": extract_code(href),
        })
        if len(rows) >= DAISO_MAX_RANK:
            break
    # 순위로 정렬/중복 제거
    rows = [r for r in rows if isinstance(r["rank"], int)]
    rows.sort(key=lambda x: x["rank"])
    uniq, out = set(), []
    for r in rows:
        key = (r["product_code"] or r["url"])
        if key in uniq:
            continue
        uniq.add(key)
        out.append(r)
    return out

# =========================
# HTTP 수집 → 폴백
# =========================
def http_fetch() -> List[Dict]:
    logging.info(f"[HTTP] GET {BASE_URL}")
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"),
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    r = requests.get(BASE_URL, headers=headers, timeout=20)
    r.raise_for_status()
    rows = extract_cards_from_html(r.text)
    logging.info(f"[HTTP] 파싱 개수: {len(rows)}")
    return rows

async def playwright_fetch() -> List[Dict]:
    logging.info("[PW] Playwright 폴백 진입")
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1400, "height": 1000},
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
        )
        page = await context.new_page()
        await page.goto(BASE_URL, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
        await page.wait_for_timeout(1200)

        # 탭 전환: '뷰티/위생' → '일간'
        try:
            await page.get_by_text("뷰티/위생", exact=False).first.click(timeout=SEL_TIMEOUT)
            await page.wait_for_timeout(500)
        except Exception:
            pass
        try:
            # 기본이 '급상승'일 수 있으므로 '일간' 탭 클릭
            await page.get_by_text("일간", exact=False).first.click(timeout=SEL_TIMEOUT)
            await page.wait_for_timeout(800)
        except Exception:
            pass

        # 스크롤로 최대 로드
        last_h = 0
        for _ in range(SCROLL_ROUNDS):
            await page.mouse.wheel(0, 5000)
            await page.wait_for_timeout(SCROLL_WAIT_MS)
            h = await page.evaluate("() => document.body.scrollHeight")
            if h == last_h:
                break
            last_h = h

        # 카드 컨테이너 HTML 수집(범용 셀렉터)
        html = await page.content()
        await context.close()
        await browser.close()

    rows = extract_cards_from_html(html)
    logging.info(f"[PW] 파싱 개수: {len(rows)}")
    return rows

# =========================
# 비교/선정 로직
# =========================
def load_prev_csv(path: str) -> Optional[pd.DataFrame]:
    if not path or not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path, dtype={"product_code": str})
        return df
    except Exception as e:
        logging.warning(f"[CSV] 전일 파일 로드 실패: {e}")
        return None

def compute_comparison(df_cur: pd.DataFrame, df_prev: Optional[pd.DataFrame]):
    # 공통 키: product_code 우선, 없으면 url
    df_cur["key"] = df_cur["product_code"].fillna("")
    df_cur.loc[df_cur["key"] == "", "key"] = df_cur["url"]
    if df_prev is None or df_prev.empty:
        df_cur["prev_rank"] = None
    else:
        df_prev = df_prev.copy()
        df_prev["key"] = df_prev["product_code"].fillna("")
        df_prev.loc[df_prev["key"] == "", "key"] = df_prev["url"]
        prev_map = df_prev.set_index("key")["rank"].to_dict()
        df_cur["prev_rank"] = df_cur["key"].map(prev_map)

    def delta(row):
        try:
            if pd.isna(row["prev_rank"]):
                return None
            return int(row["prev_rank"]) - int(row["rank"])
        except Exception:
            return None

    df_cur["delta"] = df_cur.apply(delta, axis=1)

    # 선정 세트
    top10 = df_cur.nsmallest(10, "rank")

    # 급상승: prev/cur 모두 존재 & 개선폭 > 0
    up = df_cur.dropna(subset=["prev_rank"]).copy()
    up = up[up["delta"] > 0]
    up = up.sort_values(by=["delta", "rank", "prev_rank", "product_name"],
                        ascending=[False, True, True, True]).head(3)

    # 뉴랭커: 전일 Top30 밖(또는 미등장) → 당일 ≤ 30
    new_in = df_cur[(df_cur["rank"] <= 30) & ((df_cur["prev_rank"].isna()) | (df_cur["prev_rank"] > 30))]
    new_in = new_in.sort_values(by=["rank"]).head(3)

    # 급하락: prev/cur 존재 & 하락폭 > 0
    down = df_cur.dropna(subset=["prev_rank"]).copy()
    down["drop"] = down["rank"] - down["prev_rank"]
    down = down[down["drop"] > 0]
    down = down.sort_values(by=["drop", "rank", "prev_rank", "product_name"],
                            ascending=[False, True, True, True]).head(5)

    # OUT: 전일 Top30이었으나 오늘 Top30 밖
    out_prev_top = df_cur.dropna(subset=["prev_rank"]).copy()
    out_prev_top = out_prev_top[(out_prev_top["prev_rank"] <= 30) & (out_prev_top["rank"] > 30)]
    # 전일 존재하지만 오늘 완전히 사라진 항목(키 기준) 확인
    if df_prev is not None and not df_prev.empty:
        cur_keys = set(df_cur["key"].tolist())
        prev_top = df_prev.copy()
        prev_top["key"] = prev_top["product_code"].fillna("")
        prev_top.loc[prev_top["key"] == "", "key"] = prev_top["url"]
        prev_top = prev_top[prev_top["rank"] <= 30]
        missing = prev_top[~prev_top["key"].isin(cur_keys)]
        # rank>30로 간주하여 OUT 취급
        if not missing.empty:
            missing = missing.assign(rank=10_000)
            missing = missing.assign(prev_rank=missing["rank"])
            out_prev_top = pd.concat([out_prev_top, missing], ignore_index=True)

    # 인/아웃 개수
    in_count = len(new_in)
    out_count = len(out_prev_top)

    return top10, up, new_in, down, out_prev_top, in_count, out_count

def build_slack_blocks(df: pd.DataFrame,
                       top10: pd.DataFrame,
                       up: pd.DataFrame,
                       new_in: pd.DataFrame,
                       down: pd.DataFrame,
                       out_prev_top: pd.DataFrame,
                       in_count: int, out_count: int) -> List[Dict]:
    # 공통: 할인 표기
    def price_line(row):
        price = fmt_won(row.get("price"))
        dp = row.get("discount_percent")
        if pd.isna(dp) or dp is None:
            return price
        return f"{price} (↓{int(dp)}%)"

    # 링크 텍스트
    def link_txt(row):
        bn = str(row.get("brand") or "").strip()
        nm = str(row.get("product_name") or "").strip()
        title = f"{bn} {nm}".strip() if bn else nm
        url = row.get("url") or ""
        if url:
            return f"<{url}|{title}>"
        return title

    # TOP 10
    top_lines = []
    for _, r in top10.sort_values("rank").iterrows():
        top_lines.append(f"{int(r['rank'])}. {link_txt(r)} — {price_line(r)}")
    top_md = "\n".join(top_lines) if top_lines else "_데이터 없음_"

    # 급상승
    up_lines = []
    for _, r in up.iterrows():
        up_lines.append(f"- {link_txt(r)} {int(r['prev_rank'])}위 → {int(r['rank'])}위 (↑{int(r['delta'])})")
    up_md = "\n".join(up_lines) if up_lines else "_없음_"

    # 뉴랭커
    new_lines = []
    for _, r in new_in.iterrows():
        prev = r.get("prev_rank")
        if pd.isna(prev):
            new_lines.append(f"- {link_txt(r)} NEW → {int(r['rank'])}위")
        else:
            new_lines.append(f"- {link_txt(r)} {int(prev)}위 → {int(r['rank'])}위")
    new_md = "\n".join(new_lines) if new_lines else "_없음_"

    # 급하락 (+ OUT 함께)
    down_lines = []
    for _, r in down.iterrows():
        drop = int(r["drop"])
        down_lines.append(f"- {link_txt(r)} {int(r['prev_rank'])}위 → {int(r['rank'])}위 (↓{drop})")
    # OUT
    if not out_prev_top.empty:
        down_lines.append("")  # 줄바꿈
        for _, r in out_prev_top.sort_values("prev_rank").iterrows():
            prev = int(r["prev_rank"])
            down_lines.append(f"- {link_txt(r)} {prev}위 → OUT")
    down_md = "\n".join(down_lines) if down_lines else "_없음_"

    # 헤더/섹션
    title = "다이소몰 뷰티/화장품 랭킹 (일간)"
    blocks = [
        {"type":"header","text":{"type":"plain_text","text":title}},
        {"type":"context","elements":[{"type":"mrkdwn","text":f"*수집일*: {date_str()} (KST) • *출처*: 다이소몰"}]},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*TOP 10*"}},
        {"type":"section","text":{"type":"mrkdwn","text":top_md}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*급상승*"}},
        {"type":"section","text":{"type":"mrkdwn","text":up_md}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*뉴랭커*"}},
        {"type":"section","text":{"type":"mrkdwn","text":new_md}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*급하락 (OUT 포함)*"}},
        {"type":"section","text":{"type":"mrkdwn","text":down_md}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":f"*랭크 인&아웃*: {in_count + out_count}개의 제품이 인&아웃 되었습니다."}},
    ]
    return blocks

# =========================
# 메인
# =========================
def run_pipeline():
    logging.info(f"[시작] 수집 시작 URL: {BASE_URL}")

    # 1) HTTP 우선
    try:
        rows = http_fetch()
    except Exception as e:
        logging.exception("[HTTP] 오류")
        rows = []

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

    # 3) DataFrame 정리 + 저장
    today = kst_today()
    for r in rows:
        r["date"] = date_str(today)

    df = pd.DataFrame(rows, columns=[
        "date","rank","brand","product_name","price","orig_price",
        "discount_percent","url","product_code"
    ])
    # 타입 보정
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["orig_price"] = pd.to_numeric(df["orig_price"], errors="coerce")
    df["discount_percent"] = pd.to_numeric(df["discount_percent"], errors="coerce")

    local_path = local_csv_path(today)
    df.to_csv(local_path, index=False, encoding="utf-8-sig")
    logging.info(f"[저장] 로컬 저장: {local_path}")

    # 4) Drive 업/다운
    folder_id = extract_gdrive_folder_id(GDRIVE_FOLDER_ID_RAW)
    service = build_drive_service() if folder_id else None
    prev_path = None
    if service and folder_id:
        try:
            prev_path = drive_download_prev(service, folder_id, today)
        except Exception as e:
            logging.warning(f"[Drive] 전일 CSV 다운로드 실패: {e}")
        try:
            fid, op = drive_upload(service, folder_id, local_path, os.path.basename(local_path))
            logging.info(f"[Drive] 업로드 완료({op}) 파일ID={fid}")
        except Exception as e:
            logging.warning(f"[Drive] 업로드 실패: {e}")
    else:
        logging.info("[Drive] 서비스 미구성: 업/다운로드 생략")

    # 5) 비교/슬랙 공지
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
        try:
            slack_post_failure(str(e))
        except:
            pass
        sys.exit(1)
