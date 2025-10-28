# -*- coding: utf-8 -*-
"""
daisorang/app.py
- 다이소몰 베스트 페이지(C105 등)에서 Playwright로 무한 스크롤/더보기 버튼 처리해 Top200 확보
- analyze_trends() 반환 6개(언팩 에러 영구 해결)
- IN≡OUT 강제 + 인&아웃 슬랙 요약(볼드)
- 구글드라이브 업로드 후 요약 로그

필수 환경변수:
  SITE_URL=https://www.daisomall.co.kr/ds/rank/C105
  TOPN=200
  SLACK_WEBHOOK_URL=...
  GDRIVE_FOLDER_ID=...
  GOOGLE_APPLICATION_CREDENTIALS=...(서비스계정 json 경로)  # 또는 ADC

GitHub Actions 러너에서 Playwright 세팅:
  - pip install playwright
  - python -m playwright install --with-deps chromium
"""

import os, sys, csv, time, traceback, re
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

SITE_URL = os.getenv("SITE_URL", "https://www.daisomall.co.kr/ds/rank/C105")
SITE_NAME = os.getenv("SITE_NAME", "daisomall")
TOPN      = int(os.getenv("TOPN", "200"))
OUT_DIR   = os.getenv("OUT_DIR", "data")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID  = os.getenv("GDRIVE_FOLDER_ID", "")

# -------------------- 공용 유틸 --------------------
def kst_now(fmt="%Y-%m-%d"):
    return datetime.now(KST).strftime(fmt)

def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def safe_int(v, default=999999):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(str(v).replace(",", "")))
        except Exception:
            return default

# -------------------- Playwright 수집 --------------------
def scrape_rank_with_playwright(url: str, want: int = 200, max_loops: int = 120) -> List[Dict[str, Any]]:
    """
    - Chromium headless 로드
    - .card__inner 요소를 계속 늘리며 스크롤(또는 '더보기' 클릭)
    - 최소 want개(기본 200) 확보 시 종료
    """
    from playwright.sync_api import sync_playwright

    items: Dict[str, Dict[str, Any]] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        ctx = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ))
        page = ctx.new_page()
        page.set_default_timeout(20000)

        page.goto(url, wait_until="domcontentloaded")
        # 첫 카드 뜰 때까지
        page.wait_for_selector("div.card__inner")

        prev_count = 0
        loops = 0

        while True:
            loops += 1
            # 더보기 버튼 시도
            try:
                # 텍스트가 '더보기' 인 버튼이 있으면 눌러준다.
                btn = page.locator("button:has-text('더보기')")
                if awaitable(btn):  # type: ignore
                    pass
            except Exception:
                pass
            try:
                page.locator("button:has-text('더보기')").first.click(timeout=1500)
                time.sleep(0.6)
            except Exception:
                # 없으면 스크롤
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(0.9)

            # 현재 카드 수
            count = page.locator("div.card__inner").count()
            # 새로 안 늘어나면 한 번 더 강제 스크롤
            if count <= prev_count:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(0.9)
                count = page.locator("div.card__inner").count()

            # 추출
            for i in range(count):
                card = page.locator("div.card__inner").nth(i)
                # 이름은 img[alt]가 가장 안정적
                name = ""
                try:
                    name = card.locator("img[alt]").first.get_attribute("alt") or ""
                except Exception:
                    pass
                # 링크
                href = ""
                try:
                    href = card.locator("a.detail-link").first.get_attribute("href") or ""
                except Exception:
                    pass
                # 가격
                price_txt = ""
                try:
                    price_txt = (card.locator(".price-value .value").first.inner_text(timeout=500) or "").strip()
                except Exception:
                    pass
                # 랭크(없으면 i+1)
                rank_txt = ""
                try:
                    rank_txt = (card.locator(".rank .num").first.inner_text(timeout=300) or "").strip()
                except Exception:
                    pass
                rank_val = safe_int(rank_txt if rank_txt else (i + 1))

                # key: pdNo 우선
                key = ""
                m = re.search(r"pdNo=(\d+)", href or "")
                if m:
                    key = m.group(1)
                else:
                    key = href or name  # 차선

                if not key:
                    continue

                items[key] = {
                    "key": key,
                    "raw_name": name,
                    "rank": rank_val,
                    "url": ("https://www.daisomall.co.kr" + href) if href and href.startswith("/") else href,
                    "price": price_txt.replace(",", ""),
                }

            if len(items) >= want:
                break

            # 루프 종료 조건
            if loops >= max_loops:
                break
            if count == prev_count:
                # 더 이상 늘어나지 않음
                break
            prev_count = count

        browser.close()

    # 정렬/슬라이스
    arr = list(items.values())
    arr = [r for r in arr if isinstance(r.get("rank"), (int, float))]
    arr.sort(key=lambda r: safe_int(r.get("rank")))
    return arr[:want]

# -------------------- 전일 로딩/분석/저장 --------------------
def load_prev_list(csv_path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(csv_path):
        return []
    out = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            r = dict(r)
            r["rank"] = safe_int(r.get("rank"))
            out.append(r)
    return out

def analyze_trends(rows: List[Dict[str, Any]],
                   prev_list: List[Dict[str, Any]],
                   topN: int = TOPN
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], int]:
    rows = [r for r in rows if isinstance(r.get("rank"), (int, float))]
    rows.sort(key=lambda r: safe_int(r.get("rank")))
    today_aug = rows[:topN]

    def to_map(lst):
        m = {}
        for it in lst:
            key = str(it.get("key") or it.get("url") or it.get("id") or "")
            if key:
                m[key] = it
        return m

    prev_map = to_map(prev_list or [])
    today_map = to_map(today_aug)

    ups, downs = [], []
    for key, today in today_map.items():
        if key in prev_map:
            pr = safe_int(prev_map[key].get("rank"))
            tr = safe_int(today.get("rank"))
            delta = pr - tr  # 양수=상승
            base = {
                "key": key,
                "raw_name": today.get("raw_name") or "",
                "rank": tr,
                "prev_rank": pr,
                "delta": delta,
                "url": today.get("url") or prev_map[key].get("url") or ""
            }
            if delta > 0:
                ups.append(base)
            elif delta < 0:
                downs.append(base)

    prev_keys = set(prev_map.keys())
    today_keys = set(today_map.keys())
    ins_keys = list(today_keys - prev_keys)
    outs_keys = list(prev_keys - today_keys)

    chart_ins = [today_map[k] for k in ins_keys if k in today_map]
    rank_outs = [prev_map[k] for k in outs_keys if k in prev_map]

    io_cnt = min(len(chart_ins), len(rank_outs))
    if len(chart_ins) != len(rank_outs):
        chart_ins = chart_ins[:io_cnt]
        rank_outs = rank_outs[:io_cnt]

    ups.sort(key=lambda x: (-x["delta"], x["rank"]))
    downs.sort(key=lambda x: (x["delta"], x["rank"]))
    chart_ins.sort(key=lambda r: safe_int(r.get("rank")))
    rank_outs.sort(key=lambda r: safe_int(r.get("rank")))

    return today_aug, ups, downs, chart_ins, rank_outs, io_cnt

def save_csv(rows: List[Dict[str, Any]], path: str):
    ensure_dir(path)
    headers = sorted({k for r in rows for k in r.keys()}) if rows else \
        ["key","raw_name","rank","url","price"]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# -------------------- 구글드라이브/슬랙 --------------------
def upload_to_gdrive(file_path: str, folder_id: str = GDRIVE_FOLDER_ID) -> Optional[str]:
    if not folder_id:
        print("[경고] GDRIVE_FOLDER_ID 미설정 → 업로드 생략")
        return None
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        scopes = ["https://www.googleapis.com/auth/drive.file"]
        sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        if sa_path and os.path.exists(sa_path):
            creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
        else:
            from google.auth import default
            creds, _ = default(scopes=scopes)

        service = build("drive", "v3", credentials=creds)
        meta = {"name": os.path.basename(file_path), "parents": [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(body=meta, media_body=media, fields="id").execute()
        fid = file.get("id")
        print(f"[Drive] 업로드 성공: {os.path.basename(file_path)} (id={fid})")
        return fid
    except Exception as e:
        print("[Drive] 업로드 실패:", e)
        traceback.print_exc()
        return None

def post_to_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 슬랙 생략")
        return
    try:
        import requests
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("[슬랙] 전송 실패:", e)

def build_slack_inout(io_cnt: int) -> str:
    return ":양방향_화살표: 랭크 인&아웃\n" + f"**{io_cnt}개의 제품이 인&아웃 되었습니다.**"

def build_slack_message(io_cnt: int) -> str:
    title = f"📊 {SITE_NAME} Top{TOPN} ({kst_now('%Y-%m-%d')})"
    return f"*{title}*\n{build_slack_inout(io_cnt)}"

# -------------------- 메인 --------------------
def main():
    print("수집 시작:", SITE_URL)

    # 1) Playwright로 직접 200개 확보
    rows = scrape_rank_with_playwright(SITE_URL, want=TOPN, max_loops=160)
    print(f"[수집 결과] {len(rows)}개")

    if len(rows) < TOPN:
        # 네트워크/구조 변경 시 바로 알 수 있게 명확히 중단
        raise RuntimeError(f"[수집 에러] Top{TOPN} 보장 실패: 현재 {len(rows)}개. 소스 구조 변경/차단 가능성. 로그 확인 필요.")

    # 2) 전일 CSV 로딩
    date_tag = kst_now("%Y-%m-%d")
    prev_tag = (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
    out_csv  = os.path.join(OUT_DIR, f"{SITE_NAME}_뷰티위생_일간_{date_tag}.csv")
    prev_csv = os.path.join(OUT_DIR, f"{SITE_NAME}_뷰티위생_일간_{prev_tag}.csv")
    prev_list = load_prev_list(prev_csv)
    print(f"[전일 로딩] {len(prev_list)}개")

    # 3) 분석(반환 6개)
    today_aug, ups, downs, chart_ins, rank_outs, io_cnt = analyze_trends(rows, prev_list, topN=TOPN)

    # 4) 저장/업로드/슬랙
    save_csv(today_aug, out_csv)
    print("[-] CSV 저장:", out_csv)
    fid = upload_to_gdrive(out_csv, folder_id=GDRIVE_FOLDER_ID)

    slack_text = build_slack_message(io_cnt)
    post_to_slack(slack_text)

    print(f"[요약] 상승 {len(ups)} / 하락 {len(downs)} / IN {io_cnt} / OUT {io_cnt}")
    if fid:
        print(f"[Drive] file_id={fid}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("에러:", e)
        traceback.print_exc()
        sys.exit(1)
