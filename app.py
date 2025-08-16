# -*- coding: utf-8 -*-
"""
다이소몰 뷰티/위생 일간 랭킹 크롤러
- 수집 URL: https://www.daisomall.co.kr/ds/rank/C105
- 수집 후 CSV 저장: data/다이소몰_뷰티위생_일간_YYYY-MM-DD.csv (KST)
- 구글드라이브 업로드 (리포 시크릿 사용)
- 슬랙 포맷: (올리브영 버전과 동일)
    * 제목/소제목 **굵게**
    * TOP 10 → 급상승(상위3) → 뉴랭커(상위3 권장) → 급하락(상위5) → 랭크 인&아웃(개수만)
    * TOP10 라인: "1. <제품명 링크> — 2,000원"
    * 급상승/뉴랭커/급하락 라인:
        - 제품명 71위 → 7위 (↑64)
        - 제품명 NEW → 19위
        - 제품명 23위 → OUT
    * 'BEST' 표시는 모두 제거
- 전일 CSV와 비교해 변동 계산(Top30 기준)
- Playwright → 실패 시 Requests(정적 파싱) 폴백
"""

import os, re, io, sys, json, time, math, csv, pathlib, traceback, datetime as dt
from typing import List, Dict, Tuple, Optional

import pytz
import requests
from bs4 import BeautifulSoup

# 구글 드라이브
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaInMemoryUpload

# ---- 환경설정 --------------------------------------------------------------

BASE_URL = "https://www.daisomall.co.kr"
RANK_URL = f"{BASE_URL}/ds/rank/C105"   # 뷰티/위생 일간

DATA_DIR = pathlib.Path("data")
DEBUG_DIR = pathlib.Path("data/debug")
DATA_DIR.mkdir(parents=True, exist_ok=True)
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

KST = pytz.timezone("Asia/Seoul")

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "").strip()

GDRIVE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
GDRIVE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
GDRIVE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "").strip()
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()

# ---- 공통 유틸 -------------------------------------------------------------

def today_ymd_kst() -> str:
    return dt.datetime.now(KST).strftime("%Y-%m-%d")

def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")

def slack_escape(text: str) -> str:
    # 슬랙 링크 텍스트에서 문제될 수 있는 기호 어느정도 방어
    return text.replace("&", "&amp;").replace("<", "〈").replace(">", "〉")

def to_int(s: str) -> Optional[int]:
    try:
        return int(re.sub(r"[^\d]", "", s))
    except:
        return None

def format_won(num: Optional[int]) -> str:
    if num is None: return "₩0"
    return f"{num:,}원"

def load_csv(path: pathlib.Path) -> List[Dict]:
    if not path.exists(): return []
    out = []
    with path.open("r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # 안전 변환
            row["rank"] = int(row.get("rank", "0") or 0)
            row["price"] = int(row.get("price", "0") or 0)
            out.append(row)
    return out

def save_csv(path: pathlib.Path, rows: List[Dict]):
    cols = ["date","rank","name","price","url"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})

# ---- 구글 드라이브 업로드 ---------------------------------------------------

def gdrive_build():
    if not (GDRIVE_CLIENT_ID and GDRIVE_CLIENT_SECRET and GDRIVE_REFRESH_TOKEN and GDRIVE_FOLDER_ID):
        raise RuntimeError("Google Drive 폴더/자격 정보 부족. 리포 시크릿 확인 필요")
    creds = Credentials(
        None,
        refresh_token=GDRIVE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GDRIVE_CLIENT_ID,
        client_secret=GDRIVE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def gdrive_upload_csv(path: pathlib.Path) -> str:
    service = gdrive_build()
    media = MediaInMemoryUpload(path.read_bytes(), mimetype="text/csv", resumable=False)
    body = {"name": path.name, "parents": [GDRIVE_FOLDER_ID]}
    file = service.files().create(body=body, media_body=media, fields="id").execute()
    return file["id"]

# ---- Playwright 크롤러 (가능 시) --------------------------------------------

def fetch_by_playwright() -> List[Dict]:
    """
    Playwright로 상품 카드 파싱. 실패 시 예외 던져 폴백하도록 함.
    """
    from playwright.sync_api import sync_playwright

    items: List[Dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page(viewport={"width":1280, "height":2200})
        page.goto(RANK_URL, timeout=60_000)
        # 탭 '일간' 클릭 안전망 (이미 일간일 가능성 높음)
        try:
            page.get_by_role("tab", name=re.compile("일간")).click(timeout=3_000)
        except Exception:
            pass

        # 카드 로드 대기
        page.wait_for_selector(".goods-unit", timeout=30_000)
        # 스크린샷/HTML 디버그 저장
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(DEBUG_DIR / "page_rank.png"), full_page=True)
        (DEBUG_DIR / "page_rank.html").write_text(page.content(), encoding="utf-8")

        # 파싱
        html = page.content()
        browser.close()

    return parse_from_html(html)

# ---- Requests 폴백 ---------------------------------------------------------

def fetch_by_requests() -> List[Dict]:
    r = requests.get(RANK_URL, timeout=30, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    })
    r.raise_for_status()
    html = r.text
    (DEBUG_DIR / "page_rank.html").write_text(html, encoding="utf-8")
    return parse_from_html(html)

# ---- HTML 파서 --------------------------------------------------------------

def parse_from_html(html: str) -> List[Dict]:
    """
    디버그 sample 기준 구조:
      .goods-unit
        .ranking-area .rank .num         -> 현재 순위
        .goods-thumb a.goods-link[href]   -> 상품 링크(/pd/pdr/... ?pdNo=####)
        .goods-detail .goods-price .value -> 가격 숫자
        .goods-detail .tit .best          -> 'BEST' 라벨 (제거)
        .goods-detail .tit (text)         -> 상품명
    """
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(".goods-unit")
    items: List[Dict] = []

    for card in cards:
        num_el = card.select_one(".ranking-area .rank .num")
        if not num_el:
            # 배너 등은 스킵
            continue
        rank = to_int(num_el.get_text(strip=True)) or 0

        link_tag = card.select_one(".goods-thumb a.goods-link")
        url = BASE_URL + link_tag["href"] if link_tag and link_tag.has_attr("href") else RANK_URL

        # 이름
        tit = card.select_one(".goods-detail .tit")
        name = ""
        if tit:
            # span.best 제거
            for b in tit.select(".best"):
                b.extract()
            name = " ".join(tit.get_text(" ", strip=True).split())
            # 혹시 남은 "BEST" 접두어 텍스트 제거 (2중 안전망)
            name = re.sub(r"^\s*BEST\s*", "", name, flags=re.I)

        # 가격
        price_el = card.select_one(".goods-detail .goods-price .value")
        price = to_int(price_el.get_text(strip=True)) if price_el else None

        items.append({
            "rank": rank,
            "name": name,
            "price": price or 0,
            "url": url,
        })

    # 순위 기준 정렬(혹시 섞여 있으면)
    items = sorted(items, key=lambda x: x["rank"])
    return items

# ---- 변동 계산 --------------------------------------------------------------

def previous_csv_path(today_path: pathlib.Path) -> Optional[pathlib.Path]:
    # data/다이소몰_뷰티위생_일간_YYYY-MM-DD.csv 중 가장 최근 과거 파일
    stem = "다이소몰_뷰티위생_일간_"
    candidates = sorted(DATA_DIR.glob(f"{stem}*.csv"))
    prevs = [p for p in candidates if p.name < today_path.name]
    return prevs[-1] if prevs else None

def analyze_changes(today: List[Dict], prev: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Top30 기준:
      - 급상승: prev∩today 중 rank 개선폭 (prev - curr) 양수 상위 3
      - 뉴랭커: prev>30 또는 미등장 → today<=30 상위 (최대 3)
      - 급하락: prev∩today 중 rank 하락폭 (curr - prev) 양수 상위 5 + 랭크아웃(prev<=30 & today>30)
      - 인&아웃: 위 '뉴랭커' + '랭크아웃' 개수 합 (정수)
    """
    N = 30
    prev_map = {normalize_name(r["name"]): r for r in prev}
    today_map = {normalize_name(r["name"]): r for r in today}

    # 공통
    rising, falling = [], []
    new_in, out_items = [], []

    for key, t in today_map.items():
        if t["rank"] > N: 
            continue
        p = prev_map.get(key)
        if p:
            delta = p["rank"] - t["rank"]
            if delta > 0:
                rising.append({"name": t["name"], "prev": p["rank"], "curr": t["rank"], "delta": delta, "url": t["url"]})
        else:
            # 뉴랭커
            new_in.append({"name": t["name"], "prev": None, "curr": t["rank"], "url": t["url"]})

    for key, p in prev_map.items():
        if p["rank"] <= N:
            t = today_map.get(key)
            if not t or (t["rank"] > N):
                out_items.append({"name": p["name"], "prev": p["rank"], "curr": None})

    # 급하락(공통 존재 + 하락폭 큰 순)
    for key, t in today_map.items():
        p = prev_map.get(key)
        if p:
            delta_down = t["rank"] - p["rank"]
            if delta_down > 0:
                falling.append({"name": t["name"], "prev": p["rank"], "curr": t["rank"], "delta": delta_down, "url": t["url"]})

    rising.sort(key=lambda x: (-x["delta"], x["curr"], x["prev"] or 9999, normalize_name(x["name"])))
    falling.sort(key=lambda x: (-x["delta"], x["prev"], x["curr"] or 9999, normalize_name(x["name"])))
    new_in.sort(key=lambda x: (x["curr"], normalize_name(x["name"])))
    out_items.sort(key=lambda x: (x["prev"], normalize_name(x["name"])))

    return {
        "rising": rising[:3],
        "new_in": new_in[:3],
        "falling": falling[:5],
        "out": out_items,  # 급하락 섹션에서 OUT 함께 표기할 때 사용
        "inout_count": len(new_in) + len(out_items),
    }

def normalize_name(s: str) -> str:
    # 비교 안정화를 위해 공백/대소문자/특수문자 약간 정규화 (필요시 확장)
    s = re.sub(r"\s+", " ", s or "").strip()
    return s.lower()

# ---- 슬랙 메시지 ------------------------------------------------------------

def post_to_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[Slack] 미설정. 메시지 생략")
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        print("Slack 전송 완료")
    except Exception as e:
        print(f"[Slack] 전송 실패: {e}")

def make_slack_message(today_rows: List[Dict], prev_rows: List[Dict], change: Dict, csv_path: pathlib.Path, drive_file_id: Optional[str]) -> str:
    today_kst = today_ymd_kst()
    lines = []

    lines.append(f"*다이소몰 뷰티/위생 일간 — {today_kst}*")
    lines.append("")
    # TOP 10
    lines.append("*TOP 10*")
    top10 = [r for r in today_rows if r["rank"] <= 10]
    for r in top10:
        # 하이퍼링크
        nm = slack_escape(r["name"])
        url = r["url"]
        price = format_won(r["price"])
        lines.append(f"{r['rank']}. <{url}|{nm}> — {price}")
    lines.append("")

    # 급상승
    lines.append("🔥 *급상승*")
    if change["rising"]:
        for r in change["rising"]:
            nm = slack_escape(r["name"])
            lines.append(f"- {nm} {r['prev']}위 → {r['curr']}위 (↑{r['delta']})")
    else:
        lines.append("- 해당 없음")
    lines.append("")

    # 뉴랭커
    lines.append("🆕 *뉴랭커*")
    if change["new_in"]:
        for r in change["new_in"]:
            nm = slack_escape(r["name"])
            lines.append(f"- {nm} NEW → {r['curr']}위")
    else:
        lines.append("- 해당 없음")
    lines.append("")

    # 급하락 + OUT
    lines.append("📉 *급하락*")
    had_any = False
    for r in change["falling"]:
        had_any = True
        nm = slack_escape(r["name"])
        lines.append(f"- {nm} {r['prev']}위 → {r['curr']}위 (↓{r['delta']})")
    # OUT 표기 (Top30 → 오늘 Out)
    outs = [o for o in change["out"] if (o["prev"] and o["prev"] <= 30)]
    for o in outs:
        had_any = True
        nm = slack_escape(o["name"])
        lines.append(f"- {nm} {o['prev']}위 → OUT")
    if not had_any:
        lines.append("- 해당 없음")
    lines.append("")

    # 인&아웃 개수
    lines.append("🔁 *랭크 인&아웃*")
    lines.append(f"{change['inout_count']}개의 제품이 인&아웃 되었습니다.")
    lines.append("")

    # 참고 정보(파일)
    tail = [f"CSV: `{csv_path.name}`"]
    if drive_file_id:
        tail.append(f"Drive 파일 ID: `{drive_file_id}`")
    lines.append("_" + "  •  ".join(tail) + "_")

    return "\n".join(lines)

# ---- 메인 --------------------------------------------------------------

def main():
    t0 = time.time()
    print(f"수집 시작: {RANK_URL}")

    # 1) 수집
    items: List[Dict] = []
    try:
        items = fetch_by_playwright()
        print("[Playwright] 정상 수집")
    except Exception as e:
        print("[Playwright] 실패 → 폴백 진입")
        print(str(e))
        items = fetch_by_requests()

    # 최소 건수 방어 (디버그 HTML도 이미 저장됨)
    print(f"수집 완료: {len(items)}")
    if len(items) < 10:
        raise RuntimeError("제품 카드가 너무 적게 수집되었습니다. 셀렉터/렌더링 점검 필요")

    # 2) CSV 저장
    today = today_ymd_kst()
    csv_path = DATA_DIR / f"다이소몰_뷰티위생_일간_{today}.csv"
    rows = []
    for r in items:
        rows.append({
            "date": today,
            "rank": r["rank"],
            "name": r["name"],                 # 'BEST' 제거된 상태
            "price": int(r["price"] or 0),
            "url": r["url"],
        })
    save_csv(csv_path, rows)

    # 3) 전일과 비교
    prev_path = previous_csv_path(csv_path)
    prev_rows = load_csv(prev_path) if prev_path else []
    change = analyze_changes(rows, prev_rows)

    # 4) 드라이브 업로드
    drive_id = None
    try:
        drive_id = gdrive_upload_csv(csv_path)
        print(f"Google Drive 업로드 완료: {csv_path.name}, id={drive_id}")
    except Exception as e:
        print(f"[Drive] 업로드 실패: {e}")

    # 5) 슬랙 전송
    text = make_slack_message(rows, prev_rows, change, csv_path, drive_id)
    post_to_slack(text)

    # 6) 로그
    elapsed = time.time() - t0
    print(f"총 {len(rows)}건, 경과: {elapsed:.1f}s")
    print(f"CSV: {csv_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
