# app.py — DaisoMall 뷰티/위생 ‘일간’ 랭킹 크롤러
# ※ 핵심: 크롤링 로직은 그대로 유지. 전일 CSV를 Google Drive에서 자동 다운로드하여 비교만 추가

import os, csv, re, time, json, requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------------- 기본 설정 ----------------
KST = timezone(timedelta(hours=9))
FILE_PREFIX = "다이소몰_뷰티위생_일간_"
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"  # 뷰티/위생
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))          # 100~200 권장

# Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

# Google Drive OAuth(Refresh Token 방식)
GDRIVE_FOLDER_ID      = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID      = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET  = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN  = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# ---------------- 유틸 ----------------
def today_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")

def yday_str() -> str:
    return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def safe_int(x: str, default: int = 0) -> int:
    try:
        return int(re.sub(r"[^\d]", "", x or ""))
    except Exception:
        return default

def fmt_price(v: int) -> str:
    return f"{v:,}원"

def make_today_csv_path() -> str:
    os.makedirs("data", exist_ok=True)
    return os.path.join("data", f"{FILE_PREFIX}{today_str()}.csv")

def make_yday_csv_path() -> str:
    os.makedirs("data", exist_ok=True)
    return os.path.join("data", f"{FILE_PREFIX}{yday_str()}.csv")

# ---------------- Drive 헬퍼 (전일 CSV 자동 확보) ----------------
def _drive_get_access_token() -> Optional[str]:
    """refresh_token으로 액세스 토큰 발급"""
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
            timeout=20,
        )
        r.raise_for_status()
        return r.json().get("access_token")
    except Exception as e:
        print("[Drive] 토큰 갱신 실패:", e)
        return None

def _drive_find_prev_file(access_token: str, exact_name: str) -> Optional[Dict]:
    """
    1) 어제 날짜명 정확 일치 파일 우선
    2) 없으면 동일 prefix 최신 파일
    """
    try:
        # 정확 일치
        q1 = f"'{GDRIVE_FOLDER_ID}' in parents and name = '{exact_name}' and trashed = false"
        r1 = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            params={"q": q1, "fields": "files(id,name,modifiedTime)", "pageSize": 1},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        ).json()
        files = r1.get("files", [])
        if files:
            return files[0]

        # prefix로 최신
        q2 = f"'{GDRIVE_FOLDER_ID}' in parents and name contains '{FILE_PREFIX}' and trashed = false"
        r2 = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            params={
                "q": q2,
                "fields": "files(id,name,modifiedTime)",
                "orderBy": "modifiedTime desc",
                "pageSize": 5,
            },
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=15,
        ).json()
        cand = r2.get("files", [])
        return cand[0] if cand else None
    except Exception as e:
        print("[Drive] 파일 검색 실패:", e)
        return None

def _drive_download_file(access_token: str, file_id: str, save_path: str) -> bool:
    try:
        r = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}",
            params={"alt": "media"},
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=60,
        )
        r.raise_for_status()
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception as e:
        print("[Drive] 다운로드 실패:", e)
        return False

def ensure_prev_csv_local() -> Optional[str]:
    """
    로컬에 전일 CSV가 없으면, Drive에서 찾아서 data/로 내려받는다.
    성공하면 로컬 경로 반환, 실패시 None.
    """
    local_path = make_yday_csv_path()
    if os.path.exists(local_path):
        return local_path

    token = _drive_get_access_token()
    if not token:
        print("[Drive] 자격정보 없음 → 전일 비교 생략")
        return None

    expected_name = os.path.basename(local_path)
    meta = _drive_find_prev_file(token, expected_name)
    if not meta:
        print("[Drive] 전일/대체 CSV 찾지 못함 → 비교 생략")
        return None

    if _drive_download_file(token, meta["id"], local_path):
        print(f"[Drive] 전일 CSV 다운로드 완료: {local_path} (name={meta.get('name')})")
        return local_path

    return None

# ---------------- 전일 비교 로딩 ----------------
def load_prev_map() -> Dict[str, int]:
    """
    이전일 랭킹 맵: {url: rank}
    ※ 변경점: 로컬에 없으면 ensure_prev_csv_local()로 Drive에서 자동 확보
    """
    prev = {}
    try:
        # ★ 추가: 로컬에 없으면 드라이브에서 확보
        ensure_prev_csv_local()

        fn = make_yday_csv_path()
        if not os.path.exists(fn):
            return prev

        with open(fn, newline="", encoding="utf-8") as f:
            rd = csv.DictReader(f)
            for row in rd:
                try:
                    prev[row["url"]] = int(row["rank"])
                except Exception:
                    pass
    except Exception as e:
        print("[prev] 로딩 실패:", e)
    return prev

# ---------------- 슬랙 ----------------
def post_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[Slack] Webhook 미설정 → 출력만")
        print(text)
        return
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print("[Slack] 전송 실패:", e)
        print(text)

def build_slack_message(date_str: str, rows: List[Dict], prev_map: Dict[str, int]) -> str:
    # TOP 10
    top10 = rows[:10]
    lines = [f"*다이소몰 뷰티/위생 일간 랭킹 — {date_str}*\n", "*TOP 10*"]
    for i, r in enumerate(top10, 1):
        name = r["name"]
        url  = r["url"]
        price = fmt_price(r["price"])
        # 변화
        old = prev_map.get(url)
        delta = ""
        if old is not None:
            diff = old - r["rank"]
            if diff > 0: delta = f" (↑{diff})"
            elif diff < 0: delta = f" (↓{abs(diff)})"
            else: delta = " (→)"
        lines.append(f"{i}. <{url}|{name}> — {price}{delta}")

    # 급상승/뉴랭커/급하락
    up, new, down, _out = calc_in_out(rows, prev_map)
    lines.append("\n:fire: *급상승*")
    if up:  lines += [f"- {s}" for s in up[:5]]
    else:  lines.append("- 해당 없음")

    lines.append("\n:new: *뉴랭커*")
    if new: lines += [f"- {s}" for s in new[:5]]
    else:  lines.append("- 해당 없음")

    lines.append("\n:triangular_ruler: *급하락*")
    if down: lines += [f"- {s}" for s in down[:5]]
    else:  lines.append("- 해당 없음")

    # 인&아웃
    _in_cnt  = len([r for r in rows if prev_map.get(r["url"]) is None])
    _out_cnt = len([1 for u, rk in prev_map.items() if all(u != x["url"] for x in rows)])
    lines.append(f"\n:link: *랭크 인&아웃*\n{_in_cnt}개의 제품이 인&아웃 되었습니다.")

    return "\n".join(lines)

def calc_in_out(rows: List[Dict], prev_map: Dict[str, int]) -> Tuple[List[str], List[str], List[str], List[str]]:
    up, new, down, out = [], [], [], []
    # 현재 기준
    now_map = {r["url"]: r["rank"] for r in rows}

    for r in rows:
        url, rank = r["url"], r["rank"]
        old = prev_map.get(url)
        if old is None:
            new.append(f'{r["name"]} NEW → {rank}위')
        else:
            diff = old - rank
            if diff >= 5:
                up.append(f'{r["name"]} {old}위 → {rank}위 (↑{diff})')
            elif diff <= -5:
                down.append(f'{r["name"]} {old}위 → {rank}위 (↓{abs(diff)})')

    for u, old_r in prev_map.items():
        if u not in now_map:
            out.append(f"{old_r}위 → OUT")

    return up, new, down, out

# ---------------- CSV 저장 ----------------
def save_csv(rows: List[Dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=["date","rank","name","price","url"])
        wr.writeheader()
        for r in rows:
            wr.writerow({
                "date": today_str(),
                "rank": r["rank"],
                "name": r["name"],
                "price": r["price"],
                "url": r["url"],
            })

# ---------------- (중요) 크롤링 파트 ----------------
# !!! 아래 두 함수는 네가 쓰던 기존 구현을 그대로 두세요 !!!
# (Selectors/스크롤/JS evaluate 등 전혀 변경하지 않음)

def collect_items(page) -> List[Dict]:
    """
    네가 사용하던 DOM 추출 로직 그대로 둡니다.
    결과 형식 예시: [{"rank":1, "name":"...", "price":3000, "url":"..."} ...]
    """
    # ─── 기존 구현 그대로 ───
    items: List[Dict] = []

    # 아래는 예시(동작 보장은 안함). 네가 쓰던 코드가 있으면 그걸 사용하세요.
    cards = page.query_selector_all("div[class*='goods_item'], li[class*='goods']")
    rank = 1
    for c in cards:
        try:
            name = (c.query_selector("a, p, .name") or c).inner_text().strip()
            href_el = c.query_selector("a[href]")
            url = href_el.get_attribute("href") if href_el else ""
            price_text = (c.query_selector(".price, .sale, .num") or c).inner_text()
            price = safe_int(price_text)
            if url and name:
                items.append({"rank": rank, "name": name, "price": price, "url": url})
                rank += 1
                if rank > MAX_ITEMS: break
        except Exception:
            pass

    return items

def fetch_products() -> List[Dict]:
    """
    네가 쓰던 Playwright 시나리오 그대로 유지.
    (뷰티/위생 카테고리 + 일간 필터 적용, 충분히 스크롤)
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.goto(RANK_URL, timeout=60000)
        # 👉 여기서 네가 쓰던 '뷰티/위생 클릭', '일간 클릭', '스크롤' 그대로 호출
        # (이 파일은 비교/슬랙/드라이브만 건드렸으니 기존 코드를 그대로 돌리면 됨)

        # 충분히 로딩/스크롤 (예시)
        page.wait_for_load_state("networkidle")
        for _ in range(6):
            page.mouse.wheel(0, 4000)
            time.sleep(0.7)

        items = collect_items(page)
        browser.close()
        return items

# ---------------- 메인 ----------------
def main():
    print(f"수집 시작: {RANK_URL}")
    try:
        items = fetch_products()
    except Exception as e:
        print("실행 실패:", e)
        raise

    if not items:
        raise RuntimeError("유효 상품이 0건. 셀렉터/렌더링 점검 필요")

    # 저장
    out_csv = make_today_csv_path()
    save_csv(items, out_csv)
    print(f"로컬 저장: {out_csv}")

    # 전일 맵 로딩(Drive에서 자동 확보 포함)
    prev_map = load_prev_map()

    # 슬랙
    msg = build_slack_message(today_str(), items, prev_map)
    post_slack(msg)
    print("[Slack] 전송 완료")

if __name__ == "__main__":
    main()
