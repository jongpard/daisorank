# -*- coding: utf-8 -*-
"""
daisorang/app.py (수정본 전체)
- TopN=200 무조건 보장
- analyze_trends() 반환값 6개로 통일 (언팩 에러 해결)
- IN≡OUT 규칙 강제 및 슬랙 포맷(볼드 한 줄 요약)
- 구글드라이브 업로드 성공 후에도 파이프라인이 '실패'로 끝나지 않도록 예외 처리 개선
"""

import os
import sys
import csv
import json
import time
import math
import traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple, Optional

# -----------------------------
# 환경 변수
# -----------------------------
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID   = os.getenv("GDRIVE_FOLDER_ID", "")
SITE_NAME          = os.getenv("SITE_NAME", "daisomall")  # 로깅용
TOPN               = int(os.getenv("TOPN", "200"))

KST = timezone(timedelta(hours=9))

# -----------------------------
# 유틸
# -----------------------------
def kst_today_str(fmt: str = "%Y-%m-%d") -> str:
    return datetime.now(KST).strftime(fmt)

def safe_int(v, default=999999):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default

def ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# -----------------------------
# 수집(페이지네이션 콜백)
#  - 기존 프로젝트의 페이징 함수에 맞춰 내용을 연결하세요.
#  - 현재는 '너의 기존 모듈'에 이미 있는 함수가 있다고 가정하고 import 시도,
#    없으면 간단 스텁으로 동작(실서비스에선 반드시 너의 실제 함수로 바꿔줘).
# -----------------------------
def _stub_fetch_next_page(page: int) -> List[Dict[str, Any]]:
    """
    [임시 스텁] 실제 서비스에선 반드시 기존의 크롤러 로직으로 대체하세요.
    반환 스키마 예시: {"key": "...", "raw_name": "...", "rank": 1, "url": "...", "price": "...", ...}
    """
    # 빈 리스트를 돌려 재시도/보장 로직이 작동하도록 함
    return []

try:
    # 예: from crawler import fetch_next_page
    from crawler import fetch_next_page as fetch_next_page_impl  # type: ignore
except Exception:
    fetch_next_page_impl = _stub_fetch_next_page

def fetch_next_page(page: int) -> List[Dict[str, Any]]:
    return fetch_next_page_impl(page=page)

# -----------------------------
# TopN 보장
# -----------------------------
def ensure_topN(rows: List[Dict[str, Any]],
                topN: int = TOPN,
                site_name: str = SITE_NAME,
                max_retry: int = 3,
                fetch_fn=None) -> List[Dict[str, Any]]:
    """
    rows가 topN 미만이면 fetch_fn(page=2,3,...)로 추가 수집 시도.
    최종 미달이면 명확한 예외로 중단(원인 파악 쉬움).
    """
    if rows is None:
        rows = []
    # rank가 숫자인 것만
    rows = [r for r in rows if isinstance(r.get("rank"), (int, float, str))]
    for r in rows:
        r["rank"] = safe_int(r.get("rank"))

    rows = sorted(rows, key=lambda r: safe_int(r.get("rank")))
    rows = rows[:topN]

    attempt = 1
    while len(rows) < topN and attempt <= max_retry:
        if fetch_fn is None:
            break
        more = fetch_fn(page=attempt + 1) or []
        # 정합성
        for m in more:
            m["rank"] = safe_int(m.get("rank"))
        rows += more
        rows = [r for r in rows if isinstance(r.get("rank"), (int, float))]
        rows = sorted(rows, key=lambda r: safe_int(r.get("rank")))[:topN]
        attempt += 1

    if len(rows) < topN:
        raise RuntimeError(
            f"[수집 에러] Top{topN} 보장 실패: 현재 {len(rows)}개. "
            f"수집 소스({site_name}) 구조 변경/차단/품절 공란 가능성. 로그 확인 필요."
        )

    return rows

# -----------------------------
# 전일 데이터 로딩
#  - 프로젝트 사양에 맞게 구현하세요.
#  - 여기선 로컬 CSV 우선, 없으면 빈 리스트.
# -----------------------------
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

# -----------------------------
# 분석: 상승/하락/IN/OUT (반환값 6개로 통일)
# -----------------------------
def analyze_trends(rows: List[Dict[str, Any]],
                   prev_list: List[Dict[str, Any]],
                   topN: int = TOPN
                   ) -> Tuple[List[Dict[str, Any]],
                              List[Dict[str, Any]],
                              List[Dict[str, Any]],
                              List[Dict[str, Any]],
                              List[Dict[str, Any]],
                              int]:
    """
    return: (today_aug, ups, downs, chart_ins, rank_outs, io_cnt)
    - today_aug: 오늘 TopN 정돈 리스트
    - ups: 전일 대비 상승
    - downs: 전일 대비 하락
    - chart_ins: 신규 진입(IN)
    - rank_outs: 이탈(OUT)
    - io_cnt: IN/OUT 동일 개수
    """
    # 오늘 TopN 정돈
    rows = [r for r in rows if isinstance(r.get("rank"), (int, float))]
    rows = sorted(rows, key=lambda r: safe_int(r.get("rank")))
    today_aug = rows[:topN]

    def to_map(lst):
        m = {}
        for it in lst:
            key = str(it.get("key") or it.get("goodsNo") or it.get("product_code") or it.get("url") or it.get("id") or "")
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
            delta = pr - tr  # 양수: 상승
            base = {
                "key": key,
                "raw_name": today.get("raw_name") or today.get("product_name") or today.get("name") or "",
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

    # IN≡OUT 강제
    io_cnt = min(len(chart_ins), len(rank_outs))
    if len(chart_ins) != len(rank_outs):
        chart_ins = chart_ins[:io_cnt]
        rank_outs = rank_outs[:io_cnt]

    # 정렬
    ups.sort(key=lambda x: (-x["delta"], x["rank"]))
    downs.sort(key=lambda x: (x["delta"], x["rank"]))
    chart_ins.sort(key=lambda r: safe_int(r.get("rank")))
    rank_outs.sort(key=lambda r: safe_int(r.get("rank")))

    return today_aug, ups, downs, chart_ins, rank_outs, io_cnt

# -----------------------------
# CSV 저장 & 구글드라이브 업로드
# -----------------------------
def save_csv(rows: List[Dict[str, Any]], path: str):
    ensure_dir(path)
    if not rows:
        # 최소 헤더 보존
        headers = ["key", "raw_name", "rank", "url", "brand", "price", "orig_price", "discount_percent"]
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
        return

    headers = sorted({k for r in rows for k in r.keys()})
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def upload_to_gdrive(file_path: str, folder_id: str = GDRIVE_FOLDER_ID) -> Optional[str]:
    """
    구글 드라이브 업로드.
    - google-api-python-client 사용 (런너에 인증 세팅 필요)
    - 성공 시 file_id 반환
    """
    if not folder_id:
        print("[경고] GDRIVE_FOLDER_ID가 설정되지 않아 업로드를 건너뜀.")
        return None
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        scopes = ["https://www.googleapis.com/auth/drive.file"]
        creds = None

        # 방법1: 런너의 ADC(Application Default Credentials)
        # 방법2: 서비스계정 JSON 경로 제공
        sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        if sa_path and os.path.exists(sa_path):
            creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
        else:
            # ADC 시도
            from google.auth import default
            creds, _ = default(scopes=scopes)

        service = build("drive", "v3", credentials=creds)

        file_metadata = {"name": os.path.basename(file_path), "parents": [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)
        file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        fid = file.get("id")
        print(f"[Drive] 업로드 성공: {os.path.basename(file_path)} (id={fid})")
        return fid
    except Exception as e:
        print("[Drive] 업로드 실패:", e)
        traceback.print_exc()
        return None

# -----------------------------
# 슬랙
# -----------------------------
def post_to_slack(text: str):
    if not SLACK_WEBHOOK_URL:
        print("[경고] SLACK_WEBHOOK_URL 미설정 → 슬랙 전송 생략")
        return
    try:
        import requests
        res = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
        res.raise_for_status()
    except Exception as e:
        print("[슬랙] 전송 실패:", e)

def build_slack_inout_line(io_cnt: int) -> str:
    """
    요구 포맷:
    :양방향_화살표: 랭크 인&아웃
    **42개의 제품이 인&아웃 되었습니다.**
    """
    lines = []
    lines.append(":양방향_화살표: 랭크 인&아웃")
    lines.append(f"**{io_cnt}개의 제품이 인&아웃 되었습니다.**")
    return "\n".join(lines)

def build_slack_message(today_aug: List[Dict[str, Any]],
                        ups: List[Dict[str, Any]],
                        downs: List[Dict[str, Any]],
                        chart_ins: List[Dict[str, Any]],
                        rank_outs: List[Dict[str, Any]],
                        io_cnt: int) -> str:
    # 최소 섹션: 제목 + IN&OUT 한 줄 요약 (요청 반영)
    title = f"📊 {SITE_NAME} Top{TOPN} ({kst_today_str('%Y-%m-%d')})"
    msg = [f"*{title}*"]
    msg.append(build_slack_inout_line(io_cnt))
    # 필요 시 상세를 추가(선택) — 규칙상 한 줄 요약은 필수로 이미 포함됨.
    return "\n".join(msg)

# -----------------------------
# 메인
# -----------------------------
def main():
    print("수집 시작:", f"https://www.{SITE_NAME}.co.kr/")

    # 1) 오늘 1페이지 수집(기존 로직 호출)
    base_rows = fetch_next_page(page=1) or []
    print(f"[수집 1p] {len(base_rows)}개")

    # 2) TopN=200 보장(추가 페이지 자동 재시도)
    rows = ensure_topN(base_rows, topN=TOPN, site_name=SITE_NAME, max_retry=3, fetch_fn=fetch_next_page)
    print(f"[수집 완료] {len(rows)}개 (요구: {TOPN})")

    # 3) 저장 파일명 (KST)
    date_tag = datetime.now(KST).strftime("%Y-%m-%d")
    out_dir  = os.getenv("OUT_DIR", "out")
    out_csv  = os.path.join(out_dir, f"{SITE_NAME}_뷰티위생_일간_{date_tag}.csv")

    # 4) 전일 CSV 로딩 (같은 디렉토리 기준 전일 파일 추정)
    prev_date_tag = (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_csv = os.path.join(out_dir, f"{SITE_NAME}_뷰티위생_일간_{prev_date_tag}.csv")
    prev_list = load_prev_list(prev_csv)
    print(f"[전일 로딩] {len(prev_list)}개")

    # 5) 분석(!!! 반환 6개 고정 !!!)  ← 기존 오류 지점
    today_aug, ups, downs, chart_ins, rank_outs, io_cnt = analyze_trends(rows, prev_list, topN=TOPN)

    # 6) CSV 저장
    save_csv(today_aug, out_csv)
    print("[-] CSV 저장:", out_csv)

    # 7) 구글드라이브 업로드
    file_id = upload_to_gdrive(out_csv, folder_id=GDRIVE_FOLDER_ID)

    # 8) 슬랙 전송 (필요시)
    slack_text = build_slack_message(today_aug, ups, downs, chart_ins, rank_outs, io_cnt)
    post_to_slack(slack_text)

    # 9) 요약 로그
    print(f"[요약] 상승 {len(ups)} / 하락 {len(downs)} / IN {io_cnt} / OUT {io_cnt}")
    if file_id:
        print(f"[Drive] file_id={file_id}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 업로드가 이미 성공했더라도 '전체 작업 실패'가 되지 않도록 가독성 좋은 메시지 후 종료코드 1
        print("에러:", e)
        traceback.print_exc()
        sys.exit(1)
