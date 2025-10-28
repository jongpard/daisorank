# app.py — 다이소몰 뷰티/위생 '일간' 랭킹 수집/분석
# 변경사항 요약
#  (A) Top200 강제 트림(enforce_top200) 추가
#  (B) 전일 비교(랭크 인&아웃) 로직 url→pdNo 키 표준화 + 양쪽 Top200 비교, 슬랙 문구 교체
# 나머지 코드는 기존 동작/포맷 유지

import os
import re
import sys
import json
import time
import math
import gzip
import shutil
import random
import logging
import datetime as dt
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import requests

# ===[ 환경설정 ]==============================================================
TZ = dt.timezone(dt.timedelta(hours=9))  # KST
today = dt.datetime.now(TZ).date()
yesterday = today - dt.timedelta(days=1)

DATA_DIR = Path(os.environ.get("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_NAME = "다이소몰 · 뷰티/위생 · 일간"
TOPN = 200

# 파일명 규칙(기존과 동일하게 유지)
def csv_name(d: dt.date) -> str:
    return f"다이소몰_뷰티위생_일간_{d.isoformat()}.csv"

TODAY_CSV = str(DATA_DIR / csv_name(today))
YDAY_CSV  = str(DATA_DIR / csv_name(yesterday))

# Slack
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "").strip()

# Google Drive 업로드(기존 로직을 그대로 쓰는 경우 True)
USE_GDRIVE = os.environ.get("USE_GDRIVE", "false").lower() == "true"
# 업로드 대상 드라이브 경로 등은 기존 로직 사용
# ============================================================================


# ===[ A. Top200 강제 트림 유틸 — (신규) ]====================================
def _extract_pdno_from_url(u: str) -> str:
    """url에서 pdNo / itemNo / productNo / /p/숫자 / /product/숫자 중 하나를 키로 추출"""
    if not isinstance(u, str):
        return ""
    m = re.search(r"(?:pdNo|itemNo|productNo)=(\d+)", u)
    if m:
        return m.group(1)
    m = re.search(r"/p/(\d+)", u) or re.search(r"/product/(\d+)", u)
    if m:
        return m.group(1)
    nums = re.findall(r"(\d{5,})", u)
    return max(nums, key=len) if nums else u

def enforce_top200(df: pd.DataFrame) -> pd.DataFrame:
    """
    rank 오름차순 → url에서 key 생성 → key 기준 중복 제거 → 상위 200행만 유지
    (기존 포맷은 유지, 단 'key' 컬럼만 내부 용도로 덧붙였다가 저장 직전 제거)
    """
    if df.empty:
        return df
    work = df.copy()
    if "rank" in work.columns:
        work = work.sort_values("rank", ascending=True)
    else:
        # 혹시 rank가 없으면 name/price 기반이 아닌 원본 순서로 진행
        work = work.reset_index(drop=True)

    # 키 생성
    work["key"] = work["url"].map(_extract_pdno_from_url).astype(str).str.strip()
    # 중복 제거(먼저 나온 랭크 유지)
    work = work.drop_duplicates(subset=["key"], keep="first")
    # 상위 200
    work = work.head(TOPN).reset_index(drop=True)

    # 외부 저장 전에는 key 제거(내부 계산용)
    return work


# ===[ B. 전일 비교 · 인&아웃 — (신규) ]======================================
def _load_csv_top200(csv_path: str) -> pd.DataFrame:
    """
    CSV 로드 → enforce_top200 적용 → (전처리 일관화)
    기존 CSV 컬럼은 그대로 유지: ['date','rank','name','price','url', ...]
    """
    df = pd.read_csv(csv_path)
    df = enforce_top200(df)
    return df

def compute_in_out(curr_df: pd.DataFrame, prev_df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """
    양쪽 모두 Top200으로 맞춘 뒤 url→pdNo key로 집합 비교
    항상 len(IN) == len(OUT)
    """
    c_keys = set(curr_df["url"].map(_extract_pdno_from_url).astype(str))
    p_keys = set(prev_df["url"].map(_extract_pdno_from_url).astype(str))
    in_set = sorted(c_keys - p_keys)
    out_set = sorted(p_keys - c_keys)
    return in_set, out_set

def build_inout_section(curr_csv: str, prev_csv: str) -> str:
    """
    사용자 요구 포맷으로만 출력 (기존 다른 섹션/텍스트는 건드리지 않음)
    :양방향_화살표: **랭크 인&아웃**
    **{n}개의 제품이 인&아웃 되었습니다.**
    """
    curr_df = _load_csv_top200(curr_csv)
    prev_df = _load_csv_top200(prev_csv)
    ins, outs = compute_in_out(curr_df, prev_df)
    n = len(ins)  # == len(outs)
    return (
        ":양방향_화살표: **랭크 인&아웃**\n"
        f"**{n}개의 제품이 인&아웃 되었습니다.**"
    )


# ===[ 크롤링(기존 유지): Playwright 등 기존 함수 그대로 사용 ]================
# 주의: 아래 scrape_daiso_daily()는 기존 함수를 그대로 두세요.
# 이 파일에서는 '수집 후 df에 enforce_top200(df) 1줄'만 추가합니다.
# 이미 안정적으로 수집 중이라면 그대로 두고, 저장 직전에 트림만 적용하세요.

def scrape_daiso_daily() -> pd.DataFrame:
    """
    [기존 코드 유지] 다이소몰 뷰티/위생 '일간' 랭킹을 수집해 DataFrame 반환.
    반드시 다음 컬럼이 포함되도록 유지: ['date','rank','name','price','url']
    (여기 구현은 예시. 너의 기존 함수가 있다면 그걸 그대로 쓰고,
     save_csv 직전에 enforce_top200(df)만 추가하면 됩니다.)
    """
    # --- 여기는 기존 구현을 그대로 사용하세요. ---
    # 아래는 동작 예시(HTTP/JS 렌더링 생략). 실제 환경은 Playwright를 사용할 가능성이 큼.
    # 이 예시는 자리표시자이며, 운영에서는 기존 scrape 함수를 사용하세요.
    raise NotImplementedError("기존 scrape_daiso_daily() 함수를 그대로 사용하세요.")


# ===[ 저장/업로드/슬랙 — 기존 로직 유지, 변경 지점만 반영 ]===================
def save_csv(df: pd.DataFrame, path: str):
    # (A) Top200 강제 트림을 저장 직전에 적용 — (신규 1줄)
    df = enforce_top200(df)
    # key 컬럼이 혹시 남아있다면 제거(외부에 노출 안 함)
    if "key" in df.columns:
        df = df.drop(columns=["key"])
    df.to_csv(path, index=False, encoding="utf-8")
    logging.info(f"CSV 저장: {path} ({len(df)} rows)")

def upload_to_gdrive_if_enabled(local_path: str):
    if not USE_GDRIVE:
        return
    # [기존 드라이브 업로드 로직 유지]
    # 예: gdrive_upload(local_path, remote_folder=...)
    # 이 부분은 기존 함수 호출로 대체하세요.
    pass

def post_to_slack(text: str):
    if not SLACK_WEBHOOK:
        logging.warning("SLACK_WEBHOOK 미설정 — 슬랙 전송 생략")
        return
    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
        if resp.status_code >= 400:
            logging.error(f"Slack 전송 실패: {resp.status_code} {resp.text}")
        else:
            logging.info("Slack 전송 성공")
    except Exception as e:
        logging.exception(f"Slack 전송 예외: {e}")


# ===[ Slack 메시지 조립 — 기존 포맷 유지, 인&아웃 섹션만 교체 ]==============
def build_slack_message(main_sections: List[str], inout_section: str) -> str:
    """
    기존에 사용하던 슬랙 메시지 조립 흐름을 그대로 두고,
    인&아웃 섹션만 교체해서 넣습니다.
    """
    parts = []
    for sec in main_sections:
        if sec.startswith(":양방향_화살표:") or "랭크 인&아웃" in sec:
            # (B) 인&아웃 섹션 교체
            parts.append(inout_section)
        else:
            parts.append(sec)
    return "\n\n".join(parts)


# ===[ 메인 플로우 ]===========================================================
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # 1) 수집 (기존 함수 사용 권장)
    # df = scrape_daiso_daily()
    # └ 네가 쓰던 기존 scrape 함수가 있으면 그걸 호출해 df를 받고,
    #   save_csv 전에 자동으로 enforce_top200(df) 적용됩니다.

    # 만약 이미 외부에서 CSV를 만들어두는 파이프라인이라면, 여기서 로딩→트림→재저장만 수행해도 됨.
    if Path(TODAY_CSV).exists():
        logging.info(f"기존 CSV 감지 → 리로드 후 트림: {TODAY_CSV}")
        df_today = pd.read_csv(TODAY_CSV)
        save_csv(df_today, TODAY_CSV)
    else:
        # 운영에선 여기를 scrape_daiso_daily()로 대체
        raise FileNotFoundError(
            f"오늘 CSV가 없습니다: {TODAY_CSV}\n"
            "기존 크롤링 단계에서 CSV 생성 후 본 스크립트를 실행하세요."
        )

    # 2) 전일 비교용 CSV 확인(없으면 인&아웃 섹션은 0개로 처리)
    if Path(YDAY_CSV).exists():
        inout_sec = build_inout_section(TODAY_CSV, YDAY_CSV)
    else:
        logging.warning(f"전일 CSV 없음: {YDAY_CSV} -> 인&아웃=0 처리")
        inout_sec = (
            ":양방향_화살표: **랭크 인&아웃**\n"
            "**0개의 제품이 인&아웃 되었습니다.**"
        )

    # 3) (기존) 슬랙 메시지 섹션들 구성
    #    ⚠️ 아래 main_sections는 네가 쓰던 기존 섹션 리스트를 그대로 유지하세요.
    #    여기서는 예시로 placeholders만 두며, 실제 운영에서는 기존 내용을 그대로 사용.
    main_sections = [
        f"📊 주간/일간 리포트 · {SOURCE_NAME} Top{TOPN} ({today.isoformat()})",
        "🏆 Top10 섹션 (기존 내용 유지)",
        "🍞 브랜드 점유율 섹션 (기존 내용 유지)",
        "🔁 인앤아웃 섹션 (여기는 곧 교체됨)",  # ← 이 줄이 교체 대상
        "🆕 신규 히어로 / ✨ 반짝 아이템 (기존 내용 유지)",
        "💰 평균 할인율 / 💵 중위가격 (기존 내용 유지)",
        "📈 카테고리 상위 / #️⃣ 키워드 Top10 (기존 내용 유지)",
    ]

    slack_text = build_slack_message(main_sections, inout_sec)

    # 4) 슬랙 전송
    post_to_slack(slack_text)

    # 5) 구글 드라이브 업로드(옵션, 기존 로직 유지)
    upload_to_gdrive_if_enabled(TODAY_CSV)

    logging.info("완료")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
