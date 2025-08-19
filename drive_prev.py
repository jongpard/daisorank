# drive_prev.py
from __future__ import annotations

import os
import requests
from datetime import datetime, timedelta, timezone
from csv import DictReader
from typing import Dict

# ---- 환경/상수 ---------------------------------------------------------------

# 한국시간
KST = timezone(timedelta(hours=9))

# GitHub Actions Secrets에서 읽어옴 (이미 레포에 설정해둔 것 그대로 사용)
GDRIVE_FOLDER_ID     = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# 로컬 저장 폴더
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)


# ---- Google Drive 토큰/다운로드 유틸 ----------------------------------------

def _drive_access_token() -> str | None:
    """
    refresh_token으로 Google OAuth access_token 발급
    (google-auth 라이브러리 없이 HTTP 호출만 사용)
    """
    if not (GDRIVE_FOLDER_ID and GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] 환경변수 누락(GDRIVE_FOLDER_ID/CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN)")
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
        token = r.json().get("access_token")
        if not token:
            print("[Drive] 토큰 응답에 access_token 없음:", r.text[:200])
        return token
    except Exception as e:
        print("[Drive] 토큰 발급 실패:", e)
        return None


def _drive_download_exact_name(basename: str) -> bool:
    """
    Drive 폴더(GDRIVE_FOLDER_ID)에서 파일명을 '정확히' 매칭해 1개 내려받아 data/에 저장.
    """
    token = _drive_access_token()
    if not token:
        return False

    try:
        # 파일명 정확 매칭 + 지정 폴더 내 검색
        q = f"name = '{basename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        params = {"q": q, "fields": "files(id,name)", "pageSize": 1}
        res = requests.get(
            "https://www.googleapis.com/drive/v3/files",
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=20,
        )
        res.raise_for_status()
        files = res.json().get("files", [])
        if not files:
            print("[Drive] 폴더에서 전일 CSV를 찾지 못했습니다:", basename)
            return False

        file_id = files[0]["id"]
        dl = requests.get(
            f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media",
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )
        dl.raise_for_status()

        path = os.path.join(DATA_DIR, basename)
        with open(path, "wb") as f:
            f.write(dl.content)

        print("[Drive] 전일 CSV 다운로드 완료:", basename)
        return True
    except Exception as e:
        print("[Drive] 전일 CSV 다운로드 실패:", e)
        return False


# ---- 전일 CSV 로더 -----------------------------------------------------------

def _candidate_basenames(prefix: str) -> list[str]:
    """
    어제~그제~사흘전까지 3개 날짜 후보 파일명을 만들어 반환
    (어쩌다 전일 파일이 없을 때 대비)
    """
    names: list[str] = []
    now = datetime.now(KST)
    for d in (1, 2, 3):
        ymd = (now - timedelta(days=d)).strftime("%Y-%m-%d")
        names.append(f"{prefix}{ymd}.csv")
    return names


def _read_prev_csv(path: str, url_col: str, rank_col: str) -> Dict[str, int]:
    prev_map: Dict[str, int] = {}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in DictReader(f):
                url  = (row.get(url_col) or "").strip()
                rstr = (row.get(rank_col) or "").strip()
                if url and rstr.isdigit():
                    prev_map[url] = int(rstr)
    except Exception as e:
        print(f"[prev] CSV 읽기 실패({path}):", e)
    return prev_map


def load_prev_map(
    prefix: str = "다이소몰_뷰티위생_일간_",  # 파일명 규칙(프로젝트에 맞춰 조정 가능)
    url_col: str = "url",
    rank_col: str = "rank",
) -> Dict[str, int]:
    """
    전일(또는 전전일~사흘전) CSV를 data/에서 탐색 → 없으면 Drive에서 다운로드 → 로드.
    반환: {url: rank}
    """
    for basename in _candidate_basenames(prefix):
        local_path = os.path.join(DATA_DIR, basename)

        # 1) 로컬 먼저 탐색
        if not os.path.exists(local_path):
            # 2) 없으면 Drive에서 시도
            _drive_download_exact_name(basename)

        if os.path.exists(local_path):
            prev_map = _read_prev_csv(local_path, url_col, rank_col)
            if prev_map:
                print(f"[prev] 전일 CSV 로드 완료: {basename}, {len(prev_map)}건")
                return prev_map
            else:
                print(f"[prev] CSV 로드했지만 데이터가 비어있음: {basename}")

    print("[prev] 전일 CSV 부재 – 비교 없이 진행")
    return {}
