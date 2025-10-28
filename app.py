# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (이름 기준 비교 · 200개 고정 · Slack 포맷 수정)
import os, re, csv, io, sys, time, random, traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple

import requests
from playwright.sync_api import sync_playwright, Page

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ========= 설정 =========
RANK_URL = "https://www.daisomall.co.kr/ds/rank/C105"
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))     # 200개 강제
TOPN = int(os.getenv("TOP_WINDOW", str(MAX_ITEMS))) # 비교 윈도우도 200
SCROLL_PAUSE_MS = int(float(os.getenv("SCROLL_PAUSE", "650")))
SCROLL_STABLE_ROUNDS = int(os.getenv("SCROLL_STABLE_ROUNDS", "10"))
SCROLL_MAX_ROUNDS = int(os.getenv("SCROLL_MAX_ROUNDS", "220"))
SCROLL_JIGGLE_PX = int(os.getenv("SCROLL_JIGGLE_PX", "600"))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))

# ========= 유틸 =========
def now_kst(): return datetime.now(KST)
def today_str(): return now_kst().strftime("%Y-%m-%d")
def yday_str(): return (now_kst() - timedelta(days=1)).strftime("%Y-%m-%d")
def log(msg): print(f"[{now_kst().strftime('%H:%M:%S')}] {msg}", flush=True)
def ensure_dirs(): os.makedirs("data/debug", exist_ok=True); os.makedirs("data", exist_ok=True)

def strip_best(name: str) -> str:
    if not name: return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()

PRICE_STOPWORDS = r"(택배배송|매장픽업|오늘배송|별점|리뷰|구매|쿠폰|장바구니|찜|상세|배송비|혜택|적립)"
def parse_name_price(text: str) -> Tuple[Optional[str], Optional[int]]:
    text = re.sub(r"\s+", " ", (text or "")).strip()
    m = re.search(r"([0-9][0-9,]*)\s*원\s*(.+?)(?:\s*(?:%s))" % PRICE_STOPWORDS, text)
    if not m:
        m = re.search(r"([0-9][0-9,]*)\s*원\s*(.+)$", text)
        if not m: return None, None
    try: price = int(m.group(1).replace(",", ""))
    except Exception: price = None
    name = strip_best(m.group(2).strip())
    if name and len(name) < 2: name = None
    return name or None, price

# ========= DOM 조작 =========
def close_overlays(page: Page):
    for sel in [
        ".layer-popup .btn-close", ".modal .btn-close", ".popup .btn-close",
        ".layer-popup .close", ".modal .close", ".popup .close",
        ".btn-x", ".btn-close, button[aria-label='닫기']",
    ]:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=800)
                page.wait_for_timeout(150)
        except Exception: pass

def _click_beauty_chip(page: Page) -> bool:
    close_overlays(page); ok=False
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count()>0:
            page.locator('.prod-category .cate-btn[value="CTGR_00014"]').first.click(timeout=1500); ok=True
        else:
            page.get_by_role("button", name=re.compile("뷰티\\/?위생")).click(timeout=1500); ok=True
    except Exception:
        try:
            page.evaluate("""
                () => {
                  const v=document.querySelector('.prod-category .cate-btn[value="CTGR_00014"]');
                  if (v) v.click();
                  else {
                    const btns=[...document.querySelectorAll('.prod-category .cate-btn, .prod-category *')];
                    const t=btns.find(b=>/뷰티\\/?위생/.test((b.textContent||'').trim()));
                    if (t) t.click();
                  }
                }
            """); ok=True
        except Exception: ok=False
    page.wait_for_timeout(300); return ok

def _click_daily(page: Page) -> bool:
    ok=False
    try:
        if page.locator('.ipt-sorting input[value="2"]').count()>0:
            page.locator('.ipt-sorting input[value="2"]').first.click(timeout=1500); ok=True
    except Exception: pass
    if not ok:
        try: page.get_by_role("button", name=re.compile("일간")).click(timeout=1500); ok=True
        except Exception:
            try:
                page.evaluate("""
                    () => {
                      const els=[...document.querySelectorAll('button,[role=button],.ipt-sorting *')];
                      const t=els.find(e=>/일간/.test((e.textContent||'').trim())); if (t) t.click();
                    }
                """); ok=True
            except Exception: ok=False
    try:
        page.wait_for_function(
            "()=>document.querySelectorAll('div.product-info a[href*=\"/pd/pdr/\"]').length>0", timeout=5000
        ); ok=True
    except Exception: ok=False
    page.wait_for_timeout(350); return ok

def _count_cards(page: Page) -> int:
    try:
        return int(page.evaluate("()=>document.querySelectorAll('div.product-info a[href*=\"/pd/pdr/\"]').length"))
    except Exception:
        return 0

def _load_all(page: Page, target_min: int = MAX_ITEMS) -> int:
    prev=0; stable=0
    for _ in range(SCROLL_MAX_ROUNDS):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        try:
            more = page.locator("button:has-text('더보기'), button:has-text('더 보기'), a:has-text('더보기')")
            if more.count()>0: more.first.click(timeout=800); page.wait_for_timeout(400)
        except Exception: pass
        try:
            jiggle = random.randint(200, SCROLL_JIGGLE_PX)
            page.evaluate(f"window.scrollBy(0, -{jiggle})"); page.wait_for_timeout(120)
            page.evaluate(f"window.scrollBy(0, {jiggle + 200})")
        except Exception: pass
        try:
            page.wait_for_function(
                f"(prev)=>{{const n=document.querySelectorAll('div.product-info a[href*=\"/pd/pdr/\"]').length;return n>prev||n>={target_min};}}",
                timeout=4000, arg=prev
            )
        except Exception: pass
        cnt=_count_cards(page)
        if cnt>=target_min: return cnt
        if cnt==prev: stable+=1
        else: stable=0; prev=cnt
        if stable>=SCROLL_STABLE_ROUNDS: break
    return _count_cards(page)

# ========= 추출 + 200개 고정 =========
def _extract_items(page: Page) -> List[Dict]:
    data = page.evaluate("""
      () => {
        const cards = [...document.querySelectorAll('div.product-info')];
        const items = [];
        for (const info of cards) {
          const a = info.querySelector('a[href*="/pd/pdr/"]');
          if (!a) continue;
          let href = a.getAttribute('href') || a.href || '';
          if (!href) continue;
          if (!/^https?:/i.test(href)) href = new URL(href, location.origin).href;
          const text = (info.textContent || '').replace(/\\s+/g,' ').trim();
          items.push({ raw: text, url: href });
        }
        return items;
      }
    """)
    cleaned=[]
    for it in data:
        name, price = parse_name_price(it.get("raw",""))
        url = it.get("url","")
        if not (name and price and price>0 and url): continue
        cleaned.append({"name": name, "price": price, "url": url})
    # 200개로 컷 + 랭크 재부여
    rows = cleaned[:MAX_ITEMS]
    for i, it in enumerate(rows, 1): it["rank"] = i
    return rows

# ========= CSV =========
def save_csv(rows: List[Dict]) -> Tuple[str,str]:
    ensure_dirs()
    filename = f"다이소몰_뷰티위생_일간_{today_str()}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","rank","name","price","url"])
        for r in rows:
            w.writerow([today_str(), r["rank"], r["name"], r["price"],
