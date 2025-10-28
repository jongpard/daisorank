# app.py — 다이소몰 뷰티/위생 '일간' 랭킹 수집/분석
# - 카테고리·일간 강제 검증(라디오/라벨/버튼/JS)
# - Top200 확로드(컨테이너 직접 스크롤 + 위글 + 더보기)
# - 견고 추출(제품명/가격/링크), pdNo 비교, Slack 포맷 고정

import os, re, csv, io
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import requests
from playwright.sync_api import sync_playwright, Page

# Google Drive (OAuth)
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ===== 설정 =====
RANK_URL = os.getenv("RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
TOPN = int(os.getenv("TOPN", "200"))
SCROLL_MAX_ROUNDS = int(os.getenv("SCROLL_MAX_ROUNDS", "260"))
SCROLL_PAUSE_MS = int(os.getenv("SCROLL_PAUSE_MS", "700"))

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Drive OAuth
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

KST = timezone(timedelta(hours=9))
def today_str(): return datetime.now(KST).strftime("%Y-%m-%d")
def yday_str():  return (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")

def strip_best(s: str) -> str:
    if not s: return ""
    s = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", s, flags=re.I)
    s = re.sub(r"\s*\bBEST\b\s*", " ", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip()

def extract_pdno(url: str) -> Optional[str]:
    if not url: return None
    m = re.search(r"[?&]pdNo=(\d+)", url)
    if m: return m.group(1)
    m = re.search(r"/pd/(?:pdr|detail)/(\d+)", url)
    if m: return m.group(1)
    return None

# ===== 공통 =====
def _count_cards(page: Page) -> int:
    try:
        return page.evaluate("""
          () => document.querySelectorAll(
            'a[href*="pdNo="], a[href*="/pd/pdr/"], a[href*="/product/detail/"]'
          ).length
        """)
    except Exception:
        return 0

def _click_via_js(page: Page, text: str):
    page.evaluate("""
      (txt) => {
        const nodes = [...document.querySelectorAll('button, a, .cate-btn, .chip, .tab *, .category *')];
        const t = nodes.find(n => (n.textContent||'').trim().includes(txt));
        if (t) {
          t.scrollIntoView({block:'center'});
          t.dispatchEvent(new MouseEvent('mouseover', {bubbles:true}));
          t.dispatchEvent(new MouseEvent('mousedown', {bubbles:true}));
          t.click();
          t.dispatchEvent(new MouseEvent('mouseup', {bubbles:true}));
        }
      }
    """, text)

def _beauty_chip_active(page: Page) -> bool:
    try:
        return page.evaluate("""
          () => {
            const all = [...document.querySelectorAll('.prod-category * , .category, .cate, .chips, .tab, .filter *')];
            const isActive = (el) => {
              const c = (el.className||'') + ' ' + (el.parentElement?.className||'');
              return /\\bis-active\\b|\\bon\\b|\\bactive\\b|\\bselected\\b/i.test(c)
                  || el.getAttribute('aria-selected')==='true'
                  || el.getAttribute('aria-pressed')==='true';
            };
            const t = all.find(n => /뷰티\\/?위생/.test((n.textContent||'').trim()));
            return !!(t && isActive(t));
          }
        """)
    except Exception:
        return False

def _period_is_daily(page: Page) -> bool:
    try:
        return page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp && (inp.checked || inp.getAttribute('checked')==='true')) return true;
            const nodes = [...document.querySelectorAll('*')];
            const isActive = (el) => {
              const c = (el.className||'') + ' ' + (el.parentElement?.className||'');
              return /\\bis-active\\b|\\bon\\b|\\bactive\\b|\\bselected\\b/i.test(c)
                  || el.getAttribute('aria-selected')==='true'
                  || el.getAttribute('aria-pressed')==='true'
            };
            const t = nodes.filter(n => /일간/.test((n.textContent||'').trim()));
            return t.some(isActive);
          }
        """)
    except Exception:
        return False

def _click_beauty_chip(page: Page) -> bool:
    try:
        page.evaluate("""
          () => document.querySelector('.prod-category')?.scrollIntoView({block:'center'})
        """)
        page.wait_for_timeout(150)
    except Exception:
        pass
    before = _count_cards(page)
    for attempt in range(8):
        clicked = False
        for sel in [
            '.prod-category .cate-btn[value="CTGR_00014"]',
            "button:has-text('뷰티/위생')",
            "a:has-text('뷰티/위생')",
        ]:
            try:
                loc = page.locator(sel)
                if loc.count() > 0:
                    loc.first.scroll_into_view_if_needed()
                    page.wait_for_timeout(100)
                    loc.first.click(timeout=800)
                    clicked = True; break
            except Exception:
                continue
        if not clicked:
            _click_via_js(page, "뷰티/위생")
        page.wait_for_timeout(350)
        if _beauty_chip_active(page) or _count_cards(page) != before:
            return True
    return _beauty_chip_active(page)

def _click_daily(page: Page) -> bool:
    for _ in range(10):
        try:
            page.evaluate("window.scrollTo(0,0)")
        except Exception:
            pass
        try:
            inp = page.locator('.ipt-sorting input[value="2"]')
            if inp.count() > 0: inp.first.click(timeout=800)
        except Exception: pass
        # label for=
        page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp && inp.id) {
              const lb = document.querySelector('label[for="'+inp.id+'"]');
              if (lb) lb.click();
            }
          }
        """)
        # 버튼/텍스트
        try:
            page.get_by_role("button", name=re.compile("일간")).first.click(timeout=800)
        except Exception:
            _click_via_js(page, "일간")
        # 최후수단: 체크 강제 + 이벤트
        page.evaluate("""
          () => {
            const inp = document.querySelector('.ipt-sorting input[value="2"]');
            if (inp) {
              inp.checked = true;
              inp.setAttribute('checked','true');
              ['input','change','click'].forEach(ev => inp.dispatchEvent(new Event(ev,{bubbles:true})));
            }
          }
        """)
        page.wait_for_timeout(350)
        try: page.wait_for_load_state("networkidle", timeout=1200)
        except Exception: pass
        if _period_is_daily(page): return True
    return _period_is_daily(page)

# ===== 스크롤 로딩(강화판) =====
def _try_more_button(page: Page) -> bool:
    try:
        btn = page.locator("button:has-text('더보기'), a:has-text('더보기')")
        if btn.count() > 0:
            btn.first.scroll_into_view_if_needed()
            btn.first.click(timeout=700)
            page.wait_for_timeout(300)
            return True
    except Exception:
        pass
    return False

def _load_all(page: Page, want: int):
    """
    - 컨테이너(.goods-list 등) 직접 scrollTop=scrollHeight
    - 마지막 카드 scrollIntoView
    - 위글(살짝 위로 올렸다가 끝까지 내리기)
    - '더보기' 버튼 병행
    - 증가 멈추면 네트워크 안정 대기 + 추가 라운드
    """
    def js_scroll_once():
        page.evaluate("""
          () => {
            const list = document.querySelector('.goods-list') || document.querySelector('.list') ||
                         document.scrollingElement || document.body;
            list.scrollTop = list.scrollHeight;
            window.scrollTo(0, document.body.scrollHeight);
            const cards = document.querySelectorAll('a[href*="pdNo="], a[href*="/pd/pdr/"], a[href*="/product/detail/"]');
            if (cards.length) {
              const last = cards[cards.length-1].closest('.product-info, .goods-unit, .goods-item, .goods-unit-v2, li') || cards[cards.length-1];
              last.scrollIntoView({block:'end'});
            }
            window.dispatchEvent(new Event('scroll'));
          }
        """)

    def js_wiggle():
        page.evaluate("""
          () => {
            window.scrollBy(0, -600);
            window.scrollBy(0,  5000);
          }
        """)

    prev = 0
    same_ticks = 0

    for round_idx in range(SCROLL_MAX_ROUNDS):
        js_scroll_once()
        page.wait_for_timeout(SCROLL_PAUSE_MS)
        _try_more_button(page)
        try: page.wait_for_load_state("networkidle", timeout=1200)
        except Exception: pass

        cnt = _count_cards(page)
        if cnt >= want:
            break

        if cnt == prev:
            same_ticks += 1
            js_wiggle()
            page.wait_for_timeout(int(SCROLL_PAUSE_MS*0.8))
        else:
            same_ticks = 0
            prev = cnt

        if same_ticks >= 4:
            # 트리거 재가동
            _try_more_button(page)
            js_scroll_once()
            same_ticks = 0
            page.wait_for_timeout(SCROLL_PAUSE_MS)

    # 부족하면 3라운드 더 강제 밀기
    for _ in range(3):
        if _count_cards(page) >= want: break
        for __ in range(8):
            js_scroll_once()
            page.wait_for_timeout(int(SCROLL_PAUSE_MS*0.9))
        _try_more_button(page)
        page.wait_for_timeout(600)

# ===== 추출 =====
def _extract_items(page: Page) -> List[Dict]:
    data = page.evaluate("""
      () => {
        const anchors = [...document.querySelectorAll('a[href*="pdNo="], a[href*="/pd/pdr/"], a[href*="/product/detail/"]')];
        const seenCard = new Set();
        const rows = [];

        const cleanName = (s) => {
          if (!s) return '';
          s = s.trim();
          s = s.replace(/(택배배송|오늘배송|매장픽업|별점\\s*\\d+[.,\\d]*점|\\d+[.,\\d]*\\s*건\\s*작성).*$/g, '').trim();
          return s;
        };

        const cardOf = (a) => a.closest('.product-info, .goods-unit, .goods-item, .goods-unit-v2, li') || a.parentElement;

        for (const a of anchors) {
          const card = cardOf(a);
          if (!card || seenCard.has(card)) continue;
          seenCard.add(card);

          const nameEl = card.querySelector('.goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .goods-name, .name') || a;
          let name = cleanName(nameEl?.textContent || '');
          if (!name) continue;

          const priceEl = card.querySelector('.goods-detail .goods-price .value, .price .num, .sale-price .num, .sale .price, .goods-price .num, .price');
          let priceText = (priceEl?.textContent || '').replace(/[^0-9]/g, '');
          let price = parseInt(priceText or '0', 10);
          if (!price or price <= 0):
              pass
        }
        return rows;
      }
    """)
