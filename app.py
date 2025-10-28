# app.py — DaisoMall 뷰티/위생 '일간' 랭킹 수집 (간헐적 0개 방지 3중 방어판)
# - 수집 안정화: 팝업/동의 닫기, 카테고리/일간 고정, 스크롤+더보기 병행, 멀티 셀렉터 & 최후 안전망
# - Slack 포맷: 예전 형식 유지 + 인&아웃 **볼드**
# - Drive: 사용자 OAuth만 사용(ADC 미사용), 전일 파일 Drive→로컬 폴백

import os, re, csv, time, io
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Union

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Page, Locator

# ====== 설정 ======
RANK_URL = os.getenv("RANK_URL", "https://www.daisomall.co.kr/ds/rank/C105")
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "200"))   # 최소 확보 목표
TOP_WINDOW = int(os.getenv("TOP_WINDOW", "150"))
KST = timezone(timedelta(hours=9))
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Drive (사용자 OAuth)
GDRIVE_FOLDER_ID     = os.getenv("GDRIVE_FOLDER_ID", "")
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.getenv("GOOGLE_REFRESH_TOKEN", "")

# ====== Google API ======
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.oauth2.credentials import Credentials as UserCredentials
from google.auth.transport.requests import Request as GoogleRequest

# ================= 공용 유틸 =================
def today_str(): return datetime.now(KST).strftime("%Y-%m-%d")
def yday_str():  return (datetime.now(KST)-timedelta(days=1)).strftime("%Y-%m-%d")
def ensure_dir(p): d=os.path.dirname(p);  (d and not os.path.exists(d)) and os.makedirs(d, exist_ok=True)
def strip_best(name: str) -> str:
    if not name: return ""
    name = re.sub(r"^\s*BEST\s*[\|\-:\u00A0]*", "", name, flags=re.I)
    name = re.sub(r"\s*\bBEST\b\s*", " ", name, flags=re.I)
    return re.sub(r"\s+", " ", name).strip()

# ================= Playwright 보조 =================
def _to_locator(page: Page, target: Union[str, Locator]) -> Locator:
    return target if isinstance(target, Locator) else page.locator(target)

def close_overlays(page: Page):
    # 동의/쿠키/앱유도/공지 닫기
    sels = [
        "button[aria-label='닫기']", ".btn-close", ".popup .close", ".modal .close", ".layer-popup .btn-close",
        "button:has-text('닫기')", "button:has-text('동의')", "button:has-text('확인')"
    ]
    for s in sels:
        try:
            while page.locator(s).count() > 0:
                page.locator(s).first.click(timeout=500)
                page.wait_for_timeout(120)
        except Exception:
            pass

def click_hard(page: Page, target: Union[str, Locator], name_for_log=""):
    loc = _to_locator(page, target)
    try: loc.first.wait_for(state="attached", timeout=2500)
    except Exception: raise RuntimeError(f"[click_hard] 대상 미존재: {name_for_log}")
    for _ in range(3):
        try:
            loc.first.click(timeout=900); return
        except Exception:
            try: loc.first.scroll_into_view_if_needed(timeout=600); page.wait_for_timeout(120)
            except Exception: pass
    try: loc.first.evaluate("(el)=>el.click()")
    except Exception: raise RuntimeError(f"[click_hard] 클릭 실패: {name_for_log}")

# ================= 수집: 고정 + 스크롤/더보기 병행 =================
def select_beauty_daily(page: Page):
    close_overlays(page)
    # 카테고리: 뷰티/위생
    try:
        if page.locator('.prod-category .cate-btn[value="CTGR_00014"]').count()>0:
            click_hard(page, '.prod-category .cate-btn[value="CTGR_00014"]', "뷰티/위생(value)")
        else:
            click_hard(page, page.get_by_role("button", name=re.compile("뷰티\\/?위생")), "뷰티/위생(text)")
    except Exception:
        page.evaluate("""
            () => {
              const v=document.querySelector('.prod-category .cate-btn[value="CTGR_00014"]');
              if(v) v.click();
              else{
                const n=[...document.querySelectorAll('.prod-category *')].find(x=>/뷰티\\/?위생/.test((x.textContent||'').trim()));
                if(n) (n.closest('button')||n).click();
              }
            }
        """)
    page.wait_for_load_state("networkidle"); page.wait_for_timeout(250)
    # 정렬: 일간
    try: click_hard(page, '.ipt-sorting input[value="2"]', "일간(value)")
    except Exception:
        try: click_hard(page, page.get_by_role("button", name=re.compile("일간")), "일간(text)")
        except Exception: pass
    page.wait_for_load_state("networkidle"); page.wait_for_timeout(300)

def _scroll_mix(page: Page):
    # End키 + wheel + scrollTo 혼합
    page.keyboard.press("End")
    page.wait_for_timeout(300)
    try: page.mouse.wheel(0, 20000)
    except Exception: pass
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(400)

def _try_more_button(page: Page):
    try:
        btn = page.locator("button:has-text('더보기')")
        if btn.count() > 0:
            btn.first.click(timeout=800)
            page.wait_for_timeout(400)
            return True
    except Exception:
        pass
    return False

def infinite_load(page: Page, want: int):
    prev = 0; stable = 0
    for _ in range(160):
        clicked = _try_more_button(page)
        if not clicked: _scroll_mix(page)
        cnt = page.evaluate("""() => document.querySelectorAll(
            '.goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2'
        ).length""")
        if cnt >= want: break
        if cnt == prev:
            stable += 1
            if stable >= 5: break
        else:
            stable = 0; prev = cnt

def collect_items(page: Page) -> List[Dict]:
    # 1차: 카드 셀렉터 기반
    data = page.evaluate("""
        () => {
          const qs = s => [...document.querySelectorAll(s)];
          const units = qs('.goods-list .goods-unit, .goods-list .goods-item, .goods-list li.goods, .goods-unit-v2');
          const seen = new Set(); const items = [];
          const getTxt = el => (el?.textContent||'').trim();
          for (const el of units) {
            const nameEl = el.querySelector('img[alt], .goods-detail .tit a, .goods-detail .tit, .tit a, .tit, .name, .goods-name');
            let name = (nameEl?.getAttribute?.('alt')) || getTxt(nameEl);
            if (!name) continue;
            const priceEl = el.querySelector('.goods-detail .goods-price .value, .price .num, .sale-price .num, .sale .price, .goods-price .num');
            let priceTxt = getTxt(priceEl).replace(/[^0-9]/g,'');
            if (!priceTxt) {
              // 주변에서 숫자만 최후 추출
              const near = getTxt(el);
              const m = near.match(/([0-9][0-9,]{3,})원?/);
              priceTxt = m ? m[1].replace(/,/g,'') : '';
            }
            if (!priceTxt) continue;
            const price = parseInt(priceTxt,10);
            if (!price||price<=0) continue;
            let href = null;
            const a = el.querySelector('a[href*="/pd/pdr/"]') || el.querySelector('a[href*="/pd/"]');
            if (a && a.href) href = a.href;
            if (!href) continue;
            if (seen.has(href)) continue; seen.add(href);
            items.push({ name, price, url: href });
          }
          return items;
        }
    """)
    # 2차: 페이지 전체 a[href*="/pd/pdr/"] 스윕(카드가 못 잡혔을 때)
    if len(data) < 20:
        more = page.evaluate("""
            () => {
              const a = [...document.querySelectorAll('a[href*="/pd/pdr/"], a[href*="/pd/"]')];
              const items=[]; const seen=new Set();
              for(const el of a){
                const href=el.href; if(!href||seen.has(href)) continue; seen.add(href);
                const name = el.getAttribute('title') || (el.querySelector('img[alt]')?.getAttribute('alt')) || (el.textContent||'').trim();
                if(!name) continue;
                items.push({name, price: 0, url: href});
              }
              return items;
            }
        """)
        # 이름만 얻었으면 가격은 0으로, 후처리에서 걸러짐
        data += more

    cleaned = []
    for it in data:
        nm = strip_best(it.get("name"))
        price = int(re.sub(r"[^0-9]", "", str(it.get("price") or "0")) or 0)
        url = it.get("url")
        if not nm or not url or price <= 0:  # 가격 없으면 버림(최후 안전망으로 다시 채운 상태라면 0이 많을 수 있음)
            continue
        cleaned.append({"name": nm, "price": price, "url": url})
    for i, it in enumerate(cleaned, 1):
        it["rank"] = i
    return cleaned

def fetch_products() -> List[Dict]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-dev-shm-usage"])
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/123.0.0.0 Safari/537.36"),
            java_script_enabled=True,
        )
        page = context.new_page()
        page.set_default_timeout(30000)
        page.goto(RANK_URL, wait_until="domcontentloaded", timeout=60000)
        try: page.wait_for_selector(".prod-category", timeout=15000)
        except PWTimeout: pass
        select_beauty_daily(page)
        try: page.wait_for_selector(".goods-list", timeout=20000)
        except PWTimeout: pass

        infinite_load(page, MAX_ITEMS)
        items = collect_items(page)

        # 최후: 정말 0개면 한 번 더 강제 로드
        if len(items) == 0:
            for _ in range(6): _scroll_mix(page)
            time.sleep(1.0)
            items = collect_items(page)

        context.close(); browser.close()
        return items

# ================= CSV =================
def save_csv(rows: List[Dict]):
    date_str = today_str()
    os.makedirs("data", exist_ok=True)
    filename = f"다이소몰_뷰티위생_일간_{date_str}.csv"
    path = os.path.join("data", filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["date","rank","name","price","url"])
        for r in rows: w.writerow([date_str, r["rank"], r["name"], r["price"], r["url"]])
    return path, filename

# ================= Drive (사용자 OAuth만) =================
def build_drive_service():
    if not (GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REFRESH_TOKEN):
        print("[Drive] 사용자 OAuth 미설정 → Drive 비활성화"); return None
    try:
        creds = UserCredentials(
            None, refresh_token=GOOGLE_REFRESH_TOKEN,
            client_id=GOOGLE_CLIENT_ID, client_secret=GOOGLE_CLIENT_SECRET,
            token_uri="https://oauth2.googleapis.com/token",
            scopes=["https://www.googleapis.com/auth/drive.file"],
        )
        creds.refresh(GoogleRequest())
        return build("drive","v3",credentials=creds,cache_discovery=False)
    except Exception as e:
        print("[Drive] 서비스 생성 실패:", e); return None

def upload_to_drive(svc, filepath: str, filename: str):
    if not svc or not GDRIVE_FOLDER_ID:
        print("[Drive] 서비스/폴더 미설정 → 업로드 생략"); return None
    try:
        media = MediaIoBaseUpload(io.FileIO(filepath,'rb'), mimetype="text/csv", resumable=True)
        body  = {"name": filename, "parents": [GDRIVE_FOLDER_ID]}
        file  = svc.files().create(body=body, media_body=media, fields="id,name").execute()
        print(f"[Drive] 업로드 성공: {file.get('name')} (ID: {file.get('id')})"); return file.get("id")
    except Exception as e:
        print("[Drive] 업로드 실패:", e); return None

def find_file_in_drive(svc, filename: str):
    if not svc or not GDRIVE_FOLDER_ID: return None
    try:
        q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
        res = svc.files().list(q=q,pageSize=1,fields="files(id,name)").execute()
        return res.get("files", [])[0] if res.get("files") else None
    except Exception as e:
        print(f"[Drive] 파일 검색 실패 ({filename}):", e); return None

def download_from_drive(svc, file_id: str) -> Optional[str]:
    try:
        req = svc.files().get_media(fileId=file_id); fh = io.BytesIO()
        dl = MediaIoBaseDownload(fh, req); done=False
        while not done: _, done = dl.next_chunk()
        fh.seek(0); return fh.read().decode("utf-8")
    except Exception as e:
        print(f"[Drive] 파일 다운로드 실패 (ID:{file_id}):", e); return None

def read_local_yday_csv() -> Optional[str]:
    path = os.path.join("data", f"다이소몰_뷰티위생_일간_{yday_str()}.csv")
    if not os.path.exists(path): return None
    try:
        with open(path,"r",encoding="utf-8") as f: return f.read()
    except Exception: return None

# ================= 분석 =================
def parse_prev_csv(csv_text: str) -> List[Dict]:
    items=[]; rdr = csv.DictReader(io.StringIO(csv_text))
    for row in rdr:
        try: items.append({"rank": int(row.get("rank")), "name": row.get("name"), "url": row.get("url")})
        except Exception: continue
    return items

def analyze_trends(today_items: List[Dict], prev_items: List[Dict]):
    prev_map = {p["url"]: p["rank"] for p in prev_items if p.get("url")}
    prev_top = {p["url"] for p in prev_items if p.get("url") and p.get("rank",9999) <= TOP_WINDOW}
    trends=[]
    for it in today_items:
        u=it.get("url");  pr=prev_map.get(u)
        trends.append({"name":it["name"],"url":u,"rank":it["rank"],"prev_rank":pr,"change":(pr-it["rank"]) if pr else None})
    movers=[t for t in trends if t["prev_rank"] is not None]
    ups   = sorted([t for t in movers if t["change"]>0], key=lambda x:x["change"], reverse=True)
    downs = sorted([t for t in movers if t["change"]<0], key=lambda x:x["change"])
    chart_ins=[t for t in trends if t["prev_rank"] is None and t["rank"] <= TOP_WINDOW]
    today_urls={t["url"] for t in trends}; rank_out_urls = prev_top - today_urls
    rank_outs=[p for p in prev_items if p.get("url") in rank_out_urls]
    if prev_items:
        today_keys={t["url"] for t in trends if t.get("url") and t.get("rank",9999)<=TOP_WINDOW}
        prev_keys ={p["url"] for p in prev_items if p.get("url") and p.get("rank",9999)<=TOP_WINDOW}
        io_cnt = len(today_keys.symmetric_difference(prev_keys)) // 2
    else:
        io_cnt = min(len(chart_ins), len(rank_outs))
    return ups, downs, chart_ins, rank_outs, io_cnt

# ================= Slack (예전 포맷 유지) =================
def post_slack(rows: List[Dict], analysis_results, prev_items: Optional[List[Dict]] = None):
    if not SLACK_WEBHOOK: print("[Slack] Webhook 미설정 → 생략"); return
    ups, downs, chart_ins, rank_outs, _ = analysis_results
    def _link(n,u): return f"<{u}|{n}>" if u else (n or "")
    def _key(d):   return (d.get("url") or "").strip() or (d.get("name") or "").strip()
    prev_map={}
    if prev_items:
        for p in prev_items:
            try: r=int(p.get("rank") or 0)
            except: continue
            k=_key(p)
            if k and r>0: prev_map[k]=r
    now = datetime.now(KST)
    lines=[f"*다이소몰 뷰티/위생 일간 랭킹 200* ({now.strftime('%Y-%m-%d %H:%M KST')})"]
    lines.append("\n*TOP 10*")
    for it in (rows or [])[:10]:
        cur=int(it.get("rank") or 0); price=f"{int(it.get('price') or 0):,}원"
        k=_key(it); marker="(new)"
        if k in prev_map:
            prev=prev_map[k]; diff=prev-cur
            marker = f"(↑{diff})" if diff>0 else (f"(↓{abs(diff)})" if diff<0 else "(-)")
        lines.append(f"{cur}. {marker} {_link(it.get('name') or '', it.get('url'))} — {price}")
    lines.append("\n*🔥 급상승*")
    if ups:
        for m in ups[:5]:
            lines.append(f"- {_link(m.get('name'), m.get('url'))} {m.get('prev_rank')}위 → {m.get('rank')}위 (↑{m.get('change')})")
    else: lines.append("- (해당 없음)")
    lines.append("\n*🆕 뉴랭커*")
    if chart_ins:
        for t in chart_ins[:5]:
            lines.append(f"- {_link(t.get('name'), t.get('url'))} NEW → {t.get('rank')}위")
    else: lines.append("- (해당 없음)")
    lines.append("\n*📉 급하락*")
    if downs:
        downs_sorted=sorted(downs,key=lambda m:(-abs(int(m.get("change") or 0)), int(m.get("rank") or 9999)))
        for m in downs_sorted[:5]:
            drop=abs(int(m.get("change") or 0))
            lines.append(f"- {_link(m.get('name'), m.get('url'))} {m.get('prev_rank')}위 → {m.get('rank')}위 (↓{drop})")
    else: lines.append("- (급하락 없음)")
    if rank_outs:
        outs_sorted=sorted(rank_outs,key=lambda x:int(x.get("rank") or 9999))
        for ro in outs_sorted[:5]:
            lines.append(f"- {_link(ro.get('name'), ro.get('url'))} {int(ro.get('rank') or 0)}위 → OUT")
    else: lines.append("- (OUT 없음)")
    # 인&아웃(볼드)
    if prev_items:
        today_keys={_key(it) for it in (rows or [])[:200] if _key(it)}
        prev_keys ={_key(p)  for p  in (prev_items or []) if _key(p) and 1<=int(p.get("rank") or 0)<=200}
        io_cnt=len(today_keys.symmetric_difference(prev_keys))//2
    else:
        io_cnt=min(len(chart_ins or []), len(rank_outs or []))
    lines.append("\n*↔ 랭크 인&아웃*")
    lines.append(f"**{io_cnt}개의 제품이 인&아웃 되었습니다.**")
    try:
        requests.post(SLACK_WEBHOOK, json={"text":"\n".join(lines)}, timeout=12).raise_for_status()
        print("[Slack] 전송 성공")
    except Exception as e:
        print("[Slack] 전송 실패:", e)

# ================= main =================
def main():
    print("수집 시작:", RANK_URL)
    t0=time.time()
    rows=fetch_products()
    print(f"[수집 완료] 개수: {len(rows)}")
    if len(rows)<MAX_ITEMS:
        print(f"[경고] 목표 {MAX_ITEMS} < 실제 {len(rows)} — 소스 구조/로딩 이슈 가능")
    # CSV 저장
    csv_path, csv_filename=save_csv(rows)
    print("로컬 저장:", csv_path)
    # Drive
    svc=build_drive_service()
    if svc: upload_to_drive(svc, csv_path, csv_filename)
    # 전일 파일 로딩
    prev_items=[]
    yname=f"다이소몰_뷰티위생_일간_{yday_str()}.csv"
    csv_content=None
    if svc:
        prev_file=find_file_in_drive(svc, yname)
        if prev_file:
            print(f"[Drive] 전일 파일 발견: {prev_file['name']} (ID: {prev_file['id']})")
            csv_content=download_from_drive(svc, prev_file['id'])
        else:
            print(f"[Drive] 전일 파일 미발견 → 로컬 폴백: data/{yname}")
    if csv_content is None:
        try:
            with open(os.path.join("data", yname),"r",encoding="utf-8") as f:
                csv_content=f.read(); print("[Local] 전일 CSV 로컬 로드")
        except Exception:
            print("[Local] 전일 CSV 없음")
    if csv_content:
        prev_items=parse_prev_csv(csv_content)
        print(f"[분석] 전일 데이터 {len(prev_items)}건 로드")
    # 분석 & 슬랙
    analysis=analyze_trends(rows, prev_items)
    post_slack(rows, analysis, prev_items)
    print(f"총 {len(rows)}건, 경과 {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
