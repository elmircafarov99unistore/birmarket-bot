"""
Birmarket.az Qiymət İzləmə Botu - NİHAİ VE STABİL VERSİYA
"""

import json, time, re, schedule, logging, os, requests
from datetime import datetime
from typing import Optional
from io import BytesIO
import openpyxl
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from bs4 import BeautifulSoup

# KONFİGURASİYA (Railway Environment Variables)
EXCEL_FILE_URL         = os.environ.get("EXCEL_FILE_URL", "")
TELEGRAM_BOT_TOKEN     = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID       = os.environ.get("TELEGRAM_CHAT_ID", "")
CHECK_INTERVAL_MINUTES = 10
PRICE_UNDERCUT         = 0.01
MAX_WORKERS            = 3  # Railway donmaması üçün 3 idealdir
DATA_START_ROW         = 2

# Sütun nömrələri
COL_BARKOD=0; COL_QIYMET=7; COL_URL=13; COL_MIN=14; COL_MAX=15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# KÖMƏKÇİ FUNKSİYALAR
# ─────────────────────────────────────────────
def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except: pass

def get_credentials(scopes: list) -> Credentials:
    info = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
    return Credentials.from_service_account_info(info, scopes=scopes)

def download_excel() -> bytes:
    file_id = EXCEL_FILE_URL.split("/d/")[1].split("/")[0]
    resp = requests.get(f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx", timeout=30)
    resp.raise_for_status()
    return resp.content

def upload_excel(data: bytes) -> bool:
    try:
        file_id = EXCEL_FILE_URL.split("/d/")[1].split("/")[0]
        creds = get_credentials(["https://www.googleapis.com/auth/drive"])
        creds.refresh(Request())
        resp = requests.patch(f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media",
            headers={"Authorization": f"Bearer {creds.token}", "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            data=data, timeout=60)
        return resp.status_code == 200
    except Exception as e:
        log.error(f"Upload xətası: {e}"); return False

# ─────────────────────────────────────────────
# RƏQİB QİYMƏT SCRAPER (PLAYWRIGHT YOXDUR - REQUESTS + JS SCAN)
# ─────────────────────────────────────────────
def get_competitor_prices(barkod: str, product_url: str = "") -> list:
    prices = []
    try:
        url = product_url if (product_url and "http" in product_url) else f"https://birmarket.az/search?q={barkod}"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200: return []
        
        html_text = resp.text
        # JS və HTML içindəki gizli rəqibləri tap (NUXT Data)
        found = re.findall(r'(?:merchantName|name)["\']?\s*:\s*["\']([^"\']+)["\'].{1,200}?price["\']?\s*:\s*([\d\.]+)', html_text, re.I | re.S)
        for seller, p in found:
            if "unistore" not in seller.lower():
                prices.append(float(p))
    except Exception as e:
        log.warning(f"Scrape xətası: {e}")
    return list(set(prices))

# ─────────────────────────────────────────────
# MƏHSUL EMALI
# ─────────────────────────────────────────────
def process_product(p: dict):
    try:
        comp_prices = get_competitor_prices(p["barkod"], p["url"])
        current, min_p, max_p = p["current"], p["min"], p["max"]

        # Rəqib yoxdursa -> Qiyməti MAX et
        if not comp_prices:
            if current < max_p:
                log.info(f"📈 {p['name']}: Tək satıcıyıq -> {max_p}₼")
                return {"status": "updated", "new": max_p, "row": p["row"], "name": p["name"], "direction": "up"}
            return {"status": "best"}

        cheapest = min(comp_prices)
        log.info(f"🔍 {p['name']} | Rəqib: {cheapest} | Biz: {current}")

        if current > cheapest:
            target = max(cheapest - PRICE_UNDERCUT, min_p)
            if abs(target - current) > 0.05:
                return {"status": "updated", "new": round(target, 2), "row": p["row"], "name": p["name"], "direction": "down"}
        elif current < cheapest - 0.05:
            target = min(cheapest - PRICE_UNDERCUT, max_p)
            if abs(target - current) > 0.05:
                return {"status": "updated", "new": round(target, 2), "row": p["row"], "name": p["name"], "direction": "up"}
    except: pass
    return {"status": "best"}

def run_check():
    log.info("="*50)
    log.info(f"🚀 Yoxlama başladı: {datetime.now().strftime('%H:%M:%S')}")
    try:
        excel_data = download_excel()
        wb = openpyxl.load_workbook(BytesIO(excel_data), data_only=True)
        ws = wb.active # Və ya CONFIG["sheet_name"]
        
        products = []
        for i, row in enumerate(ws.iter_rows(min_row=DATA_START_ROW, values_only=True), DATA_START_ROW):
            if not row[COL_BARKOD]: continue
            try:
                products.append({
                    "barkod": str(row[COL_BARKOD]), "row": i,
                    "name": f"{row[3]} {row[2]}",
                    "current": float(str(row[COL_QIYMET]).replace(",",".").replace(" ","")),
                    "min": float(str(row[COL_MIN]).replace(",",".").replace(" ","")),
                    "max": float(str(row[COL_MAX]).replace(",",".").replace(" ","")),
                    "url": str(row[COL_URL]) if row[COL_URL] else ""
                })
            except: continue

        changes = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_product, p) for p in products]
            for f in as_completed(futures):
                res = f.result()
                if res["status"] == "updated":
                    changes.append(res)

        if changes:
            log.info(f"💾 {len(changes)} məhsul yenilənir...")
            wb = openpyxl.load_workbook(BytesIO(excel_data))
            ws = wb.active
            for c in changes:
                ws.cell(row=c["row"], column=COL_QIYMET+1, value=c["new"])
                send_telegram(f"💰 <b>{c['name']}</b>\n{c['direction'] == 'up' and '📈' or '📉'} Qiymət: <b>{c['new']}₼</b>")
            
            out = BytesIO()
            wb.save(out)
            if upload_excel(out.getvalue()):
                send_telegram(f"✅ <b>Yoxlama bitdi.</b> {len(changes)} qiymət dəyişdi.")
        else:
            send_telegram("✅ <b>Yoxlama bitdi.</b> Qiymət dəyişikliyi yoxdur.")

    except Exception as e:
        log.error(f"Sistem xətası: {e}")

if __name__ == "__main__":
    run_check()
    schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(1)
