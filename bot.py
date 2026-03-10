import json, time, re, schedule, logging, os, requests
from datetime import datetime
from io import BytesIO
import openpyxl
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request

# KONFİQURASİYA
EXCEL_FILE_URL = os.environ.get("EXCEL_FILE_URL", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
PRICE_UNDERCUT = 0.01
MAX_WORKERS = 3  # Serverin donmaması üçün

# Sütunlar (Excel-ə əsasən: H=8, N=14, O=15, P=16)
COL_QIYMET = 8   # H sütunu (Qiymət yazılan yer)
COL_URL = 14     # N sütunu (Məhsul linki)
COL_MIN = 15     # O sütunu (Minimum qiymət)
COL_MAX = 16     # P sütunu (Maksimum qiymət)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def send_telegram(msg):
    if TELEGRAM_BOT_TOKEN:
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                           json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except: pass

def get_competitor_prices(url):
    prices = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200: return []
        
        # Səhifədəki gizli qiymətləri (rəqibləri) tapır
        found = re.findall(r'(?:merchantName|name)["\']?\s*:\s*["\']([^"\']+)["\'].{1,200}?price["\']?\s*:\s*([\d\.]+)', resp.text, re.I | re.S)
        for seller, p in found:
            if "unistore" not in seller.lower():
                prices.append(float(p))
    except: pass
    return list(set(prices))

def process_product(p):
    try:
        current = p['current']
        min_p = p['min']
        max_p = p['max']
        
        comp_prices = get_competitor_prices(p['url'])
        
        # Rəqib yoxdursa -> Maksimum qiymətə qaldır
        if not comp_prices:
            if current < max_p:
                return {"row": p['row'], "new": max_p, "name": p['name'], "msg": f"📈 Tək satıcıyıq: {max_p}₼"}
            return None

        cheapest_competitor = min(comp_prices)

        # QAYDA 1: Əgər məndəki qiymət rəqiblə eynidirsə (və ya daha ucuzdur) -> Dəyişmə
        if current <= cheapest_competitor:
            return None

        # QAYDA 2: Rəqib məndən ucuzdursa -> 0.01 düş, amma O xanasından (Min) aşağı düşmə
        target = max(cheapest_competitor - PRICE_UNDERCUT, min_p)
        
        if abs(target - current) > 0.01:
            return {"row": p['row'], "new": round(target, 2), "name": p['name'], "msg": f"📉 Rəqib: {cheapest_competitor}₼ | Yeni: {round(target, 2)}₼"}
            
    except: pass
    return None

def run_check():
    log.info("🚀 Yoxlama başladı...")
    try:
        # Google Drive-dan faylı yüklə
        file_id = EXCEL_FILE_URL.split("/d/")[1].split("/")[0]
        creds = Credentials.from_service_account_info(json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}")), 
                                                      scopes=["https://www.googleapis.com/auth/drive"])
        resp = requests.get(f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx", timeout=30)
        
        wb = openpyxl.load_workbook(BytesIO(resp.content), data_only=True)
        ws = wb.active
        
        products = []
        for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), 2):
            url = row[COL_URL-1]
            if not url or "http" not in str(url): continue
            
            try:
                products.append({
                    "row": i, "url": str(url), "name": f"{row[3]} {row[2]}",
                    "current": float(str(row[COL_QIYMET-1]).replace(",",".").replace(" ","")),
                    "min": float(str(row[COL_MIN-1]).replace(",",".").replace(" ","")),
                    "max": float(str(row[COL_MAX-1]).replace(",",".").replace(" ",""))
                })
            except: continue

        changes = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_product, p) for p in products]
            for f in as_completed(futures):
                res = f.result()
                if res: changes.append(res)

        if changes:
            # Excel-i yenilə və geri yüklə
            wb = openpyxl.load_workbook(BytesIO(resp.content))
            ws = wb.active
            for c in changes:
                ws.cell(row=c['row'], column=COL_QIYMET, value=c['new'])
                send_telegram(f"💰 <b>{c['name']}</b>\n{c['msg']}")
            
            out = BytesIO()
            wb.save(out)
            requests.patch(f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media",
                headers={"Authorization": f"Bearer {creds.token}"}, data=out.getvalue(), timeout=60)
            log.info(f"✅ {len(changes)} məhsul yeniləndi.")
        else:
            log.info("✅ Dəyişiklik yoxdur.")

    except Exception as e:
        log.error(f"Xəta: {e}")

if __name__ == "__main__":
    run_check()
    schedule.every(10).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(1)
