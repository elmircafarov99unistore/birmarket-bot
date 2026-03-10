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
MAX_WORKERS = 3 

# Sütunlar (Excel-ə əsasən: H=8, N=14, O=15, P=16)
COL_QIYMET = 8; COL_URL = 14; COL_MIN = 15; COL_MAX = 16

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def get_competitor_prices(url):
    prices = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200: return []
        
        raw_data = resp.text
        
        # 1. Bütün mümkün qiymət formatlarını tap (JS və JSON)
        # Qiymətləri dırnaqlı, dırnaqsız, vergüllü və ya nöqtəli şəkildə axtarır
        found_prices = re.findall(r'"price"\s*:\s*"?([\d\.,]+)"?', raw_data)
        
        for p_str in found_prices:
            try:
                # Qiyməti təmizlə: sondakı nöqtələri sil və vergülü nöqtəyə çevir
                clean_p = p_str.strip().rstrip('.')
                clean_p = clean_p.replace(",", ".")
                val = float(clean_p)
                if 1 < val < 100000:
                    prices.append(val)
            except: continue

        # 2. Satıcı adları ilə birlikdə axtarış (Daha dəqiq)
        matches = re.findall(r'(?:"merchantName"|"name")\s*:\s*["\']([^"\']+)["\'].{1,150}?"price"\s*:\s*"?([\d\.,]+)"?', raw_data, re.I | re.S)
        for seller, p_str in matches:
            if "unistore" not in seller.lower():
                try:
                    clean_p = p_str.strip().rstrip('.').replace(",", ".")
                    val = float(clean_p)
                    prices.append(val)
                except: continue

    except Exception as e:
        log.warning(f"Link oxuma xətası: {e}")
    
    return list(set(prices))

def process_product(p):
    try:
        current = p['current']
        min_p = p['min']
        max_p = p['max']
        
        comp_prices = get_competitor_prices(p['url'])
        
        # Öz qiymətimizdən 0.05-dən çox fərqlənənləri rəqib sayırıq (eyni qiyməti çıxarırıq)
        competitors = [p for p in comp_prices if abs(p - current) > 0.05]
        
        log.info(f"🔍 {p['name']} | Cari: {current} | Rəqiblər: {sorted(competitors)}")

        if not competitors:
            # Rəqib yoxdursa və ya hamısı bizimlə eyni qiymətdədirsə -> Maksimuma qaldır
            if current < max_p - 0.05:
                return {"row": p['row'], "new": max_p, "name": p['name'], "msg": f"📈 Max-a qalxdı: {max_p}₼"}
            return None

        cheapest_competitor = min(competitors)

        # Əgər kimsə bizdən ucuzdursa -> 0.01 düş, amma Min-dən aşağı düşmə
        if cheapest_competitor < current:
            target = max(cheapest_competitor - PRICE_UNDERCUT, min_p)
            if current - target > 0.01: # Əgər 1 qəpikdən çox dəyişiklik varsa
                return {"row": p['row'], "new": round(target, 2), "name": p['name'], "msg": f"📉 Rəqib ({cheapest_competitor}₼) tapıldı. Yeni: {round(target, 2)}₼"}
            
    except Exception as e:
        log.error(f"Xəta: {e}")
    return None

def run_check():
    log.info("🚀 Yoxlama başladı...")
    try:
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
                curr_val = float(str(row[COL_QIYMET-1] or 0).replace(",",".").replace(" ",""))
                min_val = float(str(row[COL_MIN-1] or 0).replace(",",".").replace(" ",""))
                max_val = float(str(row[COL_MAX-1] or 0).replace(",",".").replace(" ",""))
                
                if curr_val == 0 or min_val == 0: continue

                products.append({
                    "row": i, "url": str(url), "name": f"{row[3]} {row[2]}",
                    "current": curr_val, "min": min_val, "max": max_val
                })
            except: continue

        changes = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_product, p) for p in products]
            for f in as_completed(futures):
                res = f.result()
                if res: changes.append(res)

        if changes:
            wb = openpyxl.load_workbook(BytesIO(resp.content))
            ws = wb.active
            for c in changes:
                ws.cell(row=c['row'], column=COL_QIYMET, value=c['new'])
                try:
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                                   json={"chat_id": TELEGRAM_CHAT_ID, "text": f"💰 <b>{c['name']}</b>\n{c['msg']}", "parse_mode": "HTML"}, timeout=5)
                except: pass
            
            out = BytesIO()
            wb.save(out)
            requests.patch(f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media",
                headers={"Authorization": f"Bearer {creds.token}"}, data=out.getvalue(), timeout=60)
            log.info(f"✅ {len(changes)} məhsul yeniləndi.")
        else:
            log.info("✅ Dəyişiklik yoxdur.")

    except Exception as e:
        log.error(f"Sistem xətası: {e}")

if __name__ == "__main__":
    run_check()
    schedule.every(10).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(1)
