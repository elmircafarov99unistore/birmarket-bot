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
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        }
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200: return []
        
        raw_data = resp.text
        
        # 1. Bütün mümkün "price" etiketlərini axtarırıq (dırnaqlı və dırnaqsız)
        # Bəzi yerlərdə price: 1075.55, bəzi yerlərdə "price": "1075,55"
        found_prices = re.findall(r'["\']?price["\']?\s*[:=]\s*["\']?([\d\.,\s]+)["\']?', raw_data)
        
        for p_str in found_prices:
            try:
                # Qiyməti təmizləyirik: boşluqları sil, vergülü nöqtəyə çevir, sondakı nöqtəni sil
                clean_p = p_str.replace(" ", "").replace(",", ".").strip().rstrip('.')
                val = float(clean_p)
                if 10 < val < 100000: # Çox kiçik rəqəmləri (ID-ləri) yox, real qiymətləri götürürük
                    prices.append(val)
            except: continue

        # 2. Satıcı adlarını taparaq qiyməti onlarla müqayisəli axtarırıq (NUXT formatı)
        nuxt_matches = re.findall(r'merchantName["\']?\s*:\s*["\']([^"\']+)["\'].*?price["\']?\s*:\s*["\']?([\d\.,]+)["\']?', raw_data, re.S)
        for seller, p_str in nuxt_matches:
            if "unistore" not in seller.lower():
                try:
                    val = float(p_str.replace(",", ".").strip().rstrip('.'))
                    prices.append(val)
                except: continue

    except Exception as e:
        log.warning(f"Bağlantı xətası: {e}")
    
    return list(set(prices))

def process_product(p):
    try:
        current = p['current']
        min_p = p['min']
        max_p = p['max']
        
        comp_prices = get_competitor_prices(p['url'])
        
        # Öz qiymətimizdən 0.10₼-dan çox fərqlənən rəqibləri tapırıq
        # (Eyni qiymətdə olanları rəqib saymırıq)
        competitors = [price for price in comp_prices if abs(price - current) > 0.1]
        
        # LOG: Tapılan hər şeyi göstər ki, niyə [] olduğunu bilək
        log.info(f"🔍 {p['name']} | Cari: {current} | Tapılan Rəqiblər: {sorted(competitors)}")

        if not competitors:
            # Əgər rəqib yoxdursa və cari qiymətimiz Max-dan aşağıdırsa -> Max-a qaldır
            if current < max_p - 0.5:
                return {"row": p['row'], "new": max_p, "name": p['name'], "msg": f"📈 Tək satıcıyıq. Max-a qaldırıldı: {max_p}₼"}
            return None

        cheapest_competitor = min(competitors)

        # Əgər kimsə bizdən ucuzdursa -> 0.01₼ aşağı düş, amma Min-dən aşağı düşmə
        if cheapest_competitor < current:
            target = max(cheapest_competitor - PRICE_UNDERCUT, min_p)
            if current - target > 0.01:
                return {"row": p['row'], "new": round(target, 2), "name": p['name'], "msg": f"📉 Rəqib ({cheapest_competitor}₼) ucuzdur. Yeni: {round(target, 2)}₼"}
            
    except Exception as e:
        log.error(f"Xəta: {e}")
    return None

def run_check():
    log.info("🚀 Yoxlama başladı...")
    try:
        # Faylı yükləyirik
        file_id = EXCEL_FILE_URL.split("/d/")[1].split("/")[0]
        creds_data = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
        creds = Credentials.from_service_account_info(creds_data, scopes=["https://www.googleapis.com/auth/drive"])
        
        resp = requests.get(f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx", timeout=30)
        wb = openpyxl.load_workbook(BytesIO(resp.content), data_only=True)
        ws = wb.active
        
        products = []
        for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), 2):
            url = row[COL_URL-1]
            if not url or "http" not in str(url): continue
            
            try:
                # Excel-dən məlumatları təmiz oxuyuruq
                def clean_num(v): return float(str(v or 0).replace(",",".").replace(" ",""))
                
                curr_val = clean_num(row[COL_QIYMET-1])
                min_val = clean_num(row[COL_MIN-1])
                max_val = clean_num(row[COL_MAX-1])
                
                if curr_val == 0 or min_val == 0: continue

                products.append({
                    "row": i, "url": str(url).strip(), "name": f"{row[3]} {row[2]}",
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
            # Dəyişiklikləri Excel-ə yazırıq
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
            # Geri yükləyirik
            creds.refresh(Request())
            requests.patch(f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media",
                headers={"Authorization": f"Bearer {creds.token}"}, data=out.getvalue(), timeout=60)
            log.info(f"✅ {len(changes)} məhsul yeniləndi.")
        else:
            log.info("✅ Heç bir qiymət dəyişikliyi ehtiyacı yoxdur.")

    except Exception as e:
        log.error(f"Sistem xətası: {e}")

if __name__ == "__main__":
    run_check()
    schedule.every(10).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(1)
