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

# Sütunlar (Excel-ə əsasən tənzimləndi)
COL_QIYMET = 8   # H
COL_URL = 14     # N
COL_MIN = 15     # O
COL_MAX = 16     # P

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def get_competitor_prices(url):
    prices = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200: return []
        
        # Daha güclü qiymət axtarış şablonu (vergül və nöqtə fərqi üçün)
        # Həm merchantName, həm də qiyməti birlikdə axtarırıq
        raw_data = resp.text
        
        # NUXT və JSON formatlarını dərindən analiz edirik
        pattern = r'(?:"merchantName"|"name")\s*:\s*["\']([^"\']+)["\'].{1,150}?"price"\s*:\s*"?([\d\.,]+)"?'
        matches = re.findall(pattern, raw_data, re.IGNORECASE | re.DOTALL)
        
        for seller, p_str in matches:
            if "unistore" not in seller.lower():
                # Vergülü nöqtəyə çevirib float edirik
                clean_p = float(p_str.replace(",", "."))
                if clean_p > 0:
                    prices.append(clean_p)
        
        # Əgər yuxarıdakı tapmasa, sadəcə qiymət bloklarını yoxla
        if not prices:
            simple_prices = re.findall(r'"price"\s*:\s*"?([\d\.,]+)"?', raw_data)
            for p_str in simple_prices:
                p_val = float(p_str.replace(",", "."))
                if 0 < p_val < 100000 and abs(p_val - 0) > 0.1: # Boş qiymətləri at
                    prices.append(p_val)

    except Exception as e:
        log.warning(f"Link oxuma xətası: {e}")
    
    return list(set(prices))

def process_product(p):
    try:
        current = p['current']
        min_p = p['min']
        max_p = p['max']
        
        comp_prices = get_competitor_prices(p['url'])
        
        # Tapılan bütün qiymətlərdən öz qiymətimizi çıxarırıq ki, rəqibləri görək
        competitors = [p for p in comp_prices if abs(p - current) > 0.1]
        
        log.info(f"🔍 {p['name']} | Biz: {current} | Tapılan Rəqiblər: {competitors}")

        if not competitors:
            if current < max_p:
                return {"row": p['row'], "new": max_p, "name": p['name'], "msg": f"📈 Tək satıcıyıq (və ya ən ucuzuq). Max-a qalxdı: {max_p}₼"}
            return None

        cheapest_competitor = min(competitors)

        # Əgər kimsə bizdən ucuzdursa
        if cheapest_competitor < current:
            target = max(cheapest_competitor - PRICE_UNDERCUT, min_p)
            if abs(target - current) > 0.01:
                return {"row": p['row'], "new": round(target, 2), "name": p['name'], "msg": f"📉 Rəqib ({cheapest_competitor}₼) tapıldı. Yeni qiymət: {round(target, 2)}₼"}
        
        # Əgər biz ən ucuzuqsa amma rəqiblə aramızda çox fərq varsa (məs. 1100 rəqibdir, biz 1000-ik), 
        # qiyməti rəqibə yaxınlaşdıra bilərik (məs. 1099.99). Amma sizin istəyinizlə hələlik toxunmuruq.
            
    except Exception as e:
        log.error(f"Məhsul emal xətası: {e}")
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
                # Qiymətləri oxuyarkən xəta olmaması üçün təmizləyirik
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
                send_telegram(f"💰 <b>{c['name']}</b>\n{c['msg']}")
            
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
