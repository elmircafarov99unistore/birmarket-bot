import json, time, re, schedule, logging, os, requests
from datetime import datetime
from io import BytesIO
import openpyxl
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from bs4 import BeautifulSoup

# KONFİQURASİYA
EXCEL_FILE_URL = os.environ.get("EXCEL_FILE_URL", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
PRICE_UNDERCUT = 0.01
MAX_WORKERS = 3 

# Sütunlar: H=8, N=14, O=15, P=16
COL_QIYMET = 8; COL_URL = 14; COL_MIN = 15; COL_MAX = 16

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def parse_price(text):
    """Bütün formatlardakı qiymətləri rəqəmə çevirir (məs: '1 200,09' -> 1200.09)"""
    if not text: return 0.0
    cleaned = re.sub(r'[^0-9\.,]', '', str(text))
    if not cleaned: return 0.0
    if ',' in cleaned and '.' in cleaned:
        cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        cleaned = cleaned.replace(',', '.')
    try:
        return float(cleaned)
    except:
        return 0.0

def get_competitor_prices(url):
    competitors = []
    has_block = False
    try:
        # Daha real brauzer başlıqları
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "az-AZ,az;q=0.9,en-US;q=0.8",
            "Cache-Control": "no-cache"
        }
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200: return [], False
        
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # 1. BLOKUN YOXLANILMASI (Ən kritik hissə)
        # Birmarket-də o xana adətən bu class-larda olur:
        other_sellers_indicators = [
            "item-other-seller", "other-sellers", "product-sellers", 
            "Bütün qiymətlər", "Digər satıcılar", "satıcıdan da"
        ]
        
        if any(x in html for x in other_sellers_indicators):
            has_block = True

        # 2. QİYMƏTLƏRİN TAPILMASI (Nuxt State daxil olmaqla)
        # Səhifədəki bütün "price" dəyərlərini və satıcıları tapırıq
        data_pairs = re.findall(r'merchantName["\']?\s*:\s*["\']([^"\']+)["\'].{0,500}?price["\']?\s*:\s*["\']?([\d\.,\s]+)["\']?', html, re.S | re.I)
        
        for seller, p_str in data_pairs:
            seller_low = seller.lower()
            if "unistore" not in seller_low:
                p = parse_price(p_str)
                if p > 1:
                    competitors.append(p)
                    has_block = True # Əgər rəqib qiyməti varsa, deməli blok da mütləq var

        # Ehtiyat: Əgər yuxarıda heç nə tapmasa, birbaşa qiymət rəqəmlərini axtar
        if not competitors and has_block:
            # Səhifədəki ən böyük (Buybox) qiyməti də rəqib ola bilər
            all_prices = re.findall(r'price["\']?\s*:\s*["\']?([\d\.,\s]+)["\']?', html)
            for p_str in all_prices:
                p = parse_price(p_str)
                if p > 1: competitors.append(p)

    except Exception as e:
        log.warning(f"Səhifə xətası: {e}")
    
    return list(set(competitors)), has_block

def process_product(p):
    try:
        current = p['current']
        min_p = p['min']
        
        comp_prices, has_block = get_competitor_prices(p['url'])
        
        # Cari qiymətimizlə eyni olanları siyahıdan silirik
        competitors = [price for price in comp_prices if abs(price - current) > 0.1]
        
        log.info(f"🔍 {p['name']} | Biz: {current} | Rəqiblər: {sorted(competitors)} | Blok: {'VAR' if has_block else 'YOXDUR'}")

        # Sizin Qayda: Blok yoxdursa -> Tək satıcıyıq, DƏYMƏ.
        if not has_block:
            log.info("  ℹ️  Səhifədə 'Bütün qiymətlər' bloku yoxdur. Toxunulmur.")
            return None

        # Blok var, amma rəqib tapılmadısa (Yalnız bizik) -> DƏYMƏ.
        if not competitors:
            log.info("  ℹ️  Blok var, amma rəqib tapılmadı. Toxunulmur.")
            return None

        cheapest = min(competitors)

        # Rəqib bizdən ucuzdursa -> 0.01 düş, amma O xanasından (Min) aşağı düşmə
        if cheapest < current:
            target = max(cheapest - PRICE_UNDERCUT, min_p)
            if current - target > 0.009:
                return {"row": p['row'], "new": round(target, 2), "name": p['name'], "msg": f"📉 Rəqib ({cheapest}₼) tapıldı. Yeni: {round(target, 2)}₼"}
        
        log.info("  ℹ️  Qiymətimiz artıq ən ucuzdur.")
            
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
                def f_val(v): return float(str(v or 0).replace(",",".").replace(" ","").replace("\xa0",""))
                curr = f_val(row[COL_QIYMET-1])
                mn = f_val(row[COL_MIN-1])
                if curr == 0 or mn == 0: continue
                products.append({"row": i, "url": str(url).strip(), "name": f"{row[3]} {row[2]}", "current": curr, "min": mn})
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
            creds.refresh(Request())
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
