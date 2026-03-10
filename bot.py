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

# Sütunlar: H=8, N=14, O=15
COL_QIYMET = 8; COL_URL = 14; COL_MIN = 15

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def parse_price(text):
    """Mətndəki qiyməti cərrahi dəqiqliklə rəqəmə çevirir"""
    if not text: return 0.0
    # Boşluqlar, ₼ simvolu və digər rəqəm olmayan hər şeyi təmizlə
    cleaned = str(text).replace("₼", "").replace(" ", "").replace("\xa0", "").replace("\u00a0", "")
    # Yalnız rəqəm, nöqtə və vergülü saxla
    cleaned = re.sub(r'[^0-9\.,]', '', cleaned)
    if not cleaned: return 0.0
    
    # Formatı standartlaşdır (1,200.09 -> 1200.09)
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
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept-Language": "az-AZ,az;q=0.9,en-US;q=0.8"
        }
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200: return [], False
        
        html = resp.text
        
        # 1. BLOKUN YOXLANIŞI (Həm HTML, həm Nuxt State daxilində)
        if any(x in html for x in ["item-other-seller-list", "Bütün satıcıların", "Digər satıcılar"]):
            has_block = True

        # 2. AQRESSİV QİYMƏT VƏ SATICI SKANERİ
        # Bu pattern satıcı adı və qiyməti arasındakı məsafədən asılı olmayaraq hər şeyi tapır
        # (Birmarket-in yeni strukturuna uyğunlaşdırıldı)
        
        # A: JSON/Nuxt formatında axtarış
        # merchantName və price cütlüklərini bütün massivlərdə axtarırıq
        blocks = re.split(r'(?=merchantName)', html)
        for b in blocks:
            name_match = re.search(r'merchantName["\']?\s*:\s*["\']([^"\']+)["\']', b, re.I)
            price_match = re.search(r'price["\']?\s*:\s*["\']?([\d\.,\s]+)["\']?', b, re.I)
            
            if name_match and price_match:
                seller = name_match.group(1).lower()
                p = parse_price(price_match.group(1))
                if "unistore" not in seller and p > 1:
                    competitors.append(p)
                    has_block = True

        # 3. HTML ÜZƏRİNDƏN ƏLAVƏ SÜZGƏC (Buybox və siyahı üçün)
        soup = BeautifulSoup(html, "html.parser")
        
        # Əsas (yuxarıdakı) satıcı
        main_seller = soup.find(attrs={"data-info": "item-main-seller-name"}) or soup.select_one(".product-seller-name")
        if main_seller and "unistore" not in main_seller.get_text().lower():
            price_el = soup.find(attrs={"data-info": "item-main-price-new"}) or soup.select_one(".product-price")
            if price_el:
                p = parse_price(price_el.get_text())
                if p > 1: competitors.append(p)

        # Digər satıcılar siyahısı
        others = soup.find_all(attrs={"data-info": "item-other-seller-list"})
        for box in others:
            s_name = box.find(attrs={"data-info": "item-other-seller-name"})
            s_price = box.find(attrs={"data-info": "item-desc-price-new"})
            if s_name and s_price:
                if "unistore" not in s_name.get_text().lower():
                    p = parse_price(s_price.get_text())
                    if p > 1: competitors.append(p)

    except Exception as e:
        log.warning(f"Səhifə xətası: {e}")
    
    return list(set(competitors)), has_block

def process_product(p):
    try:
        current = p['current']
        min_p = p['min']
        
        all_found, has_block = get_competitor_prices(p['url'])
        # Öz qiymətimizlə eyni olanları (və ya çox yaxın olanları) silirik
        competitors = [price for price in all_found if abs(price - current) > 0.1]
        
        log.info(f"🔍 {p['name']} | Biz: {current} | Rəqiblər: {sorted(competitors)} | Blok: {'VAR' if has_block else 'YOXDUR'}")

        # Əgər blok yoxdursa -> Toxunma
        if not has_block:
            log.info("  ℹ️  Rəqib bloku tapılmadı. Qiymət dəyişdirilmir.")
            return None

        # Əgər blok var amma rəqib siyahısı boşdursa (siyahıda yalnız bizik) -> Toxunma
        if not competitors:
            log.info("  ℹ️  Siyahıda başqa rəqib tapılmadı. Qiymət dəyişdirilmir.")
            return None

        cheapest = min(competitors)

        # Rəqib bizdən ucuzdursa -> 0.01₼ düş, amma Min-dən aşağı düşmə
        if cheapest < current:
            target = max(cheapest - PRICE_UNDERCUT, min_p)
            if current - target > 0.009:
                return {"row": p['row'], "new": round(target, 2), "name": p['name'], "msg": f"📉 Rəqib ({cheapest}₼) tapıldı. Yeni: {round(target, 2)}₼"}
        
        log.info("  ℹ️  Ən ucuz qiymət artıq bizdədir.")
            
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
            log.info("✅ Dəyişiklik ehtiyacı yoxdur.")
    except Exception as e:
        log.error(f"Sistem xətası: {e}")

if __name__ == "__main__":
    run_check()
    schedule.every(10).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(1)
