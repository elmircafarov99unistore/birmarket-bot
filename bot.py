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
    """Mətndəki qiyməti hər cür maneəni aşaraq rəqəmə çevirir"""
    if not text: return 0.0
    # Rəqəm, nöqtə və vergül xaric hər şeyi (boşluq, ₼, gizli simvollar) sil
    cleaned = re.sub(r'[^0-9\.,]', '', str(text))
    if not cleaned: return 0.0
    
    # 1.200,09 formatını 1200.09 formatına sal
    if ',' in cleaned and '.' in cleaned:
        cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        cleaned = cleaned.replace(',', '.')
        
    try:
        val = float(cleaned)
        return val if val > 10 else 0.0 # 10₼-dan aşağı rəqəmləri (ID-ləri) sayma
    except:
        return 0.0

def get_competitor_prices(url):
    competitors = []
    has_block = False
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200: return [], False
        
        html = resp.text
        
        # 1. BLOK YOXLANIŞI (Hər hansı bir rəqib adı varsa blok VAR sayılır)
        if any(x in html for x in ["item-other-seller-list", "Digər satıcılar", "Bütün satıcıların", "other-sellers"]):
            has_block = True

        # 2. AQRESSİV JSON-LD SKANERI (E-ticarət standartı)
        # Birmarket adətən qiymətləri bu SEO blokunun içində saxlayır
        json_ld = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.S)
        for j_str in json_ld:
            try:
                data = json.loads(j_str)
                offers = data.get("offers", {})
                if isinstance(offers, dict):
                    price = offers.get("price")
                    if price: competitors.append(parse_price(price))
                elif isinstance(offers, list):
                    for off in offers:
                        price = off.get("price")
                        if price: competitors.append(parse_price(price))
            except: pass

        # 3. NUXT STATE DEEP SCAN (Birmarket-in əsas beyni)
        # MerchantName-dən sonra gələn bütün "price" açarlarını tutur (Məsafə limiti olmadan)
        chunks = re.split(r'merchantName["\']?\s*:\s*', html, flags=re.I)
        for chunk in chunks[1:]:
            # İlk mağaza adını tap
            name_match = re.match(r'["\']([^"\']+)["\']', chunk)
            if name_match:
                seller = name_match.group(1).lower()
                if "unistore" not in seller:
                    # Bu satıcıya aid bütün rəqəmləri axtar
                    p_matches = re.findall(r'price["\']?\s*[:=]\s*["\']?([\d\.,\s]+)["\']?', chunk, re.I)
                    for p_str in p_matches:
                        p = parse_price(p_str)
                        if p > 0:
                            competitors.append(p)
                            has_block = True

        # 4. HTML TAG SKANERI (BS4)
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(attrs={"data-info": re.compile(r'price|seller')}):
            p = parse_price(tag.get_text())
            if p > 0: competitors.append(p)

    except Exception as e:
        log.warning(f"Bağlantı xətası: {e}")
    
    return list(set(competitors)), has_block

def process_product(p):
    try:
        current = p['current']
        min_p = p['min']
        
        comp_prices, has_block = get_competitor_prices(p['url'])
        # Öz qiymətimizlə eyni olanları (və ya çox yaxın olanları) silirik
        competitors = [price for price in comp_prices if abs(price - current) > 0.1]
        
        log.info(f"🔍 {p['name']} | Biz: {current} | Rəqiblər: {sorted(competitors)} | Blok: {'VAR' if has_block else 'YOXDUR'}")

        if not has_block:
            return None

        if not competitors:
            log.info("  ℹ️  Rəqib qiyməti oxuna bilmədi və ya yoxdur.")
            return None

        cheapest = min(competitors)

        # Əgər rəqib bizdən ucuzdursa -> 0.01₼ düş, amma Min-dən aşağı düşmə
        if cheapest < current:
            target = max(cheapest - PRICE_UNDERCUT, min_p)
            if current - target > 0.009:
                return {"row": p['row'], "new": round(target, 2), "name": p['name'], "msg": f"📉 Rəqib ({cheapest}₼) tapıldı. Yeni: {round(target, 2)}₼"}
        
        log.info("  ℹ️  Qiymət artıq ən ucuzdur.")
            
    except Exception as e:
        log.error(f"Xəta: {e}")
    return None

def run_check():
    log.info("🚀 Yoxlama başladı...")
    try:
        file_id = EXCEL_FILE_URL.split("/d/")[1].split("/")[0]
        creds_json = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
        creds = Credentials.from_service_account_info(creds_json, scopes=["https://www.googleapis.com/auth/drive"])
        
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
