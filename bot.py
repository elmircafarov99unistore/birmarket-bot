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

def clean_p_text(s):
    """Qiyməti bütün gizli boşluqlardan təmizləyir və rəqəmə çevirir"""
    if not s: return 0.0
    # Birmarket-in istifadə etdiyi xüsusi boşluqları (\xa0, \u00a0) və normal boşluqları silir
    s = str(s).replace("₼", "").replace(" ", "").replace("\xa0", "").replace("\u00a0", "").replace(",", ".").strip()
    # Əgər rəqəmdə birdən çox nöqtə qalıbsa (minlik ayırıcı kimi), sonuncunu saxla
    if s.count('.') > 1:
        parts = s.split('.')
        s = "".join(parts[:-1]) + "." + parts[-1]
    s = s.rstrip('.')
    try: return float(s)
    except: return 0.0

def get_competitor_prices(url):
    competitors = []
    has_block = False
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "az-AZ,az;q=0.9,en-US;q=0.8"
        }
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200: return [], False
        
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # 1. "Digər satıcılar" blokunun və ya mətninin olmasını yoxla
        if any(x in html for x in ["item-other-seller-list", "Digər satıcılar", "Bütün qiymətlər"]):
            has_block = True

        # 2. JSON-LD ANALİZİ (SEO üçün olan bu blokda adətən bütün təkliflər olur)
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                data = json.loads(script.string)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") == "Product" and "offers" in item:
                        offers = item["offers"]
                        # Çoxlu təklifləri (AggregateOffer) yoxla
                        sub_offers = offers.get("offers", [offers]) if isinstance(offers, dict) else offers
                        if isinstance(sub_offers, dict): sub_offers = [sub_offers]
                        
                        for off in sub_offers:
                            seller = (off.get("seller", {}).get("name") or off.get("merchant", {}).get("name") or "").lower()
                            p = float(off.get("price", 0))
                            if p > 1 and "unistore" not in seller:
                                competitors.append(p)
                                has_block = True
            except: pass

        # 3. BUYBOX (ƏSAS SATICI) ANALİZİ
        # Əgər əsas satıcı Unistore deyilsə, onun qiymətini rəqib siyahısına sal
        main_seller_el = soup.find(attrs={"data-info": "item-main-seller-name"})
        if main_seller_el:
            main_name = main_seller_el.get_text(strip=True).lower()
            if "unistore" not in main_name:
                price_el = soup.find(attrs={"data-info": "item-main-price-new"}) or soup.find("div", class_="product-price")
                if price_el:
                    p = clean_p_text(price_el.get_text())
                    if p > 1: competitors.append(p)

        # 4. GİZLİ JS STATE (NUXT) ANALİZİ - Ən aqressiv rəqib tapma üsulu
        # merchantName və price cütlüklərini bütün kodda axtarırıq
        matches = re.findall(r'merchantName["\']?\s*:\s*["\']([^"\']+)["\'].{0,500}?price["\']?\s*:\s*["\']?([\d\.,\s]+)["\']?', html, re.I | re.S)
        for seller, p_str in matches:
            if "unistore" not in seller.lower():
                p = clean_p_text(p_str)
                if p > 1:
                    competitors.append(p)
                    has_block = True

    except Exception as e:
        log.warning(f"Səhifə oxuma xətası: {e}")
    
    return list(set(competitors)), has_block

def process_product(p):
    try:
        current = p['current']
        min_p = p['min']
        
        comp_prices, has_block = get_competitor_prices(p['url'])
        
        # Cari qiymətimizlə eyni olanları rəqib saymırıq (0.10₼ həssaslıqla)
        competitors = [price for price in comp_prices if abs(price - current) > 0.1]
        
        log.info(f"🔍 {p['name']} | Cari: {current} | Rəqiblər: {sorted(competitors)} | Blok var: {has_block}")

        # Əgər rəqib tapılmadısa VƏ "Digər satıcılar" bloku yoxdursa -> TOXUNMA
        if not competitors and not has_block:
            log.info("  ℹ️  Tək satıcıyıq, qiymət sabit saxlanılır.")
            return None

        if not competitors:
            log.info("  ℹ️  Rəqib tapılmadı, qiymət dəyişdirilmir.")
            return None

        # Ən ucuz rəqib bizdən ucuzdursa -> 0.01₼ düş, amma Min-dən aşağı düşmə
        cheapest_competitor = min(competitors)
        if cheapest_competitor < current:
            target = max(cheapest_competitor - PRICE_UNDERCUT, min_p)
            if current - target > 0.009:
                return {"row": p['row'], "new": round(target, 2), "name": p['name'], "msg": f"📉 Rəqib ({cheapest_competitor}₼) ucuzdur. Yeni: {round(target, 2)}₼"}
        
        log.info("  ℹ️  Biz artıq ən ucuzuq və ya qiymət uyğundur.")
            
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
            log.info("✅ Dəyişiklik ehtiyacı yoxdur.")
    except Exception as e:
        log.error(f"Sistem xətası: {e}")

if __name__ == "__main__":
    run_check()
    schedule.every(10).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(1)
