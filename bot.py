import json, time, re, schedule, logging, os, requests, random
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

COL_QIYMET = 8; COL_URL = 14; COL_MIN = 15

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
    except: pass

def parse_price(text):
    if not text: return 0.0
    cleaned = re.sub(r'[^0-9\.,]', '', str(text))
    if not cleaned: return 0.0
    if ',' in cleaned and '.' in cleaned: cleaned = cleaned.replace(',', '')
    elif ',' in cleaned: cleaned = cleaned.replace(',', '.')
    try: return round(float(cleaned), 2)
    except: return 0.0

def get_competitor_prices(url):
    competitors = []
    has_block = False
    try:
        # CACHE BUSTER: URL-in sonuna təsadüfi rəqəm əlavə edirik ki, sayt bizə köhnə yox, TƏZƏ məlumatı versin
        clean_url = url.split('?')[0]
        bust_url = f"{clean_url}?t={random.randint(100000, 999999)}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
        
        resp = requests.get(bust_url, headers=headers, timeout=20)
        if resp.status_code != 200: return [], False
        
        html = resp.text
        
        # 1. Genişləndirilmiş Blok Yoxlanışı
        indicators = ["bütün satıcıların", "digər satıcılar", "bütün qiymətlər", "other-seller", "other_price", "mağaza"]
        if any(x in html.lower() for x in indicators):
            has_block = True

        # 2. JSON-LD Skaneri (Google üçün olan gizli qiymətlər)
        raw_prices = re.findall(r'["\']?price["\']?\s*[:=]\s*["\']?([\d\.,\s]+)["\']?', html, re.I)
        for p_str in raw_prices:
            p = parse_price(p_str)
            if p > 10: competitors.append(p)

        # 3. HTML Skaneri
        soup = BeautifulSoup(html, "html.parser")
        # ₼ simvolu olan hər şeyi yoxla
        for element in soup.find_all(string=re.compile(r'₼')):
            p = parse_price(element)
            if p > 10: competitors.append(p)

        # 4. Satıcı bazalı dərin skan
        chunks = re.split(r'merchantName["\']?\s*:\s*', html, flags=re.I)
        for chunk in chunks[1:]:
            name_match = re.match(r'["\']([^"\']+)["\']', chunk)
            if name_match:
                seller = name_match.group(1).lower()
                if "unistore" not in seller:
                    has_block = True
                    p_match = re.search(r'price["\']?\s*[:=]\s*["\']?([\d\.,\s]+)["\']?', chunk, re.I)
                    if p_match:
                        p = parse_price(p_match.group(1))
                        if p > 10: competitors.append(p)

    except Exception as e:
        log.warning(f"Səhifə xətası: {e}")
    
    return list(set(competitors)), has_block

def process_product(p):
    try:
        current = round(p['current'], 2)
        min_p = round(p['min'], 2)
        all_found, has_block = get_competitor_prices(p['url'])
        
        # SÜZGƏC: Taksitləri silirik (Artıq daha həssasdır: Qiymətin 30%-indən aşağıdırsa taksitdir)
        competitors = [round(price, 2) for price in all_found if price > (current * 0.3) and abs(price - current) > 0.009]
        
        log.info(f"🔍 {p['name']} | Biz: {current} | Rəqiblər: {sorted(competitors)} | Blok: {'VAR' if has_block else 'YOXDUR'}")

        if not has_block or not competitors:
            return {"status": "no_change", "name": p['name']}

        cheapest = min(competitors)

        # Rəqib bizdən ucuzdursa düşürük
        if cheapest < current:
            target = max(cheapest - PRICE_UNDERCUT, min_p)
            if current - target >= 0.009:
                return {
                    "status": "updated", 
                    "row": p['row'], 
                    "new": round(target, 2), 
                    "name": p['name'], 
                    "msg": f"📉 <b>{p['name']}</b>\nRəqib: {cheapest}₼ | Yeni: <b>{round(target, 2)}₼</b>"
                }
        
        return {"status": "best_price", "name": p['name']}
            
    except Exception as e:
        return {"status": "error", "name": p['name'], "error": str(e)}

def run_check():
    log.info("🚀 Yoxlama başladı...")
    stats = {"total": 0, "updated": 0, "best": 0, "error": 0}
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
                products.append({"row": i, "url": str(url).strip(), "name": f"{row[3]} {row[2]}", "current": f_val(row[COL_QIYMET-1]), "min": f_val(row[COL_MIN-1])})
            except: continue

        stats["total"] = len(products)
        changes = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_product, p) for p in products]
            for f in as_completed(futures):
                res = f.result()
                if res["status"] == "updated":
                    changes.append(res); stats["updated"] += 1
                    send_telegram(res["msg"])
                elif res["status"] == "best_price": stats["best"] += 1
                elif res["status"] == "error": stats["error"] += 1

        if changes:
            wb = openpyxl.load_workbook(BytesIO(resp.content))
            ws = wb.active
            for c in changes:
                ws.cell(row=c['row'], column=COL_QIYMET, value=c['new'])
            out = BytesIO()
            wb.save(out)
            creds.refresh(Request())
            requests.patch(f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media",
                headers={"Authorization": f"Bearer {creds.token}"}, data=out.getvalue(), timeout=60)

        report = (f"📊 <b>Hesabat</b>\n📅 {datetime.now().strftime('%H:%M')}\n━━━━━━━━━━\n"
                  f"📦 Ümumi: {stats['total']}\n📉 Yeniləndi: {stats['updated']}\n✅ Optimal: {stats['best']}")
        send_telegram(report)
    except Exception as e:
        send_telegram(f"❌ Xəta: {str(e)}")

if __name__ == "__main__":
    run_check()
    schedule.every(10).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(1)
