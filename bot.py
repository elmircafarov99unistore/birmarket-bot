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

def send_telegram(message):
    """Telegram vasitəsilə bildiriş göndərir"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"Telegram göndərmə xətası: {e}")

def parse_price(text):
    if not text: return 0.0
    cleaned = re.sub(r'[^0-9\.,]', '', str(text))
    if not cleaned: return 0.0
    if ',' in cleaned and '.' in cleaned:
        cleaned = cleaned.replace(',', '')
    elif ',' in cleaned:
        cleaned = cleaned.replace(',', '.')
    try:
        return round(float(cleaned), 2)
    except:
        return 0.0

def get_competitor_prices(url):
    competitors = []
    has_block = False
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200: return [], False
        
        html = resp.text
        if any(x in html.lower() for x in ["bütün satıcıların", "digər satıcılar", "bütün qiymətlər", "other-seller"]):
            has_block = True

        # Regex skan
        raw_prices = re.findall(r'["\']?price["\']?\s*[:=]\s*["\']?([\d\.,\s]+)["\']?', html, re.I)
        for p_str in raw_prices:
            p = parse_price(p_str)
            if p > 0: competitors.append(p)

        # BS4 skan
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(attrs={"data-info": True}):
            if "price" in tag["data-info"].lower():
                p = parse_price(tag.get_text())
                if p > 0: competitors.append(p)

        # Satıcı bazalı skan
        chunks = re.split(r'merchantName["\']?\s*:\s*', html, flags=re.I)
        for chunk in chunks[1:]:
            name_match = re.match(r'["\']([^"\']+)["\']', chunk)
            if name_match and "unistore" not in name_match.group(1).lower():
                has_block = True
                p_match = re.search(r'price["\']?\s*[:=]\s*["\']?([\d\.,\s]+)["\']?', chunk, re.I)
                if p_match:
                    p = parse_price(p_match.group(1))
                    if p > 0: competitors.append(p)

    except: pass
    return list(set(competitors)), has_block

def process_product(p):
    try:
        current = round(p['current'], 2)
        min_p = round(p['min'], 2)
        all_found, has_block = get_competitor_prices(p['url'])
        
        # Taksitləri və öz qiymətimizi silirik
        competitors = [round(price, 2) for price in all_found if price > (current * 0.6) and abs(price - current) > 0.009]
        
        log.info(f"🔍 {p['name']} | Biz: {current} | Rəqiblər: {sorted(competitors)}")

        if not has_block or not competitors:
            return {"status": "no_change", "name": p['name']}

        cheapest = min(competitors)

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
                    changes.append(res)
                    stats["updated"] += 1
                    send_telegram(res["msg"]) # Hər dəyişiklik üçün dərhal mesaj
                elif res["status"] == "best_price":
                    stats["best"] += 1
                elif res["status"] == "error":
                    stats["error"] += 1

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
            log.info(f"✅ {len(changes)} məhsul Excel-də yeniləndi.")

        # FINAL HESABAT MESAJI
        report = (
            f"📊 <b>Yoxlama Hesabatı</b>\n"
            f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📦 Ümumi məhsul: <b>{stats['total']}</b>\n"
            f"📉 Qiymət endirildi: <b>{stats['updated']}</b>\n"
            f"✅ Ən ucuz bizik: <b>{stats['best']}</b>\n"
            f"❌ Xəta/Keçildi: <b>{stats['error']}</b>"
        )
        send_telegram(report)

    except Exception as e:
        log.error(f"Sistem xətası: {e}")
        send_telegram(f"❌ <b>Sistem Xətası:</b>\n{str(e)}")

if __name__ == "__main__":
    run_check()
    schedule.every(10).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(1)
