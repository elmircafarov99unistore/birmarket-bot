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
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"Telegram göndərmə xətası: {e}")

def send_telegram_document(file_bytes, filename, caption=""):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
        files = {"document": (filename, file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        requests.post(url, data=data, files=files, timeout=20)
    except Exception as e:
        log.error(f"Telegram sənəd göndərmə xətası: {e}")

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

def get_competitor_prices(url, product_name):
    competitors = []
    has_block = False
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"}
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200: return [], False
        
        html = resp.text
        if any(x in html.lower() for x in ["bütün satıcıların", "digər satıcılar", "bütün qiymətlər", "other-seller"]):
            has_block = True

        raw_prices = re.findall(r'["\']?price["\']?\s*[:=]\s*["\']?([\d\.,\s]+)["\']?', html, re.I)
        for p_str in raw_prices:
            p = parse_price(p_str)
            if p > 0: competitors.append(p)

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(attrs={"data-info": True}):
            if "price" in tag["data-info"].lower():
                p = parse_price(tag.get_text())
                if p > 0: competitors.append(p)

        # -------------------------------------------------------------
        # DİAQNOSTİKA HİSSƏSİ: Satıcı adlarını tam olaraq necə oxuyur?
        # -------------------------------------------------------------
        chunks = re.split(r'merchantName["\']?\s*:\s*', html, flags=re.I)
        for chunk in chunks[1:]:
            name_match = re.match(r'["\']([^"\']+)["\']', chunk)
            if name_match:
                raw_merchant_name = name_match.group(1) # Saytda yazılan EYNİ ad
                
                # Bu satıcının qiymətini tapırıq
                parsed_p = 0.0
                p_match = re.search(r'price["\']?\s*[:=]\s*["\']?([\d\.,\s]+)["\']?', chunk, re.I)
                if p_match:
                    parsed_p = parse_price(p_match.group(1))
                
                # 🔴 LOG-a ÇAP EDİRİK:
                log.info(f"🕵️ [{product_name}] üçün tapıldı -> SATICI ADI: '{raw_merchant_name}' | QİYMƏT: {parsed_p}")
                
                # Sizin adınız "unistore" deyilsə rəqib say
                merchant_name_lower = raw_merchant_name.lower().replace(" ", "").replace("-", "")
                if "unistore" not in merchant_name_lower:
                    has_block = True
                    if parsed_p > 0: competitors.append(parsed_p)

    except: pass
    return list(set(competitors)), has_block

def process_product(p):
    try:
        current = round(p['current'], 2)
        min_p = round(p['min'], 2)
        
        # Maksimum limit: 5% (1.05)
        max_p = round(min_p * 1.05, 2)
        
        all_found, has_block = get_competitor_prices(p['url'], p['name'])
        
        if not has_block:
            competitors = []
        else:
            # Köhnə sisteminiz, lakin taksit rəqəmlərindən (10-15 min) qorunmaq üçün max_p * 1.5 filtri qaldı
            competitors = [
                round(price, 2) for price in all_found 
                if price > (current * 0.6) and price < (max_p * 1.5) and abs(price - current) > 0.009
            ]
        
        log.info(f"🔍 {p['name']} | Biz: {current} | Min: {min_p} | Max: {max_p} | Yekun Rəqiblər: {sorted(competitors)}")

        # Hədəf qiyməti hesablamaq
        if not competitors:
            target = max_p
        else:
            cheapest = min(competitors)
            target = max(cheapest - PRICE_UNDERCUT, min_p)
            target = min(target, max_p) 

        # Qiymət dəyişikliyini yoxla
        if abs(current - target) >= 0.009:
            emoji = "📉" if target < current else "📈"
            status_text = "Endirildi" if target < current else "Qaldırıldı"
            
            return {
                "status": "updated", 
                "row": p['row'], 
                "new": round(target, 2), 
                "name": p['name'], 
                "msg": f"{emoji} <b>{p['name']}</b>\nKöhnə: {current}₼ | Yeni: <b>{round(target, 2)}₼</b> ({status_text})"
            }
        
        if competitors and min(competitors) < target:
             return {
                "status": "limit_reached",
                "name": p['name'],
                "url": p['url'],
                "current": current,
                "competitor": min(competitors),
                "min": min_p,
                "max": max_p 
            }

        return {"status": "no_change", "name": p['name']}
            
    except Exception as e:
        return {"status
