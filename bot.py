"""
Birmarket.az Qiymət İzləmə Botu (Sürətli Versiya - HTML + NUXT JS Gizli Data)
================================
İş prinsipi:
  1. Google Drive-dakı Excel faylını yükləyir
  2. Birmarket-də rəqib qiymətlərini HTML blokundan və gizli JS kodlarından oxuyur
  3. Yeni qiyməti hesablayır (min/max limitə görə)
  4. Excel faylının G sütununa yeni qiyməti yazır
  5. Faylı Google Drive-a geri yükləyir → Umico avtomatik dəyişir
"""

import json
import time
import re
import schedule
import logging
import os
import requests
from datetime import datetime
from typing import Optional
from bs4 import BeautifulSoup
from io import BytesIO
import openpyxl

# Google Auth
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request

# ─────────────────────────────────────────────
# KONFİQURASİYA
# ─────────────────────────────────────────────
CONFIG = {
    "excel_file_url": os.environ.get("EXCEL_FILE_URL", ""),
    "sheet_name": os.environ.get("SHEET_NAME", "Əsas"),
    "data_start_row": 2,

    "telegram_bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id":   os.environ.get("TELEGRAM_CHAT_ID", ""),

    "check_interval_minutes": 10,
    "price_undercut":         0.01,
    "log_file": "birmarket_bot.log",
}

# ─────────────────────────────────────────────
# SÜTUN XƏRİTƏSİ
# ─────────────────────────────────────────────
COL = {
    "barkod": 0, "mpn": 1, "model": 2, "brend": 3,
    "olke": 4, "say": 5, "endirimli": 6,
    "qiymet": 7, "tesvir": 8, "start": 9,
    "finish": 10, "taksit": 11, "aylar": 12,
    "url": 13,        # N — məhsulun Birmarket URL-i
    "min_qiymet": 14, # O — min qiymət
    "max_qiymet": 15, # P — max qiymət
}

# ─────────────────────────────────────────────
# LOG
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(CONFIG["log_file"], encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# TARİXÇƏ
# ─────────────────────────────────────────────
HISTORY_FILE = "price_history.json"

def load_history() -> dict:
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_history(history: dict):
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

def record_price_change(barkod: str, old: float, new: float, reason: str):
    h = load_history()
    if barkod not in h:
        h[barkod] = []
    h[barkod].append({
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "old_price": old, "new_price": new, "reason": reason,
    })
    save_history(h)

# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────
def send_telegram(message: str):
    token   = CONFIG.get("telegram_bot_token", "")
    chat_id = CONFIG.get("telegram_chat_id", "")
    if not token or "YOUR_" in token:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram xətası: {e}")

# ─────────────────────────────────────────────
# GOOGLE CREDENTIALS
# ─────────────────────────────────────────────
def get_credentials(scopes: list) -> Credentials:
    google_creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not google_creds_json:
        raise Exception("GOOGLE_CREDENTIALS environment variable tapılmadı!")
    info = json.loads(google_creds_json)
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return creds

# ─────────────────────────────────────────────
# EXCEL FAYL — YÜKLƏ VƏ YAZ
# ─────────────────────────────────────────────
def get_file_id() -> str:
    url = CONFIG["excel_file_url"]
    return url.split("/d/")[1].split("/")[0]

def download_excel() -> bytes:
    file_id = get_file_id()
    export_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
    resp = requests.get(export_url, timeout=30)
    resp.raise_for_status()
    return resp.content

def upload_excel(data: bytes) -> bool:
    try:
        file_id = get_file_id()
        creds = get_credentials(["https://www.googleapis.com/auth/drive"])
        creds.refresh(Request())

        upload_url = f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media"
        resp = requests.patch(
            upload_url,
            headers={
                "Authorization": f"Bearer {creds.token}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
            data=data,
            timeout=30,
        )
        if resp.status_code == 200:
            return True
        else:
            log.error(f"❌ Upload HTTP xətası: {resp.status_code} — {resp.text[:300]}")
            return False
    except Exception as e:
        log.error(f"❌ Upload xətası: {e}")
        return False

# ─────────────────────────────────────────────
# MƏHSUL OXUMA
# ─────────────────────────────────────────────
def to_float(val, default=0.0) -> float:
    try:
        return float(str(val).replace(",", ".").replace(" ", "").replace("₼", ""))
    except (ValueError, TypeError):
        return default

def load_products() -> list:
    products = []
    try:
        excel_data = download_excel()
        wb = openpyxl.load_workbook(BytesIO(excel_data), data_only=True)

        sheet_name = CONFIG.get("sheet_name", "")
        ws = None
        if sheet_name:
            for name in wb.sheetnames:
                if name.strip().lower() == sheet_name.strip().lower():
                    ws = wb[name]
                    break
        if ws is None:
            ws = wb.active

        for i, raw_row in enumerate(ws.iter_rows(min_row=CONFIG["data_start_row"], values_only=True)):
            row = [str(c).strip() if c is not None else "" for c in raw_row]

            while len(row) <= COL["max_qiymet"]:
                row.append("")

            barkod = row[COL["barkod"]].strip()
            if not barkod:
                barkod = row[COL["mpn"]].strip()
            if not barkod:
                continue

            qiymet    = to_float(row[COL["qiymet"]])
            endirimli = to_float(row[COL["endirimli"]])
            min_p     = to_float(row[COL["min_qiymet"]])
            max_p     = to_float(row[COL["max_qiymet"]])

            current = qiymet if qiymet > 0 else endirimli
            if current <= 0 or min_p <= 0:
                continue
            if max_p <= 0:
                max_p = round(min_p * 1.1, 2)

            brend = row[COL["brend"]].strip()
            model = row[COL["model"]].strip()
            name  = f"{brend} {model}".strip() or barkod

            product_url = row[COL["url"]].strip() if len(row) > COL["url"] else ""

            products.append({
                "barkod":        barkod,
                "name":          name,
                "current_price": current,
                "min_price":     min_p,
                "max_price":     max_p,
                "sheet_row":     i + CONFIG["data_start_row"],
                "url":           product_url,
            })

    except Exception as e:
        log.error(f"❌ Məhsul oxuma xətası: {e}")
    return products

def write_prices_batch(changes: list) -> bool:
    try:
        excel_data = download_excel()
        wb = openpyxl.load_workbook(BytesIO(excel_data))

        sheet_name = CONFIG.get("sheet_name", "")
        ws = None
        if sheet_name:
            for name in wb.sheetnames:
                if name.strip().lower() == sheet_name.strip().lower():
                    ws = wb[name]
                    break
        if ws is None:
            ws = wb.active

        for change in changes:
            ws.cell(row=change["row"], column=8, value=change["price"])

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        return upload_excel(output.read())
    except Exception as e:
        log.error(f"❌ Batch yazma xətası: {e}")
        return False

# ─────────────────────────────────────────────
# RƏQİB QİYMƏT SCRAPER (NİHAİ VERSİYA: HTML + JS + GİZLİ DATA)
# ─────────────────────────────────────────────
def get_competitor_prices(barkod: str, my_price: float, product_url: str = "") -> list:
    prices = []
    try:
        if product_url and product_url.startswith("http"):
            url = product_url
        else:
            url = f"https://birmarket.az/search?q={barkod}"

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "az-AZ,az;q=0.9,en-US;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }

        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")

        # Əgər axtarış səhifəsidirsə, məhsulun içinə gir
        if "/search" in response.url or "/search" in url:
            import re
            first_product = soup.find("a", href=re.compile(r"/product/"))
            if first_product:
                link = first_product["href"]
                if not link.startswith("http"):
                    link = "https://birmarket.az" + link
                url = link
                response = requests.get(url, headers=headers, timeout=15)
                soup = BeautifulSoup(response.text, "html.parser")
            else:
                return []

        html_text = response.text

        # YÖNTƏM 1: HTML DOM (Əgər açıqdırsa)
        seller_blocks = soup.find_all(attrs={"data-info": "item-other-seller-list"})
        if seller_blocks:
            for block in seller_blocks:
                name_el = block.find(attrs={"data-info": "item-other-seller-name"})
                seller_name = name_el.get_text(strip=True).lower() if name_el else ""
                
                if "unistore" in seller_name: 
                    continue
                    
                price_el = block.find(attrs={"data-info": "item-desc-price-new"})
                if price_el:
                    import re
                    text = re.sub(r"[^\d.,]", "", price_el.get_text(strip=True)).replace(",", ".")
                    try:
                        p = float(text)
                        if 1 < p < 100000: prices.append(p)
                    except ValueError: pass

        # YÖNTƏM 2: JSON-LD (SEO)
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string)
                data_list = data if isinstance(data, list) else [data]
                for item in data_list:
                    if item.get("@type") == "Product" and "offers" in item:
                        offers = item["offers"]
                        if isinstance(offers, dict): offers = [offers]
                        for offer in offers:
                            seller_name = offer.get("seller", {}).get("name", offer.get("merchant", {}).get("name", "")).lower()
                            p = float(offer.get("price", 0))
                            if "unistore" not in seller_name and 1 < p < 100000:
                                prices.append(p)
            except Exception: pass

        # YÖNTƏM 3: NUXT JS GİZLİ MƏLUMATLAR (BÜTÜN SATICILAR BURADADIR)
        import re
        
        # 3a. Obyektlərin içindəki "merchantName" və "price" axtarışı
        chunks = re.findall(r'\{([^{}]+)\}', html_text)
        for chunk in chunks:
            if ('merchantName' in chunk or 'name' in chunk) and 'price' in chunk:
                m_name = re.search(r'(?:merchantName|name)["\']?\s*:\s*["\']([^"\']+)["\']', chunk)
                m_price = re.search(r'price["\']?\s*:\s*([\d\.]+)', chunk)
                if m_name and m_price:
                    seller = m_name.group(1).lower()
                    p = float(m_price.group(1))
                    if "unistore" not in seller and 1 < p < 100000:
                        prices.append(p)

        # 3b. Səhifənin istənilən yerində gizlənmiş Rəqib və Qiymət vəhdəti
        pattern1 = r'(?:merchantName|name)["\']?\s*:\s*["\']([^"\']+)["\'].{1,200}?price["\']?\s*:\s*([\d\.]+)'
        pattern2 = r'price["\']?\s*:\s*([\d\.]+).{1,200}?(?:merchantName|name)["\']?\s*:\s*["\']([^"\']+)["\']'
        
        for match in re.findall(pattern1, html_text, re.IGNORECASE | re.DOTALL):
            seller = match[0].lower()
            p = float(match[1])
            if "unistore" not in seller and 1 < p < 100000: prices.append(p)
                
        for match in re.findall(pattern2, html_text, re.IGNORECASE | re.DOTALL):
            seller = match[1].lower()
            p = float(match[0])
            if "unistore" not in seller and 1 < p < 100000: prices.append(p)

        # Nəticəni təmizləyib loga yazırıq
        prices = list(set(prices))
        if prices:
            log.info(f"  🔎 Tapılan rəqib qiymətləri: {sorted(prices)}")
        else:
            log.info(f"  ℹ️  'Bütün satıcılar' siyahısı boşdur (Tək satıcıyıq)")

    except Exception as e:
        log.warning(f"Scrape xətası [{barkod}]: {e}")

    return prices

def calculate_new_price(current: float, comp_prices: list, min_p: float, max_p: float) -> Optional[float]:
    if not comp_prices:
        return None
    others = [p for p in comp_prices if abs(p - current) > 0.05]
    if not others:
        return None
    cheapest = min(others)

    if current < cheapest:
        target = cheapest - CONFIG["price_undercut"]
        if target > max_p: target = max_p
        if target < min_p: target = min_p
        if abs(target - current) < 0.01:
            return None
        return round(target, 2)

    target = cheapest - CONFIG["price_undercut"]
    if target < min_p: target = min_p
    if target > max_p: target = max_p
    if abs(target - current) < 0.01:
        return None
    return round(target, 2)

# ─────────────────────────────────────────────
# ƏSAS YOXLAMA
# ─────────────────────────────────────────────
def process_product(p: dict) -> dict:
    barkod  = p["barkod"]
    name    = p["name"]
    current = p["current_price"]
    min_p   = p["min_price"]
    max_p   = p["max_price"]
    row     = p["sheet_row"]

    log.info(f"🔍 {name} | Mövcud: {current:.2f}₼ | Min: {min_p:.2f} | Max: {max_p:.2f}")

    if current > max_p:
        return {"status": "updated", "direction": "down", "name": name, "old": current, "new": max_p, "cheapest": max_p, "row": row, "barkod": barkod}
    if current < min_p:
        return {"status": "updated", "direction": "up", "name": name, "old": current, "new": min_p, "cheapest": min_p, "row": row, "barkod": barkod}

    comp_prices = get_competitor_prices(barkod, current, p.get("url", ""))
    if comp_prices is None:
        return {"status": "error"}
    
    # Rəqib yoxdursa max qiymətə qaldırır (Tek satıcı)
    if comp_prices == []:
        if current < max_p:
            return {"status": "updated", "direction": "up", "name": name, "old": current, "new": max_p, "cheapest": max_p, "row": row, "barkod": barkod}
        return {"status": "no_competitor"}

    others = [x for x in comp_prices if abs(x - current) > 0.05]
    cheapest = min(others) if others else current

    if not others or current <= cheapest:
        return {"status": "best_price", "name": name, "current": current}

    new_price = calculate_new_price(current, comp_prices, min_p, max_p)
    if new_price is None:
        return {"status": "best_price", "name": name, "current": current}

    direction = "up" if new_price > current else "down"
    return {"status": "updated", "direction": direction, "name": name, "old": current, "new": new_price, "cheapest": cheapest, "row": row, "barkod": barkod}

def run_check():
    log.info("=" * 55)
    log.info(f"🚀 Yoxlama (Final HTML + JS Sürümü) — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    products = load_products()
    if not products:
        return

    stats = {"updated_down": 0, "updated_up": 0, "best_price": 0, "no_competitor": 0, "error": 0}
    changes = []
    updated_results = []

    from concurrent.futures import ThreadPoolExecutor, as_completed
    # Sunucuyu yormayacağı için aynı anda 10 ürün kontrol edilebilir
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_product, p): p for p in products}
        for future in as_completed(futures):
            try:
                result = future.result()
                status = result.get("status", "error")

                if status == "updated":
                    changes.append({"row": result["row"], "price": result["new"]})
                    updated_results.append(result)
                    if result["direction"] == "down": stats["updated_down"] += 1
                    else: stats["updated_up"] += 1
                elif status == "best_price": stats["best_price"] += 1
                elif status == "no_competitor": stats["no_competitor"] += 1
                else: stats["error"] += 1

            except Exception:
                stats["error"] += 1

    if changes:
        success = write_prices_batch(changes)
        if success:
            for result in updated_results:
                cheapest = result.get('cheapest', result['new'])
                record_price_change(result["barkod"], result["old"], result["new"], f"Rəqib: {cheapest:.2f}₼")
                send_telegram(
                    f"💰 <b>{result['name']}</b>\n"
                    f"{result['old']:.2f}₼ → <b>{result['new']:.2f}₼</b>\n"
                    f"🏷 Rəqib: {cheapest:.2f}₼"
                )
        else:
            stats["error"] += len(changes)

    report = (
        f"📊 <b>Yoxlama Hesabatı</b>\n"
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📦 Ümumi məhsul: <b>{len(products)}</b>\n"
        f"📉 Qiymət endirildi: <b>{stats['updated_down']}</b>\n"
        f"📈 Qiymət artırıldı: <b>{stats['updated_up']}</b>\n"
        f"✅ Ən yaxşı qiymət bizdə: <b>{stats['best_price']}</b>\n"
        f"🔍 Rəqib tapılmadı: <b>{stats['no_competitor']}</b>\n"
        f"❌ Xəta: <b>{stats['error']}</b>"
    )
    send_telegram(report)

if __name__ == "__main__":
    run_check()
    schedule.every(CONFIG["check_interval_minutes"]).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(30)
