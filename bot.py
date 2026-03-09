"""
Birmarket.az Qiymət İzləmə Botu - STABİL VERSİYA
================================================
Dəyişikliklər:
1. Sürətli Requests sistemi (Ağır Chrome brauzeri yoxdur)
2. Nuxt.js gizli data oxuma (Rəqibləri 100% tapır)
3. Railway üçün azaldılmış worker sayı (max_workers=3) - Donmanın qarşısını alır
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
from concurrent.futures import ThreadPoolExecutor, as_completed

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

COL = {
    "barkod": 0, "mpn": 1, "model": 2, "brend": 3,
    "olke": 4, "say": 5, "endirimli": 6,
    "qiymet": 7, "tesvir": 8, "start": 9,
    "finish": 10, "taksit": 11, "aylar": 12,
    "url": 13, "min_qiymet": 14, "max_qiymet": 15,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(CONFIG["log_file"], encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# TELEGRAM VƏ GOOGLE SERVİSLƏRİ
# ─────────────────────────────────────────────
def send_telegram(message: str):
    token = CONFIG.get("telegram_bot_token", "")
    chat_id = CONFIG.get("telegram_chat_id", "")
    if not token or not chat_id: return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}, timeout=15)
    except: pass

def get_credentials(scopes: list) -> Credentials:
    info = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
    return Credentials.from_service_account_info(info, scopes=scopes)

def download_excel() -> bytes:
    file_id = CONFIG["excel_file_url"].split("/d/")[1].split("/")[0]
    resp = requests.get(f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx", timeout=30)
    resp.raise_for_status()
    return resp.content

def upload_excel(data: bytes) -> bool:
    try:
        file_id = CONFIG["excel_file_url"].split("/d/")[1].split("/")[0]
        creds = get_credentials(["https://www.googleapis.com/auth/drive"])
        creds.refresh(Request())
        resp = requests.patch(f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media",
            headers={"Authorization": f"Bearer {creds.token}", "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
            data=data, timeout=60)
        return resp.status_code == 200
    except Exception as e:
        log.error(f"Upload xətası: {e}")
        return False

# ─────────────────────────────────────────────
# RƏQİB QİYMƏT SCRAPER (HTML + JS SCAN)
# ─────────────────────────────────────────────
def get_competitor_prices(barkod: str, product_url: str = "") -> list:
    prices = []
    try:
        url = product_url if (product_url and "http" in product_url) else f"https://birmarket.az/search?q={barkod}"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200: return []
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Eğer aramadan geliyorsa ilk ürüne gir
        if "/search" in resp.url:
            first = soup.find("a", href=re.compile(r"/product/"))
            if first:
                url = "https://birmarket.az" + first["href"] if not first["href"].startswith("http") else first["href"]
                resp = requests.get(url, headers=headers, timeout=20)
                soup = BeautifulSoup(resp.text, "html.parser")
        
        html_text = resp.text
        
        # 1. Gizli JS verilerini (Nuxt) tara
        found = re.findall(r'(?:merchantName|name)["\']?\s*:\s*["\']([^"\']+)["\'].{1,200}?price["\']?\s*:\s*([\d\.]+)', html_text, re.I | re.S)
        for seller, p in found:
            if "unistore" not in seller.lower():
                prices.append(float(p))
                
        # 2. Açık HTML bloklarını tara
        for block in soup.find_all(attrs={"data-info": "item-other-seller-list"}):
            name_el = block.find(attrs={"data-info": "item-other-seller-name"})
            if name_el and "unistore" not in name_el.get_text().lower():
                price_el = block.find(attrs={"data-info": "item-desc-price-new"})
                if price_el:
                    p_val = re.sub(r"[^\d.]", "", price_el.get_text().replace(",", "."))
                    prices.append(float(p_val))
    except: pass
    return list(set(prices))

# ─────────────────────────────────────────────
# İŞLƏMƏ MANTIQI
# ─────────────────────────────────────────────
def process_product(p: dict):
    barkod, current, min_p, max_p = p["barkod"], p["current_price"], p["min_price"], p["max_p"]
    
    comp_prices = get_competitor_prices(barkod, p["url"])
    
    # Rəqib yoxdursa -> Qiyməti MAX-a qaldır
    if not comp_prices:
        if current < max_p:
            log.info(f"📈 {p['name']}: Tək satıcıyıq -> {max_p}₼")
            return {"status": "up", "new": max_p, "p": p}
        return {"status": "ok"}
    
    cheapest = min(comp_prices)
    log.info(f"🔍 {p['name']} | Rəqib: {cheapest} | Biz: {current}")

    # Biz ən ucuzuqsa qiyməti rəqibə yaxınlaşdır (qaldır)
    if current < cheapest - 0.05:
        target = min(cheapest - CONFIG["price_undercut"], max_p)
        if abs(target - current) > 0.05:
            return {"status": "up", "new": round(target, 2), "p": p}
    # Rəqib bizdən ucuzdursa qiyməti endir
    elif current > cheapest:
        target = max(cheapest - CONFIG["price_undercut"], min_p)
        if abs(target - current) > 0.05:
            return {"status": "down", "new": round(target, 2), "p": p}
            
    return {"status": "ok"}

def run_check():
    log.info("="*50)
    log.info(f"🚀 Yoxlama başladı: {datetime.now().strftime('%H:%M:%S')}")
    products = []
    try:
        data = download_excel()
        wb = openpyxl.load_workbook(BytesIO(data), data_only=True)
        ws = wb[CONFIG["sheet_name"]] if CONFIG["sheet_name"] in wb.sheetnames else wb.active
        
        for i, row in enumerate(ws.iter_rows(min_row=CONFIG["data_start_row"], values_only=True), CONFIG["data_start_row"]):
            if not row[0]: continue
            try:
                products.append({
                    "barkod": str(row[0]), "name": f"{row[3]} {row[2]}",
                    "current_price": float(str(row[7] or row[6]).replace(",",".").replace(" ","")),
                    "min_price": float(str(row[14]).replace(",",".").replace(" ","")),
                    "max_p": float(str(row[15]).replace(",",".").replace(" ","")),
                    "url": str(row[13]) if row[13] else "", "row": i
                })
            except: continue
    except Exception as e:
        log.error(f"Excel oxuma xətası: {e}"); return

    changes = []
    # DİQQƏT: Railway-in donmaması üçün worker sayı 3 edildi
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_product, p) for p in products]
        for f in as_completed(futures):
            res = f.result()
            if res["status"] in ["up", "down"]:
                changes.append(res)

    if changes:
        log.info(f"💾 {len(changes)} məhsul yenilənir...")
        wb = openpyxl.load_workbook(BytesIO(data))
        ws = wb[CONFIG["sheet_name"]] if CONFIG["sheet_name"] in wb.sheetnames else wb.active
        
        for c in changes:
            ws.cell(row=c["p"]["row"], column=8, value=c["new"]) # H sütunu
            send_telegram(f"💰 <b>{c['p']['name']}</b>\n{c['p']['current_price']}₼ ➔ <b>{c['new']}₼</b>")
        
        out = BytesIO()
        wb.save(out)
        if upload_excel(out.getvalue()):
            log.info("✅ Excel və qiymətlər uğurla yeniləndi.")
            send_telegram(f"✅ <b>Yoxlama bitdi.</b> {len(changes)} qiymət dəyişdi.")
        else:
            log.error("❌ Excel yüklənə bilmədi!")
    else:
        log.info("✅ Dəyişiklik yoxdur.")
        send_telegram("✅ <b>Yoxlama bitdi.</b> Heç bir qiymət dəyişmədi.")

# ─────────────────────────────────────────────
# BOTU BAŞLAT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    run_check() # İlk yoxlama
    schedule.every(CONFIG["check_interval_minutes"]).minutes.do(run_check)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
