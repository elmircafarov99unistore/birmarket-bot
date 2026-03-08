"""
Birmarket.az Qiymət İzləmə Botu
================================
İş prinsipi:
  1. Google Sheets-dən məhsulları oxuyur (barkod, qiymət, min/max limit)
  2. Birmarket-də hər məhsulun rəqib qiymətlərini tapır
  3. Yeni qiyməti hesablayır (min/max limitə görə)
  4. Google Sheets-in G sütununa (Endirimli qiymət) yeni qiyməti yazır
  5. Umico biznes profilində avtomatik dəyişir

Quraşdırma:
    pip install -r requirements.txt

İstifadə:
    python bot.py
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

# Google Sheets
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


# ─────────────────────────────────────────────
# KONFIQURASIYA — buraya öz məlumatlarınızı yazın
# ─────────────────────────────────────────────
CONFIG = {
    # Google Sheets — Railway Variables-dan oxunur
    "spreadsheet_id":         os.environ.get("SPREADSHEET_ID", ""),
    "sheet_name":             os.environ.get("SHEET_NAME", "Sheet1"),
    "data_start_row":         2,

    # Google Service Account JSON faylı
    "google_credentials_file": "credentials.json",

    # Telegram — Railway Variables-dan oxunur
    "telegram_bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id":   os.environ.get("TELEGRAM_CHAT_ID", ""),

    # Bot parametrləri
    "check_interval_minutes": 10,    # neçə dəqiqədən bir yoxlasın
    "price_undercut":         0.01,  # rəqibdən neçə manat ucuz olsun
    "sheets_reload_interval": 30,    # Sheets-dən neçə dəqiqədən bir yenilənsin

    "log_file": "birmarket_bot.log",
}

# ─────────────────────────────────────────────
# SHEETS SÜTUN XƏRİTƏSİ
# A=Barkod  B=MPN     C=Model  D=Brend
# E=Ölkə   F=Say     G=Endirimli qiymət ← BOT BURA YAZIR
# H=Qiymət I=Təsvir  J=Start  K=Finiş
# L=Taksit M=Aylar   N=Min ₼  O=Max ₼
# ─────────────────────────────────────────────
COL = {
    "barkod":           0,   # A
    "mpn":              1,   # B
    "model":            2,   # C
    "brend":            3,   # D
    "olke":             4,   # E
    "say":              5,   # F
    "endirimli":        6,   # G  ← bot bu sütuna yazır
    "qiymet":           7,   # H
    "tesvir":           8,   # I
    "start":            9,   # J
    "finish":           10,  # K
    "taksit":           11,  # L
    "aylar":            12,  # M
    "min_qiymet":       13,  # N
    "max_qiymet":       14,  # O
}

# G sütununun hərfi (yazma üçün)
PRICE_WRITE_COL = "G"


# ─────────────────────────────────────────────
# LOG SİSTEMİ
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
# QİYMƏT TARİXÇƏSİ
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
        "time":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "old_price": old,
        "new_price": new,
        "reason":    reason,
    })
    save_history(h)


# ─────────────────────────────────────────────
# TELEGRAM BİLDİRİŞ
# ─────────────────────────────────────────────
def send_telegram(message: str):
    token   = CONFIG.get("telegram_bot_token", "")
    chat_id = CONFIG.get("telegram_chat_id", "")
    if not token or "YOUR_" in token:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.status_code == 200:
            log.info("📨 Telegram bildirişi göndərildi.")
        else:
            log.warning(f"Telegram xətası: {resp.status_code}")
    except Exception as e:
        log.warning(f"Telegram göndərilə bilmədi: {e}")


# ─────────────────────────────────────────────
# GOOGLE SHEETS — OXUMA
# ─────────────────────────────────────────────
def get_sheets_service(readonly=True):
    scope = (
        "https://www.googleapis.com/auth/spreadsheets.readonly"
        if readonly else
        "https://www.googleapis.com/auth/spreadsheets"
    )
    # Railway-də GOOGLE_CREDENTIALS variable-dan oxu
    # Yereldə credentials.json faylından oxu
    google_creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if google_creds_json:
        import json as _json
        from google.oauth2.service_account import Credentials as _Creds
        info = _json.loads(google_creds_json)
        creds = _Creds.from_service_account_info(info, scopes=[scope])
    else:
        creds = Credentials.from_service_account_file(
            CONFIG["google_credentials_file"],
            scopes=[scope],
        )
    return build("sheets", "v4", credentials=creds)

def to_float(val, default=0.0) -> float:
    try:
        return float(str(val).replace(",", ".").replace(" ", "").replace("₼", ""))
    except (ValueError, TypeError):
        return default

def load_products_from_sheets() -> list:
    """Sheets-dən bütün məhsulları oxuyur."""
    products = []
    try:
        service = get_sheets_service(readonly=True)
        range_name = f"{CONFIG['sheet_name']}!A{CONFIG['data_start_row']}:O"
        result = service.spreadsheets().values().get(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=range_name,
        ).execute()

        rows = result.get("values", [])
        for i, row in enumerate(rows):
            while len(row) <= COL["max_qiymet"]:
                row.append("")

            barkod = row[COL["barkod"]].strip()
            if not barkod:
                continue

            qiymet     = to_float(row[COL["qiymet"]])
            endirimli  = to_float(row[COL["endirimli"]])
            min_p      = to_float(row[COL["min_qiymet"]])
            max_p      = to_float(row[COL["max_qiymet"]])

            # Cari qiymət: endirimli varsa onu, yoxsa əsas qiyməti götür
            current = endirimli if endirimli > 0 else qiymet

            if current <= 0 or min_p <= 0:
                continue

            if max_p <= 0:
                max_p = current * 2

            brend = row[COL["brend"]].strip()
            model = row[COL["model"]].strip()
            name  = f"{brend} {model}".strip() or barkod

            # Sheets-də bu sətrin real nömrəsi (yazma üçün lazım)
            sheet_row = i + CONFIG["data_start_row"]

            products.append({
                "barkod":        barkod,
                "name":          name,
                "current_price": current,
                "min_price":     min_p,
                "max_price":     max_p,
                "sheet_row":     sheet_row,
            })

        log.info(f"📋 Sheets-dən {len(products)} məhsul oxundu.")
    except Exception as e:
        log.error(f"❌ Sheets oxuma xətası: {e}")
    return products


# ─────────────────────────────────────────────
# GOOGLE SHEETS — YAZMA (G sütunu)
# ─────────────────────────────────────────────
def write_price_to_sheets(sheet_row: int, new_price: float) -> bool:
    """Sheets-in G sütununa yeni qiyməti yazır."""
    try:
        service = get_sheets_service(readonly=False)
        cell = f"{CONFIG['sheet_name']}!{PRICE_WRITE_COL}{sheet_row}"
        service.spreadsheets().values().update(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=cell,
            valueInputOption="RAW",
            body={"values": [[new_price]]},
        ).execute()
        return True
    except Exception as e:
        log.error(f"❌ Sheets yazma xətası (sətir {sheet_row}): {e}")
        return False


# ─────────────────────────────────────────────
# RƏQİB QİYMƏT SCRAPER
# ─────────────────────────────────────────────
def get_competitor_prices(barkod: str, my_price: float) -> list:
    """Birmarket-də barkodla axtarır, rəqib qiymətlərini qaytarır."""
    prices = []
    try:
        url = f"https://birmarket.az/search?q={barkod}"
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        })
        soup = BeautifulSoup(resp.text, "html.parser")

        for el in soup.select(
            ".product-offer__price, .seller-price, .offer-price, "
            "[data-price], .price-value, .product__price"
        ):
            text = re.sub(r"[^\d.,]", "", el.get_text(strip=True)).replace(",", ".")
            try:
                p = float(text)
                if p > 0:
                    prices.append(p)
            except ValueError:
                pass

        if not prices:
            for meta in soup.select("meta[itemprop='price']"):
                try:
                    p = float(meta.get("content", "0"))
                    if p > 0:
                        prices.append(p)
                except ValueError:
                    pass

    except Exception as e:
        log.warning(f"Scrape xətası [{barkod}]: {e}")

    return prices


# ─────────────────────────────────────────────
# QİYMƏT HESABLAMA
# ─────────────────────────────────────────────
def calculate_new_price(current: float, comp_prices: list, min_p: float, max_p: float) -> Optional[float]:
    if not comp_prices:
        return None

    # Özümüzün qiymətini çıxar
    others = [p for p in comp_prices if abs(p - current) > 0.05]
    if not others:
        return None

    cheapest = min(others)

    # Artıq ucuzuqsa — dəyişiklik yoxdur
    if current < cheapest:
        return None

    target = cheapest - CONFIG["price_undercut"]

    # Limit qoruması
    if target < min_p:
        log.info(f"  ⚠️  Min limitə çatıldı → {min_p:.2f}₼")
        target = min_p

    if target > max_p:
        target = max_p

    if abs(target - current) < 0.01:
        return None

    return round(target, 2)


# ─────────────────────────────────────────────
# ƏSAS YOXLAMA
# ─────────────────────────────────────────────
_products_cache: list = []
_last_load: float = 0.0

def get_products() -> list:
    global _products_cache, _last_load
    interval = CONFIG["sheets_reload_interval"] * 60
    if not _products_cache or (time.time() - _last_load) > interval:
        log.info("🔄 Sheets-dən məhsullar yüklənir...")
        _products_cache = load_products_from_sheets()
        _last_load = time.time()
    return _products_cache


def run_check():
    log.info("=" * 55)
    log.info(f"🚀 Yoxlama — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    products = get_products()
    if not products:
        log.error("❌ Məhsul siyahısı boşdur!")
        return

    log.info(f"📦 {len(products)} məhsul yoxlanılır...\n")
    updated = 0

    for p in products:
        barkod  = p["barkod"]
        name    = p["name"]
        current = p["current_price"]
        min_p   = p["min_price"]
        max_p   = p["max_price"]
        row     = p["sheet_row"]

        log.info(f"🔍 {name} | {current:.2f}₼ | Min:{min_p:.2f} Max:{max_p:.2f}")

        # Rəqib qiymətlərini tap
        comp_prices = get_competitor_prices(barkod, current)
        if not comp_prices:
            log.warning(f"  ⚠️  Rəqib tapılmadı.")
            time.sleep(1)
            continue

        others = [x for x in comp_prices if abs(x - current) > 0.05]
        if others:
            log.info(f"  📊 Rəqiblər: {sorted(others)}")

        # Yeni qiymət hesabla
        new_price = calculate_new_price(current, comp_prices, min_p, max_p)
        if new_price is None:
            log.info(f"  ✅ Dəyişiklik lazım deyil.")
            time.sleep(1)
            continue

        cheapest = min(others) if others else current
        log.info(f"  💰 {current:.2f}₼ → {new_price:.2f}₼")

        # Sheets-ə yaz → Umico avtomatik dəyişir
        success = write_price_to_sheets(row, new_price)
        if success:
            record_price_change(barkod, current, new_price, f"Rəqib: {cheapest:.2f}₼")
            p["current_price"] = new_price
            updated += 1
            log.info(f"  ✅ Sheets-ə yazıldı! (Umico avtomatik dəyişəcək)")
            send_telegram(
                f"💰 <b>{name}</b>\n"
                f"{current:.2f}₼ → <b>{new_price:.2f}₼</b>\n"
                f"🏷 Rəqib: {cheapest:.2f}₼"
            )
        else:
            log.error(f"  ❌ Sheets-ə yazıla bilmədi!")

        time.sleep(2)  # saytı yükləməmək üçün

    log.info(f"\n✅ Tamamlandı. {updated} məhsul yeniləndi.\n")
    if updated > 0:
        send_telegram(f"✅ BirmarketBot: {updated} məhsulun qiyməti yeniləndi.")


# ─────────────────────────────────────────────
# ƏSAS PROQRAM
# ─────────────────────────────────────────────
if __name__ == "__main__":
    log.info("🤖 Birmarket Bot işə salındı")
    log.info(f"⏱️  Yoxlama: hər {CONFIG['check_interval_minutes']} dəq")
    log.info(f"🔄 Sheets yeniləmə: hər {CONFIG['sheets_reload_interval']} dəq")
    log.info(f"📝 Qiymət yazılır: G sütununa (Endirimli qiymət)")

    run_check()
    schedule.every(CONFIG["check_interval_minutes"]).minutes.do(run_check)

    log.info("🔄 Bot aktiv — Ctrl+C ilə dayandırın\n")
    while True:
        schedule.run_pending()
        time.sleep(30)
