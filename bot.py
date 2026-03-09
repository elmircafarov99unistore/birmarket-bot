"""
Birmarket.az Qiymət İzləmə Botu
================================
İş prinsipi:
  1. Google Drive-dakı Excel faylını yükləyir
  2. Birmarket-də rəqib qiymətlərini tapır
  3. Yeni qiyməti hesablayır (min/max limitə görə)
  4. Excel faylının G sütununa yeni qiyməti yazır
  5. Faylı Google Drive-a geri yükləyir → Umico avtomatik dəyişir

Quraşdırma:
    pip install -r requirements.txt
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
# KONFIQURASIYA
# ─────────────────────────────────────────────
CONFIG = {
    # Excel faylının Google Drive paylaşma linki
    "excel_file_url": os.environ.get("EXCEL_FILE_URL", ""),

    # Vərəqin adı (aşağıdakı tab)
    "sheet_name": os.environ.get("SHEET_NAME", "Əsas"),

    # Məlumat neçənci sətirdən başlayır (1=başlıq)
    "data_start_row": 2,

    # Telegram
    "telegram_bot_token": os.environ.get("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id":   os.environ.get("TELEGRAM_CHAT_ID", ""),

    # Bot parametrləri
    "check_interval_minutes": 10,
    "price_undercut":         0.01,

    "log_file": "birmarket_bot.log",
}

# ─────────────────────────────────────────────
# SÜTUN XƏRİTƏSİ
# A=Barkod B=MPN C=Model D=Brend E=Ölkə
# F=Say G=Endirimli← BOT BURA YAZIR
# H=Qiymət I=Təsvir J=Start K=Finiş
# L=Taksit M=Aylar N=Min ₼ O=Max ₼
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
        log.info("📨 Telegram bildirişi göndərildi.")
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
    """Google Drive-dan Excel faylını yükləyir."""
    file_id = get_file_id()
    export_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
    resp = requests.get(export_url, timeout=30)
    resp.raise_for_status()
    return resp.content

def upload_excel(data: bytes) -> bool:
    """Dəyişdirilmiş Excel faylını Google Drive-a yükləyir."""
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
    """Excel faylından məhsulları oxuyur."""
    products = []
    try:
        excel_data = download_excel()
        wb = openpyxl.load_workbook(BytesIO(excel_data), read_only=True, data_only=True)

        # Vərəqi tap — encoding problemini önləmək üçün wb.active istifadə edirik
        sheet_name = CONFIG.get("sheet_name", "")
        ws = None
        if sheet_name:
            for name in wb.sheetnames:
                if name.strip().lower() == sheet_name.strip().lower():
                    ws = wb[name]
                    break
        if ws is None:
            ws = wb.active
            log.info(f"📋 Aktiv vərəq istifadə edilir: '{ws.title}'")
        else:
            log.info(f"📋 Vərəq: {ws.title}")

        for i, raw_row in enumerate(ws.iter_rows(min_row=CONFIG["data_start_row"], values_only=True)):
            row = [str(c).strip() if c is not None else "" for c in raw_row]

            while len(row) <= COL["max_qiymet"]:  # P sütunu = index 15
                row.append("")

            # Barkod yoxdursa MPN istifadə et
            barkod = row[COL["barkod"]].strip()
            if not barkod:
                barkod = row[COL["mpn"]].strip()
            if not barkod:
                continue

            qiymet    = to_float(row[COL["qiymet"]])
            endirimli = to_float(row[COL["endirimli"]])
            min_p     = to_float(row[COL["min_qiymet"]])
            max_p     = to_float(row[COL["max_qiymet"]])

            current = endirimli if endirimli > 0 else qiymet
            if current <= 0 or min_p <= 0:
                continue
            if max_p <= 0:
                max_p = current * 2

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

        log.info(f"📦 {len(products)} məhsul oxundu.")
    except Exception as e:
        log.error(f"❌ Məhsul oxuma xətası: {e}")
    return products


def write_prices_batch(changes: list) -> bool:
    """Bütün dəyişiklikləri bir dəfəyə Excel-ə yazır. changes = [{row, price}, ...]"""
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
            ws.cell(row=change["row"], column=8, value=change["price"])  # H = 8

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        success = upload_excel(output.read())
        if success:
            log.info(f"✅ {len(changes)} dəyişiklik Excel-ə yazıldı.")
        return success
    except Exception as e:
        log.error(f"❌ Batch yazma xətası: {e}")
        return False


# ─────────────────────────────────────────────
# RƏQİB QİYMƏT SCRAPER
# ─────────────────────────────────────────────
def get_competitor_prices(barkod: str, my_price: float, product_url: str = "") -> list:
    prices = []
    try:
        # URL etibarlımı yoxla
        if product_url and product_url.startswith("http"):
            url = product_url
        else:
            log.warning(f"  ⚠️  URL tapılmadı [{barkod}], axtarış ilə cəhd edilir.")
            url = f"https://birmarket.az/search?q={barkod}"

        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "az,en;q=0.5",
        })
        soup = BeautifulSoup(resp.text, "html.parser")

        # Digər satıcıların bloklarını tap (data-info="item-other-seller-list")
        seller_blocks = soup.find_all(attrs={"data-info": "item-other-seller-list"})

        if seller_blocks:
            for block in seller_blocks:
                # Satıcı adını tap
                name_el = block.find(attrs={"data-info": "item-other-seller-name"})
                seller_name = name_el.get_text(strip=True).lower() if name_el else ""

                # Unistore-u atla
                if "unistore" in seller_name:
                    log.info(f"  ℹ️  Özümüzün listinqi atlandı ({seller_name})")
                    continue

                # Həmin satıcının qiymətini tap
                price_el = block.find("span", attrs={"data-info": "item-desc-price-new"})
                if not price_el:
                    price_el = block.find(attrs={"data-info": "item-desc-price-new"})
                if price_el:
                    text = re.sub(r"[^\d.,\s]", "", price_el.get_text(strip=True)).replace(",", ".").replace(" ", "")
                    try:
                        p = float(text)
                        if 1 < p < 100000:
                            prices.append(p)
                            log.info(f"  🏪 {seller_name}: {p:.2f}₼")
                    except ValueError:
                        pass

        # Əgər digər satıcı bloku yoxdursa — əsas qiymətə bax (tək satıcı)
        if not seller_blocks:
            # Əsas satıcı adını yoxla — bütün mətni tara
            page_text = soup.get_text(separator=" ", strip=True).lower()
            if "unistore" in page_text:
                log.info(f"  ℹ️  Tək satıcı — özümüzük, rəqib yoxdur.")
            else:
                for el in soup.select('span[data-info="item-desc-price-new"]'):
                    text = re.sub(r"[^\d.,\s]", "", el.get_text(strip=True)).replace(",", ".").replace(" ", "")
                    try:
                        p = float(text)
                        if 1 < p < 100000:
                            prices.append(p)
                    except ValueError:
                        pass

        # 2. Meta itemprop
        if not prices:
            for meta in soup.select("meta[itemprop='price']"):
                try:
                    p = float(meta.get("content", "0"))
                    if p > 0:
                        prices.append(p)
                except ValueError:
                    pass

        # 3. JSON-LD strukturlu data
        if not prices:
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    # Tək məhsul
                    offers = data.get("offers", {})
                    if isinstance(offers, dict):
                        p = float(offers.get("price", 0))
                        if p > 0:
                            prices.append(p)
                    elif isinstance(offers, list):
                        for o in offers:
                            p = float(o.get("price", 0))
                            if p > 0:
                                prices.append(p)
                except Exception:
                    pass

        # 4. Regex ilə HTML-dən qiymət axtar (son çarə)
        if not prices:
            matches = re.findall(r'"price"\s*:\s*"?([\d.]+)"?', resp.text)
            for m in matches:
                try:
                    p = float(m)
                    if 1 < p < 100000:
                        prices.append(p)
                except ValueError:
                    pass

        if prices:
            log.info(f"  🔎 Tapılan qiymətlər: {sorted(set(prices))}")
        else:
            log.debug(f"  HTML nümunə (500 simvol): {resp.text[:500]}")

    except Exception as e:
        log.warning(f"Scrape xətası [{barkod}]: {e}")
    return prices


# ─────────────────────────────────────────────
# QİYMƏT HESABLAMA
# ─────────────────────────────────────────────
def calculate_new_price(current: float, comp_prices: list, min_p: float, max_p: float) -> Optional[float]:
    if not comp_prices:
        return None
    others = [p for p in comp_prices if abs(p - current) > 0.05]
    if not others:
        return None
    cheapest = min(others)

    if current < cheapest:
        # Biz ən ucuzuq — qiyməti qaldır (rəqibdən 0.01 aşağı, amma max-dan çox olmasın)
        target = cheapest - CONFIG["price_undercut"]
        if target > max_p:
            target = max_p
        if target < min_p:
            target = min_p
        if abs(target - current) < 0.01:
            return None
        log.info(f"  📈 Qiymət artırılır: {current:.2f}₼ → {target:.2f}₼")
        return round(target, 2)

    # Biz ən ucuz deyilik — aşağı sal
    target = cheapest - CONFIG["price_undercut"]
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
def process_product(p: dict) -> dict:
    """Tək məhsulu yoxlayır — paralel işləmə üçün."""
    barkod  = p["barkod"]
    name    = p["name"]
    current = p["current_price"]
    min_p   = p["min_price"]
    max_p   = p["max_price"]
    row     = p["sheet_row"]

    log.info(f"🔍 {name} | {current:.2f}₼ | Min:{min_p:.2f} Max:{max_p:.2f}")

    comp_prices = get_competitor_prices(barkod, current, p.get("url", ""))
    if not comp_prices:
        log.warning(f"  ⚠️  Rəqib tapılmadı.")
        return {"status": "no_competitor"}

    others = [x for x in comp_prices if abs(x - current) > 0.05]
    if others:
        log.info(f"  📊 Rəqiblər: {sorted(others)}")

    cheapest = min(others) if others else current

    # Ən ucuz bizdədir?
    if not others or current <= cheapest:
        log.info(f"  ✅ Dəyişiklik lazım deyil.")
        return {"status": "best_price", "name": name, "current": current}

    new_price = calculate_new_price(current, comp_prices, min_p, max_p)
    if new_price is None:
        log.info(f"  ✅ Dəyişiklik lazım deyil.")
        return {"status": "best_price", "name": name, "current": current}

    # Qiymət artır ya azalır?
    direction = "up" if new_price > current else "down"
    log.info(f"  💰 {current:.2f}₼ → {new_price:.2f}₼")

    return {"status": "updated", "direction": direction, "name": name,
            "old": current, "new": new_price, "cheapest": cheapest,
            "row": row, "barkod": barkod}


def run_check():
    log.info("=" * 55)
    log.info(f"🚀 Yoxlama — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    products = load_products()
    if not products:
        log.error("❌ Məhsul siyahısı boşdur!")
        return

    log.info(f"📦 {len(products)} məhsul yoxlanılır...\n")

    stats = {"updated_down": 0, "updated_up": 0, "best_price": 0, "no_competitor": 0, "error": 0}
    changes = []  # Bütün dəyişikliklər burada toplanır

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(process_product, p): p for p in products}
        for future in as_completed(futures):
            try:
                result = future.result()
                status = result.get("status", "error")

                if status == "updated":
                    changes.append({"row": result["row"], "price": result["new"]})
                    if result["direction"] == "down":
                        stats["updated_down"] += 1
                    else:
                        stats["updated_up"] += 1
                elif status == "best_price":
                    stats["best_price"] += 1
                elif status == "no_competitor":
                    stats["no_competitor"] += 1
                else:
                    stats["error"] += 1

            except Exception as e:
                log.error(f"❌ Thread xətası: {e}")
                stats["error"] += 1

    # Bütün dəyişiklikləri BİR DƏFƏ Excel-ə yaz
    if changes:
        log.info(f"\n💾 {len(changes)} dəyişiklik Excel-ə yazılır...")
        success = write_prices_batch(changes)
        if success:
            # Tarixçəyə yaz və Telegram bildirişi göndər
            for result in [f for f in [fut.result() for fut in futures if not fut.exception()] if f.get("status") == "updated"]:
                record_price_change(result["barkod"], result["old"], result["new"], f"Rəqib: {result['cheapest']:.2f}₼")
                send_telegram(
                    f"💰 <b>{result['name']}</b>\n"
                    f"{result['old']:.2f}₼ → <b>{result['new']:.2f}₼</b>\n"
                    f"🏷 Rəqib: {result['cheapest']:.2f}₼"
                )
        else:
            stats["error"] += len(changes)
            stats["updated_down"] = 0
            stats["updated_up"] = 0

    total_updated = stats["updated_down"] + stats["updated_up"]
    log.info(f"\n✅ Tamamlandı. {total_updated} məhsul yeniləndi.\n")

    # Telegram hesabatı
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


# ─────────────────────────────────────────────
# ƏSAS PROQRAM
# ─────────────────────────────────────────────
if __name__ == "__main__":
    log.info("🤖 Birmarket Bot işə salındı")
    log.info(f"⏱️  Yoxlama: hər {CONFIG['check_interval_minutes']} dəq")
    log.info(f"📝 Qiymət yazılır: H sütununa (Qiymət)")

    run_check()
    schedule.every(CONFIG["check_interval_minutes"]).minutes.do(run_check)

    log.info("🔄 Bot aktiv — Ctrl+C ilə dayandırın\n")
    while True:
        schedule.run_pending()
        time.sleep(30)
