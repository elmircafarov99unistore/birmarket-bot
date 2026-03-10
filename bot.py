"""
Birmarket.az Qiymət İzləmə Botu
================================
Məntiq:
  - Birmarket səhifəsindəki ən aşağı qiymət (ana satıcı) bizdədirsə → Max-a qaldır
  - Bizdə deyilsə → Ana qiymət - 0.01 qoy (min limitə qədər)
"""

import json, time, re, schedule, logging, os, requests
from datetime import datetime
from typing import Optional, Tuple
from bs4 import BeautifulSoup
from io import BytesIO
import openpyxl
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request

# ─────────────────────────────────────────────
# KONFİQURASİYA
# ─────────────────────────────────────────────
CONFIG = {
    "excel_file_url":         os.environ.get("EXCEL_FILE_URL", ""),
    "sheet_name":             os.environ.get("SHEET_NAME", "Əsas"),
    "data_start_row":         2,
    "telegram_bot_token":     os.environ.get("TELEGRAM_BOT_TOKEN", ""),
    "telegram_chat_id":       os.environ.get("TELEGRAM_CHAT_ID", ""),
    "check_interval_minutes": 10,
    "price_undercut":         0.01,
    "max_workers":            5,
    "log_file":               "birmarket_bot.log",
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
    handlers=[
        logging.FileHandler(CONFIG["log_file"], encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "az,en;q=0.5",
}

# ─────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────
def send_telegram(message: str):
    token   = CONFIG.get("telegram_bot_token", "")
    chat_id = CONFIG.get("telegram_chat_id", "")
    if not token or not chat_id:
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
# GOOGLE DRİVE
# ─────────────────────────────────────────────
def get_credentials(scopes):
    info = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
    return Credentials.from_service_account_info(info, scopes=scopes)

def download_excel() -> bytes:
    file_id = CONFIG["excel_file_url"].split("/d/")[1].split("/")[0]
    resp = requests.get(
        f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx",
        timeout=30,
    )
    resp.raise_for_status()
    return resp.content

def upload_excel(data: bytes) -> bool:
    try:
        file_id = CONFIG["excel_file_url"].split("/d/")[1].split("/")[0]
        creds = get_credentials(["https://www.googleapis.com/auth/drive"])
        creds.refresh(Request())
        resp = requests.patch(
            f"https://www.googleapis.com/upload/drive/v3/files/{file_id}?uploadType=media",
            headers={
                "Authorization": f"Bearer {creds.token}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
            data=data,
            timeout=60,
        )
        return resp.status_code == 200
    except Exception as e:
        log.error(f"Upload xətası: {e}")
        return False

# ─────────────────────────────────────────────
# EXCEL OXUMA
# ─────────────────────────────────────────────
def to_float(val) -> float:
    try:
        return float(str(val).replace(",", ".").replace(" ", "").strip())
    except:
        return 0.0

def load_products() -> list:
    products = []
    try:
        data = download_excel()
        wb = openpyxl.load_workbook(BytesIO(data), data_only=True)
        ws = wb.active
        log.info(f"📋 Vərəq: {ws.title}")

        for i, row in enumerate(ws.iter_rows(min_row=CONFIG["data_start_row"], values_only=True), CONFIG["data_start_row"]):
            row = list(row)
            while len(row) <= 15:
                row.append(None)

            barkod = str(row[COL["barkod"]]).strip() if row[COL["barkod"]] else ""
            mpn    = str(row[COL["mpn"]]).strip() if row[COL["mpn"]] else ""
            key    = barkod or mpn
            if not key:
                continue

            h_qiymet  = to_float(row[COL["qiymet"]])
            g_endrim  = to_float(row[COL["endirimli"]])
            min_p     = to_float(row[COL["min_qiymet"]])
            max_p     = to_float(row[COL["max_qiymet"]])
            url       = str(row[COL["url"]]).strip() if row[COL["url"]] else ""
            brend     = str(row[COL["brend"]]).strip() if row[COL["brend"]] else ""
            model     = str(row[COL["model"]]).strip() if row[COL["model"]] else ""
            name      = f"{brend} {model}".strip() or key

            current = h_qiymet if h_qiymet > 0 else g_endrim
            if current <= 0 or min_p <= 0:
                continue
            if max_p <= 0:
                max_p = round(min_p * 1.1, 2)

            products.append({
                "barkod":        key,
                "name":          name,
                "current_price": current,
                "min_price":     min_p,
                "max_price":     max_p,
                "sheet_row":     i,
                "url":           url,
            })

        log.info(f"📦 {len(products)} məhsul oxundu.")
    except Exception as e:
        log.error(f"Excel oxuma xətası: {e}")
    return products

# ─────────────────────────────────────────────
# EXCEL YAZMA
# ─────────────────────────────────────────────
def write_prices_batch(changes: list) -> bool:
    try:
        data = download_excel()
        wb = openpyxl.load_workbook(BytesIO(data))
        ws = wb.active

        for ch in changes:
            ws.cell(row=ch["row"], column=8, value=ch["price"])  # H sütunu

        out = BytesIO()
        wb.save(out)
        success = upload_excel(out.getvalue())
        if success:
            log.info(f"✅ {len(changes)} dəyişiklik Excel-ə yazıldı.")
        return success
    except Exception as e:
        log.error(f"Batch yazma xətası: {e}")
        return False

# ─────────────────────────────────────────────
# SCRAPER — Ana qiymət və satıcı adı
# ─────────────────────────────────────────────
def get_page_info(url: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Birmarket səhifəsindən:
      - Ana (ən aşağı) qiyməti
      - Ana satıcının adını
    qaytarır. Xəta olsa (None, None).
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None, None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Ana qiymət
        price_el = soup.find("span", attrs={"data-info": "item-desc-price-new"})
        if not price_el:
            # JSON-LD fallback
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    offers = data.get("offers", {})
                    if isinstance(offers, dict):
                        p = float(offers.get("price", 0))
                        if p > 0:
                            seller = offers.get("seller", {})
                            seller_name = seller.get("name", "") if isinstance(seller, dict) else ""
                            return p, seller_name.lower()
                except:
                    pass
            return None, None

        price_text = re.sub(r"[^\d.,\s]", "", price_el.get_text(strip=True))
        price_text = price_text.replace(",", ".").replace(" ", "")
        main_price = float(price_text)

        # Ana satıcı adı
        seller_el = soup.find(attrs={"data-info": "item-seller-name"})
        if not seller_el:
            seller_el = soup.find(attrs={"data-info": "item-main-seller-name"})
        if not seller_el:
            # Geniş axtarış — "Satıcı-şirkət" bölməsi
            seller_el = soup.find(class_=re.compile(r"seller.?name|store.?name", re.I))

        seller_name = seller_el.get_text(strip=True).lower() if seller_el else ""

        # Əgər satıcı adı tapılmadısa JSON-LD-dən cəhd et
        if not seller_name:
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    offers = data.get("offers", {})
                    if isinstance(offers, dict):
                        seller = offers.get("seller", {})
                        if isinstance(seller, dict) and seller.get("name"):
                            seller_name = seller["name"].lower()
                            break
                    elif isinstance(offers, list) and offers:
                        seller = offers[0].get("seller", {})
                        if isinstance(seller, dict) and seller.get("name"):
                            seller_name = seller["name"].lower()
                            break
                except:
                    pass

        return main_price, seller_name

    except Exception as e:
        log.warning(f"  Scrape xətası: {e}")
        return None, None

# ─────────────────────────────────────────────
# MƏHSUL İŞLƏMƏ
# ─────────────────────────────────────────────
def process_product(p: dict) -> dict:
    barkod  = p["barkod"]
    name    = p["name"]
    current = p["current_price"]
    min_p   = p["min_price"]
    max_p   = p["max_price"]
    row     = p["sheet_row"]
    url     = p["url"]

    log.info(f"🔍 {name} | {current:.2f}₼ | Min:{min_p:.2f} Max:{max_p:.2f}")

    # Min limit yoxlaması
    if current < min_p:
        log.info(f"  ⬆️  Min-dən aşağı — Min-ə qaldırılır: {min_p:.2f}₼")
        return {"status": "updated", "direction": "up", "name": name,
                "old": current, "new": min_p, "row": row, "barkod": barkod}

    if not url or not url.startswith("http"):
        log.warning(f"  ⚠️  URL yoxdur — keçilir")
        return {"status": "error"}

    main_price, seller_name = get_page_info(url)

    if main_price is None:
        log.warning(f"  ⚠️  Scraping xətası — qiymət saxlanılır")
        return {"status": "error"}

    log.info(f"  🌐 Birmarket: {main_price:.2f}₼ | Satıcı: '{seller_name}'")

    is_ours = "unistore" in seller_name

    if is_ours:
        # Ən aşağı qiymət bizdədir → heç nə etmə
        log.info(f"  ✅ Ən aşağı qiymət bizdədir — saxlanılır: {current:.2f}₼")
        return {"status": "best_price"}
    else:
        # Başqa satıcı ucuzdur → ondan 0.01 aşağı qoy
        target = round(main_price - CONFIG["price_undercut"], 2)
        if target < min_p:
            target = min_p
            log.info(f"  ⚠️  Min limitə çatıldı → {min_p:.2f}₼")
        if target > max_p:
            target = max_p
        if abs(target - current) < 0.01:
            log.info(f"  ✅ Qiymət artıq düzgündür")
            return {"status": "best_price"}
        log.info(f"  📉 Rəqib: {main_price:.2f}₼ → Bizim: {target:.2f}₼")
        return {"status": "updated", "direction": "down", "name": name,
                "old": current, "new": target, "row": row, "barkod": barkod}

# ─────────────────────────────────────────────
# ƏSAS YOXLAMA
# ─────────────────────────────────────────────
def run_check():
    log.info("=" * 55)
    log.info(f"🚀 Yoxlama — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    products = load_products()
    if not products:
        log.error("❌ Məhsul siyahısı boşdur!")
        return

    stats = {"updated_down": 0, "updated_up": 0, "best_price": 0, "error": 0}
    changes = []
    updated_results = []

    with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as executor:
        futures = {executor.submit(process_product, p): p for p in products}
        for future in as_completed(futures):
            try:
                result = future.result()
                status = result.get("status", "error")
                if status == "updated":
                    changes.append({"row": result["row"], "price": result["new"]})
                    updated_results.append(result)
                    if result["direction"] == "down":
                        stats["updated_down"] += 1
                    else:
                        stats["updated_up"] += 1
                elif status == "best_price":
                    stats["best_price"] += 1
                else:
                    stats["error"] += 1
            except Exception as e:
                log.error(f"Thread xətası: {e}")
                stats["error"] += 1

    if changes:
        log.info(f"\n💾 {len(changes)} dəyişiklik Excel-ə yazılır...")
        write_prices_batch(changes)

    total = stats["updated_down"] + stats["updated_up"]
    log.info(f"\n✅ Tamamlandı. {total} məhsul yeniləndi.\n")

    report = (
        f"📊 <b>Yoxlama Hesabatı</b>\n"
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📦 Ümumi məhsul: <b>{len(products)}</b>\n"
        f"📉 Qiymət endirildi: <b>{stats['updated_down']}</b>\n"
        f"📈 Qiymət artırıldı: <b>{stats['updated_up']}</b>\n"
        f"✅ Düzgün qiymət: <b>{stats['best_price']}</b>\n"
        f"❌ Xəta: <b>{stats['error']}</b>"
    )
    send_telegram(report)

# ─────────────────────────────────────────────
# ƏSAS PROQRAM
# ─────────────────────────────────────────────
if __name__ == "__main__":
    log.info("🤖 Birmarket Bot işə salındı")
    log.info(f"⏱️  Yoxlama: hər {CONFIG['check_interval_minutes']} dəq")

    run_check()
    schedule.every(CONFIG["check_interval_minutes"]).minutes.do(run_check)

    log.info("🔄 Bot aktiv\n")
    while True:
        schedule.run_pending()
        time.sleep(30)
