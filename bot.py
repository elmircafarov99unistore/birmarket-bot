"""
Birmarket.az Qiymət İzləmə Botu
"""

import json, time, re, schedule, logging, os, requests
from datetime import datetime
from typing import Optional
from io import BytesIO
import openpyxl
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request

EXCEL_FILE_URL         = os.environ.get("EXCEL_FILE_URL", "")
TELEGRAM_BOT_TOKEN     = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID       = os.environ.get("TELEGRAM_CHAT_ID", "")
CHECK_INTERVAL_MINUTES = 10
PRICE_UNDERCUT         = 0.01
MAX_WORKERS            = 5
DATA_START_ROW         = 2

COL_BARKOD=0; COL_MPN=1; COL_MODEL=2; COL_BREND=3
COL_ENDIRIMLI=6; COL_QIYMET=7; COL_URL=13; COL_MIN=14; COL_MAX=15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("birmarket_bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram xətası: {e}")


def get_file_id():
    return EXCEL_FILE_URL.split("/d/")[1].split("/")[0]

def download_excel() -> bytes:
    resp = requests.get(
        f"https://docs.google.com/spreadsheets/d/{get_file_id()}/export?format=xlsx",
        timeout=30,
    )
    resp.raise_for_status()
    return resp.content

def upload_excel(data: bytes) -> bool:
    try:
        info  = json.loads(os.environ.get("GOOGLE_CREDENTIALS", "{}"))
        creds = Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/drive"]
        )
        creds.refresh(Request())
        resp = requests.patch(
            f"https://www.googleapis.com/upload/drive/v3/files/{get_file_id()}?uploadType=media",
            headers={
                "Authorization": f"Bearer {creds.token}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
            data=data, timeout=60,
        )
        return resp.status_code == 200
    except Exception as e:
        log.error(f"Upload xətası: {e}")
        return False


def to_float(val) -> float:
    try:
        return float(str(val).replace(",", ".").replace(" ", "").strip())
    except:
        return 0.0

def load_products() -> list:
    products = []
    try:
        raw = download_excel()
        wb  = openpyxl.load_workbook(BytesIO(raw), data_only=True)
        ws  = wb.active
        log.info(f"📋 Vərəq: '{ws.title}'")
        for i, row in enumerate(ws.iter_rows(min_row=DATA_START_ROW, values_only=True), DATA_START_ROW):
            row = list(row)
            while len(row) <= COL_MAX:
                row.append(None)
            barkod  = str(row[COL_BARKOD]).strip() if row[COL_BARKOD] else ""
            mpn     = str(row[COL_MPN]).strip()    if row[COL_MPN]    else ""
            key     = barkod or mpn
            if not key or key == "None":
                continue
            h_val   = to_float(row[COL_QIYMET])
            g_val   = to_float(row[COL_ENDIRIMLI])
            min_p   = to_float(row[COL_MIN])
            max_p   = to_float(row[COL_MAX])
            url     = str(row[COL_URL]).strip() if row[COL_URL] else ""
            name    = f"{row[COL_BREND] or ''} {row[COL_MODEL] or ''}".strip() or key
            current = h_val if h_val > 0 else g_val
            if current <= 0 or min_p <= 0:
                continue
            if max_p <= 0:
                max_p = round(min_p * 1.1, 2)
            products.append({
                "key": key, "name": name, "current": current,
                "min_p": min_p, "max_p": max_p, "row": i, "url": url,
            })
        log.info(f"📦 {len(products)} məhsul oxundu.")
    except Exception as e:
        log.error(f"Excel oxuma xətası: {e}")
    return products

def write_prices_batch(changes: list) -> bool:
    try:
        raw = download_excel()
        wb  = openpyxl.load_workbook(BytesIO(raw))
        ws  = wb.active
        for ch in changes:
            ws.cell(row=ch["row"], column=8, value=ch["price"])
        out = BytesIO()
        wb.save(out)
        ok = upload_excel(out.getvalue())
        if ok:
            log.info(f"✅ {len(changes)} dəyişiklik Excel-ə yazıldı.")
        return ok
    except Exception as e:
        log.error(f"Batch yazma xətası: {e}")
        return False


def scrape_min_price(url: str) -> Optional[float]:
    """
    Hər thread öz brauzerlini açır.
    Playwright sync_api thread-safe deyil —
    buna görə hər çağırışda yeni playwright/brauzer context istifadə edilir.
    """
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
            )
            page = browser.new_page(user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ))

            page.goto(url, wait_until="domcontentloaded", timeout=25000)

            try:
                page.wait_for_selector('[data-info="item-desc-price-new"]', timeout=8000)
            except:
                pass

            prices = []

            # Ana qiymət
            el = page.query_selector('[data-info="item-desc-price-new"]')
            if el:
                raw = re.sub(r"[^\d.]", "", el.inner_text().replace(",", "."))
                if raw:
                    try:
                        prices.append(float(raw))
                    except:
                        pass

            # Digər satıcılar
            for block in page.query_selector_all('[data-info="item-other-seller-list"]'):
                try:
                    pel = block.query_selector('[data-info="item-desc-price-new"]')
                    if pel:
                        raw = re.sub(r"[^\d.]", "", pel.inner_text().replace(",", "."))
                        if raw:
                            prices.append(float(raw))
                except:
                    pass

            browser.close()

            if not prices:
                log.warning(f"  ⚠️ Qiymət tapılmadı")
                return None

            min_p = round(min(prices), 2)
            log.info(f"  🌐 {sorted(round(x,2) for x in prices)} → min:{min_p}")
            return min_p

    except Exception as e:
        log.warning(f"  Scrape xətası [{type(e).__name__}]: {e}")
        return None


def process_product(p: dict) -> dict:
    key     = p["key"]
    name    = p["name"]
    current = p["current"]
    min_p   = p["min_p"]
    max_p   = p["max_p"]
    row     = p["row"]
    url     = p["url"]

    log.info(f"🔍 {name} | Cari:{current:.2f}₼ | Min:{min_p:.2f} Max:{max_p:.2f}")

    if not url or not url.startswith("http"):
        log.warning(f"  ⚠️ URL yoxdur")
        return {"status": "error"}

    site_price = scrape_min_price(url)

    if site_price is None:
        return {"status": "error"}

    if site_price >= current:
        log.info(f"  ✅ Biz ən ucuzuq — dəyişmir")
        return {"status": "best_price"}

    target = round(site_price - PRICE_UNDERCUT, 2)
    log.info(f"  ❌ Rəqib: {site_price:.2f}₼ → hədəf: {target:.2f}₼")

    if target < min_p:
        target = min_p
        log.info(f"  ⚠️ Min limitə çatıldı → {min_p:.2f}₼")
    if target > max_p:
        target = max_p

    target = round(target, 2)

    if abs(target - current) < 0.005:
        log.info(f"  ✅ Fərq çox kiçikdir — dəyişmir")
        return {"status": "best_price"}

    direction = "up" if target > current else "down"
    log.info(f"  💰 {current:.2f}₼ → {target:.2f}₼")
    return {"status": "updated", "direction": direction,
            "name": name, "old": current, "new": target, "row": row, "key": key}


def run_check():
    log.info("=" * 55)
    log.info(f"🚀 Yoxlama — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)

    products = load_products()
    if not products:
        log.error("❌ Məhsul siyahısı boşdur!")
        return

    stats   = {"down": 0, "up": 0, "best": 0, "error": 0}
    changes = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_product, p): p for p in products}
        for future in as_completed(futures):
            try:
                result = future.result()
                status = result.get("status", "error")
                if status == "updated":
                    changes.append({"row": result["row"], "price": result["new"]})
                    stats["down" if result["direction"] == "down" else "up"] += 1
                elif status == "best_price":
                    stats["best"] += 1
                else:
                    stats["error"] += 1
            except Exception as e:
                log.error(f"Thread xətası: {e}")
                stats["error"] += 1

    if changes:
        log.info(f"\n💾 {len(changes)} dəyişiklik yazılır...")
        write_prices_batch(changes)

    total = stats["down"] + stats["up"]
    log.info(f"\n✅ Tamamlandı — {total} məhsul yeniləndi.\n")

    report = (
        f"📊 <b>Yoxlama Hesabatı</b>\n"
        f"🕐 {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📦 Ümumi məhsul: <b>{len(products)}</b>\n"
        f"📉 Qiymət endirildi: <b>{stats['down']}</b>\n"
        f"📈 Qiymət artırıldı: <b>{stats['up']}</b>\n"
        f"✅ Düzgün qiymət: <b>{stats['best']}</b>\n"
        f"❌ Xəta/keçildi: <b>{stats['error']}</b>"
    )
    send_telegram(report)


if __name__ == "__main__":
    log.info("🤖 Birmarket Bot işə salındı")
    log.info(f"⏱️  Hər {CHECK_INTERVAL_MINUTES} dəqiqədə bir yoxlanır")
    run_check()
    schedule.every(CHECK_INTERVAL_MINUTES).minutes.do(run_check)
    while True:
        schedule.run_pending()
        time.sleep(30)
