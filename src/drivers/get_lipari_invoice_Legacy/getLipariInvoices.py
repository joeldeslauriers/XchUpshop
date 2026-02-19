import csv
import json
import os
import requests
import pyodbc
import shutil
import sys
import logging
from datetime import datetime

# =============================================================================
# Logging (writes in script/exe root)
# =============================================================================
def _is_frozen() -> bool:
    return getattr(sys, "frozen", False)

def _base_dir() -> str:
    return os.path.dirname(sys.executable) if _is_frozen() else os.path.dirname(os.path.abspath(__file__))

BASE_DIR = _base_dir()
LOG_FILE = os.path.join(BASE_DIR, f"GetLipariInvoice_{datetime.now().strftime('%Y_%m_%d')}.log")

def setup_logging() -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S")

    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    root.addHandler(fh)
    root.addHandler(ch)

def log_info(msg: str) -> None:
    logging.info(msg)

def log_warn(msg: str) -> None:
    logging.warning(msg)

def log_error(msg: str) -> None:
    logging.error(msg)

setup_logging()
log_info("=== Start run ===")
log_info(f"Frozen={_is_frozen()} | BASE_DIR={BASE_DIR}")
log_info(f"LOG_FILE={LOG_FILE}")

# =============================================================================
# Config
# =============================================================================
INPUT_DIR = r"\\10.0.130.18\c$\Vendor Files\Lipari\Invoices"
OUTPUT_DIR = r"\\10.0.130.18\c$\Vendor Files\Lipari\Invoices\Archive"

AUTH_URL = "https://plummarket-larry.invafresh.com/v1/login"
POST_URL = "https://plummarket-larry.invafresh.com/v1/backroom_products/receipt_transactions"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)
log_info(f"INPUT_DIR={INPUT_DIR}")
log_info(f"OUTPUT_DIR={OUTPUT_DIR}")

# =============================================================================
# Authenticate to get token
# =============================================================================
try:
    auth_payload = {
        "username": "wsuser2",
        "password": "dqgX69cvRhHNmnHRhJy6"
    }
    log_info("Invafresh: login...")
    auth_response = requests.post(AUTH_URL, json=auth_payload, timeout=60)
    auth_response.raise_for_status()
    token = auth_response.json().get("access_token")
    if not token:
        raise ValueError("Token not found in auth response.")
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    log_info("Invafresh: login OK (token acquired)")
except Exception as e:
    log_error(f"Authentication failed: {type(e).__name__}: {e}")
    log_info("=== End run (fatal) ===")
    sys.exit(1)

# =============================================================================
# SQL connection setup
# =============================================================================
try:
    log_info("SQL: connecting to WBL-SMS/STORESQL ...")
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=WBL-SMS;DATABASE=STORESQL;Trusted_Connection=yes'
    )
    cursor = conn.cursor()
    log_info("SQL: connection OK")
except Exception as e:
    log_error(f"Database connection failed: {type(e).__name__}: {e}")
    log_info("=== End run (fatal) ===")
    sys.exit(1)

# =============================================================================
# Function to get department number from SKU
# =============================================================================
def get_department_number(sku):
    try:
        cursor.execute("""
            SELECT DPT.F03
            FROM dbo.POS_TAB POS
            JOIN dbo.SDP_TAB DPT ON POS.F04 = DPT.F04
            WHERE POS.F01 = ?
        """, sku)
        result = cursor.fetchone()
        return int(result[0]) if result else 0  # fallback to 0 if not found
    except Exception as e:
        log_warn(f"Error retrieving department number for SKU {sku}: {type(e).__name__}: {e}")
        return 0

# =============================================================================
# Process CSV files
# =============================================================================
processed = 0
posted = 0
failed = 0
skipped_files = 0

try:
    files = os.listdir(INPUT_DIR)
except Exception as e:
    log_error(f"Cannot list INPUT_DIR: {type(e).__name__}: {e}")
    log_info("=== End run (fatal) ===")
    sys.exit(1)

log_info(f"Scanning folder: {INPUT_DIR} | files={len(files)}")

for filename in files:
    if not filename.lower().endswith(".csv") or "10950" not in filename:
        skipped_files += 1
        continue

    processed += 1
    input_csv_path = os.path.join(INPUT_DIR, filename)
    output_json_path = os.path.join(OUTPUT_DIR, os.path.splitext(filename)[0] + ".json")

    log_info(f"Processing file={filename}")

    transactions = []
    line_total = 0
    skipped_empty_item = 0
    skipped_sku_not_found = 0
    dept0_count = 0

    try:
        with open(input_csv_path, newline='', encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                line_total += 1

                lipari_item = (row.get("LipariItem") or "").strip()
                if not lipari_item:
                    skipped_empty_item += 1
                    continue

                cursor.execute("SELECT F01 FROM dbo.COST_TAB WHERE F26 = ?", lipari_item)
                result = cursor.fetchone()
                if not result:
                    skipped_sku_not_found += 1
                    log_warn(f"SKU not found for LipariItem={lipari_item} | file={filename} | line={line_total}")
                    continue

                sku = result[0]
                department_number = get_department_number(sku)
                if department_number == 0:
                    dept0_count += 1

                store_number = 0
                try:
                    location_id = int((row.get("StoreNumber") or "0"))
                except Exception:
                    location_id = 0

                if 12557001 <= location_id <= 12557999:
                    store_number = 3
                elif 13431001 <= location_id <= 13431999:
                    store_number = 1

                po_num = (row.get("PONumber") or "").strip()
                inv_date = (row.get("InvoiceDate") or "").strip()

                # Keep original behavior (minimal changes): quantity and date as-is
                try:
                    amount_cents = int(float((row.get("Price") or "0")) * 100)
                except Exception:
                    amount_cents = 0

                try:
                    order_number = int(po_num) // 100000
                except Exception:
                    order_number = 0

                transaction = {
                    "invoice_number": po_num,
                    "order_number": order_number,
                    "external_order_number": po_num,
                    "delivery_or_invoice_date": inv_date,
                    "store_number": store_number,
                    "department_number": department_number,
                    "sku": sku,
                    "quantity": row.get("Quantity"),
                    "quantity_unit_of_measure": "cs",
                    "amount": amount_cents
                }

                transactions.append(transaction)

    except Exception as e:
        failed += 1
        log_error(f"Failed reading/parsing file={filename}: {type(e).__name__}: {e}")
        continue

    log_info(
        f"File summary file={filename} lines={line_total} tx={len(transactions)} "
        f"skipped_empty_item={skipped_empty_item} skipped_sku_not_found={skipped_sku_not_found} dept0={dept0_count}"
    )

    if not transactions:
        log_warn(f"No transactions built for file={filename}. Skipping POST.")
        continue

    output_data = {"receipt_transactions": transactions}

    # Write JSON file locally
    try:
        with open(output_json_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=4)
        log_info(f"JSON saved: {output_json_path} | tx={len(transactions)}")
    except Exception as e:
        log_warn(f"Could not write JSON for file={filename}: {type(e).__name__}: {e}")

    # Post to API
    try:
        log_info(f"POST Invafresh: file={filename} tx={len(transactions)}")
        response = requests.post(POST_URL, headers=headers, json=output_data, timeout=90)
        response.raise_for_status()
        posted += 1
        log_info(f"POST successful file={filename} status={response.status_code} body={response.text[:300]}")

        # Move the CSV to the archive folder
        archived_csv_path = os.path.join(OUTPUT_DIR, filename)
        shutil.move(input_csv_path, archived_csv_path)
        log_info(f"Moved file to archive: {archived_csv_path}")

    except requests.exceptions.RequestException as e:
        failed += 1
        status = getattr(getattr(e, "response", None), "status_code", None)
        body = getattr(getattr(e, "response", None), "text", "")
        log_error(f"POST failed file={filename} status={status} error={e} body={body[:500]}")

# Cleanup
try:
    cursor.close()
except Exception:
    pass
try:
    conn.close()
except Exception:
    pass

log_info(f"Done. processed_files={processed} posted={posted} failed={failed} skipped_files={skipped_files}")
log_info("=== End run ===")
