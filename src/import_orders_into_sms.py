import os
import requests
import json
import time
import pyodbc
import configparser
import logging
from datetime import datetime
import sys
import ctypes
from requests.exceptions import HTTPError, Timeout, RequestException
from http_errors import HTTP_ERROR_MESSAGES

# --- UI ---
import threading
from queue import Queue

UI_ENABLED = True  # False si tu veux silent mode (ex: lancé par SQI sans UI)
ui = None
ui_queue = Queue()

if UI_ENABLED:
    from ui_status import StatusUI


def get_config_path():
    script_dir = (
        os.path.dirname(sys.executable)
        if getattr(sys, "frozen", False)
        else os.path.dirname(os.path.abspath(__file__))
    )
    return os.path.join(script_dir, "config.ini")


def read_debugscreen(config_path: str) -> bool:
    cfg = configparser.ConfigParser()
    cfg.read(config_path)
    return cfg.getint("Settings", "DebugScreen", fallback=0) == 1


def ensure_console():
    """
    Crée une console Windows (utile quand l'EXE est compilé en --windowed).
    """
    if os.name != "nt":
        return

    kernel32 = ctypes.windll.kernel32

    if kernel32.GetConsoleWindow():
        return

    if kernel32.AllocConsole() == 0:
        return

    sys.stdout = open("CONOUT$", "w", buffering=1, encoding="utf-8", errors="replace")
    sys.stderr = open("CONOUT$", "w", buffering=1, encoding="utf-8", errors="replace")
    sys.stdin  = open("CONIN$", "r", encoding="utf-8", errors="replace")


def status(msg, detail=""):
    logging.info(msg + (f" | {detail}" if detail else ""))
    if UI_ENABLED:
        ui_queue.put(("INFO", msg, detail))


def ui_error(msg, detail=""):
    logging.error(msg + (f" | {detail}" if detail else ""))
    if UI_ENABLED:
        ui_queue.put(("ERROR", msg, detail))


def ui_warn(msg, detail=""):
    logging.warning(msg + (f" | {detail}" if detail else ""))
    if UI_ENABLED:
        ui_queue.put(("WARN", msg, detail))


def explain_http_exception(exc: Exception, context: str = ""):
    """
    Convert requests HTTP/timeout errors into user-friendly UI messages using HTTP_ERROR_MESSAGES.
    Returns: (title, detail)
    """
    prefix = (context + " | ") if context else ""

    # Timeout
    if isinstance(exc, Timeout):
        return ("Request timeout", prefix + "The API did not respond in time. Try again or check network/VPN/firewall.")

    # HTTP status code
    if isinstance(exc, HTTPError):
        resp = getattr(exc, "response", None)
        code = getattr(resp, "status_code", None)

        # Log the body for troubleshooting (safe truncate)
        try:
            if resp is not None:
                body = (resp.text or "")[:2000]
                logging.error(f"{prefix}HTTP {code} response body (first 2000 chars): {body}")
        except Exception:
            pass

        if code in HTTP_ERROR_MESSAGES:
            t = HTTP_ERROR_MESSAGES[code]["title"]
            d = HTTP_ERROR_MESSAGES[code]["detail"]
            return (t, prefix + d)

        return (f"HTTP Error {code}", prefix + str(exc))

    # Other request errors (DNS, TLS, connection refused, proxy, etc.)
    if isinstance(exc, RequestException):
        return ("Network error", prefix + str(exc))

    # fallback
    return ("Error", prefix + str(exc))


def request_json(method: str, url: str, *, headers=None, json_body=None, timeout=90, context=""):
    """
    Unified request helper that:
      - runs requests.<method>
      - raise_for_status
      - returns response.json()
      - on error: maps to friendly message (http_errors.py) + ui_error + re-raises
    """
    try:
        m = method.strip().lower()
        if m == "get":
            resp = requests.get(url, headers=headers, timeout=timeout)
        elif m == "post":
            resp = requests.post(url, headers=headers, json=json_body, timeout=timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        resp.raise_for_status()

        # JSON parse
        try:
            return resp.json()
        except Exception as je:
            # Sometimes API returns HTML/text on error pages
            body = (resp.text or "")[:2000]
            logging.error(f"{context} | JSON parse failed. Body (first 2000 chars): {body}")
            ui_error("Invalid API response", f"{context} | Response is not valid JSON.")
            raise je

    except Exception as e:
        title, detail = explain_http_exception(e, context)
        ui_error(title, detail)
        raise


# --------------------------
# Base directory
# --------------------------
base_dir = (
    os.path.dirname(sys.executable)
    if getattr(sys, "frozen", False)
    else os.path.dirname(os.path.abspath(__file__))
)

# --------------------------
# Config path + DebugScreen (DOIT être avant logging.basicConfig)
# --------------------------
config_path = get_config_path()
debug_console = read_debugscreen(config_path)

if debug_console:
    ensure_console()

# --------------------------
# Logging setup
# --------------------------
log_ts = datetime.now().strftime("%Y-%m-%d")
log_filename = f"ImportOrdersIntoSMS_logs_{log_ts}.log"

log_dir = os.path.join(base_dir, "Log")
os.makedirs(log_dir, exist_ok=True)

log_path = os.path.join(log_dir, log_filename)

logging.basicConfig(
    filename=log_path,
    filemode="a",
    level=logging.INFO,
    format="[%(asctime)s]: %(message)s",
    datefmt="%H:%M:%S",
)

if debug_console:
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("[%(asctime)s]: %(message)s", datefmt="%H:%M:%S"))
    logging.getLogger().addHandler(console)

logging.info("=== Start run ===")

# --------------------------
# Config loading
# --------------------------
config = configparser.ConfigParser()

script_dir = (
    os.path.dirname(sys.executable)
    if getattr(sys, "frozen", False)
    else os.path.dirname(os.path.abspath(__file__))
)

config_path = os.path.join(script_dir, "config.ini")
logging.info(f"Loading config from: {config_path}")

if not os.path.exists(config_path):
    raise FileNotFoundError(f"config.ini not found at: {config_path}")

config.read(config_path)

server_name = config["Settings"]["ServerName"]
database_name = config["Settings"]["DatabaseName"]
sql_driver = config["Settings"]["SQLDriver"]
store_number = int(config["Settings"]["StoreNumber"])

base_url = config["ImportOrders"]["BaseUrl"].rstrip("/")
api_username = config["ImportOrders"]["Username"]
api_password = config["ImportOrders"]["Password"]


def _get_sql_connection():
    connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes"
    status("Connecting to SQL Server...", f"{server_name} / {database_name}")
    conn = pyodbc.connect(connection_string)
    status("SQL connection established.")
    return conn


def open_and_validate_database_connection():
    status("Validating database connectivity...")
    conn = _get_sql_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    cur.close()
    status("Database connectivity validated.")
    return conn


def safe_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(v)
    except Exception:
        return default


def safe_str(v):
    if v is None:
        return ""
    return str(v).strip()


def get_job_id(auth_token):
    url = f"{base_url}/export/orders"
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
    }
    payload = {"approved_flag": True, "store_number": [store_number]}

    status("Creating export job...", "Upshop /export/orders")
    resp_json = request_json("post", url, headers=headers, json_body=payload, timeout=90, context="Upshop /export/orders")

    job_id = resp_json.get("job_id")
    status("Job created.", f"job_id={job_id}")
    logging.info(f"Job creation API response: {json.dumps(resp_json)}")
    return job_id


def check_job_status(auth_token, job_id):
    url = f"{base_url}/job_status/{job_id}"
    headers = {"Authorization": f"Bearer {auth_token}"}

    return request_json("get", url, headers=headers, timeout=90, context=f"Upshop /job_status/{job_id}")


def wait_for_job_completion(auth_token, job_id, poll_interval_seconds=5, timeout_seconds=1800):
    terminal_success = {"finished"}
    terminal_failure = {"failed", "error", "cancelled", "canceled"}

    start = time.time()
    last_status = None

    status("Waiting for job completion...", f"job_id={job_id}")

    while True:
        status_payload = check_job_status(auth_token, job_id)

        status_raw = status_payload.get("status") or status_payload.get("state")
        status_val = (status_raw or "").strip().lower()
        message = status_payload.get("message")

        if status_val != last_status:
            status("Job status changed", f"{last_status} -> {status_val} ({message})")
            last_status = status_val

        if status_val in terminal_success:
            status("Job completed.", message or "")
            return status_payload

        if status_val in terminal_failure:
            logging.error(f"Final job status payload: {json.dumps(status_payload)}")
            ui_error("Upshop job failed", f"status={status_raw} | message={message}")
            raise RuntimeError(f"Job failed with status={status_raw}. message={message}")

        elapsed = time.time() - start
        if elapsed > timeout_seconds:
            ui_error("Upshop job timeout", f"Last status={status_raw} | waited={timeout_seconds}s")
            raise TimeoutError(f"Job did not finish within {timeout_seconds}s. Last status={status_raw}")

        time.sleep(poll_interval_seconds)


def get_vendor_name_cached(conn, vendor_number, vendor_cache):
    key = safe_str(vendor_number)

    if key in vendor_cache:
        return vendor_cache[key]

    try:
        cur = conn.cursor()
        cur.execute("SELECT F334 FROM VENDOR_TAB WHERE F27 = ?", str(vendor_number))
        row = cur.fetchone()
        cur.close()

        vendor_name = str(row[0]).strip() if row and row[0] is not None else ""
        vendor_cache[key] = vendor_name
        return vendor_name
    except Exception as e:
        logging.exception(f"Vendor lookup failed for vendor_number={vendor_number}: {e}")
        vendor_cache[key] = ""
        ui_warn("Vendor lookup failed", f"vendor={vendor_number} | {e}")
        return ""


def send_rechdr(conn, job_data_entry, vendor_cache):
    cursor = conn.cursor()

    case_order_number = job_data_entry.get("case_order_number")
    effective_date = job_data_entry.get("effective_date")
    store_number_local = job_data_entry.get("store_number")
    approval_date = job_data_entry.get("approval_date")
    vendor_number = job_data_entry.get("vendor_number")

    vendor_name = get_vendor_name_cached(conn, vendor_number, vendor_cache)
    sms_order_number = str(case_order_number)

    query = """
        INSERT INTO [dbo].[TMP_REC_BAT] (
            [F1032], [F27], [F76], [F91], [F253], [F254], [F334], [F352], [F1035], [F1036],
            [F1056], [F1057], [F1067], [F1068], [F1101], [F1126], [F1127], [F1246], [F1653]
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """

    store_number_string = "00" + str(store_number_local)

    values = (
        sms_order_number,
        vendor_number,
        approval_date,
        case_order_number,
        approval_date,
        effective_date,
        vendor_name,
        88454,
        121609,
        121609,
        store_number_string,
        "901",
        "OPEN",
        "ORDER",
        1,
        757,
        "Upshop Order",
        effective_date,
        effective_date,
    )

    cursor.execute(query, values)
    conn.commit()
    rows_affected = cursor.rowcount
    cursor.close()
    return rows_affected


def send_recdtl(conn, job_data_entry, line_num):
    cursor = conn.cursor()

    case_order_number = safe_int(job_data_entry.get("case_order_number"))
    department_number = safe_int(job_data_entry.get("department_number"))
    sku = safe_str(job_data_entry.get("sku"))
    description = safe_str(job_data_entry.get("description"))
    order_quantity = safe_int(job_data_entry.get("order_quantity"), 0)
    approval_date = job_data_entry.get("approval_date")

    if not sku:
        raise ValueError(f"SKU is empty (PO={case_order_number}, line={line_num})")

    insert_query = """
    INSERT INTO [dbo].[TMP_REC_DTL] (
        [F1032], [F1101], [F01], [F03], [F1003], [F1041], [F1063], [F1067], [F1184], [F1887], [F75], [F76]
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """

    insert_values = (
        case_order_number,
        safe_int(line_num),
        sku,
        department_number,
        float(order_quantity),
        description,
        3510,
        "ITEM",
        "CASE",
        "C",
        float(order_quantity),
        approval_date,
    )

    cursor.execute(insert_query, insert_values)
    conn.commit()
    rows_affected = cursor.rowcount
    cursor.close()
    return rows_affected


def run_import():
    totals = {
        "hdr_inserts": 0,
        "dtl_inserts": 0,
        "items_seen": 0,
        "hdr_skipped": 0,
        "dtl_skipped": 0,
    }

    conn = None
    vendor_cache = {}

    try:
        status("Opening database connection...")
        conn = open_and_validate_database_connection()

        # API: Login
        status("Connecting to Upshop API...", "Requesting auth token")
        urlt = f"{base_url}/login"
        payloadt = {"username": api_username, "password": api_password}
        headerst = {"Content-Type": "application/json"}

        response_data = request_json(
            "post",
            urlt,
            headers=headerst,
            json_body=payloadt,
            timeout=90,
            context="Upshop /login"
        )

        auth_token = response_data.get("access_token")
        if not auth_token:
            ui_error("Auth token missing", "Upshop /login response has no access_token")
            raise RuntimeError("Auth token missing in response.")

        status("Auth token retrieved.")

        # API: Create job + poll
        job_id = get_job_id(auth_token)
        job_status = wait_for_job_completion(auth_token, job_id)

        data_items = job_status.get("data", [])
        status("Download complete.", f"{len(data_items)} item(s)")

        if not data_items:
            totals["items_seen"] = 0
            status("No approved orders found.", "0 order / 0 item.")
            return totals

        # Insert item in TMP tables
        status("Inserting into SMS TMP tables...")
        seen_headers = set()
        line_number = 1

        for item in data_items:
            totals["items_seen"] += 1

            sku = item.get("sku")
            po = item.get("case_order_number")
            vendor_case_key = f"{item.get('vendor_number')}{po}"

            status("Importing item...", f"{line_number}/{len(data_items)} | PO={po} | SKU={sku}")

            if vendor_case_key not in seen_headers:
                try:
                    inserted = send_rechdr(conn, item, vendor_cache)
                    totals["hdr_inserts"] += inserted if inserted else 0
                    seen_headers.add(vendor_case_key)
                except Exception as e:
                    totals["hdr_skipped"] += 1
                    logging.exception(f"Skipped TMP_REC_BAT for sku={sku}: {e}")
                    ui_error("Skipped TMP_REC_BAT", f"PO={po} | SKU={sku} | {e}")

            try:
                inserted = send_recdtl(conn, item, line_number)
                totals["dtl_inserts"] += inserted if inserted else 0
            except Exception as e:
                totals["dtl_skipped"] += 1
                logging.exception(f"Skipped TMP_REC_DTL for sku={sku}: {e}")
                ui_error("Skipped TMP_REC_DTL", f"PO={po} | line={line_number} | SKU={sku} | {e}")

            line_number += 1

        if totals["hdr_skipped"] or totals["dtl_skipped"]:
            ui_warn(
                "Import finished with skipped rows",
                f"hdr_skipped={totals['hdr_skipped']} | dtl_skipped={totals['dtl_skipped']}"
            )

        status("Import completed.", f"PO(s)={len(seen_headers)} | Items={totals['items_seen']}")
        return totals

    finally:
        if conn is not None:
            try:
                conn.close()
                status("SQL connection closed.")
            except Exception:
                logging.exception("Error closing SQL connection.")

        logging.info(
            "Run summary: "
            f"items_seen={totals['items_seen']}, "
            f"hdr_inserts={totals['hdr_inserts']}, hdr_skipped={totals['hdr_skipped']}, "
            f"dtl_inserts={totals['dtl_inserts']}, dtl_skipped={totals['dtl_skipped']}"
        )

        orders_imported = totals["hdr_inserts"]
        if orders_imported > 0:
            status(
                f"{orders_imported} order{'s' if orders_imported > 1 else ''} were imported",
                "You can close this window"
            )
        else:
            status("No orders were imported", "You can close this window")

        logging.info("=== End run ===")


def main():
    if not UI_ENABLED:
        run_import()
        return

    global ui
    ui = StatusUI(title="Upshop Import", queue=ui_queue)

    def worker():
        try:
            totals = run_import()

            orders_imported = totals.get("hdr_inserts", 0)
            items_seen = totals.get("items_seen", 0)

            if items_seen == 0:
                title = "No approved orders"
                detail = "0 order / 0 item. You can close this window."
            elif orders_imported == 0:
                title = "No orders imported"
                detail = f"{items_seen} item(s) downloaded but 0 order imported. You can close this window."
            else:
                title = "Done"
                detail = f"{orders_imported} order(s) were imported. You can close this window."

            ui.root.after(0, ui.done, title, detail)

        except Exception as e:
            logging.exception(f"Import failed: {e}")
            ui.root.after(0, ui.error, "Import failed", str(e))

    threading.Thread(target=worker, daemon=True).start()
    ui.run()


if __name__ == "__main__":
    main()
