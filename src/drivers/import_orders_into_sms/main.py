# main.py (import_orders_into_sms)

import os
import sys
import time
import json
import ctypes
import logging
import threading
import configparser
from queue import Queue
from datetime import datetime, timedelta
from typing import Optional, Dict, Set, Tuple, List

import requests
import pyodbc
from requests.exceptions import HTTPError, Timeout, RequestException

# =============================================================================
# IMPORTANT: shared is a PACKAGE now:
#   src/shared/__init__.py
#   src/shared/http_errors.py
# =============================================================================
from shared.http_errors import HTTP_ERROR_MESSAGES

# =============================================================================
# BASE DIR
# =============================================================================

IS_FROZEN = getattr(sys, "frozen", False)
BASE_DIR = os.path.dirname(sys.executable) if IS_FROZEN else os.path.dirname(os.path.abspath(__file__))

# =============================================================================
# CONFIG / FLAGS
# =============================================================================

UI_ENABLED = True
AUTO_CLOSE_SECONDS = 20

if UI_ENABLED:
    from ui_status import StatusUI

ui = None
ui_queue = Queue()

# =============================================================================
# PATH HELPERS
# =============================================================================

def get_config_path() -> str:
    # config.ini must be next to the EXE or the script
    return os.path.join(BASE_DIR, "config.ini")

# =============================================================================
# SETTINGS / PARSERS
# =============================================================================

def read_debugscreen(config_path: str) -> bool:
    cfg = configparser.ConfigParser()
    cfg.read(config_path, encoding="utf-8")

    raw = cfg.get("Settings", "DebugScreen", fallback="0")
    raw = (raw or "").strip()

    if raw == "":
        return False

    low = raw.lower()
    if low in ("true", "yes", "y", "on"):
        return True
    if low in ("false", "no", "n", "off"):
        return False

    try:
        return int(raw) == 1
    except Exception:
        return False


def _get_cfg_int(cfg: configparser.ConfigParser, section: str, key: str, default: int = 0) -> int:
    try:
        raw = cfg.get(section, key, fallback=str(default))
        raw = (raw or "").strip()
        if raw == "":
            return default
        return int(raw)
    except Exception:
        return default

# =============================================================================
# CONSOLE (DEBUG)
# =============================================================================

def ensure_console():
    if os.name != "nt":
        return

    kernel32 = ctypes.windll.kernel32

    if kernel32.GetConsoleWindow():
        return

    if kernel32.AllocConsole() == 0:
        return

    sys.stdout = open("CONOUT$", "w", buffering=1, encoding="utf-8", errors="replace")
    sys.stderr = open("CONOUT$", "w", buffering=1, encoding="utf-8", errors="replace")
    sys.stdin = open("CONIN$", "r", encoding="utf-8", errors="replace")

# =============================================================================
# UI / LOGGING HELPERS
# =============================================================================

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


def start_auto_close_countdown(ui_obj, seconds: int = AUTO_CLOSE_SECONDS):
    if not UI_ENABLED or ui_obj is None:
        return

    base_title = "Upshop Import"

    def _tick(remaining: int):
        if remaining <= 0:
            try:
                ui_obj.root.destroy()
            except Exception:
                pass
            return

        try:
            ui_obj.root.title(f"{base_title} - closing in {remaining}s")
        except Exception:
            pass

        try:
            ui_obj.root.after(1000, _tick, remaining - 1)
        except Exception:
            return

    try:
        ui_obj.root.after(1000, _tick, int(seconds))
    except Exception:
        pass

# =============================================================================
# HTTP HELPERS
# =============================================================================

def explain_http_exception(exc: Exception, context: str = ""):
    prefix = (context + " | ") if context else ""

    if isinstance(exc, Timeout):
        return (
            "Request timeout",
            prefix + "The API did not respond in time. Try again or check network/VPN/firewall.",
        )

    if isinstance(exc, HTTPError):
        resp = getattr(exc, "response", None)
        code = getattr(resp, "status_code", None)

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

    if isinstance(exc, RequestException):
        return ("Network error", prefix + str(exc))

    return ("Error", prefix + str(exc))


def request_json(method: str, url: str, *, headers=None, json_body=None, timeout=90, context=""):
    try:
        m = method.strip().lower()
        if m == "get":
            resp = requests.get(url, headers=headers, timeout=timeout)
        elif m == "post":
            resp = requests.post(url, headers=headers, json=json_body, timeout=timeout)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        resp.raise_for_status()

        try:
            return resp.json()
        except Exception as je:
            body = (resp.text or "")[:2000]
            logging.error(f"{context} | JSON parse failed. Body (first 2000 chars): {body}")
            ui_error("Invalid API response", f"{context} | Response is not valid JSON.")
            raise je

    except Exception as e:
        title, detail = explain_http_exception(e, context)
        ui_error(title, detail)
        raise

# =============================================================================
# LOGGING SETUP
# =============================================================================

CONFIG_PATH = get_config_path()
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"config.ini not found at: {CONFIG_PATH}")

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

debug_console = read_debugscreen(CONFIG_PATH)
if debug_console:
    ensure_console()

log_ts = datetime.now().strftime("%Y-%m-%d")
log_filename = f"ImportOrdersIntoSMS_logs_{log_ts}.log"

log_dir = os.path.join(BASE_DIR, "Log")
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
logging.info(f"BASE_DIR={BASE_DIR}")

# =============================================================================
# CONFIG VALUES
# =============================================================================

server_name = config["Settings"]["ServerName"]
database_name = config["Settings"]["DatabaseName"]
sql_driver = config["Settings"]["SQLDriver"]
store_number = int(config["Settings"]["StoreNumber"])

base_url = config["ImportOrders"]["BaseUrl"].rstrip("/")
api_username = config["ImportOrders"]["Username"]
api_password = config["ImportOrders"]["Password"]

LOG_PURGE_DAYS = _get_cfg_int(config, "Settings", "LogPurge", 0)
REPORT_PURGE_DAYS = _get_cfg_int(config, "Settings", "ReportPurge", 0)

# =============================================================================
# PURGE FUNCTIONS
# =============================================================================

def purge_log_files(folder: str, keep_days: int) -> dict:
    stats = {"deleted": 0, "kept": 0, "errors": 0, "skipped_dirs": 0}

    if keep_days <= 0:
        status("Log purge skipped", "LogPurge <= 0")
        return stats

    if not os.path.isdir(folder):
        ui_warn("Log purge folder not found", folder)
        return stats

    cutoff = datetime.now() - timedelta(days=keep_days)
    status(
        "Log purge started",
        f"path={folder} | keep_days={keep_days} | cutoff<{cutoff.strftime('%Y-%m-%d %H:%M:%S')}>",
    )

    try:
        for name in os.listdir(folder):
            path = os.path.join(folder, name)

            if os.path.isdir(path):
                stats["skipped_dirs"] += 1
                continue

            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(path))
                if mtime < cutoff:
                    os.remove(path)
                    stats["deleted"] += 1
                else:
                    stats["kept"] += 1
            except Exception as e:
                stats["errors"] += 1
                logging.exception(f"Log purge error for file={path}: {e}")

    except Exception as e:
        stats["errors"] += 1
        logging.exception(f"Log purge failed for folder={folder}: {e}")

    status("Log purge finished", f"deleted={stats['deleted']} kept={stats['kept']} errors={stats['errors']}")
    return stats


def purge_ravyx_po_status(conn, keep_days: int) -> int:
    if keep_days <= 0:
        status("Report purge skipped", "ReportPurge <= 0")
        return 0

    cutoff = datetime.now() - timedelta(days=keep_days)
    status("Report purge started", f"keep_days={keep_days} | cutoff<{cutoff.strftime('%Y-%m-%d %H:%M:%S')}>")

    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM dbo.RAVYX_PO_STATUS WHERE F254 < ?", cutoff)
        deleted = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
        status("Report purge finished", f"deleted={deleted}")
        return deleted
    except Exception as e:
        logging.exception(f"Report purge failed: {e}")
        ui_warn("Report purge failed", str(e))
        try:
            conn.rollback()
        except Exception:
            pass
        return 0
    finally:
        try:
            cur.close()
        except Exception:
            pass

# =============================================================================
# SQL HELPERS
# =============================================================================

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


def _clip(s: str, n: int) -> str:
    s = "" if s is None else str(s)
    return s.strip()[:n]


def po_key(item) -> Tuple[str, str]:
    f91 = _clip(item.get("case_order_number"), 20)
    f27 = _clip(item.get("vendor_number"), 14)
    return f91, f27

# =============================================================================
# PO STATUS (dbo.RAVYX_PO_STATUS)
# =============================================================================

def upsert_po_status(
    conn,
    *,
    f91: str,
    f27: str,
    step: str = "Order Import",
    vendor_name: str = "",
    status_txt: str = "SUCCESS",
    message: str = "",
    dept: Optional[int] = None,
    f1032: int = 0,
):
    """
    NOTE:
    - Your table currently stores row-per-PO (F1032/F91/F27 etc.)
    - We keep "system/global" rows with F91='0', F27='0', F1032=0
    - We avoid NULL inserts since your F1032 cannot be NULL (PK dependency)
    """
    f91 = _clip(f91, 20)
    f27 = _clip(f27, 14)
    step = _clip(step, 40)
    vendor_name = _clip(vendor_name, 60)
    status_txt = _clip(status_txt, 60)

    message_db = None if (status_txt or "").strip().upper() == "SUCCESS" else _clip(message, 5000)
    now = datetime.now()

    f1032_i = safe_int(f1032, 0)
    dept_i = None if dept is None else safe_int(dept, 0)

    cur = conn.cursor()
    sql = """
    MERGE dbo.RAVYX_PO_STATUS AS T
    USING (SELECT ? AS F91, ? AS F27) AS S
      ON (T.F91 = S.F91 AND T.F27 = S.F27)
    WHEN MATCHED THEN
      UPDATE SET
        T.F254  = ?,
        T.F02   = ?,
        T.F29   = ?,
        T.F1081 = ?,
        T.F334  = ?,
        T.F03   = COALESCE(?, T.F03),
        T.F1032 = CASE WHEN (T.F1032 IS NULL OR T.F1032 = 0) THEN ? ELSE T.F1032 END
    WHEN NOT MATCHED THEN
      INSERT (F1032, F91, F02, F27, F334, F254, F29, F1081, F03)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    params = (
        f91,         # S.F91
        f27,         # S.F27
        now,         # update: F254
        step,        # update: F02
        status_txt,  # update: F29
        message_db,  # update: F1081
        vendor_name, # update: F334
        dept_i,      # update: F03
        f1032_i,     # update: F1032 (if empty)
        f1032_i,     # insert: F1032
        f91,         # insert: F91
        step,        # insert: F02
        f27,         # insert: F27
        vendor_name, # insert: F334
        now,         # insert: F254
        status_txt,  # insert: F29
        message_db,  # insert: F1081
        dept_i,      # insert: F03
    )

    cur.execute(sql, params)
    conn.commit()
    cur.close()


def log_api_error_to_status(conn, endpoint: str, exc: Exception):
    if conn is None:
        return
    try:
        title, detail = explain_http_exception(exc, endpoint)
        upsert_po_status(
            conn,
            f91="0",
            f27="0",
            step=_clip(endpoint, 40),
            vendor_name="",
            status_txt="FAILED",
            message=f"{title} | {detail}",
            dept=None,
            f1032=0,
        )
    except Exception:
        logging.exception("Failed to write API error to RAVYX_PO_STATUS")

# =============================================================================
# UPSHOP API
# =============================================================================

def get_job_id(auth_token):
    url = f"{base_url}/export/orders"
    headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}
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

# =============================================================================
# VENDOR LOOKUP
# =============================================================================

def get_vendor_name_cached(conn, vendor_number, vendor_cache: Dict[str, str]) -> str:
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

# =============================================================================
# TMP TABLE INSERTS
# =============================================================================

def send_rechdr(conn, job_data_entry, vendor_cache: Dict[str, str]):
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
        sms_order_number,            # F1032
        vendor_number,               # F27
        approval_date,               # F76
        case_order_number,           # F91
        approval_date,               # F253
        effective_date,              # F254
        vendor_name,                 # F334
        88454,                       # F352
        121609,                      # F1035
        121609,                      # F1036
        store_number_string,         # F1056
        "901",                       # F1057
        "OPEN",                      # F1067
        "ORDER",                     # F1068
        1,                           # F1101
        757,                         # F1126
        "Upshop Order",              # F1127
        effective_date,              # F1246
        effective_date,              # F1653
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
        [F1032], [F1101], [F01], [F03], [F1003], [F1041], [F1063], [F1067],
        [F1184], [F1887], [F75], [F76]
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """

    insert_values = (
        case_order_number,           # F1032
        safe_int(line_num),          # F1101
        sku,                         # F01
        department_number,           # F03
        float(order_quantity),       # F1003
        description,                 # F1041
        3510,                        # F1063
        "ITEM",                      # F1067
        "CASE",                      # F1184
        "C",                         # F1887
        float(order_quantity),       # F75
        approval_date,               # F76
    )

    cursor.execute(insert_query, insert_values)
    conn.commit()
    rows_affected = cursor.rowcount
    cursor.close()
    return rows_affected

# =============================================================================
# MAIN IMPORT FLOW
# =============================================================================

def run_import():
    totals = {
        "hdr_inserts": 0,
        "dtl_inserts": 0,
        "items_seen": 0,
        "hdr_skipped": 0,
        "dtl_skipped": 0,
        # NEW: list of PO imported successfully
        "po_imported_ok": [],
    }

    conn = None
    vendor_cache: Dict[str, str] = {}

    po_errors: Set[Tuple[str, str]] = set()
    po_first_dept: Dict[Tuple[str, str], int] = {}

    try:
        upshop_log_dir = os.path.join(BASE_DIR, "log")
        purge_log_files(upshop_log_dir, LOG_PURGE_DAYS)

        status("Opening database connection...")
        conn = open_and_validate_database_connection()

        purge_ravyx_po_status(conn, REPORT_PURGE_DAYS)

        status("Connecting to Upshop API...", "Requesting auth token")
        urlt = f"{base_url}/login"
        payloadt = {"username": api_username, "password": api_password}
        headerst = {"Content-Type": "application/json"}

        # --- LOGIN ---
        try:
            response_data = request_json(
                "post", urlt, headers=headerst, json_body=payloadt, timeout=90, context="Upshop /login"
            )
        except Exception as e:
            title, detail = explain_http_exception(e, "Upshop /login")
            try:
                upsert_po_status(
                    conn,
                    f91="0",
                    f27="0",
                    step="Upshop /login",
                    status_txt="FAILED",
                    message=f"{title} | {detail}",
                    dept=None,
                    f1032=0,
                )
            except Exception:
                pass
            raise

        auth_token = response_data.get("access_token")
        if not auth_token:
            msg = "Upshop /login response has no access_token"
            ui_error("Auth token missing", msg)
            try:
                upsert_po_status(
                    conn,
                    f91="0",
                    f27="0",
                    step="Upshop /login",
                    status_txt="FAILED",
                    message=msg,
                    dept=None,
                    f1032=0,
                )
            except Exception:
                pass
            raise RuntimeError("Auth token missing in response.")

        status("Auth token retrieved.")

        # --- CREATE EXPORT JOB ---
        try:
            job_id = get_job_id(auth_token)
        except Exception as e:
            log_api_error_to_status(conn, "Upshop /export/orders", e)
            raise

        # --- WAIT FOR COMPLETION ---
        try:
            job_status = wait_for_job_completion(auth_token, job_id)
        except Exception as e:
            log_api_error_to_status(conn, f"Upshop /job_status/{job_id}", e)
            raise

        data_items = job_status.get("data", [])
        status("Download complete.", f"{len(data_items)} item(s)")

        if not data_items:
            totals["items_seen"] = 0
            status("No approved orders found.", "0 order / 0 item.")
            try:
                upsert_po_status(
                    conn,
                    f91="0",
                    f27="0",
                    step="Order Import",
                    status_txt="SUCCESS",
                    message="No approved orders found",
                    dept=None,
                    f1032=0,
                )
            except Exception:
                pass
            return totals

        # Build unique order list (by PO + Vendor)
        orders: Dict[Tuple[str, str], dict] = {}
        for it in data_items:
            f91, f27 = po_key(it)
            if not f91 or not f27:
                continue
            if (f91, f27) not in orders:
                orders[(f91, f27)] = it

        # Init status per order (SUCCESS = “started”)
        for (f91, f27), it in orders.items():
            vendor_name = get_vendor_name_cached(conn, f27, vendor_cache)
            dept_no = safe_int(it.get("department_number"), None)
            try:
                upsert_po_status(
                    conn,
                    f91=f91,
                    f27=f27,
                    step="Order Import",
                    vendor_name=vendor_name,
                    status_txt="SUCCESS",
                    message="Import started (waiting final result)",
                    dept=dept_no,
                    f1032=0,
                )
            except Exception as e:
                logging.exception(f"Failed to init PO status (F91={f91},F27={f27}): {e}")

        status("Inserting into SMS TMP tables...")
        seen_headers: Set[str] = set()
        line_number = 1

        for item in data_items:
            totals["items_seen"] += 1

            sku = safe_str(item.get("sku"))
            po = safe_str(item.get("case_order_number"))
            dept_no = safe_int(item.get("department_number"), None)
            vendor_no = safe_str(item.get("vendor_number"))
            f91 = _clip(po, 20)
            f27 = _clip(vendor_no, 14)

            key = (f91, f27)
            if key not in po_first_dept and dept_no is not None:
                po_first_dept[key] = int(dept_no)

            vendor_name = get_vendor_name_cached(conn, vendor_no, vendor_cache)
            vendor_case_key = f"{vendor_no}{po}"

            status("Importing item...", f"{line_number}/{len(data_items)} | PO={po} | SKU={sku}")

            if vendor_case_key not in seen_headers:
                try:
                    inserted = send_rechdr(conn, item, vendor_cache)
                    totals["hdr_inserts"] += inserted if inserted else 0
                    seen_headers.add(vendor_case_key)

                    upsert_po_status(
                        conn,
                        f91=f91,
                        f27=f27,
                        step="Order Import",
                        vendor_name=vendor_name,
                        status_txt="SUCCESS",
                        message="Header inserted in TMP_REC_BAT",
                        dept=dept_no,
                        f1032=0,
                    )
                except Exception as e:
                    totals["hdr_skipped"] += 1
                    po_errors.add((f91, f27))
                    logging.exception(f"Skipped TMP_REC_BAT for sku={sku}: {e}")
                    ui_error("Skipped TMP_REC_BAT", f"PO={po} | SKU={sku} | {e}")

                    upsert_po_status(
                        conn,
                        f91=f91,
                        f27=f27,
                        step="Order Import",
                        vendor_name=vendor_name,
                        status_txt="FAILED",
                        message=f"Header insert failed: {e}",
                        dept=dept_no,
                        f1032=0,
                    )

            try:
                inserted = send_recdtl(conn, item, line_number)
                totals["dtl_inserts"] += inserted if inserted else 0
            except Exception as e:
                totals["dtl_skipped"] += 1
                po_errors.add((f91, f27))
                logging.exception(f"Skipped TMP_REC_DTL for sku={sku}: {e}")
                ui_error("Skipped TMP_REC_DTL", f"PO={po} | line={line_number} | SKU={sku} | {e}")

                upsert_po_status(
                    conn,
                    f91=f91,
                    f27=f27,
                    step="Order Import",
                    vendor_name=vendor_name,
                    status_txt="FAILED",
                    message=f"Detail insert failed (line={line_number}, sku={sku}): {e}",
                    dept=dept_no,
                    f1032=0,
                )

            line_number += 1

        for (f91, f27), it in orders.items():
            vendor_name = get_vendor_name_cached(conn, f27, vendor_cache)
            dept_no = safe_int(it.get("department_number"), None)
            try:
                upsert_po_status(
                    conn,
                    f91=f91,
                    f27=f27,
                    step="Order Import",
                    vendor_name=vendor_name,
                    status_txt="SUCCESS",
                    message="Imported in TMP tables (waiting DBGEN in CGI)",
                    dept=dept_no,
                    f1032=0,
                )
            except Exception:
                pass

        if totals["hdr_skipped"] or totals["dtl_skipped"]:
            ui_warn(
                "Import finished with skipped rows",
                f"hdr_skipped={totals['hdr_skipped']} | dtl_skipped={totals['dtl_skipped']}",
            )

        # NEW: list PO imported successfully (those that are NOT in po_errors)
        imported_ok: List[str] = []
        for (f91, f27) in orders.keys():
            if (f91, f27) not in po_errors:
                imported_ok.append(f91)

        totals["po_imported_ok"] = sorted(set(imported_ok))

        status("Import completed.", f"PO(s)={len(orders)} | Items={totals['items_seen']}")
        return totals

    except Exception as e:
        logging.exception(f"Import failed in run_import(): {e}")
        try:
            log_api_error_to_status(conn, "RUN FAILED", e)
        except Exception:
            pass
        raise

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
                "You can close this window",
            )
        else:
            status("No orders were imported", "You can close this window")

        logging.info("=== End run ===")

# =============================================================================
# PROGRAM ENTRY
# =============================================================================

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
            po_ok = totals.get("po_imported_ok", [])

            if items_seen == 0:
                title = "No approved orders"
                detail = "0 order / 0 item. You can close this window."
            elif orders_imported == 0:
                title = "No orders imported"
                detail = f"{items_seen} item(s) downloaded but 0 order imported. You can close this window."
            else:
                title = "Done"
                detail = f"{orders_imported} order(s) were imported. You can close this window."

            def on_done():
                ui.done(title, detail)

                # NEW: show imported PO numbers in the listbox (requires ui_status.py add_message())
                try:
                    if po_ok:
                        max_show = 40
                        shown = po_ok[:max_show]
                        remaining = len(po_ok) - len(shown)

                        ui.add_message("INFO", "PO imported successfully", f"Count={len(po_ok)}")
                        for po in shown:
                            ui.add_message("INFO", "Imported PO", str(po))

                        if remaining > 0:
                            ui.add_message("INFO", "Imported PO", f"... (+{remaining} more)")
                    else:
                        ui.add_message("INFO", "No successful PO", "None")
                except Exception as _:
                    # If ui_status.py wasn't updated yet, don't crash the UI.
                    pass

                start_auto_close_countdown(ui, AUTO_CLOSE_SECONDS)

            ui.root.after(0, on_done)

        except Exception as e:
            logging.exception(f"Import failed: {e}")

            def on_err():
                ui.error("Import failed", str(e))
                start_auto_close_countdown(ui, AUTO_CLOSE_SECONDS)

            ui.root.after(0, on_err)

    threading.Thread(target=worker, daemon=True).start()
    ui.run()


if __name__ == "__main__":
    main()
