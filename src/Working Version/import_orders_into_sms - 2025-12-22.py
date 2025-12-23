import os
import requests
import json
import time
import pyodbc
import configparser
import logging
from datetime import datetime
import sys

# --------------------------
# Base directory
# --------------------------
base_dir = (
    os.path.dirname(sys.executable)
    if getattr(sys, "frozen", False)
    else os.path.dirname(os.path.abspath(__file__))
)

# --------------------------
# Logging setup (append daily; [HH:MM:SS]: timestamps)
# Write logs to Log subfolder (create if missing)
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

# Access values from the "Settings" section
server_name = config["Settings"]["ServerName"]
database_name = config["Settings"]["DatabaseName"]
sql_driver = config["Settings"]["SQLDriver"]
store_number = int(config["Settings"]["StoreNumber"])

# Access values from the "ImportOrders" section
base_url = config["ImportOrders"]["BaseUrl"].rstrip("/")
api_username = config["ImportOrders"]["Username"]
api_password = config["ImportOrders"]["Password"]


def _get_sql_connection():
    connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes"
    logging.info(f"Connecting to SQL Server '{server_name}', database '{database_name}' ...")
    conn = pyodbc.connect(connection_string)
    logging.info("SQL connection established.")
    return conn


def open_and_validate_database_connection():
    """
    Open + validate DB connection before calling the API.
    Returns an OPEN connection for reuse throughout the run.
    """
    logging.info("Opening + validating database connectivity before calling API ...")
    conn = _get_sql_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    cur.close()
    logging.info("Database connectivity validated successfully.")
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

    logging.info("Creating job (export/orders) ...")
    response = requests.post(url, headers=headers, json=payload, timeout=90)
    response.raise_for_status()
    resp_json = response.json()
    job_id = resp_json.get("job_id")
    logging.info(f"Job created successfully. job_id={job_id}")
    logging.info(f"Job creation API response: {json.dumps(resp_json)}")
    return job_id


def check_job_status(auth_token, job_id):
    url = f"{base_url}/job_status/{job_id}"
    headers = {"Authorization": f"Bearer {auth_token}"}

    logging.info(f"Checking job status for job_id={job_id} ...")
    response = requests.get(url, headers=headers, timeout=90)
    response.raise_for_status()
    resp_json = response.json()
    logging.info(
        f"Job status retrieved. status={resp_json.get('status')}, item_count={len(resp_json.get('data', []))}"
    )
    return resp_json


def wait_for_job_completion(auth_token, job_id, poll_interval_seconds=5, timeout_seconds=1800):
    """
    Poll job status until it reaches a terminal status.
    Swagger indicates:
      { "status": "finished", "message": "Job finished" }
    """
    terminal_success = {"finished"}
    terminal_failure = {"failed", "error", "cancelled", "canceled"}

    start = time.time()
    last_status = None

    while True:
        status_payload = check_job_status(auth_token, job_id)

        status_raw = status_payload.get("status") or status_payload.get("state")
        status_val = (status_raw or "").strip().lower()
        message = status_payload.get("message")

        if status_val != last_status:
            logging.info(f"Job status changed: {last_status} -> {status_val} (message={message})")
            last_status = status_val

        if status_val in terminal_success:
            logging.info(f"Job reached terminal SUCCESS status={status_raw}. message={message}")
            return status_payload

        if status_val in terminal_failure:
            logging.error(f"Job reached terminal FAILURE status={status_raw}. message={message}")
            logging.error(f"Final job status payload: {json.dumps(status_payload)}")
            raise RuntimeError(f"Job failed with status={status_raw}. message={message}")

        elapsed = time.time() - start
        if elapsed > timeout_seconds:
            logging.error(
                f"Timed out waiting for job completion after {int(elapsed)}s. "
                f"Last status={status_raw}, message={message}"
            )
            raise TimeoutError(f"Job did not finish within {timeout_seconds}s. Last status={status_raw}")

        time.sleep(poll_interval_seconds)


def get_vendor_name_cached(conn, vendor_number, vendor_cache):
    """
    Cache vendor name per vendor_number.
    Using: SELECT F334 FROM VENDOR_TAB WHERE F27=?
    """
    key = safe_str(vendor_number)

    if key in vendor_cache:
        logging.info(f"Vendor cache HIT: vendor_number={vendor_number}")
        return vendor_cache[key]

    logging.info(f"Vendor cache MISS: vendor_number={vendor_number} (querying SQL)")
    try:
        cur = conn.cursor()
        cur.execute("SELECT F334 FROM VENDOR_TAB WHERE F27 = ?", str(vendor_number))
        row = cur.fetchone()
        cur.close()

        vendor_name = ""
        if row and row[0] is not None:
            vendor_name = str(row[0]).strip()
        else:
            logging.warning(f"Vendor lookup: vendor_number={vendor_number} NOT FOUND")

        vendor_cache[key] = vendor_name
        return vendor_name
    except Exception as e:
        logging.exception(f"Vendor lookup failed for vendor_number={vendor_number}: {e}")
        vendor_cache[key] = ""
        return ""


def send_rechdr(conn, job_data_entry, vendor_cache):
    """
    TMP_REC_BAT: we keep your existing insert.
    Note: F1032 is set to PO (case_order_number) for now; CGI will dbGen(F1032,1) and replace.
    """
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
        sms_order_number,      # F1032 (PO for now)
        vendor_number,         # F27
        approval_date,         # F76
        case_order_number,     # F91
        approval_date,         # F253
        effective_date,        # F254
        vendor_name,           # F334
        88454,                 # F352
        121609,                # F1035
        121609,                # F1036
        store_number_string,   # F1056
        "901",                 # F1057
        "OPEN",                # F1067
        "ORDER",               # F1068
        1,                     # F1101
        757,                   # F1126
        "STCR Inc",            # F1127
        effective_date,        # F1246
        effective_date,        # F1653
    )

    logging.info(
        f"TMP_REC_BAT insert for sms_order_number={sms_order_number}, "
        f"vendor={vendor_number}, vendor_name='{vendor_name}', "
        f"approval_date={approval_date}, effective_date={effective_date}"
    )
    logging.info(f"TMP_REC_BAT params: {values}")

    cursor.execute(query, values)
    conn.commit()
    rows_affected = cursor.rowcount
    logging.info(f"TMP_REC_BAT insert result: rows_affected={rows_affected}")

    cursor.close()
    return rows_affected


def send_recdtl(conn, job_data_entry, line_num):
    """
    TMP_REC_DTL: NO LOOKUP here (POS/COST/PRICE is done in CGI).
    We insert the minimum + keep your flow fields you already had.
    IMPORTANT: F1032 is set to PO for now; CGI will replace with dbGen(F1032,1).
    """
    cursor = conn.cursor()

    case_order_number = safe_int(job_data_entry.get("case_order_number"))
    department_number = safe_int(job_data_entry.get("department_number"))
    sku = safe_str(job_data_entry.get("sku"))
    description = safe_str(job_data_entry.get("description"))
    order_quantity = safe_int(job_data_entry.get("order_quantity"), 0)
    approval_date = job_data_entry.get("approval_date")

    if not sku:
        raise ValueError(f"SKU is empty (PO={case_order_number}, line={line_num})")

    # We keep the structure similar to your old insert but REMOVE all lookup-dependent fields.
    insert_query = """
    INSERT INTO [dbo].[TMP_REC_DTL] (
        [F1032],   -- Transaction number (PO for now)
        [F1101],   -- Line number
        [F01],     -- UPC code
        [F03],     -- Department code
        [F1003],   -- Case quantity
        [F1041],   -- Description registration
        [F1063],   -- Function code
        [F1067],   -- Registration mode
        [F1184],   -- Buying format
        [F1887],   -- Buying format (string)
        [F75],     -- Case on order
        [F76]      -- Date order
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """

    insert_values = (
        case_order_number,   # F1032 = PO for now
        safe_int(line_num),  # F1101
        sku,                 # F01
        department_number,   # F03
        float(order_quantity),  # F1003
        description,         # F1041
        3510,                # F1063
        "ITEM",              # F1067
        "CASE",              # F1184
        "C",                 # F1887
        float(order_quantity),  # F75 (case on order)
        approval_date,       # F76
    )

    logging.info(
        f"TMP_REC_DTL insert for PO={case_order_number}, line={line_num}, sku={sku}, dept={department_number}, qty={order_quantity}"
    )
    logging.info(f"TMP_REC_DTL params: {insert_values}")

    cursor.execute(insert_query, insert_values)
    conn.commit()
    rows_affected = cursor.rowcount
    logging.info(f"TMP_REC_DTL insert result: rows_affected={rows_affected}")

    cursor.close()
    return rows_affected


def main():
    totals = {
        "hdr_inserts": 0,
        "dtl_inserts": 0,
        "items_seen": 0,
        "hdr_skipped": 0,
        "dtl_skipped": 0,
    }

    conn = None

    # Cache vendor names only (no item lookup cache)
    vendor_cache = {}

    try:
        conn = open_and_validate_database_connection()
    except Exception as e:
        logging.exception(f"Aborting run because database connection validation failed: {e}")
        logging.info("=== End run ===")
        return

    try:
        # --------------------------
        # API: Login
        # --------------------------
        urlt = f"{base_url}/login"
        payloadt = {"username": api_username, "password": api_password}
        headerst = {"Content-Type": "application/json"}

        logging.info("Requesting auth token ...")
        responset = requests.post(urlt, headers=headerst, json=payloadt, timeout=90)
        responset.raise_for_status()
        response_data = responset.json()
        auth_token = response_data.get("access_token")

        if not auth_token:
            logging.error("Auth token missing in response.")
            return

        logging.info("Auth token retrieved successfully.")

        # --------------------------
        # API: Create job + poll until finished
        # --------------------------
        job_id = get_job_id(auth_token)
        logging.info(f"Job ID: {job_id}")

        job_status = wait_for_job_completion(
            auth_token,
            job_id,
            poll_interval_seconds=5,
            timeout_seconds=1800,
        )

        data_items = job_status.get("data", [])
        # Debug: show which POs are in the payload
        pos = sorted({str(x.get("case_order_number")) for x in data_items if x.get("case_order_number") is not None})
        vendors = sorted({str(x.get("vendor_number")) for x in data_items if x.get("vendor_number") is not None})

        logging.info(f"Distinct POs in payload: {pos}")
        logging.info(f"Distinct vendors in payload: {vendors}")

        
        logging.info(f"Job returned {len(data_items)} item(s).")

        if not data_items:
            logging.warning("API returned 0 items. Nothing to import.")
            return
        

        # --------------------------
        # Insert into SMS staging tables
        # --------------------------
        seen_headers = set()
        line_number = 1

        for item in data_items:
            totals["items_seen"] += 1

            qty = item.get("order_quantity")
            sku = item.get("sku")
            po = item.get("case_order_number")
            vendor_case_key = f"{item.get('vendor_number')}{po}"

            logging.info(
                f"Processing item sku={sku}, qty={qty}, po={po}, vendor_case_key={vendor_case_key}, line={line_number}"
            )

            if vendor_case_key not in seen_headers:
                try:
                    inserted = send_rechdr(conn, item, vendor_cache)
                    totals["hdr_inserts"] += inserted if inserted else 0
                    seen_headers.add(vendor_case_key)
                except Exception as e:
                    totals["hdr_skipped"] += 1
                    logging.exception(f"Skipped TMP_REC_BAT for sku={sku}: {e}")

            try:
                inserted = send_recdtl(conn, item, line_number)
                totals["dtl_inserts"] += inserted if inserted else 0
            except Exception as e:
                totals["dtl_skipped"] += 1
                logging.exception(f"Skipped TMP_REC_DTL for sku={sku}: {e}")

            line_number += 1

    except requests.RequestException as e:
        logging.exception(f"HTTP error during job workflow: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error in main workflow: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
                logging.info("SQL connection closed.")
            except Exception:
                logging.exception("Error closing SQL connection.")

        logging.info(
            "Run summary: "
            f"items_seen={totals['items_seen']}, "
            f"hdr_inserts={totals['hdr_inserts']}, hdr_skipped={totals['hdr_skipped']}, "
            f"dtl_inserts={totals['dtl_inserts']}, dtl_skipped={totals['dtl_skipped']}"
        )
        logging.info("=== End run ===")


if __name__ == "__main__":
    main()
