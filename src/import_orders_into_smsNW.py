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
    connection_string = (
        f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes"
    )
    logging.info(f"Connecting to SQL Server '{server_name}', database '{database_name}' ...")
    conn = pyodbc.connect(connection_string)
    logging.info("SQL connection established.")
    return conn


def open_and_validate_database_connection():
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
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def get_job_id(auth_token):
    url = f"{base_url}/export/orders"
    headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}
    payload = {"approved_flag": True, "store_number": [store_number]}

    logging.info("Creating job (export/orders) ...")
    response = requests.post(url, headers=headers, json=payload, timeout=90)
    response.raise_for_status()
    resp_json = response.json()
    job_id = resp_json.get("job_id")
    logging.info(f"Job created. job_id={job_id}")
    return job_id


def check_job_status(auth_token, job_id):
    url = f"{base_url}/job_status/{job_id}"
    headers = {"Authorization": f"Bearer {auth_token}"}
    response = requests.get(url, headers=headers, timeout=90)
    response.raise_for_status()
    return response.json()


def wait_for_job_completion(auth_token, job_id, poll_interval_seconds=5, timeout_seconds=1800):
    terminal_success = {"finished"}
    terminal_failure = {"failed", "error", "cancelled", "canceled"}

    start = time.time()
    last_status = None

    while True:
        payload = check_job_status(auth_token, job_id)
        status_raw = payload.get("status") or payload.get("state")
        status_val = (status_raw or "").strip().lower()
        msg = payload.get("message")

        if status_val != last_status:
            logging.info(f"Job status changed: {last_status} -> {status_val} (message={msg})")
            last_status = status_val

        if status_val in terminal_success:
            return payload

        if status_val in terminal_failure:
            raise RuntimeError(f"Job failed: status={status_raw} message={msg}")

        if (time.time() - start) > timeout_seconds:
            raise TimeoutError(f"Job timeout after {timeout_seconds}s (last status={status_raw})")

        time.sleep(poll_interval_seconds)


def get_vendor_name(conn, vendor_number):
    """
    SELECT F334 FROM VENDOR_TAB WHERE F27=?
    """
    cur = conn.cursor()
    cur.execute("SELECT F334 FROM VENDOR_TAB WHERE F27 = ?", vendor_number)
    row = cur.fetchone()
    cur.close()
    return str(row[0]).strip() if row and row[0] is not None else ""


def send_rechdr(conn, job_entry):
    """
    TMP_REC_BAT : on met F1032 = PO (case_order_number)
    CGI va faire dbGen(F1032,1) et remplacer F1032 ensuite.
    """
    case_order_number = safe_int(job_entry.get("case_order_number"))
    effective_date = job_entry.get("effective_date")
    store_number_local = job_entry.get("store_number")
    approval_date = job_entry.get("approval_date")
    vendor_number = job_entry.get("vendor_number")

    vendor_name = get_vendor_name(conn, vendor_number)

    query = """
        INSERT INTO [dbo].[TMP_REC_BAT] (
            [F1032],[F27],[F76],[F91],[F253],[F254],[F334],[F352],
            [F1035],[F1036],[F1056],[F1057],[F1067],[F1068],[F1101],
            [F1126],[F1127],[F1246],[F1653]
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    store_number_string = "00" + str(store_number_local)

    values = (
        case_order_number,     # F1032 (PO pour l’instant)
        vendor_number,         # F27
        approval_date,         # F76
        case_order_number,     # F91 (PO)
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

    cur = conn.cursor()
    cur.execute(query, values)
    conn.commit()
    cur.close()

    logging.info(f"TMP_REC_BAT inserted: PO={case_order_number}, vendor={vendor_number}, vendor_name='{vendor_name}'")
    return 1


def send_recdtl(conn, job_entry, line_num):
    """
    TMP_REC_DTL : MINIMUM REQUIRED.
    Pas de lookup POS/COST/PRICE ici (c’est le CGI qui hydrate).
    On met F1032 = PO (case_order_number) pour que le CGI puisse remplacer.
    """
    po = safe_int(job_entry.get("case_order_number"))
    dept = safe_int(job_entry.get("department_number"))
    sku = str(job_entry.get("sku") or "").strip()
    desc = str(job_entry.get("description") or "").strip()
    qty_case = safe_float(job_entry.get("order_quantity"), 0.0)
    approval_date = job_entry.get("approval_date")

    if not sku:
        raise ValueError(f"SKU empty for PO={po}, line={line_num}")

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
        po,                # F1032 = PO
        safe_int(line_num),
        sku,
        dept,
        qty_case,          # F1003
        desc,              # F1041
        3510,              # F1063
        "ITEM",            # F1067
        "CASE",            # F1184
        "C",               # F1887
        qty_case,          # F75
        approval_date,     # F76
    )

    cur = conn.cursor()
    cur.execute(insert_query, insert_values)
    conn.commit()
    cur.close()

    logging.info(f"TMP_REC_DTL inserted: PO={po}, line={line_num}, sku={sku}, dept={dept}, qty_case={qty_case}")
    return 1


def verify_tmp_counts(conn, po):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM TMP_REC_BAT WHERE F91 = ?", po)
    bat_cnt = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM TMP_REC_DTL WHERE F1032 = ?", po)
    dtl_cnt = cur.fetchone()[0]
    cur.close()
    logging.info(f"Verify TMP counts for PO={po}: TMP_REC_BAT={bat_cnt}, TMP_REC_DTL={dtl_cnt}")
    if bat_cnt <= 0:
        raise RuntimeError(f"TMP_REC_BAT missing for PO={po}")
    if dtl_cnt <= 0:
        raise RuntimeError(f"TMP_REC_DTL is EMPTY for PO={po} -> CGI/3211 will fail.")


def main():
    conn = None

    try:
        conn = open_and_validate_database_connection()

        # API Login
        urlt = f"{base_url}/login"
        payloadt = {"username": api_username, "password": api_password}
        headerst = {"Content-Type": "application/json"}

        logging.info("Requesting auth token ...")
        responset = requests.post(urlt, headers=headerst, json=payloadt, timeout=90)
        responset.raise_for_status()

        auth_token = responset.json().get("access_token")
        if not auth_token:
            raise RuntimeError("Auth token missing in response.")

        # Create job + wait
        job_id = get_job_id(auth_token)
        job_status = wait_for_job_completion(auth_token, job_id)

        data_items = job_status.get("data", [])
        logging.info(f"Job returned {len(data_items)} item(s).")
        if not data_items:
            raise RuntimeError("API returned 0 items. Nothing to import.")

        # Insert into TMP tables
        seen_headers = set()
        line_number = 1

        # on track le PO pour valider
        last_po = None

        for item in data_items:
            po = safe_int(item.get("case_order_number"))
            vendor_case_key = f"{item.get('vendor_number')}{po}"
            last_po = po

            if vendor_case_key not in seen_headers:
                send_rechdr(conn, item)
                seen_headers.add(vendor_case_key)

            send_recdtl(conn, item, line_number)
            line_number += 1

        # Verify counts for the last PO (or loop over them if you want)
        if last_po:
            verify_tmp_counts(conn, last_po)

        logging.info("Import completed successfully (TMP tables populated).")

    except Exception as e:
        logging.exception(f"Run failed: {e}")
    finally:
        if conn is not None:
            try:
                conn.close()
                logging.info("SQL connection closed.")
            except Exception:
                logging.exception("Error closing SQL connection.")

        logging.info("=== End run ===")


if __name__ == "__main__":
    main()
