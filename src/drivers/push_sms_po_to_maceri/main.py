import os
import sys
import time
import logging
import threading
import configparser
from datetime import datetime
from collections import namedtuple
from typing import List, Tuple, Optional, Dict, Any
from queue import Queue

import pyodbc
import requests
from requests import Session
from requests.exceptions import RequestException, Timeout, HTTPError

from ui_send_PO_maceri import VendorSendUI  # your UI file/class


# =============================================================================
# Version + Path helpers
# =============================================================================
def _is_frozen() -> bool:
    return getattr(sys, "frozen", False)


def _base_dir() -> str:
    # EXE: folder where the exe is located
    # Script: folder where this main.py is located
    return os.path.dirname(sys.executable) if _is_frozen() else os.path.dirname(os.path.abspath(__file__))


BASE_DIR = _base_dir()
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
VERSION_PATH = os.path.join(BASE_DIR, "version_info.txt")


def read_version(default: str = "0.0.0.0") -> str:
    """
    Reads first line from version_info.txt located beside the EXE (preferred),
    or beside main.py when running as a script.
    """
    try:
        with open(VERSION_PATH, "r", encoding="utf-8") as f:
            v = (f.readline() or "").strip()
            return v or default
    except Exception:
        return default


APP_VERSION = read_version()


# =============================================================================
# Logging (append daily; [HH:MM:SS]: timestamps)
# =============================================================================
log_ts = datetime.now().strftime("%Y-%m-%d")
log_filename = os.path.join(BASE_DIR, f"MaceriPush_logs_{log_ts}.log")

logging.basicConfig(
    filename=log_filename,
    filemode="a",
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S"))
logging.getLogger().addHandler(console)

logging.info("=== Start run (MaceriPush) ===")
logging.info(f"AppVersion={APP_VERSION} | Frozen={_is_frozen()} | BaseDir={BASE_DIR}")
logging.info(f"ConfigPath={CONFIG_PATH}")
_run_start = time.perf_counter()


# =============================================================================
# Config / constants (READ config.ini next to EXE)
# =============================================================================
config = configparser.ConfigParser()

if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"config.ini not found next to EXE/script: {CONFIG_PATH}")

config.read(CONFIG_PATH, encoding="utf-8")

server_name = config["Settings"]["ServerName"]
sql_driver = config["Settings"].get("SQLDriver", "SQL Server")
database = config["Settings"].get("DatabaseName", "STORESQL")
store_number = int(config["Settings"].get("StoreNumber", "0") or "0")

LOCATION_ID = config["Maceri"]["MaceriLocationID"]
MACERI_USERNAME = config["Maceri"].get("Username", "").strip()
MACERI_PASSWORD = config["Maceri"].get("Password", "").strip()

if not MACERI_USERNAME or not MACERI_PASSWORD:
    raise ValueError("Missing [Maceri] Username/Password in config.ini")

connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes"

CUSTOMER_ID = "PLUMS"
NOTES = "FROMAPI"
SHIP_METHOD = "1"

API_AUTH_URL = "https://freshapi.freshbyte.tech/api/v1/auth/login"
API_ORDER_URL = "https://freshapi.freshbyte.tech/api/v1/erp/onlineorders?dropinvaliditems=True"

SENT_MARKER = "SentToVendor"  # appended to REC_HDR.F1254

# keep error logs beside exe too
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)


# =============================================================================
# SQL helpers (reuse ONE connection)
# =============================================================================
def get_po_numbers(conn: pyodbc.Connection) -> List[str]:
    """
    Filter:
      - F902 = 'UPSHOP'
      - F1067 = 'CLOSE'
      - F1068 = 'ORDER'
      - not already marked SentToVendor in F1254
    """
    query = """
        SELECT HDR.F1032
        FROM REC_HDR HDR
        WHERE HDR.F902 = 'UPSHOP'
          AND HDR.F1067 = 'CLOSE'
          AND HDR.F1068 = 'ORDER'
          AND (HDR.F1254 IS NULL OR HDR.F1254 NOT LIKE ?)
        ORDER BY HDR.F1032
    """
    like_marker = f"%{SENT_MARKER}%"
    cur = conn.cursor()
    cur.execute(query, (like_marker,))
    rows = cur.fetchall()
    po_list = [str(r[0]).strip() for r in rows if r and r[0] is not None]
    logging.info(f"Found {len(po_list)} PO(s) eligible for Maceri push (UPSHOP/CLOSE/ORDER, not marked).")
    return po_list


def get_po_data(conn: pyodbc.Connection, po_number: str):
    query = """
        SELECT
            REG.F1032 AS PO,
            REG.F01   AS UPC,
            REG.F26   AS ITEMCODE,
            REG.F38   AS COST,
            REG.F75   AS QTY,
            HDR.F254  AS DDATE
        FROM [dbo].[REC_REG] REG
        JOIN [dbo].[REC_HDR] HDR ON REG.F1032 = HDR.F1032
        WHERE REG.F1032 = ?
        ORDER BY REG.F01
    """
    cur = conn.cursor()
    cur.execute(query, (po_number,))
    cols = [c[0] for c in cur.description]
    Row = namedtuple("Row", cols)
    rows = [Row(*r) for r in cur.fetchall()]
    logging.info(f"PO {po_number}: fetched {len(rows)} line(s) from DB.")
    return rows


def append_sent_marker(conn: pyodbc.Connection, po_number: str, marker: str = SENT_MARKER) -> bool:
    """
    Idempotent marker in F1254: append only if not present.
    Uses a guarded UPDATE so re-runs won't duplicate the marker.
    """
    update_query = """
        UPDATE [dbo].[REC_HDR]
        SET F1254 =
            CASE
                WHEN F1254 IS NULL OR LTRIM(RTRIM(F1254)) = '' THEN ?
                WHEN F1254 LIKE ? THEN F1254
                ELSE F1254 + ' | ' + ?
            END
        WHERE F1032 = ?
          AND (F1254 IS NULL OR F1254 NOT LIKE ?)
    """
    like_marker = f"%{marker}%"
    cur = conn.cursor()
    cur.execute(update_query, (marker, like_marker, marker, po_number, like_marker))
    changed = cur.rowcount > 0
    conn.commit()
    if changed:
        logging.info(f"PO {po_number}: appended marker '{marker}' into F1254.")
    else:
        logging.info(f"PO {po_number}: marker '{marker}' already present (no update).")
    return changed


# =============================================================================
# API helpers (Session + token refresh)
# =============================================================================
def get_api_token(session: Session) -> str:
    payload = {
        "userName": MACERI_USERNAME,
        "password": MACERI_PASSWORD,
        "persist": True,
    }
    headers = {
        "Fb-Referer": "https://tommaceri.fbportal.io/",
        "Content-Type": "application/json",
    }

    logging.info("Requesting Maceri auth token ...")
    t0 = time.perf_counter()
    resp = session.post(API_AUTH_URL, headers=headers, json=payload, timeout=90)
    elapsed = round(time.perf_counter() - t0, 3)

    try:
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token")
        if not token:
            logging.error(f"Auth missing token (status={resp.status_code}) body={resp.text[:300]!r}")
            raise RuntimeError("Missing token in auth response")
        logging.info(f"Auth OK (status={resp.status_code}) in {elapsed}s.")
        return token
    except Exception as e:
        logging.exception(f"Auth failed in {elapsed}s: {e}")
        raise


def send_to_api(session: Session, payload: Dict[str, Any], token: str) -> Tuple[bool, int, str]:
    """
    Returns (ok, status_code, response_text_snippet_or_error)
    - Session keep-alive
    - 401/403 refresh handled by caller
    - logs 400 response body to file (beside exe in /logs)
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    po_number = payload.get("customerPurchaseOrderNumber", "?")
    line_count = len(payload.get("details", []))

    logging.info(f"Sending PO {po_number} to Maceri: lines={line_count} ...")
    t0 = time.perf_counter()
    resp = session.post(API_ORDER_URL, headers=headers, json=payload, timeout=120)
    elapsed = round(time.perf_counter() - t0, 3)

    text = (resp.text or "")
    snippet = text[:400].replace("\n", " ")

    logging.info(f"PO {po_number}: API response status={resp.status_code} time={elapsed}s snippet={snippet!r}")

    if resp.status_code == 400:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = os.path.join(LOGS_DIR, f"maceri_400_{po_number}_{timestamp}.log")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(text)
        logging.error(f"PO {po_number}: 400 response logged to {log_path}")
        return False, resp.status_code, f"{snippet} (see {os.path.basename(log_path)})"

    if 200 <= resp.status_code < 300:
        logging.info(f"PO {po_number}: Submitted successfully.")
        return True, resp.status_code, snippet

    logging.error(f"PO {po_number}: Unexpected status={resp.status_code}")
    return False, resp.status_code, snippet


# =============================================================================
# Payload builder
# =============================================================================
def _safe_date_to_yyyy_mm_dd(value) -> str:
    if value is None:
        return datetime.now().strftime("%Y-%m-%d")
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    s = str(value)
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return datetime.now().strftime("%Y-%m-%d")


def build_json_payload(po_rows, po_number: str) -> Tuple[Optional[Dict[str, Any]], int, int]:
    if not po_rows:
        return None, 0, 0

    first = po_rows[0]
    delivery_date = _safe_date_to_yyyy_mm_dd(first.DDATE)

    payload: Dict[str, Any] = {
        "customerId": CUSTOMER_ID,
        "customerLocationId": LOCATION_ID,
        "requiredDeliveryDate": delivery_date,
        "notes": NOTES,
        "shipMethod": SHIP_METHOD,
        "customerPurchaseOrderNumber": str(first.PO),
        "confirmationNumber": f"SMS{str(first.PO)}",
        "details": [],
    }

    valid = 0
    skipped = 0

    for row in po_rows:
        unitOfMeasure = "CS"
        if row.ITEMCODE is not None and str(row.ITEMCODE).strip() == "SDLS45":
            unitOfMeasure = "BN"

        reason = None

        itemcode = "" if row.ITEMCODE is None else str(row.ITEMCODE).strip()
        if not itemcode:
            reason = "Missing ITEMCODE"

        try:
            qty_int = int(row.QTY)
        except Exception:
            qty_int = 0
            reason = reason or "Invalid QTY (non-integer)"

        if qty_int <= 0:
            reason = reason or "Invalid QTY (<=0)"

        try:
            cost_val = float(row.COST)
        except Exception:
            cost_val = None
            reason = reason or "Invalid COST"

        if cost_val is None:
            reason = reason or "Missing COST"

        if reason:
            skipped += 1
            logging.info(
                f"PO {po_number}: Skipped item UPC={row.UPC} VENDORCODE={row.ITEMCODE} "
                f"QTY={row.QTY} UOM={unitOfMeasure} COST={row.COST} — Reason: {reason}"
            )
            continue

        payload["details"].append(
            {"itemCode": itemcode, "uomId": unitOfMeasure, "notes": "", "quantity": qty_int, "unitPrice": cost_val}
        )
        valid += 1
        logging.info(
            f"PO {po_number}: Add line UPC={row.UPC} VENDORCODE={itemcode} QTY={qty_int} UOM={unitOfMeasure} COST={cost_val}"
        )

    logging.info(f"PO {po_number}: Payload summary — Valid={valid} Skipped={skipped}")
    return (payload if payload["details"] else None, valid, skipped)


# =============================================================================
# Worker (runs in background thread; pushes UI messages)
# =============================================================================
def worker(ui_q: Queue):
    def ui(level: str, msg: str, detail: str = ""):
        try:
            ui_q.put((level, msg, detail))
        except Exception:
            pass

    totals = {
        "processed_pos": 0,
        "sent_pos": 0,
        "skipped_pos": 0,
        "valid_lines": 0,
        "skipped_lines": 0,
        "token_refresh": 0,
    }

    session = requests.Session()
    conn = None

    try:
        ui("INFO", f"Starting Maceri Push v{APP_VERSION}", "")
        ui("INFO", "Loading config.ini...", os.path.basename(CONFIG_PATH))

        ui("INFO", "Connecting to SQL...", f"{server_name} / {database}")
        conn = pyodbc.connect(connection_string)

        ui("INFO", "Searching POs to send...", "Filter: UPSHOP + CLOSE + ORDER + not SentToVendor")
        po_numbers = get_po_numbers(conn)

        if not po_numbers:
            logging.info("No eligible POs found (UPSHOP/CLOSE/ORDER, not marked SentToVendor).")
            ui("DONE", "No PO to send", "No eligible POs found.")
            return

        ui("INFO", "Authenticating with Maceri...", "")
        token = get_api_token(session)
        ui("INFO", "Auth OK", f"PO count={len(po_numbers)}")

        for po in po_numbers:
            totals["processed_pos"] += 1
            ui("INFO", f"Processing PO {po}", "")
            logging.info(f"Processing PO {po} ...")

            po_data = get_po_data(conn, po)
            payload, valid, skipped = build_json_payload(po_data, po)
            totals["valid_lines"] += valid
            totals["skipped_lines"] += skipped

            if not payload:
                totals["skipped_pos"] += 1
                ui("WARN", f"PO {po} skipped", "No valid items to send")
                logging.info(f"Skipped PO {po}: no valid items to send.")
                continue

            try:
                line_count = len(payload.get("details", []))
                ui("INFO", f"Sending PO {po}", f"Lines={line_count}")

                ok, status_code, detail = send_to_api(session, payload, token)

                if not ok and status_code in (401, 403):
                    totals["token_refresh"] += 1
                    ui("WARN", f"PO {po} auth error", f"{status_code} - refreshing token and retrying once")
                    logging.warning(f"PO {po}: Auth error {status_code}. Refreshing token and retrying once...")
                    token = get_api_token(session)
                    ok, status_code, detail = send_to_api(session, payload, token)

                if ok:
                    totals["sent_pos"] += 1
                    append_sent_marker(conn, po, SENT_MARKER)
                    ui("INFO", f"SUCCESS PO {po}", "Sent + marked SentToVendor")
                else:
                    totals["skipped_pos"] += 1
                    ui("ERROR", f"PO {po} failed", f"HTTP {status_code}: {detail}")

            except (Timeout, RequestException, HTTPError) as e:
                totals["skipped_pos"] += 1
                ui("ERROR", f"PO {po} request error", str(e))
                logging.exception(f"PO {po}: Request error: {e}")
            except Exception as e:
                totals["skipped_pos"] += 1
                ui("ERROR", f"PO {po} unexpected error", str(e))
                logging.exception(f"PO {po}: Unexpected error: {e}")

        dur = round(time.perf_counter() - _run_start, 2)
        summary = (
            f"Processed={totals['processed_pos']} | Sent={totals['sent_pos']} | Failed/Skipped={totals['skipped_pos']} | "
            f"ValidLines={totals['valid_lines']} | SkippedLines={totals['skipped_lines']} | "
            f"TokenRefresh={totals['token_refresh']} | Duration={dur}s"
        )

        logging.info("Summary | " + summary)
        logging.info("=== End run (MaceriPush) ===")
        ui("DONE", "Maceri PO push completed", summary)

    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        ui("ERROR", "Fatal error", str(e))
        ui("DONE", "Stopped", "See error above")

    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        try:
            session.close()
        except Exception:
            pass


# =============================================================================
# Main (UI thread)
# =============================================================================
def main():
    ui_q = Queue()
    ui = VendorSendUI(title=f"PLUM – Send PO to Maceri v{APP_VERSION}", queue=ui_q)

    t = threading.Thread(target=worker, args=(ui_q,), daemon=True)
    t.start()

    ui.run()


if __name__ == "__main__":
    main()
