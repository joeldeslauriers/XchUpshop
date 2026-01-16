# main.py  (ExportOrdersToMaceriSMS)
# Fixes:
# - SQI passes PO (F1032) + Vendor (F27) as EXE args; send ONLY that PO
# - RAVYX_PO_STATUS mapping:
#     F27   = vendor# (INT)
#     F334  = lookup from VENDOR_TAB using vendor#
#     F02   = 'Order Export'
#     F29   = WARN / SUCCESS / FAILED
#     F1081 = message (skipped UPC or error)
#     F03   = NULL (optional)
# - WARN row inserted AFTER delete, only if delete happened
# - If final failure, set PO back to F1067='OPEN'

import os
import sys
import time
import logging
import threading
import configparser
import re
import json
from datetime import datetime
from collections import namedtuple
from typing import List, Tuple, Optional, Dict, Any, Set
from queue import Queue

import pyodbc
import requests
from requests import Session
from requests.exceptions import RequestException, Timeout, HTTPError

from ui_send_PO_maceri import VendorSendUI


# =============================================================================
# Version + Path helpers
# =============================================================================
def _is_frozen() -> bool:
    return getattr(sys, "frozen", False)


def _base_dir() -> str:
    return os.path.dirname(sys.executable) if _is_frozen() else os.path.dirname(os.path.abspath(__file__))


BASE_DIR = _base_dir()
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
VERSION_PATH = os.path.join(BASE_DIR, "version_info.txt")


def read_version(default: str = "0.0.0.0") -> str:
    try:
        with open(VERSION_PATH, "r", encoding="utf-8") as f:
            v = (f.readline() or "").strip()
            return v or default
    except Exception:
        return default


APP_VERSION = read_version()


# =============================================================================
# Args from SQI
# =============================================================================
def _strip_quotes(s: str) -> str:
    return (s or "").strip().strip('"').strip("'")


def get_sqi_args() -> Tuple[str, int]:
    """
    SQI calls:
      pushSMSPOtoMACERI.exe "<F1032>" "<F27>"
    """
    po = _strip_quotes(sys.argv[1]) if len(sys.argv) >= 2 else ""
    vendor_raw = _strip_quotes(sys.argv[2]) if len(sys.argv) >= 3 else ""

    if not po:
        raise ValueError('Missing SQI param: F1032 (PO). Expected: exe "<F1032>" "<F27>"')

    if not vendor_raw:
        raise ValueError('Missing SQI param: F27 (Vendor). Expected: exe "<F1032>" "<F27>"')

    try:
        vendor = int(vendor_raw)
    except Exception:
        raise ValueError(f"Vendor F27 must be numeric/int. Got: {vendor_raw!r}")

    return po, vendor


# =============================================================================
# Logging
# =============================================================================
LOG_DIR = os.path.join(BASE_DIR, "LOG")
os.makedirs(LOG_DIR, exist_ok=True)

log_ts = datetime.now().strftime("%Y_%m_%d")
log_filename = os.path.join(LOG_DIR, f"ExportOrdersToMaceriSMS_logs_{log_ts}.log")

root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(log_filename, mode="a", encoding="utf-8")
file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S"))

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S"))

root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logging.info("=== Start run (MaceriPush) ===")
logging.info(f"AppVersion={APP_VERSION} | Frozen={_is_frozen()} | BaseDir={BASE_DIR}")
logging.info(f"ConfigPath={CONFIG_PATH}")

AUTO_CLOSE_SECONDS = 20


# =============================================================================
# Daily API raw log
# =============================================================================
API_LOG_TS = datetime.now().strftime("%Y_%m_%d")
MACERI_API_LOG_PATH = os.path.join(LOG_DIR, f"MaceriApi_{API_LOG_TS}.log")
MACERI_API_LOG_ERRORS_ONLY = False


def _mask_value(v: str, keep: int = 2) -> str:
    if v is None:
        return ""
    s = str(v)
    if len(s) <= keep:
        return "*" * len(s)
    return s[:keep] + ("*" * (len(s) - keep))


def _safe_json_pretty(text: str) -> str:
    if not text:
        return ""
    try:
        obj = json.loads(text)
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return text


def _write_maceri_api_log(
    *,
    kind: str,
    url: str,
    status_code: int,
    elapsed_s: float,
    headers: Dict[str, str],
    request_json: Optional[Dict[str, Any]],
    response_text: str,
    po_number: Optional[str] = None,
):
    if MACERI_API_LOG_ERRORS_ONLY and status_code < 400:
        return

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    po_part = f"PO={po_number}\n" if po_number else ""

    hdr_lines = []
    for k, v in (headers or {}).items():
        if k.lower() == "authorization":
            hdr_lines.append(f"{k}: Bearer {_mask_value(v, keep=10)}")
        else:
            hdr_lines.append(f"{k}: {v}")
    hdr_block = "\n".join(hdr_lines)

    req_block = ""
    if request_json is not None:
        try:
            req_block = json.dumps(request_json, ensure_ascii=False, indent=2)
        except Exception:
            req_block = str(request_json)

    resp_block = _safe_json_pretty(response_text)

    entry = (
        "============================================================\n"
        f"{ts}\n"
        f"TYPE={kind}\n"
        f"{po_part}"
        f"URL={url}\n"
        f"STATUS={status_code}\n"
        f"ELAPSED={elapsed_s:.3f}s\n"
        "HEADERS:\n"
        f"{hdr_block}\n"
        "\n"
        "REQUEST JSON:\n"
        f"{req_block}\n"
        "\n"
        "RESPONSE BODY:\n"
        f"{resp_block}\n"
        "============================================================\n\n"
    )

    try:
        with open(MACERI_API_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(entry)
    except Exception as e:
        logging.warning(f"Unable to write Maceri API daily log: {e}")


# =============================================================================
# Config
# =============================================================================
config = configparser.ConfigParser()
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"config.ini not found next to EXE/script: {CONFIG_PATH}")

config.read(CONFIG_PATH, encoding="utf-8")

server_name = config["Settings"]["ServerName"]
sql_driver = config["Settings"].get("SQLDriver", "SQL Server")
database = config["Settings"].get("DatabaseName", "STORESQL")

LOCATION_ID = config["Maceri"].get("MaceriLocationID", "").strip()
MACERI_USERNAME = config["Maceri"].get("Username", "").strip()
MACERI_PASSWORD = config["Maceri"].get("Password", "").strip()

API_AUTH_URL = config["Maceri"].get("BaseUrlAuth", "").strip()
API_ORDER_URL = config["Maceri"].get("BaseUrlOrder", "").strip()
FB_REFERER = config["Maceri"].get("FbReferer", "https://tommaceri-test.fbportal.io/").strip()

if not LOCATION_ID:
    raise ValueError("Missing [Maceri] MaceriLocationID in config.ini")
if not MACERI_USERNAME or not MACERI_PASSWORD:
    raise ValueError("Missing [Maceri] Username/Password in config.ini")
if not API_AUTH_URL or not API_ORDER_URL:
    raise ValueError("Missing [Maceri] BaseUrlAuth and/or BaseUrlOrder in config.ini")

connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes"

CUSTOMER_ID = "PLUMS"
NOTES = "FROMAPI"
SHIP_METHOD = "1"

SENT_MARKER = "SentToVendor"
MAX_INVALID_ITEM_RETRIES_PER_PO = 25

logging.info(f"MaceriAuthUrl={API_AUTH_URL}")
logging.info(f"MaceriOrderUrl={API_ORDER_URL}")
logging.info(f"MaceriFbReferer={FB_REFERER}")
logging.info(f"MaceriApiDailyLog={MACERI_API_LOG_PATH} | ErrorsOnly={MACERI_API_LOG_ERRORS_ONLY}")


# =============================================================================
# Invalid item detection
# =============================================================================
_ITEM_NOT_FOUND_RE = re.compile(r"Item\s+(?P<item>\S+)\s+with\s+Uom\s+(?P<uom>\S+)\s+not\s+found", re.IGNORECASE)


def _extract_item_not_found(detail_text: str) -> Optional[Tuple[str, str]]:
    if not detail_text:
        return None
    m = _ITEM_NOT_FOUND_RE.search(detail_text)
    if not m:
        return None
    return m.group("item").strip(), m.group("uom").strip()


def _extract_detail_from_maceri_response(raw_text: str, fallback: str) -> str:
    if not raw_text:
        return fallback
    try:
        data = json.loads(raw_text)
        if isinstance(data, dict):
            d = (data.get("detail") or "").strip()
            if d:
                return d
            m = (data.get("message") or "").strip()
            if m:
                return m
    except Exception:
        pass
    return fallback


def _find_upcs_for_itemcode(po_rows, item_code: str) -> List[str]:
    upcs: Set[str] = set()
    item_code_u = (item_code or "").strip().upper()
    for r in po_rows or []:
        code = "" if r.ITEMCODE is None else str(r.ITEMCODE).strip().upper()
        if code == item_code_u:
            upc = "" if r.UPC is None else str(r.UPC).strip()
            if upc:
                upcs.add(upc)
    return sorted(upcs)


def _build_skipped_info(item_code: str, upcs: List[str]) -> str:
    upc_join = ",".join(upcs) if upcs else ""
    return f"{item_code}|UPC={upc_join}" if upc_join else f"{item_code}|UPC=?"


# =============================================================================
# SQL helpers
# =============================================================================
def get_vendor_f334(conn: pyodbc.Connection, vendor: int) -> Optional[int]:
    """
    Get F334 from VENDOR_TAB for the vendor#
    """
    q = "SELECT TOP 1 F334 FROM [dbo].[VENDOR_TAB] WHERE F27 = ?"
    cur = conn.cursor()
    cur.execute(q, (vendor,))
    row = cur.fetchone()
    if not row:
        return None
    try:
        return int(row[0]) if row[0] is not None else None
    except Exception:
        return None


def insert_ravyx_po_status(
    conn: pyodbc.Connection,
    *,
    po_number: str,
    vendor: int,
    vendor_f334: Optional[int],
    status: str,          # WARN / SUCCESS / FAILED
    f1081_text: str,      # UPC list or error text
):
    """
    Required mapping:
      F27   = vendor (int)
      F334  = vendor_tab.F334 (int)  (or 0/NULL if not found)
      F02   = 'Order Export'
      F29   = status
      F1081 = text message (UPC skipped, etc.)
      F03   = NULL
    """
    runid = datetime.now().strftime("%y%m%d%H%M%S")
    now = datetime.now()

    # Respect F1081 length if it’s limited (yours is 5000 so ok)
    f1081_val = (f1081_text or "").strip()
    if not f1081_val:
        f1081_val = None

    sql = """
        INSERT INTO [dbo].[RAVYX_PO_STATUS]
            (F1032, F91, F02, F27, F334, F254, F29, F1081, F03)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    params = (
        int(po_number),
        runid,
        "Order Export",
        int(vendor),
        int(vendor_f334) if vendor_f334 is not None else 0,
        now,
        status,
        f1081_val,
        None,   # F03 = NULL
    )

    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    logging.info(f"RAVYX_PO_STATUS inserted: PO={po_number} Vendor={vendor} F334={vendor_f334} F29={status} F1081={f1081_val!r}")


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
    return [Row(*r) for r in cur.fetchall()]


def append_sent_marker(conn: pyodbc.Connection, po_number: str, marker: str = SENT_MARKER) -> bool:
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
    return changed


def reopen_po_on_failure(conn: pyodbc.Connection, po_number: str) -> bool:
    update_query = """
        UPDATE [dbo].[REC_HDR]
        SET F1067 = 'OPEN'
        WHERE F1032 = ?
          AND F902 = 'UPSHOP'
          AND F1068 = 'ORDER'
          AND F1067 = 'CLOSE'
          AND (F1254 IS NULL OR F1254 NOT LIKE ?)
    """
    like_marker = f"%{SENT_MARKER}%"
    cur = conn.cursor()
    cur.execute(update_query, (po_number, like_marker))
    changed = cur.rowcount > 0
    conn.commit()
    if changed:
        logging.warning(f"PO {po_number}: set back to OPEN (final failure).")
    return changed


def delete_invalid_item_from_po(conn: pyodbc.Connection, po_number: str, item_code: str) -> int:
    q = """
        DELETE FROM [dbo].[REC_REG]
        WHERE F1032 = ?
          AND F26 = ?
    """
    cur = conn.cursor()
    cur.execute(q, (po_number, item_code))
    deleted = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    logging.warning(f"PO {po_number}: deleted {deleted} REC_REG line(s) for invalid itemCode(F26)={item_code}.")
    return deleted


# =============================================================================
# API helpers
# =============================================================================
def get_api_token(session: Session) -> str:
    payload = {"userName": MACERI_USERNAME, "password": MACERI_PASSWORD, "persist": True}
    headers = {"Fb-Referer": FB_REFERER, "Content-Type": "application/json"}

    t0 = time.perf_counter()
    resp = session.post(API_AUTH_URL, headers=headers, json=payload, timeout=90)
    elapsed = time.perf_counter() - t0

    raw_text = resp.text or ""
    _write_maceri_api_log(
        kind="AUTH",
        url=API_AUTH_URL,
        status_code=resp.status_code,
        elapsed_s=elapsed,
        headers=headers,
        request_json={"userName": MACERI_USERNAME, "password": "***", "persist": True},
        response_text=raw_text,
        po_number=None,
    )

    resp.raise_for_status()
    data = resp.json()
    token = data.get("token")
    if not token:
        raise RuntimeError("Missing token in auth response")
    return token


def send_to_api(session: Session, payload: Dict[str, Any], token: str) -> Tuple[bool, int, str, str]:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    po_number = payload.get("customerPurchaseOrderNumber", "?")

    t0 = time.perf_counter()
    resp = session.post(API_ORDER_URL, headers=headers, json=payload, timeout=120)
    elapsed = time.perf_counter() - t0

    raw_text = resp.text or ""
    snippet = raw_text[:400].replace("\n", " ")

    _write_maceri_api_log(
        kind="ORDER",
        url=API_ORDER_URL,
        status_code=resp.status_code,
        elapsed_s=elapsed,
        headers={"Content-Type": "application/json", "Authorization": headers["Authorization"]},
        request_json=payload,
        response_text=raw_text,
        po_number=str(po_number),
    )

    if 200 <= resp.status_code < 300:
        return True, resp.status_code, snippet, raw_text

    return False, resp.status_code, snippet, raw_text


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

        itemcode = "" if row.ITEMCODE is None else str(row.ITEMCODE).strip()
        if not itemcode:
            skipped += 1
            continue

        try:
            qty_int = int(row.QTY)
        except Exception:
            skipped += 1
            continue
        if qty_int <= 0:
            skipped += 1
            continue

        try:
            cost_val = float(row.COST)
        except Exception:
            skipped += 1
            continue

        payload["details"].append(
            {"itemCode": itemcode, "uomId": unitOfMeasure, "notes": "", "quantity": qty_int, "unitPrice": cost_val}
        )
        valid += 1

    return (payload if payload["details"] else None, valid, skipped)


# =============================================================================
# Worker
# =============================================================================
def worker(ui_q: Queue):
    def ui(level: str, msg: str, detail: str = ""):
        try:
            ui_q.put((level, msg, detail))
        except Exception:
            pass

    session = requests.Session()
    conn = None

    try:
        po, vendor = get_sqi_args()
        logging.info(f"SQI args: PO={po} Vendor={vendor}")

        ui("INFO", f"Starting Maceri Push v{APP_VERSION}", "")
        ui("INFO", "SQI Parameters", f"PO={po} | Vendor={vendor}")
        ui("INFO", "Loading config.ini...", os.path.basename(CONFIG_PATH))

        ui("INFO", "Connecting to SQL...", f"{server_name} / {database}")
        conn = pyodbc.connect(connection_string)

        vendor_f334 = get_vendor_f334(conn, vendor)
        logging.info(f"Vendor lookup: F27={vendor} => F334={vendor_f334}")

        ui("INFO", "Authenticating with Maceri...", "")
        token = get_api_token(session)
        ui("INFO", "Auth OK", "Sending single PO from SQI")

        ui("INFO", f"Processing PO {po}", "")

        invalid_retry = 0

        while True:
            po_rows = get_po_data(conn, po)
            payload, valid, skipped = build_json_payload(po_rows, po)

            if not payload:
                msg = f"PO {po} has no valid items to send after cleanup."
                ui("ERROR", f"PO {po} NOT sent", msg)
                logging.error(msg)
                insert_ravyx_po_status(conn, po_number=str(po), vendor=vendor, vendor_f334=vendor_f334, status="FAILED", f1081_text=msg)
                reopen_po_on_failure(conn, str(po))
                break

            try:
                ok, status_code, snippet, raw_text = send_to_api(session, payload, token)

                if not ok and status_code in (401, 403):
                    ui("WARN", f"PO {po} auth error", f"{status_code} - refreshing token and retrying once")
                    token = get_api_token(session)
                    ok, status_code, snippet, raw_text = send_to_api(session, payload, token)

                if ok:
                    append_sent_marker(conn, po, SENT_MARKER)
                    ui("INFO", f"SUCCESS PO {po}", "Sent + marked SentToVendor")
                    insert_ravyx_po_status(conn, po_number=str(po), vendor=vendor, vendor_f334=vendor_f334, status="SUCCESS", f1081_text="")
                    break

                detail_text = _extract_detail_from_maceri_response(raw_text, snippet)
                invalid = _extract_item_not_found(detail_text)

                if invalid:
                    item_code, uom = invalid
                    invalid_retry += 1

                    upcs = _find_upcs_for_itemcode(po_rows, item_code)
                    skipped_info = _build_skipped_info(item_code, upcs)

                    # delete
                    deleted = delete_invalid_item_from_po(conn, str(po), item_code)

                    # log WARN only if deleted
                    if deleted > 0:
                        insert_ravyx_po_status(
                            conn,
                            po_number=str(po),
                            vendor=vendor,
                            vendor_f334=vendor_f334,
                            status="WARN",
                            f1081_text=skipped_info,
                        )
                        ui("WARN", f"PO {po} item skipped", f"{skipped_info} (removed, retrying)")
                        logging.warning(f"PO {po}: Invalid itemCode '{item_code}' (UOM={uom}). UPC(s): {', '.join(upcs) if upcs else '?' }.")

                    if invalid_retry > MAX_INVALID_ITEM_RETRIES_PER_PO:
                        msg = f"PO {po} still failing after max invalid-item retries. Last invalid={item_code} UOM={uom}."
                        ui("ERROR", f"PO {po} NOT sent", msg)
                        logging.error(msg)
                        insert_ravyx_po_status(conn, po_number=str(po), vendor=vendor, vendor_f334=vendor_f334, status="FAILED", f1081_text=msg)
                        reopen_po_on_failure(conn, str(po))
                        break

                    continue

                # other error
                msg = f"Order was NOT sent. Maceri error: {detail_text}"
                ui("ERROR", f"PO {po} NOT sent", msg)
                logging.error(msg)
                insert_ravyx_po_status(conn, po_number=str(po), vendor=vendor, vendor_f334=vendor_f334, status="FAILED", f1081_text=msg)
                reopen_po_on_failure(conn, str(po))
                break

            except (Timeout, RequestException, HTTPError) as e:
                msg = f"Request error: {e}"
                ui("ERROR", f"PO {po} request error", msg)
                logging.exception(msg)
                insert_ravyx_po_status(conn, po_number=str(po), vendor=vendor, vendor_f334=vendor_f334, status="FAILED", f1081_text=msg)
                reopen_po_on_failure(conn, str(po))
                break

            except Exception as e:
                msg = f"Unexpected error: {e}"
                ui("ERROR", f"PO {po} unexpected error", msg)
                logging.exception(msg)
                insert_ravyx_po_status(conn, po_number=str(po), vendor=vendor, vendor_f334=vendor_f334, status="FAILED", f1081_text=msg)
                reopen_po_on_failure(conn, str(po))
                break

        ui("DONE", "Maceri PO push completed", f"Auto-close in {AUTO_CLOSE_SECONDS}s.")

    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        try:
            ui_q.put(("ERROR", "Fatal error", str(e)))
            ui_q.put(("DONE", "Stopped", f"Auto-close in {AUTO_CLOSE_SECONDS}s."))
        except Exception:
            pass

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
# Main
# =============================================================================
def main():
    ui_q = Queue()
    ui = VendorSendUI(
        title=f"PLUM – Send PO to Maceri v{APP_VERSION}",
        queue=ui_q,
        auto_close_seconds=AUTO_CLOSE_SECONDS,
    )

    t = threading.Thread(target=worker, args=(ui_q,), daemon=True)
    t.start()
    ui.run()


if __name__ == "__main__":
    main()
