# main.py  (ExportOrdersToMaceriSMS)

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

from ui_send_PO_maceri import VendorSendUI  # your UI file/class


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
# Logging (one file per day in /LOG)
#   ExportOrdersToMaceriSMS_logs_YYYY_MM_DD.log
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

_run_start = time.perf_counter()

# UI auto-close countdown (handled by UI once it receives DONE)
AUTO_CLOSE_SECONDS = 20


# =============================================================================
# Daily API raw log (single file per day)
# =============================================================================
API_LOG_TS = datetime.now().strftime("%Y_%m_%d")
MACERI_API_LOG_PATH = os.path.join(LOG_DIR, f"MaceriApi_{API_LOG_TS}.log")

# If True: only log errors (status >= 400) to MaceriApi_*.log
# If False: log ALL calls (Auth + Order) to MaceriApi_*.log
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
    """
    Append ONE entry to MaceriApi_YYYY_MM_DD.log
    """
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

# ---- Maceri section (URLs come from config.ini) ----
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

SENT_MARKER = "SentToVendor"  # appended to REC_HDR.F1254

logging.info(f"MaceriAuthUrl={API_AUTH_URL}")
logging.info(f"MaceriOrderUrl={API_ORDER_URL}")
logging.info(f"MaceriFbReferer={FB_REFERER}")
logging.info(f"MaceriApiDailyLog={MACERI_API_LOG_PATH} | ErrorsOnly={MACERI_API_LOG_ERRORS_ONLY}")


# =============================================================================
# Helpers: parse Maceri error + friendly message
# =============================================================================
_ITEM_NOT_FOUND_RE = re.compile(
    r"Item\s+(?P<item>\S+)\s+with\s+Uom\s+(?P<uom>\S+)\s+not\s+found", re.IGNORECASE
)


def _extract_item_not_found(detail_text: str) -> Optional[Tuple[str, str]]:
    """Returns (item_code, uom) if matches: 'Item SUMO with Uom CS not found'."""
    if not detail_text:
        return None
    m = _ITEM_NOT_FOUND_RE.search(detail_text)
    if not m:
        return None
    return m.group("item").strip(), m.group("uom").strip()


def _find_upcs_for_itemcode(po_rows, item_code: str) -> List[str]:
    """Map vendor itemCode to UPC(s) from DB rows. Returns unique UPCs."""
    upcs: Set[str] = set()
    if not po_rows:
        return []
    item_code_u = (item_code or "").strip().upper()
    for r in po_rows:
        code = "" if r.ITEMCODE is None else str(r.ITEMCODE).strip().upper()
        if code == item_code_u:
            upc = "" if r.UPC is None else str(r.UPC).strip()
            if upc:
                upcs.add(upc)
    return sorted(upcs)


def _friendly_client_error(po: str, detail_text: str, po_rows) -> str:
    """Client-friendly message from Maceri error."""
    parsed = _extract_item_not_found(detail_text)
    if parsed:
        item_code, uom = parsed
        upcs = _find_upcs_for_itemcode(po_rows, item_code)
        upc_part = f"UPC(s): {', '.join(upcs)}. " if upcs else ""
        return (
            f"Order was NOT sent. Please validate {upc_part}"
            f"The item code '{item_code}' does not exist in the Maceri database (UOM={uom})."
        )

    cleaned = (detail_text or "").strip()
    if cleaned:
        return f"Order was NOT sent. Maceri returned an error: {cleaned}"
    return "Order was NOT sent. Maceri returned an error (see log for details)."


def _extract_detail_from_maceri_response(raw_text: str, fallback: str) -> str:
    """
    Try to parse JSON like:
      { "detail": "...", "message": "..." }
    and return the best human-readable detail.
    """
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


# =============================================================================
# SQL helpers (reuse ONE connection)
# =============================================================================
def get_po_numbers(conn: pyodbc.Connection) -> List[str]:
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


def reopen_po_on_failure(conn: pyodbc.Connection, po_number: str) -> bool:
    """
    If the PO was not sent, revert it to OPEN.
    Guardrails:
      - Only UPSHOP + ORDER
      - Only if currently CLOSE
      - Only if NOT already marked SentToVendor
    """
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
        logging.warning(f"PO {po_number}: set back to OPEN (send failed).")
    else:
        logging.info(f"PO {po_number}: not re-opened (already OPEN / not CLOSE / already marked / not UPSHOP ORDER).")

    return changed


# =============================================================================
# API helpers (Session + token refresh)
# =============================================================================
def get_api_token(session: Session) -> str:
    payload = {"userName": MACERI_USERNAME, "password": MACERI_PASSWORD, "persist": True}
    headers = {"Fb-Referer": FB_REFERER, "Content-Type": "application/json"}

    logging.info("Requesting Maceri auth token ...")
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
        logging.error(f"Auth missing token (status={resp.status_code}) body={raw_text[:300]!r}")
        raise RuntimeError("Missing token in auth response")

    logging.info(f"Auth OK (status={resp.status_code}) in {elapsed:.3f}s.")
    return token


def send_to_api(session: Session, payload: Dict[str, Any], token: str) -> Tuple[bool, int, str, str]:
    """
    Returns (ok, status_code, snippet, raw_text)
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    po_number = payload.get("customerPurchaseOrderNumber", "?")
    line_count = len(payload.get("details", []))

    logging.info(f"Sending PO {po_number} to Maceri: lines={line_count} ...")
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

    logging.info(f"PO {po_number}: API response status={resp.status_code} time={elapsed:.3f}s snippet={snippet!r}")

    if 200 <= resp.status_code < 300:
        logging.info(f"PO {po_number}: Submitted successfully.")
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

    def reopen_ui(po: str, why: str):
        """
        Best-effort: reopen PO and show it in UI/log.
        """
        try:
            if conn is not None:
                reopened = reopen_po_on_failure(conn, str(po))
                if reopened:
                    ui("WARN", f"PO {po} re-opened", why)
        except Exception as e:
            logging.exception(f"PO {po}: Failed to re-open PO after failure: {e}")

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
            ui("DONE", "No PO to send", f"No eligible POs found. Auto-close in {AUTO_CLOSE_SECONDS}s.")
            return

        ui("INFO", "Authenticating with Maceri...", "")
        token = get_api_token(session)
        ui("INFO", "Auth OK", f"PO count={len(po_numbers)}")

        for po in po_numbers:
            totals["processed_pos"] += 1
            ui("INFO", f"Processing PO {po}", "")

            po_rows = get_po_data(conn, po)
            payload, valid, skipped = build_json_payload(po_rows, po)
            totals["valid_lines"] += valid
            totals["skipped_lines"] += skipped

            if not payload:
                totals["skipped_pos"] += 1
                ui("WARN", f"PO {po} skipped", "No valid items to send")
                continue

            try:
                line_count = len(payload.get("details", []))
                ui("INFO", f"Sending PO {po}", f"Lines={line_count}")

                ok, status_code, snippet, raw_text = send_to_api(session, payload, token)

                if not ok and status_code in (401, 403):
                    totals["token_refresh"] += 1
                    ui("WARN", f"PO {po} auth error", f"{status_code} - refreshing token and retrying once")
                    token = get_api_token(session)
                    ok, status_code, snippet, raw_text = send_to_api(session, payload, token)

                if ok:
                    totals["sent_pos"] += 1
                    append_sent_marker(conn, po, SENT_MARKER)
                    ui("INFO", f"SUCCESS PO {po}", "Sent + marked SentToVendor")
                else:
                    totals["skipped_pos"] += 1
                    detail_text = _extract_detail_from_maceri_response(raw_text, snippet)
                    friendly = _friendly_client_error(str(po), detail_text, po_rows)

                    ui("ERROR", f"PO {po} NOT sent", friendly)
                    logging.error(f"PO {po}: {friendly}")

                    # IMPORTANT: revert to OPEN when NOT sent
                    reopen_ui(str(po), "Order was NOT sent, PO status set back to OPEN.")

            except (Timeout, RequestException, HTTPError) as e:
                totals["skipped_pos"] += 1
                ui("ERROR", f"PO {po} request error", str(e))
                logging.exception(f"PO {po}: Request error: {e}")

                # IMPORTANT: revert to OPEN on request error
                reopen_ui(str(po), "Request failed, PO status set back to OPEN.")

            except Exception as e:
                totals["skipped_pos"] += 1
                ui("ERROR", f"PO {po} unexpected error", str(e))
                logging.exception(f"PO {po}: Unexpected error: {e}")

                # IMPORTANT: revert to OPEN on unexpected error
                reopen_ui(str(po), "Unexpected error, PO status set back to OPEN.")

        dur = round(time.perf_counter() - _run_start, 2)
        summary = (
            f"Processed={totals['processed_pos']} | Sent={totals['sent_pos']} | Failed/Skipped={totals['skipped_pos']} | "
            f"ValidLines={totals['valid_lines']} | SkippedLines={totals['skipped_lines']} | "
            f"TokenRefresh={totals['token_refresh']} | Duration={dur}s"
        )

        logging.info("Summary | " + summary)
        logging.info("=== End run (MaceriPush) ===")
        ui("DONE", "Maceri PO push completed", f"{summary} | Auto-close in {AUTO_CLOSE_SECONDS}s.")

    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        ui("ERROR", "Fatal error", str(e))
        ui("DONE", "Stopped", f"See error above | Auto-close in {AUTO_CLOSE_SECONDS}s.")

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
