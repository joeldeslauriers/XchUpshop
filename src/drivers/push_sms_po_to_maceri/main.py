# main.py  (ExportOrdersToMaceriSMS)
# Fixes:
# - SQI passes PO (F1032) + Vendor (F27) as EXE args; send ONLY that PO
# - RAVYX_PO_STATUS mapping:
#     F27   = vendor# (INT)
#     F334  = vendor name (TEXT) lookup from VENDOR_TAB using vendor#
#     F02   = 'Order Export'
#     F29   = WARN / SUCCESS / FAILED
#     F1081 = message (skipped UPC or error)
#     F03   = Dept (from REC_REG.F03, take first row)
#     F91   = REC_HDR.F91 for Order Export SUCCESS/WARN, else runid on errors
# - WARN row inserted AFTER delete, only if delete happened
# - If NOT sent successfully, keep PO OPEN and ensure NO SentToVendor marker
# - Purge logs based on [Settings] LogPurge (days)
# - Clearer network/auth messages in UI (English)

import os
import sys
import time
import logging
import threading
import configparser
import re
import json
from datetime import datetime, timedelta
from pathlib import Path
from collections import namedtuple
from typing import List, Tuple, Optional, Dict, Any, Set
from queue import Queue

import pyodbc
import requests
from requests import Session
from requests.exceptions import RequestException, Timeout, HTTPError, ConnectionError

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
# Logs Purge
# =============================================================================
def purge_logs(log_dir: str, days_to_keep: int) -> int:
    """
    Delete log files older than days_to_keep in log_dir.
    Targets:
      - MaceriApi_YYYY_MM_DD.log
      - ExportOrdersToMaceriSMS_logs_YYYY_MM_DD.log
    Returns number of deleted files.
    """
    if days_to_keep is None or days_to_keep <= 0:
        return 0

    cutoff = datetime.now() - timedelta(days=days_to_keep)
    deleted = 0

    p = Path(log_dir)
    if not p.exists():
        return 0

    patterns = [
        "MaceriApi_*.log",
        "ExportOrdersToMaceriSMS_logs_*.log",
    ]

    for pat in patterns:
        for f in p.glob(pat):
            try:
                mtime = datetime.fromtimestamp(f.stat().st_mtime)
                if mtime < cutoff:
                    f.unlink()
                    deleted += 1
            except Exception as e:
                logging.warning(f"Log purge: unable to delete {f}: {e}")

    return deleted


# =============================================================================
# Config (read once) + Purge call
# =============================================================================
config = configparser.ConfigParser()
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"config.ini not found next to EXE/script: {CONFIG_PATH}")

config.read(CONFIG_PATH, encoding="utf-8")

LOG_PURGE_DAYS = config["Settings"].getint("LogPurge", fallback=30)
try:
    deleted = purge_logs(LOG_DIR, LOG_PURGE_DAYS)
    logging.info(f"Log purge: deleted {deleted} file(s) older than {LOG_PURGE_DAYS} day(s).")
except Exception as e:
    logging.warning(f"Log purge failed: {e}")

server_name = config["Settings"]["ServerName"]
sql_driver = config["Settings"].get("SQLDriver", "SQL Server")
database = config["Settings"].get("DatabaseName", "STORESQL")
connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes"

# Maceri settings (validated inside worker so we can still force PO open on failure)
LOCATION_ID = config.get("Maceri", "MaceriLocationID", fallback="").strip()
MACERI_USERNAME = config.get("Maceri", "Username", fallback="").strip()
MACERI_PASSWORD = config.get("Maceri", "Password", fallback="").strip()
API_AUTH_URL = config.get("Maceri", "BaseUrlAuth", fallback="").strip()
API_ORDER_URL = config.get("Maceri", "BaseUrlOrder", fallback="").strip()
FB_REFERER = config.get("Maceri", "FbReferer", fallback="https://tommaceri-test.fbportal.io/").strip()

CUSTOMER_ID = "PLUMS"
NOTES = "FROMAPI"
SHIP_METHOD = "1"

SENT_MARKER = "SentToVendor"
MAX_INVALID_ITEM_RETRIES_PER_PO = 25

logging.info(f"SQL={server_name} / {database}")
logging.info(f"MaceriAuthUrl={API_AUTH_URL}")
logging.info(f"MaceriOrderUrl={API_ORDER_URL}")
logging.info(f"MaceriFbReferer={FB_REFERER}")


# =============================================================================
# Daily API raw log
# =============================================================================
API_LOG_TS = datetime.now().strftime("%Y_%m_%d")
MACERI_API_LOG_PATH = os.path.join(LOG_DIR, f"MaceriApi_{API_LOG_TS}.log")
MACERI_API_LOG_ERRORS_ONLY = False

logging.info(f"MaceriApiDailyLog={MACERI_API_LOG_PATH} | ErrorsOnly={MACERI_API_LOG_ERRORS_ONLY}")


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

    hdr_lines: List[str] = []
    for k, v in (headers or {}).items():
        if k.lower() == "authorization":
            vv = str(v or "").strip()
            if vv.lower().startswith("bearer "):
                token_only = vv.split(" ", 1)[1].strip()
                hdr_lines.append(f"{k}: Bearer {_mask_value(token_only, keep=10)}")
            else:
                hdr_lines.append(f"{k}: {_mask_value(vv, keep=10)}")
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
# Clear error messages for UI (English)
# =============================================================================
def format_requests_error(e: Exception, url: str = "") -> Tuple[str, str]:
    raw = str(e) or repr(e)
    low = raw.lower()

    if (
        "failed to establish a new connection" in low
        or "newconnectionerror" in low
        or "name or service not known" in low
        or "nodename nor servname provided" in low
        or "getaddrinfo failed" in low
    ):
        return (
            "Network error (DNS/connection)",
            f"Cannot reach the server.\nURL: {url}\nCheck: internet/VPN, DNS, firewall/proxy, and that the URL is correct.\n\nDetails: {raw}",
        )

    if "max retries exceeded" in low:
        return (
            "Network error (max retries)",
            f"Server did not respond or is unreachable (max retries exceeded).\nURL: {url}\nCheck: network, firewall/proxy, and server availability.\n\nDetails: {raw}",
        )

    if "timed out" in low or isinstance(e, Timeout):
        return (
            "Network timeout",
            f"Server is not responding in time (timeout).\nURL: {url}\nTry again later or check connectivity.\n\nDetails: {raw}",
        )

    if "401" in low or "unauthorized" in low:
        return (
            "Authentication failed (401)",
            f"Authentication was rejected.\nURL: {url}\nCheck Username/Password in config.ini.\n\nDetails: {raw}",
        )

    if "403" in low or "forbidden" in low:
        return (
            "Access forbidden (403)",
            f"Access denied by the server.\nURL: {url}\nCheck account permissions on Maceri side.\n\nDetails: {raw}",
        )

    return (
        "API communication error",
        f"Unable to communicate with the API.\nURL: {url}\n\nDetails: {raw}",
    )


# =============================================================================
# Invalid item detection
# =============================================================================
_ITEM_NOT_FOUND_RE = re.compile(
    r"Item\s+(?P<item>\S+)\s+with\s+Uom\s+(?P<uom>\S+)\s+not\s+found",
    re.IGNORECASE,
)


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
def get_vendor_name(conn: pyodbc.Connection, vendor: int) -> Optional[str]:
    q = "SELECT TOP 1 LTRIM(RTRIM(CAST(F334 AS varchar(60)))) FROM [dbo].[VENDOR_TAB] WHERE F27 = ?"
    cur = conn.cursor()
    cur.execute(q, (vendor,))
    row = cur.fetchone()
    if not row:
        return None
    name = (row[0] or "").strip()
    return name or None


def get_rec_hdr_f91(conn: pyodbc.Connection, po_number: str) -> Optional[str]:
    """
    Returns REC_HDR.F91 for the PO. Keep as string to avoid type surprises.
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT TOP 1 F91 FROM [dbo].[REC_HDR] WHERE F1032 = ?", (po_number,))
        row = cur.fetchone()
        if not row:
            return None
        v = row[0]
        if v is None:
            return None
        s = str(v).strip()
        return s or None
    except Exception:
        return None


def get_po_data(conn: pyodbc.Connection, po_number: str):
    query = """
        SELECT
            REG.F1032 AS PO,
            REG.F01   AS UPC,
            REG.F26   AS ITEMCODE,
            REG.F38   AS COST,
            REG.F75   AS QTY,
            HDR.F254  AS DDATE,
            REG.F03   AS DEPT
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


def get_first_dept(po_rows) -> Optional[int]:
    if not po_rows:
        return None
    try:
        d = po_rows[0].DEPT
        return int(d) if d is not None else None
    except Exception:
        return None


def insert_ravyx_po_status(
    conn: pyodbc.Connection,
    *,
    po_number: str,
    vendor: int,
    vendor_name: Optional[str],
    status: str,  # WARN / SUCCESS / FAILED
    f1081_text: str,
    dept: Optional[int],
    rec_hdr_f91: Optional[str],
    process: str = "Order Export",
):
    """
    F91 rule:
      - For Order Export + SUCCESS/WARN => use REC_HDR.F91 if available
      - Otherwise (errors) => use runid
    """
    runid = datetime.now().strftime("%y%m%d%H%M%S")
    now = datetime.now()

    f1081_val = (f1081_text or "").strip() or None
    vname = (vendor_name or "").strip() or None

    use_hdr_f91 = (process == "Order Export" and status in ("SUCCESS", "WARN") and (rec_hdr_f91 or "").strip())
    f91_value = (rec_hdr_f91.strip() if use_hdr_f91 else runid)

    sql = """
        INSERT INTO [dbo].[RAVYX_PO_STATUS]
            (F1032, F91, F02, F27, F334, F254, F29, F1081, F03)
        VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    params = (
        int(po_number),
        f91_value,
        process,
        int(vendor),
        vname,
        now,
        status,
        f1081_val,
        int(dept) if dept is not None else None,
    )

    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()

    logging.info(
        f"RAVYX_PO_STATUS inserted: PO={po_number} F91={f91_value} Process={process} "
        f"Vendor={vendor} VendorName={vname!r} Dept={dept!r} F29={status} F1081={f1081_val!r}"
    )


def append_sent_marker(conn: pyodbc.Connection, po_number: str, marker: str) -> bool:
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


def _clean_marker_from_f1254(f1254: Optional[str], marker: str) -> Optional[str]:
    if not f1254:
        return None
    parts = [p.strip() for p in str(f1254).split("|")]
    parts = [p for p in parts if p and p.lower() != marker.lower()]
    cleaned = " | ".join(parts).strip()
    return cleaned or None


def force_keep_po_open_on_failure(conn: pyodbc.Connection, po_number: str, marker: str = SENT_MARKER) -> None:
    cur = conn.cursor()

    cur.execute("SELECT F1254 FROM [dbo].[REC_HDR] WHERE F1032 = ?", (po_number,))
    row = cur.fetchone()
    current_f1254 = row[0] if row else None
    new_f1254 = _clean_marker_from_f1254(current_f1254, marker)

    cur.execute(
        """
        UPDATE [dbo].[REC_HDR]
        SET F1067 = 'OPEN',
            F1254 = ?
        WHERE F1032 = ?
        """,
        (new_f1254, po_number),
    )
    conn.commit()

    logging.warning(f"PO {po_number}: forced OPEN (failure path). Marker '{marker}' removed if present.")


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
def _validate_maceri_config() -> None:
    missing = []
    if not LOCATION_ID:
        missing.append("[Maceri] MaceriLocationID")
    if not MACERI_USERNAME:
        missing.append("[Maceri] Username")
    if not MACERI_PASSWORD:
        missing.append("[Maceri] Password")
    if not API_AUTH_URL:
        missing.append("[Maceri] BaseUrlAuth")
    if not API_ORDER_URL:
        missing.append("[Maceri] BaseUrlOrder")

    if missing:
        raise RuntimeError("Missing config.ini value(s): " + ", ".join(missing))


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
    conn: Optional[pyodbc.Connection] = None

    po = ""
    vendor = 0
    vendor_name: Optional[str] = None

    try:
        po, vendor = get_sqi_args()
        logging.info(f"SQI args: PO={po} Vendor={vendor}")

        ui("INFO", f"Starting Maceri Push v{APP_VERSION}", "")
        ui("INFO", "SQI Parameters", f"PO={po} | Vendor={vendor}")
        ui("INFO", "Loading config.ini...", os.path.basename(CONFIG_PATH))

        ui("INFO", "Connecting to SQL...", f"{server_name} / {database}")
        conn = pyodbc.connect(connection_string)

        vendor_name = get_vendor_name(conn, vendor)
        rec_hdr_f91 = get_rec_hdr_f91(conn, str(po))
        logging.info(f"Vendor lookup: F27={vendor} => VendorName={vendor_name!r}")
        logging.info(f"REC_HDR lookup: PO={po} => F91={rec_hdr_f91!r}")

        _validate_maceri_config()

        ui("INFO", "Authenticating with Maceri...", "")
        try:
            token = get_api_token(session)
            ui("INFO", "Auth OK", "Sending single PO from SQI")
        except (ConnectionError, Timeout, HTTPError, RequestException) as e:
            title, detail = format_requests_error(e, API_AUTH_URL)
            ui("ERROR", title, detail)
            logging.exception(f"Auth/network error: {e}")

            insert_ravyx_po_status(
                conn,
                po_number=str(po),
                vendor=vendor,
                vendor_name=vendor_name,
                status="FAILED",
                f1081_text=f"AUTH failed: {title} | {detail}",
                dept=None,
                rec_hdr_f91=rec_hdr_f91,
                process="Order Export",
            )
            force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
            ui("DONE", "Stopped", f"Auto-close in {AUTO_CLOSE_SECONDS}s.")
            return

        ui("INFO", f"Processing PO {po}", "")
        invalid_retry = 0

        while True:
            po_rows = get_po_data(conn, po)
            dept = get_first_dept(po_rows)

            payload, valid, skipped = build_json_payload(po_rows, po)

            if not payload:
                msg = f"PO {po} has no valid items to send after cleanup."
                ui("ERROR", f"PO {po} NOT sent", msg)
                logging.error(msg)
                insert_ravyx_po_status(
                    conn,
                    po_number=str(po),
                    vendor=vendor,
                    vendor_name=vendor_name,
                    status="FAILED",
                    f1081_text=msg,
                    dept=dept,
                    rec_hdr_f91=rec_hdr_f91,
                    process="Order Export",
                )
                force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
                break

            try:
                ok, status_code, snippet, raw_text = send_to_api(session, payload, token)

                if not ok and status_code in (401, 403):
                    ui("WARN", "Authentication error", "Received 401/403. Refreshing token and retrying once...")
                    try:
                        token = get_api_token(session)
                    except (ConnectionError, Timeout, HTTPError, RequestException) as e:
                        title, detail = format_requests_error(e, API_AUTH_URL)
                        ui("ERROR", title, detail)
                        logging.exception(f"Token refresh error: {e}")
                        insert_ravyx_po_status(
                            conn,
                            po_number=str(po),
                            vendor=vendor,
                            vendor_name=vendor_name,
                            status="FAILED",
                            f1081_text=f"Token refresh failed: {title} | {detail}",
                            dept=dept,
                            rec_hdr_f91=rec_hdr_f91,
                            process="Order Export",
                        )
                        force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
                        break

                    ok, status_code, snippet, raw_text = send_to_api(session, payload, token)

                if ok:
                    append_sent_marker(conn, po, SENT_MARKER)
                    ui("INFO", f"SUCCESS PO {po}", "Sent and marked SentToVendor")
                    insert_ravyx_po_status(
                        conn,
                        po_number=str(po),
                        vendor=vendor,
                        vendor_name=vendor_name,
                        status="SUCCESS",
                        f1081_text="",
                        dept=dept,
                        rec_hdr_f91=rec_hdr_f91,
                        process="Order Export",
                    )
                    break

                detail_text = _extract_detail_from_maceri_response(raw_text, snippet)
                invalid = _extract_item_not_found(detail_text)

                if invalid:
                    item_code, uom = invalid
                    invalid_retry += 1

                    upcs = _find_upcs_for_itemcode(po_rows, item_code)
                    skipped_info = _build_skipped_info(item_code, upcs)

                    deleted = delete_invalid_item_from_po(conn, str(po), item_code)

                    if deleted > 0:
                        insert_ravyx_po_status(
                            conn,
                            po_number=str(po),
                            vendor=vendor,
                            vendor_name=vendor_name,
                            status="WARN",
                            f1081_text=skipped_info,
                            dept=dept,
                            rec_hdr_f91=rec_hdr_f91,
                            process="Order Export",
                        )
                        ui("WARN", f"PO {po} - item skipped", f"{skipped_info} (removed, retrying)")
                        logging.warning(
                            f"PO {po}: Invalid itemCode '{item_code}' (UOM={uom}). "
                            f"UPC(s): {', '.join(upcs) if upcs else '?'}."
                        )

                    if invalid_retry > MAX_INVALID_ITEM_RETRIES_PER_PO:
                        msg = (
                            f"PO {po} still failing after max invalid-item retries. "
                            f"Last invalid itemCode={item_code} UOM={uom}."
                        )
                        ui("ERROR", f"PO {po} NOT sent", msg)
                        logging.error(msg)
                        insert_ravyx_po_status(
                            conn,
                            po_number=str(po),
                            vendor=vendor,
                            vendor_name=vendor_name,
                            status="FAILED",
                            f1081_text=msg,
                            dept=dept,
                            rec_hdr_f91=rec_hdr_f91,
                            process="Order Export",
                        )
                        force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
                        break

                    continue

                msg = f"Order was NOT sent. Maceri error: {detail_text}"
                ui("ERROR", f"PO {po} NOT sent", msg)
                logging.error(msg)
                insert_ravyx_po_status(
                    conn,
                    po_number=str(po),
                    vendor=vendor,
                    vendor_name=vendor_name,
                    status="FAILED",
                    f1081_text=msg,
                    dept=dept,
                    rec_hdr_f91=rec_hdr_f91,
                    process="Order Export",
                )
                force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
                break

            except (ConnectionError, Timeout, HTTPError, RequestException) as e:
                title, detail = format_requests_error(e, API_ORDER_URL)
                ui("ERROR", title, detail)
                logging.exception(f"Network/API error: {e}")
                insert_ravyx_po_status(
                    conn,
                    po_number=str(po),
                    vendor=vendor,
                    vendor_name=vendor_name,
                    status="FAILED",
                    f1081_text=f"Request failed: {title} | {detail}",
                    dept=dept,
                    rec_hdr_f91=rec_hdr_f91,
                    process="Order Export",
                )
                force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
                break

            except Exception as e:
                msg = f"Unexpected error: {e}"
                ui("ERROR", "Unexpected error", msg)
                logging.exception(msg)
                insert_ravyx_po_status(
                    conn,
                    po_number=str(po),
                    vendor=vendor,
                    vendor_name=vendor_name,
                    status="FAILED",
                    f1081_text=msg,
                    dept=dept,
                    rec_hdr_f91=rec_hdr_f91,
                    process="Order Export",
                )
                force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
                break

        ui("DONE", "Maceri PO push completed", f"Auto-close in {AUTO_CLOSE_SECONDS}s.")

    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        try:
            if conn is not None and po:
                try:
                    force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
                except Exception:
                    pass

            err_txt = str(e)
            if "Missing config.ini value(s)" in err_txt:
                ui_q.put(("ERROR", "Invalid config.ini", err_txt))
            else:
                ui_q.put(("ERROR", "Fatal error", err_txt))

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
