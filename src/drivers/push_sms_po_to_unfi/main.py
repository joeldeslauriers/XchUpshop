# main.py - pushSMSPOtoUNFI (with UI + safer logging + better error capture)
# LEGACY ONLY (NO SWAGGER)
# Usage (from SMS): pushSMSPOtoUNFI.exe <F1032_PO> <F27_VENDOR>
# Example: pushSMSPOtoUNFI.exe 123456 3954
#
# Legacy endpoint format:
#   {ApiBaseUrl}/stores/{ApiStoreChainId}/orders?storeId={UNFI_STORE_ID}
#
# IMPORTANT:
#   UNFI_STORE_ID is the 4+ digit store id (ex: 127705), NOT SMS StoreNumber (=3).
#   It comes from vendor section: [UNFIGW]/[UNFIRC] StoreID
#
# DEV requires:
#   AuthUrl=https://password-auth.dev.geniuscentral.com/
#   ClientId=1mh9eiv2dn67vr5ugb651etn0h
#   ApiBaseUrl=https://posapi.dev.geniuscentral.com
#   ApiStoreChainId=142139
#   [UNFIGW]/[UNFIRC] StoreID must be one of the DEV store ids they configured (ex: 127705)

import os
import sys
import time
import json
import logging
import configparser
import threading
import queue
from datetime import datetime, timedelta
from pathlib import Path
from collections import namedtuple
from typing import Optional, Dict, Any, Tuple, List
import uuid

import pyodbc
import requests
from requests import Session
from requests.exceptions import RequestException, Timeout, HTTPError, ConnectionError


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
      pushSMSPOtoUNFI.exe "<F1032>" "<F27>"
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
# Logging (file + console + optional UI queue handler)
# =============================================================================
LOG_DIR = os.path.join(BASE_DIR, "Log")
os.makedirs(LOG_DIR, exist_ok=True)

log_ts = datetime.now().strftime("%Y_%m_%d")
log_filename = os.path.join(LOG_DIR, f"pushSMSPOtoUNFI_logs_{log_ts}.log")


class UIQueueHandler(logging.Handler):
    """Mirror log lines into a UI queue as (LEVEL, MSG, DETAIL)."""

    def __init__(self, q: "queue.Queue"):
        super().__init__()
        self.q = q

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
            lvl = record.levelname.upper()
            self.q.put((lvl, msg, ""))
        except Exception:
            pass


def setup_logging(ui_q: Optional["queue.Queue"] = None) -> None:
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

    if ui_q is not None:
        ui_handler = UIQueueHandler(ui_q)
        ui_handler.setLevel(logging.INFO)
        root_logger.addHandler(ui_handler)


# =============================================================================
# Logs Purge
# =============================================================================
def purge_logs(log_dir: str, days_to_keep: int) -> int:
    if days_to_keep is None or days_to_keep <= 0:
        return 0

    cutoff = datetime.now() - timedelta(days=days_to_keep)
    deleted = 0

    p = Path(log_dir)
    if not p.exists():
        return 0

    patterns = [
        "pushSMSPOtoUNFI_logs_*.log",
        "UNFI_Api_*.log",
        "UNFI_payload_*.json",
        "UNFI_error_*.log",
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
# Config (read once)
# =============================================================================
config = configparser.ConfigParser()
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"config.ini not found next to EXE/script: {CONFIG_PATH}")

config.read(CONFIG_PATH, encoding="utf-8")

LOG_PURGE_DAYS = config["Settings"].getint("LogPurge", fallback=30)

server_name = config["Settings"]["ServerName"]
sql_driver = config["Settings"].get("SQLDriver", "SQL Server")
database = config["Settings"].get("DatabaseName", "STORESQL")

# SMS store number is still logged (and may be useful for other integrations),
# but UNFI legacy storeId must come from vendor_cfg["StoreID"] (4+ digits).
store_number_sms = int(config["Settings"].get("StoreNumber", "0"))

connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes"

# UNFI shared settings
UNFI_AUTH_URL = config.get("UNFI", "AuthUrl", fallback="").strip()
UNFI_API_BASE = config.get("UNFI", "ApiBaseUrl", fallback="").strip().rstrip("/")
# REQUIRED for LEGACY endpoint
UNFI_CHAIN_ID = config.get("UNFI", "ApiStoreChainId", fallback="").strip()

UNFI_USERNAME = config.get("UNFI", "Username", fallback="").strip()
UNFI_PASSWORD = config.get("UNFI", "Password", fallback="").strip().strip('"')
UNFI_CLIENT_ID = config.get("UNFI", "ClientId", fallback="").strip()
TIMEOUT_AUTH_SEC = config.getint("UNFI", "TimeoutAuthSec", fallback=90)
TIMEOUT_POST_SEC = config.getint("UNFI", "TimeoutPostSec", fallback=120)

if not UNFI_AUTH_URL or not UNFI_API_BASE or not UNFI_USERNAME or not UNFI_PASSWORD or not UNFI_CLIENT_ID or not UNFI_CHAIN_ID:
    missing = []
    if not UNFI_AUTH_URL:
        missing.append("[UNFI] AuthUrl")
    if not UNFI_API_BASE:
        missing.append("[UNFI] ApiBaseUrl")
    if not UNFI_CHAIN_ID:
        missing.append("[UNFI] ApiStoreChainId")
    if not UNFI_USERNAME:
        missing.append("[UNFI] Username")
    if not UNFI_PASSWORD:
        missing.append("[UNFI] Password")
    if not UNFI_CLIENT_ID:
        missing.append("[UNFI] ClientId")
    raise RuntimeError("Missing config.ini value(s): " + ", ".join(missing))

# Do NOT block on StoreNumber anymore for UNFI. It's not used for storeId in UNFI legacy.
if store_number_sms <= 0:
    logging.warning("Config [Settings] StoreNumber is <= 0. Not used for UNFI storeId (uses vendor StoreID).")

SENT_MARKER = "SentToVendor"


def vendor_section_from_f27(vendor_id: int) -> str:
    if vendor_id == 3954:
        return "UNFIGW"
    if vendor_id == 7313:
        return "UNFIRC"
    raise ValueError(f"Unsupported UNFI vendor id (F27): {vendor_id}")


def read_vendor_cfg(vendor_id: int) -> Dict[str, str]:
    """
    LEGACY endpoint uses:
      /stores/{ApiStoreChainId}/orders?storeId={UNFI_STORE_ID}

    UNFI_STORE_ID is a 4+ digit ID (ex: 127705), NOT SMS StoreNumber.
    Read it from vendor section: [UNFIGW]/[UNFIRC] StoreID
    """
    sect = vendor_section_from_f27(vendor_id)
    if sect not in config:
        raise RuntimeError(f"Missing config.ini section: [{sect}] for vendor {vendor_id}")

    for k in ("SupplierName", "AccountNumber", "StoreID"):
        if not config[sect].get(k, "").strip():
            raise RuntimeError(f"Missing config.ini value: [{sect}] {k}")

    # Validate StoreID is numeric (4+ digits usually, but just ensure numeric here)
    store_id_raw = config[sect]["StoreID"].strip().strip('"')
    if not store_id_raw.isdigit():
        raise RuntimeError(f"Invalid config.ini value: [{sect}] StoreID must be numeric. Got: {store_id_raw!r}")

    return {
        "SupplierId": config[sect].get("SupplierId", str(vendor_id)).strip(),
        "SupplierName": config[sect]["SupplierName"].strip(),
        "AccountNumber": config[sect]["AccountNumber"].strip(),
        "StoreID": store_id_raw,  # UNFI Store ID (4+ digits)
        "Section": sect,
    }


def build_unfi_order_url(vendor_cfg: Dict[str, str]) -> str:
    """
    LEGACY endpoint:
      {ApiBaseUrl}/stores/{ApiStoreChainId}/orders?storeId={UNFI_STORE_ID}
    """
    chain_id = int(UNFI_CHAIN_ID)
    unfi_store_id = int(vendor_cfg["StoreID"])
    return f"{UNFI_API_BASE}/stores/{chain_id}/orders?storeId={unfi_store_id}"


# =============================================================================
# Daily API raw log (Maceri-style)
# =============================================================================
API_LOG_TS = datetime.now().strftime("%Y_%m_%d")
UNFI_API_LOG_PATH = os.path.join(LOG_DIR, f"UNFI_Api_{API_LOG_TS}.log")
UNFI_API_LOG_ERRORS_ONLY = False


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


def _write_unfi_api_log(
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
    if UNFI_API_LOG_ERRORS_ONLY and status_code < 400:
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
        with open(UNFI_API_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(entry)
    except Exception as e:
        logging.warning(f"Unable to write UNFI API daily log: {e}")


# =============================================================================
# Clear error messages (like Maceri)
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
            f"Authentication was rejected.\nURL: {url}\nCheck Username/Password/ClientId in config.ini.\n\nDetails: {raw}",
        )

    if "403" in low or "forbidden" in low:
        return (
            "Access forbidden (403)",
            f"Access denied by the server.\nURL: {url}\nCheck account permissions on UNFI/SPS side.\n\nDetails: {raw}",
        )

    return (
        "API communication error",
        f"Unable to communicate with the API.\nURL: {url}\n\nDetails: {raw}",
    )


# =============================================================================
# SQL helpers
# =============================================================================
def get_vendor_name(conn: pyodbc.Connection, vendor: int) -> Optional[str]:
    q = """
    SELECT TOP 1 LTRIM(RTRIM(CAST(F334 AS varchar(60))))
    FROM [dbo].[VENDOR_TAB]
    WHERE LTRIM(RTRIM(CAST(F27 AS varchar(30)))) = ?
    """
    cur = conn.cursor()
    cur.execute(q, (str(vendor),))
    row = cur.fetchone()
    if not row:
        return None
    name = (row[0] or "").strip()
    return name or None


def get_rec_hdr_f91(conn: pyodbc.Connection, po_number: str) -> Optional[str]:
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


def get_po_rows(conn: pyodbc.Connection, po_number: str):
    q = """
        SELECT
            REG.F1032 AS PO,
            REG.F03   AS DEPT,
            REG.F1041 AS DESCR,
            REG.F01   AS UPC,
            REG.F26   AS ITEMCODE,
            REG.F38   AS COST,
            REG.F75   AS QTY,
            COST.F19  AS PACKSIZE
        FROM [dbo].[REC_REG] REG
        JOIN [dbo].[COST_TAB] COST ON REG.F01 = COST.F01
        WHERE REG.F1032 = ?
          AND (COST.F90 = 1 OR COST.F90 IS NULL)
        ORDER BY REG.F01
    """
    cur = conn.cursor()
    cur.execute(q, (po_number,))
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


def get_order_total_rec_ttl(conn: pyodbc.Connection, po_number: str) -> Optional[float]:
    q = """
        SELECT TOP 1 F65
        FROM [dbo].[REC_TTL]
        WHERE F1032 = ? AND F1034 = 8201
    """
    cur = conn.cursor()
    cur.execute(q, (po_number,))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    try:
        return float(row[0])
    except Exception:
        return None


def insert_ravyx_po_status(
    conn: pyodbc.Connection,
    *,
    po_number: str,
    vendor: int,
    vendor_name: Optional[str],
    status: str,
    f1081_text: str,
    dept: Optional[int],
    rec_hdr_f91: Optional[str],
    process: str = "Order Export",
):
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


def set_po_closed(conn: pyodbc.Connection, po_number: str) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE [dbo].[REC_HDR] SET F1067 = 'CLOSE' WHERE F1032 = ?", (po_number,))
    conn.commit()


# =============================================================================
# Payload builder
# =============================================================================
def _validate_row(row) -> Tuple[bool, str]:
    if row.ITEMCODE is None or str(row.ITEMCODE).strip() == "":
        return False, "Missing ITEMCODE"
    try:
        qty_int = int(row.QTY)
        if qty_int <= 0:
            return False, "Invalid QTY (<=0)"
    except Exception:
        return False, "Invalid QTY (non-integer)"
    try:
        _ = float(row.COST)
    except Exception:
        return False, "Invalid COST (non-numeric)"
    return True, ""


def save_payload(po_number: str, vendor_id: int, payload: Dict[str, Any]) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(LOG_DIR, f"UNFI_payload_PO{po_number}_V{vendor_id}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def build_unfi_payload(
    *,
    conn: pyodbc.Connection,
    po_number: str,
    vendor_id: int,
    vendor_cfg: Dict[str, str],
    po_rows,
) -> Tuple[Optional[Dict[str, Any]], int, int, str]:
    if not po_rows:
        return None, 0, 0, "No REC_REG rows found for this PO."

    todays_date = datetime.now().strftime("%Y-%m-%dT%H:%M")
    unique_order_id = str(uuid.uuid4())  # ALWAYS new per run

    valid = 0
    skipped = 0

    rec_ttl_total = get_order_total_rec_ttl(conn, po_number)

    fallback_total = 0.0
    for r in po_rows:
        ok, reason = _validate_row(r)
        if not ok:
            skipped += 1
            logging.warning(f"PO {po_number}: Skipped line UPC={getattr(r,'UPC','?')} — {reason}")
            continue
        fallback_total += float(r.COST)

    used_total = rec_ttl_total if rec_ttl_total is not None else fallback_total

    # IMPORTANT: storeID in header must match the UNFI_STORE_ID used in the URL
    unfi_store_id = int(vendor_cfg["StoreID"])

    payload = {
        "header": {
            "orderStatus": 1,
            "orderSource": 1300,
            "storePONumber": str(po_number),
            "uniqueOrderID": unique_order_id,
            "supplierID": int(vendor_cfg.get("SupplierId") or vendor_id),
            "storeName": "Plum Market- West Bloomfield",
            "orderTotal": float(used_total),
            "dateCreated": todays_date,
            "accountNumber": vendor_cfg["AccountNumber"],
            "storeID": unfi_store_id,
            "supplierName": vendor_cfg["SupplierName"],
            "message": "Please Ship ASAP",
            "shipToAddress1": "6565 Orchard Lake Road",
            "shipToAddress2": "",
            "shipToCity": "West Bloomfield",
            "shipToState": "MI",
            "shipToPostalCode": "48322",
        },
        "details": [],
    }

    for r in po_rows:
        ok, _reason = _validate_row(r)
        if not ok:
            continue

        try:
            cps = int(float(r.PACKSIZE)) if r.PACKSIZE is not None else 1
            if cps <= 0:
                cps = 1
        except Exception:
            cps = 1

        payload["details"].append(
            {
                "casePackSize": cps,
                "uom": "CS",
                "gtin": str(r.UPC),
                "cost": float(r.COST),
                "quantity": int(r.QTY),
                "itemDescription": str(r.DESCR),
                "pagePartition": 1,
                "supplierSKU": str(r.ITEMCODE),
            }
        )
        valid += 1

    if not payload["details"]:
        return None, valid, skipped, "No valid lines to send (payload empty)."

    msg = f"orderTotal REC_TTL(F1034=8201)={rec_ttl_total} | fallback(sum valid F38)={fallback_total} | used={used_total}"
    logging.info(f"PO {po_number}: {msg}")
    return payload, valid, skipped, msg


# =============================================================================
# Auth / Send
# =============================================================================
def get_unfi_token(session: Session) -> str:
    url = UNFI_AUTH_URL
    headers = {"Content-Type": "application/json"}
    req = {"client_id": UNFI_CLIENT_ID, "username": UNFI_USERNAME, "password": UNFI_PASSWORD}

    t0 = time.perf_counter()
    resp = session.post(url, headers=headers, json=req, timeout=TIMEOUT_AUTH_SEC)
    elapsed = time.perf_counter() - t0

    _write_unfi_api_log(
        kind="AUTH",
        url=url,
        status_code=resp.status_code,
        elapsed_s=elapsed,
        headers=headers,
        request_json={"client_id": UNFI_CLIENT_ID, "username": UNFI_USERNAME, "password": "***"},
        response_text=resp.text or "",
        po_number=None,
    )

    resp.raise_for_status()
    data = resp.json()
    auth_res = data.get("AuthenticationResult") or {}

    # Legacy scripts often used IdToken, but some environments may accept AccessToken.
    token = auth_res.get("IdToken") or auth_res.get("AccessToken")
    if not token:
        raise RuntimeError("Missing AuthenticationResult.IdToken/AccessToken in auth response")
    return token


def post_unfi_order(session: Session, token: str, payload: Dict[str, Any], vendor_cfg: Dict[str, str]) -> requests.Response:
    url = build_unfi_order_url(vendor_cfg)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    po_number = str((payload.get("header") or {}).get("storePONumber") or "?")

    t0 = time.perf_counter()
    resp = session.post(url, headers=headers, json=payload, timeout=TIMEOUT_POST_SEC)
    elapsed = time.perf_counter() - t0

    _write_unfi_api_log(
        kind="ORDER",
        url=url,
        status_code=resp.status_code,
        elapsed_s=elapsed,
        headers={"Content-Type": "application/json", "Authorization": headers["Authorization"]},
        request_json=payload,
        response_text=resp.text or "",
        po_number=po_number,
    )
    return resp


# =============================================================================
# Core job (returns exit code)
# =============================================================================
def run_job() -> int:
    start = time.perf_counter()

    po = ""
    vendor = 0

    session = requests.Session()
    conn: Optional[pyodbc.Connection] = None

    try:
        logging.info("=== Start run (UNFIPush) ===")
        logging.info(f"AppVersion={APP_VERSION} | Frozen={_is_frozen()} | BaseDir={BASE_DIR}")
        logging.info(f"ConfigPath={CONFIG_PATH}")

        try:
            deleted = purge_logs(LOG_DIR, LOG_PURGE_DAYS)
            logging.info(f"Log purge: deleted {deleted} file(s) older than {LOG_PURGE_DAYS} day(s).")
        except Exception as e:
            logging.warning(f"Log purge failed: {e}")

        logging.info(f"SQL={server_name} / {database} | StoreNumber(SMS)={store_number_sms}")
        logging.info(f"UNFI AuthUrl={UNFI_AUTH_URL}")
        logging.info(f"UNFI ApiBaseUrl={UNFI_API_BASE}")
        logging.info(f"UNFI ApiStoreChainId(legacy)={UNFI_CHAIN_ID}")

        po, vendor = get_sqi_args()
        logging.info(f"SQI args: PO={po} Vendor={vendor}")

        vendor_cfg = read_vendor_cfg(vendor)
        order_url = build_unfi_order_url(vendor_cfg)

        logging.info(
            "VendorConfig | "
            f"Section={vendor_cfg['Section']} | SupplierName={vendor_cfg['SupplierName']} | "
            f"AccountNumber={vendor_cfg['AccountNumber']} | UNFI_StoreID={vendor_cfg['StoreID']}"
        )
        logging.info(f"UNFI OrderUrl(Legacy)={order_url}")

        conn = pyodbc.connect(connection_string)

        vendor_name = get_vendor_name(conn, vendor)
        rec_hdr_f91 = get_rec_hdr_f91(conn, str(po))
        logging.info(f"Vendor lookup: F27={vendor} => VendorName={vendor_name!r}")
        logging.info(f"REC_HDR lookup: PO={po} => F91={rec_hdr_f91!r}")

        po_rows = get_po_rows(conn, po)
        dept = get_first_dept(po_rows)

        payload, valid, skipped, total_msg = build_unfi_payload(
            conn=conn, po_number=str(po), vendor_id=vendor, vendor_cfg=vendor_cfg, po_rows=po_rows
        )
        logging.info(f"PO {po}: Build summary — ValidLines={valid} SkippedLines={skipped} | {total_msg}")

        if not payload:
            msg = f"PO {po} has no valid items to send."
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
            logging.info("=== End run (UNFIPush) ===")
            return 1

        payload_path = save_payload(str(po), vendor, payload)
        logging.info(f"PO {po}: Payload saved: {payload_path}")

        # Auth
        try:
            logging.info("Authenticating…")
            token = get_unfi_token(session)
            logging.info("Auth OK.")
        except (ConnectionError, Timeout, HTTPError, RequestException) as e:
            title, detail = format_requests_error(e, UNFI_AUTH_URL)
            logging.exception(f"Auth/network error: {e}")
            insert_ravyx_po_status(
                conn,
                po_number=str(po),
                vendor=vendor,
                vendor_name=vendor_name,
                status="FAILED",
                f1081_text=f"AUTH failed: {title} | {detail}",
                dept=dept,
                rec_hdr_f91=rec_hdr_f91,
                process="Order Export",
            )
            force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
            logging.info("=== End run (UNFIPush) ===")
            return 1
        except Exception as e:
            msg = f"Auth failed: {e}"
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
            logging.info("=== End run (UNFIPush) ===")
            return 1

        # Post (with 401/403 refresh once)
        try:
            logging.info("Posting order…")
            resp = post_unfi_order(session, token, payload, vendor_cfg)

            if resp.status_code in (401, 403):
                logging.warning("Received 401/403. Refreshing token and retrying once…")
                token = get_unfi_token(session)
                resp = post_unfi_order(session, token, payload, vendor_cfg)

        except (ConnectionError, Timeout, HTTPError, RequestException) as e:
            title, detail = format_requests_error(e, order_url)
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
            logging.info("=== End run (UNFIPush) ===")
            return 1

        snippet = (resp.text or "")[:500].replace("\n", " ")
        logging.info(f"PO {po}: API response status={resp.status_code} snippet={snippet!r}")

        if 200 <= resp.status_code < 300:
            try:
                set_po_closed(conn, str(po))
                append_sent_marker(conn, str(po), SENT_MARKER)
            except Exception as e:
                msg = f"Submitted OK but failed to update REC_HDR (CLOSE/SentToVendor): {e}"
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
                logging.info("=== End run (UNFIPush) ===")
                return 1

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

            dur = round(time.perf_counter() - start, 2)
            logging.info(f"SUCCESS | PO={po} Vendor={vendor} | Duration={dur}s")
            logging.info("=== End run (UNFIPush) ===")
            return 0

        # Non-2xx => keep PO OPEN and ensure no SentToVendor marker
        if resp.status_code == 400:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            err_path = os.path.join(LOG_DIR, f"UNFI_error_PO{po}_V{vendor}_{ts}.log")
            try:
                with open(err_path, "w", encoding="utf-8") as f:
                    f.write(resp.text or "")
                logging.error(f"PO {po}: 400 response logged to {err_path}")
            except Exception as e:
                logging.warning(f"Unable to write 400 error file: {e}")

        msg = f"Order NOT sent. HTTP={resp.status_code}. {snippet}"
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

        logging.info("=== End run (UNFIPush) ===")
        return 1

    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        try:
            if conn is not None and po:
                force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
        except Exception:
            pass
        logging.info("=== End run (UNFIPush) ===")
        return 2

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
# UI wrapper (opens tkinter window; falls back to console if UI not available)
# =============================================================================
def main_with_ui() -> int:
    ui_q: "queue.Queue" = queue.Queue()
    setup_logging(ui_q=ui_q)

    try:
        from ui_send_PO_unfi import VendorSendUI
    except Exception as e:
        logging.warning(f"UI not available (ui_send_PO_unfi import failed): {e}")
        return run_job()

    ui = VendorSendUI(
        title="Send PO to UNFI",
        queue=ui_q,
        auto_close_seconds=0,
    )

    def worker():
        try:
            logging.info("UI started.")
            rc = run_job()
            if rc == 0:
                ui_q.put(("DONE", "SUCCESS", "PO sent successfully"))
            else:
                ui_q.put(("ERROR", "FAILED", f"ExitCode={rc} (see log)"))
        except Exception as ex:
            ui_q.put(("ERROR", "Exception", f"{type(ex).__name__}: {ex}"))

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    ui.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main_with_ui())
