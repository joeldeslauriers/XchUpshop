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


def find_config_path() -> str:
    """
    Look for config.ini:
      1) next to EXE/script (BASE_DIR)
      2) one folder above BASE_DIR -> shared config.ini
    """
    candidates = [
        os.path.join(BASE_DIR, "config.ini"),
        os.path.join(os.path.dirname(BASE_DIR), "config.ini"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    return candidates[-1]


CONFIG_PATH = find_config_path()
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
    def __init__(self, q: "queue.Queue"):
        super().__init__()
        self.q = q

    def emit(self, record: logging.LogRecord) -> None:
        try:
            lvl = record.levelname.upper()
            msg = record.getMessage()

            # Only show WARN/ERROR in UI list to keep it clean
            if lvl in ("WARNING", "WARN"):
                self.q.put(("SUMMARY", "WARN", msg))
            elif lvl in ("ERROR", "CRITICAL"):
                self.q.put(("SUMMARY", "ERROR", msg))
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
# Config helpers (NO top-level raise)
# =============================================================================
class ConfigError(Exception):
    def __init__(self, message: str, missing: Optional[List[str]] = None):
        super().__init__(message)
        self.missing = missing or []


def load_config_or_raise() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if not os.path.exists(CONFIG_PATH):
        raise ConfigError(f"config.ini not found: {CONFIG_PATH}")
    cfg.read(CONFIG_PATH, encoding="utf-8")
    return cfg


def validate_settings(cfg: configparser.ConfigParser) -> Dict[str, Any]:
    """
    Phase 1: Settings/DB only (so we can still write RAVYX_PO_STATUS on UNFI config errors)
    """
    log_purge_days = cfg.getint("Settings", "LogPurge", fallback=30)

    server_name = (cfg.get("Settings", "ServerName", fallback="") or "").strip()
    if not server_name:
        raise ConfigError("Missing config.ini value: [Settings] ServerName")

    sql_driver = (cfg.get("Settings", "SQLDriver", fallback="SQL Server") or "SQL Server").strip()
    database = (cfg.get("Settings", "DatabaseName", fallback="STORESQL") or "STORESQL").strip()

    store_number_sms = int((cfg.get("Settings", "StoreNumber", fallback="0") or "0").strip() or "0")
    if store_number_sms <= 0:
        raise ConfigError(
            "Config error: [Settings] StoreNumber must be > 0. "
            "UNFI URL requires ?storeId=<StoreNumber> (ex: 3)."
        )

    connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes"

    return {
        "LOG_PURGE_DAYS": log_purge_days,
        "server_name": server_name,
        "sql_driver": sql_driver,
        "database": database,
        "store_number_sms": store_number_sms,
        "connection_string": connection_string,
    }


def validate_unfi(cfg: configparser.ConfigParser) -> Dict[str, Any]:
    """
    Phase 2: UNFI shared settings
    """
    UNFI_AUTH_URL = cfg.get("UNFI", "AuthUrl", fallback="").strip()
    UNFI_API_BASE = cfg.get("UNFI", "ApiBaseUrl", fallback="").strip().rstrip("/")
    UNFI_CHAIN_ID = cfg.get("UNFI", "ApiStoreChainId", fallback="").strip()

    UNFI_USERNAME = cfg.get("UNFI", "Username", fallback="").strip()
    UNFI_PASSWORD = cfg.get("UNFI", "Password", fallback="").strip().strip('"')
    UNFI_CLIENT_ID = cfg.get("UNFI", "ClientId", fallback="").strip()

    TIMEOUT_AUTH_SEC = cfg.getint("UNFI", "TimeoutAuthSec", fallback=90)
    TIMEOUT_POST_SEC = cfg.getint("UNFI", "TimeoutPostSec", fallback=120)

    missing: List[str] = []
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

    if missing:
        raise ConfigError("Missing config.ini value(s): " + ", ".join(missing), missing=missing)

    return {
        "UNFI_AUTH_URL": UNFI_AUTH_URL,
        "UNFI_API_BASE": UNFI_API_BASE,
        "UNFI_CHAIN_ID": UNFI_CHAIN_ID,
        "UNFI_USERNAME": UNFI_USERNAME,
        "UNFI_PASSWORD": UNFI_PASSWORD,
        "UNFI_CLIENT_ID": UNFI_CLIENT_ID,
        "TIMEOUT_AUTH_SEC": TIMEOUT_AUTH_SEC,
        "TIMEOUT_POST_SEC": TIMEOUT_POST_SEC,
    }


# =============================================================================
# Second validation to not export order for other vendor than UNFI
# =============================================================================
def vendor_section_from_f27(vendor_id: int) -> str:
    if vendor_id == 3954:
        return "UNFIGW"
    if vendor_id == 7313:
        return "UNFIRC"
    raise ValueError(f"Unsupported UNFI vendor id (F27): {vendor_id}")


def read_vendor_cfg(cfg: configparser.ConfigParser, vendor_id: int) -> Dict[str, str]:
    sect = vendor_section_from_f27(vendor_id)
    if sect not in cfg:
        raise ConfigError(f"Missing config.ini section: [{sect}] for vendor {vendor_id}")

    for k in ("SupplierName", "AccountNumber", "StoreID"):
        if not (cfg[sect].get(k, "") or "").strip():
            raise ConfigError(f"Missing config.ini value: [{sect}] {k}")

    store_id_raw = (cfg[sect]["StoreID"] or "").strip().strip('"')
    if not store_id_raw.isdigit():
        raise ConfigError(f"Invalid config.ini value: [{sect}] StoreID must be numeric. Got: {store_id_raw!r}")

    return {
        "SupplierId": (cfg[sect].get("SupplierId", str(vendor_id)) or str(vendor_id)).strip(),
        "SupplierName": cfg[sect]["SupplierName"].strip(),
        "AccountNumber": cfg[sect]["AccountNumber"].strip(),
        "StoreID": store_id_raw,
        "Section": sect,
    }


def build_unfi_order_url(unfi_api_base: str, unfi_chain_id: str, store_number_sms: int, vendor_cfg: Dict[str, str]) -> str:
    """
    POST https://posapi.../stores/{chainId}/orders?storeId={SMS StoreNumber}
    """
    chain_id = int(unfi_chain_id)
    return f"{unfi_api_base}/stores/{chain_id}/orders?storeId={int(store_number_sms)}"


# =============================================================================
# Daily API raw log
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
# Clear error messages
# =============================================================================
def format_requests_error(e: Exception, url: str = "") -> Tuple[str, str]:
    raw = str(e) or repr(e)

    if isinstance(e, HTTPError) and getattr(e, "response", None) is not None:
        resp = e.response
        status = getattr(resp, "status_code", None)

        try:
            body = (resp.text or "").strip()
        except Exception:
            body = ""

        snippet = body[:300].replace("\n", " ") if body else ""

        if status in (502, 503, 504):
            return (
                f"Gateway/Service unavailable ({status})",
                "The server/proxy returned a gateway/service error.\n"
                f"URL: {url}\n"
                "Check: VPN/proxy/firewall, DNS overrides (hosts), or API environment outage.\n"
                f"Response: {snippet or '(empty)'}",
            )

        if status == 401:
            return (
                "Authentication failed (401)",
                "Authentication was rejected.\n"
                f"URL: {url}\n"
                "Check Username/Password/ClientId in config.ini.\n"
                f"Response: {snippet or '(empty)'}",
            )

        if status == 403:
            return (
                "Access forbidden (403)",
                "Access denied by the server.\n"
                f"URL: {url}\n"
                "Check account permissions on UNFI/SPS side.\n"
                f"Response: {snippet or '(empty)'}",
            )

        return (
            f"HTTP error ({status})",
            "HTTP request failed.\n"
            f"URL: {url}\n"
            f"Response: {snippet or '(empty)'}\n\nDetails: {raw}",
        )

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
            f"Cannot reach the server.\nURL: {url}\n"
            "Check: internet/VPN, DNS, firewall/proxy, and that the URL is correct.\n\n"
            f"Details: {raw}",
        )

    if "max retries exceeded" in low:
        return (
            "Network error (max retries)",
            "Server did not respond or is unreachable (max retries exceeded).\n"
            f"URL: {url}\n"
            "Check: network, firewall/proxy, and server availability.\n\n"
            f"Details: {raw}",
        )

    if "timed out" in low or isinstance(e, Timeout):
        return (
            "Network timeout",
            "Server is not responding in time (timeout).\n"
            f"URL: {url}\n"
            "Try again later or check connectivity.\n\n"
            f"Details: {raw}",
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
        if not row or row[0] is None:
            return None
        s = str(row[0]).strip()
        return s or None
    except Exception:
        return None


def get_rec_hdr_f1254(conn: pyodbc.Connection, po_number: str) -> Optional[str]:
    try:
        cur = conn.cursor()
        cur.execute("SELECT TOP 1 F1254 FROM [dbo].[REC_HDR] WHERE F1032 = ?", (po_number,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        s = str(row[0]).strip()
        return s or None
    except Exception:
        return None


def _looks_like_guid(value: Optional[str]) -> bool:
    if not value:
        return False
    s = str(value).strip()
    if not s:
        return False
    try:
        uuid.UUID(s)
        return True
    except Exception:
        return False


def safe_has_existing_guid(conn: pyodbc.Connection, po_number: str) -> bool:
    """
    Fail-closed: if we cannot confirm, we treat as already-sent to avoid duplicates.
    """
    try:
        v = get_rec_hdr_f1254(conn, po_number)
        return bool(v and _looks_like_guid(v))
    except Exception:
        logging.warning(f"PO {po_number}: unable to read F1254; blocking resend for safety.")
        return True


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


# =============================================================================
# truncation to F1081 max (5000 chars)
# =============================================================================
F1081_MAX_LEN = 5000


def _truncate_5000(s: str, max_len: int = F1081_MAX_LEN) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if len(s) <= max_len:
        return s
    suffix = "…(truncated)"
    keep = max_len - len(suffix)
    if keep <= 0:
        return s[:max_len]
    return s[:keep].rstrip() + suffix


def build_skip_summary(skip_map: Dict[str, List[str]], max_upcs_per_reason: int = 200) -> str:
    parts: List[str] = []

    for reason, upcs in (skip_map or {}).items():
        if not upcs:
            continue

        seen = set()
        uniq: List[str] = []
        for u in upcs:
            u = str(u or "").strip()
            if not u:
                continue
            if u not in seen:
                seen.add(u)
                uniq.append(u)

        if not uniq:
            continue

        shown = uniq[:max_upcs_per_reason]
        more = len(uniq) - len(shown)

        seg = f"{reason}: {','.join(shown)}"
        if more > 0:
            seg += f" (+{more} more)"
        parts.append(seg)

    return _truncate_5000(" | ".join(parts))


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
    if f1081_val:
        f1081_val = _truncate_5000(f1081_val) or None

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


def force_keep_po_open_on_failure(conn: pyodbc.Connection, po_number: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE [dbo].[REC_HDR]
        SET F1067 = 'OPEN'
        WHERE F1032 = ?
        """,
        (po_number,),
    )
    conn.commit()
    logging.warning(f"PO {po_number}: forced OPEN (failure path).")


def mark_po_sent_and_store_guid(conn: pyodbc.Connection, po_number: str, guid: str) -> None:
    """
    ATOMIC:
      - REC_HDR.F1254 = <guid>
      - REC_HDR.F1067 = 'CLOSE'
    If anything fails => rollback + force OPEN.
    """
    guid = (guid or "").strip()
    if not guid:
        raise ValueError("GUID is empty; cannot write F1254.")

    try:
        conn.autocommit = False
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE [dbo].[REC_HDR]
            SET F1254 = ?, F1067 = 'CLOSE'
            WHERE F1032 = ?
            """,
            (guid, po_number),
        )

        conn.commit()
        logging.info(f"PO {po_number}: stored GUID in F1254 and CLOSED (atomic).")

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass

        logging.exception(f"PO {po_number}: failed to update REC_HDR (CLOSE+GUID): {type(e).__name__}: {e}")

        try:
            force_keep_po_open_on_failure(conn, po_number)
        except Exception:
            pass

        raise
    finally:
        try:
            conn.autocommit = True
        except Exception:
            pass


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
) -> Tuple[Optional[Dict[str, Any]], str, int, int, str, Dict[str, List[str]]]:
    if not po_rows:
        return None, "", 0, 0, "No REC_REG rows found for this PO.", {}

    todays_date = datetime.now().strftime("%Y-%m-%dT%H:%M")
    unique_order_id = str(uuid.uuid4())

    valid = 0
    skipped = 0
    skip_map: Dict[str, List[str]] = {}

    rec_ttl_total = get_order_total_rec_ttl(conn, po_number)

    fallback_total = 0.0
    for r in po_rows:
        ok, reason = _validate_row(r)
        if not ok:
            skipped += 1
            upc = getattr(r, "UPC", "") or ""
            skip_map.setdefault(reason, []).append(str(upc))
            logging.warning(f"PO {po_number}: Skipped line UPC={upc} — {reason}")
            continue
        try:
            fallback_total += float(r.COST) * int(r.QTY)
        except Exception:
            fallback_total += float(r.COST)

    used_total = rec_ttl_total if rec_ttl_total is not None else fallback_total
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
        return None, unique_order_id, valid, skipped, "No valid lines to send (payload empty).", skip_map

    msg = (
        f"orderTotal REC_TTL(F1034=8201)={rec_ttl_total} | "
        f"fallback(sum valid cost*qty)={fallback_total} | used={used_total}"
    )
    logging.info(f"PO {po_number}: {msg}")
    return payload, unique_order_id, valid, skipped, msg, skip_map


# =============================================================================
# Auth / Send (now takes config dict)
# =============================================================================
def get_unfi_token(session: Session, *, auth_url: str, client_id: str, username: str, password: str, timeout_sec: int) -> str:
    headers = {"Content-Type": "application/json"}
    req = {"client_id": client_id, "username": username, "password": password}

    t0 = time.perf_counter()
    resp = session.post(auth_url, headers=headers, json=req, timeout=timeout_sec)
    elapsed = time.perf_counter() - t0

    _write_unfi_api_log(
        kind="AUTH",
        url=auth_url,
        status_code=getattr(resp, "status_code", 0) or 0,
        elapsed_s=elapsed,
        headers=headers,
        request_json={"client_id": client_id, "username": username, "password": "***"},
        response_text=getattr(resp, "text", "") or "",
        po_number=None,
    )

    resp.raise_for_status()

    data = resp.json()
    auth_res = data.get("AuthenticationResult") or {}

    token = auth_res.get("IdToken") or auth_res.get("AccessToken")
    if not token:
        raise RuntimeError("Missing AuthenticationResult.IdToken/AccessToken in auth response")
    return token


def post_unfi_order(
    session: Session,
    *,
    order_url: str,
    token: str,
    payload: Dict[str, Any],
    timeout_sec: int,
) -> requests.Response:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    po_number = str((payload.get("header") or {}).get("storePONumber") or "?")

    t0 = time.perf_counter()
    resp = session.post(order_url, headers=headers, json=payload, timeout=timeout_sec)
    elapsed = time.perf_counter() - t0

    _write_unfi_api_log(
        kind="ORDER",
        url=order_url,
        status_code=resp.status_code,
        elapsed_s=elapsed,
        headers={"Content-Type": "application/json", "Authorization": headers["Authorization"]},
        request_json=payload,
        response_text=resp.text or "",
        po_number=po_number,
    )
    return resp


def _extract_order_id(resp: requests.Response) -> Optional[str]:
    try:
        data = resp.json()
        hdr = data.get("header") or {}
        oid = hdr.get("orderID")
        return str(oid) if oid is not None else None
    except Exception:
        return None


# =============================================================================
# Core job (returns exit code)
# =============================================================================
def run_job(ui_q: Optional["queue.Queue"] = None) -> int:
    start = time.perf_counter()

    def ui_summary(level: str, msg: str):
        if ui_q is not None:
            ui_q.put(("SUMMARY", (level or "INFO").upper(), msg))

    def ui_counters(sent: int, warns: int, errs: int):
        if ui_q is not None:
            ui_q.put(("COUNTERS", "ORDERS_SENT", str(sent)))
            ui_q.put(("COUNTERS", "WARNINGS", str(warns)))
            ui_q.put(("COUNTERS", "ERRORS", str(errs)))

    def ui_autoclose(seconds: int):
        if ui_q is not None:
            ui_q.put(("AUTOCLOSE", str(int(seconds)), ""))

    po = ""
    vendor = 0

    session = requests.Session()
    conn: Optional[pyodbc.Connection] = None
    unfi_guid: str = ""

    cfg: Optional[configparser.ConfigParser] = None
    settings: Dict[str, Any] = {}
    unfi: Dict[str, Any] = {}

    try:
        logging.info("=== Start run (UNFIPush) ===")
        logging.info(f"AppVersion={APP_VERSION} | Frozen={_is_frozen()} | BaseDir={BASE_DIR}")
        logging.info(f"ConfigPath={CONFIG_PATH}")

        # Get SQI args early (so we can report PO/vendor in status rows)
        po, vendor = get_sqi_args()

        ui_summary("INFO", "Starting…")
        ui_summary("INFO", f"Preparing PO {po}…")

        # Load config (safe)
        cfg = load_config_or_raise()

        # Validate Settings first (DB path)
        settings = validate_settings(cfg)

        # Purge logs now that we have LOG_PURGE_DAYS
        try:
            deleted = purge_logs(LOG_DIR, int(settings["LOG_PURGE_DAYS"]))
            logging.info(f"Log purge: deleted {deleted} file(s) older than {settings['LOG_PURGE_DAYS']} day(s).")
        except Exception as e:
            logging.warning(f"Log purge failed: {e}")

        # Connect DB as early as possible to be able to write RAVYX_PO_STATUS on config errors
        conn = pyodbc.connect(settings["connection_string"])
        vendor_name = get_vendor_name(conn, vendor)
        rec_hdr_f91 = get_rec_hdr_f91(conn, str(po))

        # Validate UNFI (if fails => UI + RAVYX + OPEN)
        try:
            unfi = validate_unfi(cfg)
        except ConfigError as ce:
            msg = str(ce)
            logging.error(msg)
            ui_summary("ERROR", msg)
            ui_counters(sent=0, warns=0, errs=1)
            ui_autoclose(25)

            try:
                insert_ravyx_po_status(
                    conn,
                    po_number=str(po),
                    vendor=vendor,
                    vendor_name=vendor_name,
                    status="FAILED",
                    f1081_text=_truncate_5000(f"CONFIG error: {msg}"),
                    dept=None,
                    rec_hdr_f91=rec_hdr_f91,
                    process="Order Export",
                )
            except Exception as e:
                logging.warning(f"Unable to insert RAVYX_PO_STATUS for config error: {e}")

            try:
                force_keep_po_open_on_failure(conn, str(po))
            except Exception:
                pass

            return 2

        # Vendor-specific config
        vendor_cfg = read_vendor_cfg(cfg, vendor)

        # Build endpoint URL
        order_url = build_unfi_order_url(
            unfi_api_base=unfi["UNFI_API_BASE"],
            unfi_chain_id=unfi["UNFI_CHAIN_ID"],
            store_number_sms=int(settings["store_number_sms"]),
            vendor_cfg=vendor_cfg,
        )

        logging.info(
            f"UNFI endpoint (POST) = {order_url} | "
            f"storeId(query)=StoreNumber={settings['store_number_sms']} | "
            f"payload header.storeID(UNFI)={vendor_cfg['StoreID']}"
        )

        # Prevent duplicate sends (fail-closed).
        if safe_has_existing_guid(conn, str(po)):
            existing_f1254 = get_rec_hdr_f1254(conn, str(po))
            msg = (
                f"PO {po} already has UNFI GUID in F1254 ({existing_f1254}). "
                "Skipping send to prevent duplicate."
            )
            logging.warning(msg)
            ui_summary("WARN", msg)

            try:
                insert_ravyx_po_status(
                    conn,
                    po_number=str(po),
                    vendor=vendor,
                    vendor_name=vendor_name,
                    status="WARN",
                    f1081_text=_truncate_5000(f"Skipped duplicate send. Existing GUID={existing_f1254}"),
                    dept=None,
                    rec_hdr_f91=rec_hdr_f91,
                    process="Order Export",
                )
            except Exception as e:
                logging.warning(f"Unable to insert RAVYX_PO_STATUS for duplicate-skip: {e}")

            ui_counters(sent=0, warns=1, errs=0)
            ui_autoclose(10)
            return 0

        po_rows = get_po_rows(conn, po)
        dept = get_first_dept(po_rows)

        payload, unfi_guid, valid, skipped, total_msg, skip_map = build_unfi_payload(
            conn=conn, po_number=str(po), vendor_id=vendor, vendor_cfg=vendor_cfg, po_rows=po_rows
        )
        skip_text = build_skip_summary(skip_map)

        ui_counters(sent=0, warns=int(skipped), errs=0)

        if not payload:
            msg = f"PO {po} has no valid items to send."
            if skip_text:
                msg = f"{msg} | {skip_text}"
            msg = _truncate_5000(msg)

            logging.error(msg)
            ui_summary("ERROR", msg)
            ui_counters(sent=0, warns=int(skipped), errs=1)

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
            force_keep_po_open_on_failure(conn, str(po))
            return 1

        save_payload(str(po), vendor, payload)

        # Auth
        try:
            ui_summary("INFO", "Connecting…")
            token = get_unfi_token(
                session,
                auth_url=unfi["UNFI_AUTH_URL"],
                client_id=unfi["UNFI_CLIENT_ID"],
                username=unfi["UNFI_USERNAME"],
                password=unfi["UNFI_PASSWORD"],
                timeout_sec=int(unfi["TIMEOUT_AUTH_SEC"]),
            )
            ui_summary("INFO", "Connected.")
        except (ConnectionError, Timeout, HTTPError, RequestException) as e:
            title, detail = format_requests_error(e, unfi["UNFI_AUTH_URL"])
            logging.exception(f"Auth/network error: {e}")

            ui_summary("ERROR", f"Connection failed: {title}")
            ui_counters(sent=0, warns=int(skipped), errs=1)

            insert_ravyx_po_status(
                conn,
                po_number=str(po),
                vendor=vendor,
                vendor_name=vendor_name,
                status="FAILED",
                f1081_text=_truncate_5000(f"AUTH failed: {title} | {detail}"),
                dept=dept,
                rec_hdr_f91=rec_hdr_f91,
                process="Order Export",
            )
            force_keep_po_open_on_failure(conn, str(po))
            return 1

        # Post (with 401/403 refresh once)
        try:
            ui_summary("INFO", f"Sending PO {po}…")
            resp = post_unfi_order(
                session,
                order_url=order_url,
                token=token,
                payload=payload,
                timeout_sec=int(unfi["TIMEOUT_POST_SEC"]),
            )

            if resp.status_code in (401, 403):
                logging.warning("Received 401/403. Refreshing token and retrying once…")
                ui_summary("WARN", "Token expired. Retrying…")
                token = get_unfi_token(
                    session,
                    auth_url=unfi["UNFI_AUTH_URL"],
                    client_id=unfi["UNFI_CLIENT_ID"],
                    username=unfi["UNFI_USERNAME"],
                    password=unfi["UNFI_PASSWORD"],
                    timeout_sec=int(unfi["TIMEOUT_AUTH_SEC"]),
                )
                resp = post_unfi_order(
                    session,
                    order_url=order_url,
                    token=token,
                    payload=payload,
                    timeout_sec=int(unfi["TIMEOUT_POST_SEC"]),
                )

        except (ConnectionError, Timeout, HTTPError, RequestException) as e:
            title, detail = format_requests_error(e, order_url)
            logging.exception(f"Network/API error: {e}")

            ui_summary("ERROR", f"Send failed: {title}")
            ui_counters(sent=0, warns=int(skipped), errs=1)

            insert_ravyx_po_status(
                conn,
                po_number=str(po),
                vendor=vendor,
                vendor_name=vendor_name,
                status="FAILED",
                f1081_text=_truncate_5000(f"Request failed: {title} | {detail}"),
                dept=dept,
                rec_hdr_f91=rec_hdr_f91,
                process="Order Export",
            )
            force_keep_po_open_on_failure(conn, str(po))
            return 1

        snippet = (resp.text or "")[:500].replace("\n", " ")
        logging.info(f"PO {po}: API response status={resp.status_code} snippet={snippet!r}")

        if 200 <= resp.status_code < 300:
            order_id = _extract_order_id(resp)

            try:
                mark_po_sent_and_store_guid(conn, str(po), unfi_guid)
            except Exception as e:
                msg = _truncate_5000(f"Submitted OK but failed to update REC_HDR (CLOSE+GUID): {e}")
                logging.exception(msg)

                ui_summary("ERROR", "Sent OK but failed to update SMS (REC_HDR).")
                ui_counters(sent=0, warns=int(skipped), errs=1)

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
                return 1

            final_status = "WARN" if skipped > 0 else "SUCCESS"
            final_f1081 = skip_text

            insert_ravyx_po_status(
                conn,
                po_number=str(po),
                vendor=vendor,
                vendor_name=vendor_name,
                status=final_status,
                f1081_text=final_f1081,
                dept=dept,
                rec_hdr_f91=rec_hdr_f91,
                process="Order Export",
            )

            ui_counters(sent=1, warns=int(skipped), errs=0)
            dur = round(time.perf_counter() - start, 2)

            if skipped > 0 and skip_text:
                ui_summary("WARN", f"Skipped items: {skip_text}")

            if order_id:
                ui_summary("SUCCESS", f"Order sent: PO {po} → OrderID {order_id} | GUID {unfi_guid} ({dur}s)")
            else:
                ui_summary("SUCCESS", f"Order sent: PO {po} | GUID {unfi_guid} ({dur}s)")

            ui_autoclose(20)
            return 0

        msg = _truncate_5000(f"Order NOT sent. HTTP={resp.status_code}. {snippet}")
        logging.error(msg)

        ui_summary("ERROR", f"Order NOT sent (HTTP {resp.status_code}).")
        ui_counters(sent=0, warns=int(skipped), errs=1)
        ui_autoclose(20)

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
        force_keep_po_open_on_failure(conn, str(po))
        return 1

    except Exception as e:
        logging.exception(f"Fatal error: {e}")

        if ui_q is not None:
            ui_q.put(("SUMMARY", "ERROR", f"Fatal error: {type(e).__name__}: {e}"))
            ui_q.put(("COUNTERS", "ERRORS", "1"))
            ui_q.put(("AUTOCLOSE", "25", ""))

        # Try to write FAILED to status report if DB is open
        try:
            if conn is not None and po:
                vn = None
                try:
                    vn = get_vendor_name(conn, vendor)
                except Exception:
                    vn = None

                insert_ravyx_po_status(
                    conn,
                    po_number=str(po),
                    vendor=vendor,
                    vendor_name=vn,
                    status="FAILED",
                    f1081_text=_truncate_5000(f"Fatal error: {type(e).__name__}: {e}"),
                    dept=None,
                    rec_hdr_f91=get_rec_hdr_f91(conn, str(po)),
                    process="Order Export",
                )
        except Exception as dbex:
            logging.warning(f"Unable to insert RAVYX_PO_STATUS for fatal error: {dbex}")

        try:
            if conn is not None and po:
                force_keep_po_open_on_failure(conn, str(po))
        except Exception:
            pass

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
def wants_ui() -> bool:
    args = [a.lower() for a in sys.argv[1:]]
    if "--noui" in args:
        return False
    if "--ui" in args:
        return True
    return _is_frozen()


def main_with_ui() -> int:
    ui_q: "queue.Queue" = queue.Queue()
    setup_logging(ui_q=ui_q)

    try:
        from ui_send_PO_unfi import VendorSendUI
    except Exception as e:
        logging.warning(f"UI not available (ui_send_PO_unfi import failed): {e}")
        return run_job(ui_q=None)

    ui = VendorSendUI(
        title="Send PO to UNFI",
        queue=ui_q,
        auto_close_seconds=0,
    )

    def worker():
        try:
            logging.info("UI started.")
            rc = run_job(ui_q=ui_q)

            if rc == 0:
                ui_q.put(("DONE", "SUCCESS", "Done"))
            else:
                ui_q.put(("ERROR", "FAILED", f"ExitCode={rc}"))
                ui_q.put(("AUTOCLOSE", "20", ""))
        except Exception as ex:
            ui_q.put(("ERROR", "FAILED", f"{type(ex).__name__}: {ex}"))
            ui_q.put(("AUTOCLOSE", "20", ""))

    t = threading.Thread(target=worker, daemon=True)
    t.start()

    ui.run()
    return 0


if __name__ == "__main__":
    setup_logging(ui_q=None)
    if wants_ui():
        raise SystemExit(main_with_ui())
    raise SystemExit(run_job(ui_q=None))
