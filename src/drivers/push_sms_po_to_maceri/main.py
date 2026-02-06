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

from ui_send_PO_maceri import VendorSendUI


# =============================================================================
# Version + Path + Config helpers
# =============================================================================
def _is_frozen() -> bool:
    return getattr(sys, "frozen", False)


def _base_dir() -> str:
    return os.path.dirname(sys.executable) if _is_frozen() else os.path.dirname(os.path.abspath(__file__))


def _find_config_path() -> str:
    """
    Look for config.ini in:
      1) Parent folder of the EXE/script folder (shared config)
      2) Same folder as the EXE/script (local fallback)

    Example:
      ...\XchUpShop\PushSMSPOToMaceri\push.exe
      -> tries ...\XchUpShop\config.ini first
      -> then ...\XchUpShop\PushSMSPOToMaceri\config.ini
    """
    base = _base_dir()
    parent = os.path.dirname(base)

    candidates = [
        os.path.join(parent, "config.ini"),
        os.path.join(base, "config.ini"),
    ]

    for p in candidates:
        if os.path.exists(p):
            return p

    return candidates[0]  # best “expected” path for error messages


BASE_DIR = _base_dir()
CONFIG_PATH = _find_config_path()
VERSION_PATH = os.path.join(BASE_DIR, "version_info.txt")


def read_version(default: str = "0.0.0.0") -> str:
    try:
        with open(VERSION_PATH, "r", encoding="utf-8") as f:
            v = (f.readline() or "").strip()
            return v or default
    except Exception:
        return default


APP_VERSION = read_version()
AUTO_CLOSE_SECONDS = 20

# Globals loaded inside worker (so UI opens instantly)
SETTINGS: Dict[str, Any] = {}


# =============================================================================
# Args from SQI
# =============================================================================
def _strip_quotes(s: str) -> str:
    return (s or "").strip().strip('"').strip("'")


def get_sqi_args() -> Tuple[str, int, bool]:
    """
    SQI calls:
      pushSMSPOtoMACERI.exe "<F1032>" "<F27>"

    Optional:
      pushSMSPOtoMACERI.exe "<F1032>" "<F27>" --force
        -> bypass 'already sent' protection (default is to SKIP if already sent)
    """
    po = _strip_quotes(sys.argv[1]) if len(sys.argv) >= 2 else ""
    vendor_raw = _strip_quotes(sys.argv[2]) if len(sys.argv) >= 3 else ""
    extra_args = [a.lower().strip() for a in sys.argv[3:]]

    force = ("--force" in extra_args)

    if not po:
        raise ValueError('Missing SQI param: F1032 (PO). Expected: exe "<F1032>" "<F27>"')
    if not vendor_raw:
        raise ValueError('Missing SQI param: F27 (Vendor). Expected: exe "<F1032>" "<F27>"')

    try:
        vendor = int(vendor_raw)
    except Exception:
        raise ValueError(f"Vendor F27 must be numeric/int. Got: {vendor_raw!r}")

    return po, vendor, force


# =============================================================================
# Logging + purge (moved inside worker to speed UI)
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


def setup_logging(log_dir: str) -> str:
    os.makedirs(log_dir, exist_ok=True)

    log_ts = datetime.now().strftime("%Y_%m_%d")
    log_filename = os.path.join(log_dir, f"ExportOrdersToMaceriSMS_logs_{log_ts}.log")

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

    return log_filename


def load_settings() -> Dict[str, Any]:
    cfg = configparser.ConfigParser()

    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(
            "config.ini not found.\n"
            f"Tried:\n"
            f"  1) {os.path.join(os.path.dirname(BASE_DIR), 'config.ini')}\n"
            f"  2) {os.path.join(BASE_DIR, 'config.ini')}\n"
        )

    cfg.read(CONFIG_PATH, encoding="utf-8")

    server_name = (cfg.get("Settings", "ServerName", fallback="") or "").strip()
    if not server_name:
        raise RuntimeError("Missing config.ini value: [Settings] ServerName")

    sql_driver = (cfg.get("Settings", "SQLDriver", fallback="SQL Server") or "SQL Server").strip()
    database = (cfg.get("Settings", "DatabaseName", fallback="STORESQL") or "STORESQL").strip()

    log_purge_days = cfg.getint("Settings", "LogPurge", fallback=30)

    # Maceri settings
    maceri_location_id = (cfg.get("Maceri", "MaceriLocationID", fallback="") or "").strip()
    maceri_username = (cfg.get("Maceri", "Username", fallback="") or "").strip()
    maceri_password = (cfg.get("Maceri", "Password", fallback="") or "").strip()
    api_auth_url = (cfg.get("Maceri", "BaseUrlAuth", fallback="") or "").strip()
    api_order_url = (cfg.get("Maceri", "BaseUrlOrder", fallback="") or "").strip()
    fb_referer = (cfg.get("Maceri", "FbReferer", fallback="https://tommaceri-test.fbportal.io/") or "").strip()

    connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes"

    return {
        "cfg": cfg,
        "server_name": server_name,
        "sql_driver": sql_driver,
        "database": database,
        "connection_string": connection_string,
        "log_dir": os.path.join(BASE_DIR, "LOG"),  # logs stay per-EXE folder (good)
        "log_purge_days": log_purge_days,
        "maceri": {
            "LOCATION_ID": maceri_location_id,
            "USERNAME": maceri_username,
            "PASSWORD": maceri_password,
            "API_AUTH_URL": api_auth_url,
            "API_ORDER_URL": api_order_url,
            "FB_REFERER": fb_referer,
        },
    }


# =============================================================================
# Clear error messages for UI
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

    if "timed out" in low:
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


def _format_item_not_found_reason(item_code: str) -> str:
    code = (item_code or "").strip() or "?"
    return f'Item not found "{code}"'


# =============================================================================
# Skipped item tracking -> RAVYX_PO_STATUS.F1081 (max 5000 chars)
# =============================================================================
F1081_MAX_CHARS = 5000
_NO_UPC_MARKER = "<NO_UPC>"


def _normalize_upc(v: Any) -> str:
    return "" if v is None else str(v).strip()


def add_skip(skip_map: Dict[str, Set[str]], reason: str, upc: Any) -> None:
    reason = (reason or "").strip() or "Skipped"
    upc_s = _normalize_upc(upc)

    if reason not in skip_map:
        skip_map[reason] = set()

    if upc_s:
        skip_map[reason].add(upc_s)
    else:
        skip_map[reason].add(_NO_UPC_MARKER)


def merge_skip_maps(dst: Dict[str, Set[str]], src: Dict[str, Set[str]]) -> None:
    for reason, upcs in (src or {}).items():
        if reason not in dst:
            dst[reason] = set()
        dst[reason].update(upcs or set())


def build_skip_summary(
    skip_map: Dict[str, Set[str]],
    *,
    max_chars: int = F1081_MAX_CHARS,
    max_upcs_per_reason: int = 250,
) -> str:
    if not skip_map:
        return ""

    parts: List[str] = []

    for reason in sorted(skip_map.keys()):
        s = skip_map.get(reason) or set()

        real_upcs = sorted(u for u in s if u and u != _NO_UPC_MARKER)
        has_no_upc = (_NO_UPC_MARKER in s)

        if real_upcs:
            if max_upcs_per_reason and len(real_upcs) > max_upcs_per_reason:
                real_upcs = real_upcs[:max_upcs_per_reason]
            parts.append(f"{reason} " + ",".join(real_upcs))
        elif has_no_upc:
            parts.append(f"{reason}")
        else:
            continue

    if not parts:
        return ""

    text = " | ".join(parts)
    if len(text) <= max_chars:
        return text

    trimmed_parts: List[str] = []
    used = 0
    for p in parts:
        extra = (3 if trimmed_parts else 0) + len(p)
        if used + extra > max_chars:
            break
        trimmed_parts.append(p)
        used += extra

    if not trimmed_parts:
        return text[: max_chars - 3] + "..."

    out = " | ".join(trimmed_parts)
    if len(out) > max_chars:
        out = out[: max_chars - 3] + "..."
    return out


def _remove_missing_itemcode_dupes(all_skips: Dict[str, Set[str]]) -> None:
    missing = all_skips.get("Missing ITEMCODE")
    if not missing:
        return

    item_not_found_upcs: Set[str] = set()
    for reason, upcs in all_skips.items():
        if reason.startswith('Item not found "'):
            for u in (upcs or set()):
                if u and u != _NO_UPC_MARKER:
                    item_not_found_upcs.add(u)

    if not item_not_found_upcs:
        return

    missing -= item_not_found_upcs
    if not missing:
        all_skips.pop("Missing ITEMCODE", None)


# =============================================================================
# Daily API raw log (settings loaded in worker)
# =============================================================================
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
    maceri_api_log_path: str,
    errors_only: bool,
    kind: str,
    url: str,
    status_code: int,
    elapsed_s: float,
    headers: Dict[str, str],
    request_json: Optional[Dict[str, Any]],
    response_text: str,
    po_number: Optional[str] = None,
):
    if errors_only and status_code < 400:
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
        with open(maceri_api_log_path, "a", encoding="utf-8") as f:
            f.write(entry)
    except Exception as e:
        logging.warning(f"Unable to write Maceri API daily log: {e}")


# =============================================================================
# SQL helpers
# =============================================================================
def get_vendor_name(conn, vendor: int) -> Optional[str]:
    q = "SELECT TOP 1 LTRIM(RTRIM(CAST(F334 AS varchar(60)))) FROM [dbo].[VENDOR_TAB] WHERE F27 = ?"
    cur = conn.cursor()
    cur.execute(q, (vendor,))
    row = cur.fetchone()
    if not row:
        return None
    name = (row[0] or "").strip()
    return name or None


def get_rec_hdr_f91(conn, po_number: str) -> Optional[str]:
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


def get_rec_hdr_f1254(conn, po_number: str) -> Optional[str]:
    try:
        cur = conn.cursor()
        cur.execute("SELECT TOP 1 F1254 FROM [dbo].[REC_HDR] WHERE F1032 = ?", (po_number,))
        row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return str(row[0]).strip() or None
    except Exception:
        return None


def is_po_already_sent(conn, po_number: str, marker: str) -> bool:
    """
    True if REC_HDR.F1254 already contains marker (case-insensitive).
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 1
            FROM [dbo].[REC_HDR]
            WHERE F1032 = ?
              AND F1254 IS NOT NULL
              AND LOWER(F1254) LIKE ?
            """,
            (po_number, f"%{marker.lower()}%"),
        )
        return cur.fetchone() is not None
    except Exception:
        logging.warning(f"PO {po_number}: unable to verify sent marker in F1254; blocking resend for safety.")
        return True


def get_po_data(conn, po_number: str):
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
    conn,
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
    runid = datetime.now().strftime("%y%m%d%H%M%S")
    now = datetime.now()

    f1081_val_raw = (f1081_text or "").strip()
    f1081_val = (
        (f1081_val_raw[: F1081_MAX_CHARS - 3] + "...")
        if len(f1081_val_raw) > F1081_MAX_CHARS
        else f1081_val_raw
    )
    f1081_val = f1081_val or None

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


def append_sent_marker(conn, po_number: str, marker: str) -> bool:
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


def force_keep_po_open_on_failure(conn, po_number: str, marker: str) -> None:
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


def delete_invalid_item_from_po(conn, po_number: str, item_code: str) -> int:
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
def _validate_maceri_config(maceri: Dict[str, str]) -> None:
    missing = []
    if not maceri.get("LOCATION_ID"):
        missing.append("[Maceri] MaceriLocationID")
    if not maceri.get("USERNAME"):
        missing.append("[Maceri] Username")
    if not maceri.get("PASSWORD"):
        missing.append("[Maceri] Password")
    if not maceri.get("API_AUTH_URL"):
        missing.append("[Maceri] BaseUrlAuth")
    if not maceri.get("API_ORDER_URL"):
        missing.append("[Maceri] BaseUrlOrder")

    if missing:
        raise RuntimeError("Missing config.ini value(s): " + ", ".join(missing))


def get_api_token(session, *, maceri: Dict[str, str], api_log_path: str, api_log_errors_only: bool) -> str:
    payload = {"userName": maceri["USERNAME"], "password": maceri["PASSWORD"], "persist": True}
    headers = {"Fb-Referer": maceri["FB_REFERER"], "Content-Type": "application/json"}

    t0 = time.perf_counter()
    resp = session.post(maceri["API_AUTH_URL"], headers=headers, json=payload, timeout=90)
    elapsed = time.perf_counter() - t0

    raw_text = resp.text or ""
    _write_maceri_api_log(
        maceri_api_log_path=api_log_path,
        errors_only=api_log_errors_only,
        kind="AUTH",
        url=maceri["API_AUTH_URL"],
        status_code=resp.status_code,
        elapsed_s=elapsed,
        headers=headers,
        request_json={"userName": maceri["USERNAME"], "password": "***", "persist": True},
        response_text=raw_text,
        po_number=None,
    )

    resp.raise_for_status()
    data = resp.json()
    token = data.get("token")
    if not token:
        raise RuntimeError("Missing token in auth response")
    return token


def send_to_api(
    session,
    payload: Dict[str, Any],
    token: str,
    *,
    maceri: Dict[str, str],
    api_log_path: str,
    api_log_errors_only: bool,
) -> Tuple[bool, int, str, str]:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    po_number = payload.get("customerPurchaseOrderNumber", "?")

    t0 = time.perf_counter()
    resp = session.post(maceri["API_ORDER_URL"], headers=headers, json=payload, timeout=120)
    elapsed = time.perf_counter() - t0

    raw_text = resp.text or ""
    snippet = raw_text[:400].replace("\n", " ")

    _write_maceri_api_log(
        maceri_api_log_path=api_log_path,
        errors_only=api_log_errors_only,
        kind="ORDER",
        url=maceri["API_ORDER_URL"],
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
CUSTOMER_ID = "PLUMS"
NOTES = "FROMAPI"
SHIP_METHOD = "1"

SENT_MARKER = "SENT_MACERI"
MAX_INVALID_ITEM_RETRIES_PER_PO = 25


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


def build_json_payload(po_rows, po_number: str) -> Tuple[Optional[Dict[str, Any]], int, int, Dict[str, Set[str]]]:
    if not po_rows:
        return None, 0, 0, {}

    first = po_rows[0]
    delivery_date = _safe_date_to_yyyy_mm_dd(first.DDATE)

    payload: Dict[str, Any] = {
        "customerId": CUSTOMER_ID,
        "customerLocationId": SETTINGS["maceri"]["LOCATION_ID"],
        "requiredDeliveryDate": delivery_date,
        "notes": NOTES,
        "shipMethod": SHIP_METHOD,
        "customerPurchaseOrderNumber": str(first.PO),
        "confirmationNumber": f"SMS{str(first.PO)}",
        "details": [],
    }

    valid = 0
    skipped = 0
    skip_map: Dict[str, Set[str]] = {}

    for row in po_rows:
        upc = getattr(row, "UPC", None)

        unitOfMeasure = "CS"
        if row.ITEMCODE is not None and str(row.ITEMCODE).strip() == "SDLS45":
            unitOfMeasure = "BN"

        itemcode = "" if row.ITEMCODE is None else str(row.ITEMCODE).strip()
        if not itemcode:
            skipped += 1
            add_skip(skip_map, "Missing ITEMCODE", upc)
            continue

        try:
            qty_int = int(row.QTY)
        except Exception:
            skipped += 1
            add_skip(skip_map, "Invalid QTY (non-integer)", upc)
            continue
        if qty_int <= 0:
            skipped += 1
            add_skip(skip_map, "Invalid QTY (<=0)", upc)
            continue

        try:
            cost_val = float(row.COST)
        except Exception:
            skipped += 1
            add_skip(skip_map, "Invalid COST (non-numeric)", upc)
            continue

        payload["details"].append(
            {"itemCode": itemcode, "uomId": unitOfMeasure, "notes": "", "quantity": qty_int, "unitPrice": cost_val}
        )
        valid += 1

    return (payload if payload["details"] else None, valid, skipped, skip_map)


# =============================================================================
# Worker
# =============================================================================
def worker(ui_q: Queue):
    import pyodbc
    import requests
    from requests.exceptions import RequestException, Timeout, HTTPError, ConnectionError

    def ui(level: str, msg: str, detail: str = ""):
        try:
            ui_q.put((level, msg, detail))
        except Exception:
            pass

    conn: Optional[pyodbc.Connection] = None
    session = requests.Session()

    po = ""
    vendor = 0
    vendor_name: Optional[str] = None
    force_send = False
    rec_hdr_f91: Optional[str] = None

    try:
        global SETTINGS
        SETTINGS = load_settings()

        log_dir = SETTINGS["log_dir"]
        setup_logging(log_dir)

        try:
            deleted = purge_logs(log_dir, SETTINGS["log_purge_days"])
            logging.info(f"Log purge: deleted {deleted} file(s) older than {SETTINGS['log_purge_days']} day(s).")
        except Exception as e:
            logging.warning(f"Log purge failed: {e}")

        maceri = SETTINGS["maceri"]

        api_log_ts = datetime.now().strftime("%Y_%m_%d")
        maceri_api_log_path = os.path.join(log_dir, f"MaceriApi_{api_log_ts}.log")
        maceri_api_log_errors_only = False

        logging.info(f"SQL={SETTINGS['server_name']} / {SETTINGS['database']}")
        logging.info(f"MaceriAuthUrl={maceri['API_AUTH_URL']}")
        logging.info(f"MaceriOrderUrl={maceri['API_ORDER_URL']}")
        logging.info(f"MaceriFbReferer={maceri['FB_REFERER']}")
        logging.info(f"MaceriApiDailyLog={maceri_api_log_path} | ErrorsOnly={maceri_api_log_errors_only}")

        po, vendor, force_send = get_sqi_args()
        logging.info(f"SQI args: PO={po} Vendor={vendor} Force={force_send}")

        ui("INFO", f"Starting Maceri Push v{APP_VERSION}", "")
        ui("INFO", "SQI Parameters", f"PO={po} | Vendor={vendor} | Force={force_send}")
        ui("INFO", "Loading config.ini...", os.path.basename(CONFIG_PATH))

        ui("INFO", "Connecting to SQL...", f"{SETTINGS['server_name']} / {SETTINGS['database']}")
        conn = pyodbc.connect(SETTINGS["connection_string"])

        vendor_name = get_vendor_name(conn, vendor)
        rec_hdr_f91 = get_rec_hdr_f91(conn, str(po))
        logging.info(f"Vendor lookup: F27={vendor} => VendorName={vendor_name!r}")
        logging.info(f"REC_HDR lookup: PO={po} => F91={rec_hdr_f91!r}")

        if not force_send:
            if is_po_already_sent(conn, str(po), SENT_MARKER):
                f1254 = get_rec_hdr_f1254(conn, str(po))
                msg = f"PO {po} already marked as sent ({SENT_MARKER}). Not sending again."
                detail = f"REC_HDR.F1254={f1254!r}"
                ui("WARN", msg, detail)
                logging.warning(f"{msg} {detail}")

                try:
                    insert_ravyx_po_status(
                        conn,
                        po_number=str(po),
                        vendor=vendor,
                        vendor_name=vendor_name,
                        status="WARN",
                        f1081_text=msg,
                        dept=None,
                        rec_hdr_f91=rec_hdr_f91,
                        process="Order Export",
                    )
                except Exception as e2:
                    logging.warning(f"PO {po}: unable to insert RAVYX_PO_STATUS (WARN already-sent): {e2}")

                ui("DONE", "Skipped", f"Auto-close in {AUTO_CLOSE_SECONDS}s.")
                return

        _validate_maceri_config(maceri)

        ui("INFO", "Authenticating with Maceri...", "")
        try:
            token = get_api_token(
                session,
                maceri=maceri,
                api_log_path=maceri_api_log_path,
                api_log_errors_only=maceri_api_log_errors_only,
            )
            ui("INFO", "Auth OK", "Sending single PO from SQI")
        except (ConnectionError, Timeout, HTTPError, RequestException) as e:
            title, detail = format_requests_error(e, maceri["API_AUTH_URL"])
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

        all_skips: Dict[str, Set[str]] = {}

        while True:
            po_rows = get_po_data(conn, po)
            dept = get_first_dept(po_rows)

            payload, _valid, _skipped, skip_map = build_json_payload(po_rows, po)
            merge_skip_maps(all_skips, skip_map)
            _remove_missing_itemcode_dupes(all_skips)

            if not payload:
                f1081_msg = build_skip_summary(all_skips, max_chars=F1081_MAX_CHARS)
                msg = f"PO {po} has no valid items to send after cleanup."
                if f1081_msg:
                    msg = msg + " | " + f1081_msg

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
                ok, status_code, snippet, raw_text = send_to_api(
                    session,
                    payload,
                    token,
                    maceri=maceri,
                    api_log_path=maceri_api_log_path,
                    api_log_errors_only=maceri_api_log_errors_only,
                )

                if not ok and status_code in (401, 403):
                    ui("WARN", "Authentication error", "Received 401/403. Refreshing token and retrying once...")
                    try:
                        token = get_api_token(
                            session,
                            maceri=maceri,
                            api_log_path=maceri_api_log_path,
                            api_log_errors_only=maceri_api_log_errors_only,
                        )
                    except (ConnectionError, Timeout, HTTPError, RequestException) as e:
                        title, detail = format_requests_error(e, maceri["API_AUTH_URL"])
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

                    ok, status_code, snippet, raw_text = send_to_api(
                        session,
                        payload,
                        token,
                        maceri=maceri,
                        api_log_path=maceri_api_log_path,
                        api_log_errors_only=maceri_api_log_errors_only,
                    )

                if ok:
                    append_sent_marker(conn, po, SENT_MARKER)

                    f1081_msg = build_skip_summary(all_skips, max_chars=F1081_MAX_CHARS)
                    final_status = "WARN" if f1081_msg.strip() else "SUCCESS"

                    ui("INFO", f"{final_status} PO {po}", "Sent and marked SentToVendor")

                    insert_ravyx_po_status(
                        conn,
                        po_number=str(po),
                        vendor=vendor,
                        vendor_name=vendor_name,
                        status=final_status,
                        f1081_text=f1081_msg,
                        dept=dept,
                        rec_hdr_f91=rec_hdr_f91,
                        process="Order Export",
                    )
                    break

                detail_text = _extract_detail_from_maceri_response(raw_text, snippet)
                invalid = _extract_item_not_found(detail_text)

                if invalid:
                    item_code, _uom = invalid
                    invalid_retry += 1

                    upcs_before_delete = _find_upcs_for_itemcode(po_rows, item_code)
                    reason = _format_item_not_found_reason(item_code)

                    if upcs_before_delete:
                        for u in upcs_before_delete:
                            add_skip(all_skips, reason, u)
                    else:
                        add_skip(all_skips, reason, "")

                    deleted = delete_invalid_item_from_po(conn, str(po), item_code)

                    if deleted > 0:
                        ui("WARN", f"PO {po} - item skipped", f"{reason} (removed, retrying)")
                        logging.warning(
                            f"PO {po}: Invalid itemCode '{item_code}'. "
                            f"UPC(s) pre-delete: {', '.join(upcs_before_delete) if upcs_before_delete else '(none)'}."
                        )

                    if invalid_retry > MAX_INVALID_ITEM_RETRIES_PER_PO:
                        msg = f"PO {po} still failing after max invalid-item retries. Last invalid itemCode={item_code}."
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
                title, detail = format_requests_error(e, maceri["API_ORDER_URL"])
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
        try:
            logging.exception(f"Fatal error: {e}")
        except Exception:
            pass

        try:
            if conn is not None and po:
                try:
                    force_keep_po_open_on_failure(conn, str(po), SENT_MARKER)
                except Exception:
                    pass

            err_txt = str(e)
            if "Missing config.ini value(s)" in err_txt or "Missing config.ini value:" in err_txt:
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
