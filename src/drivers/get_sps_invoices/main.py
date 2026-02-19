import argparse
import configparser
import ctypes
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pyodbc
import requests
from requests.exceptions import RequestException, Timeout, HTTPError, ConnectionError


# =============================================================================
# No UI (scheduled task friendly) + Optional DEV console (DebugScreen=1)
# =============================================================================

# =============================================================================
# Path helpers
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

# =============================================================================
# Optional DEV console (only when frozen + DebugScreen=1)
# =============================================================================
def ensure_console() -> None:
    """
    Open a CMD console even if app is built with --windowed.
    Safe no-op if a console already exists.
    """
    if os.name != "nt":
        return

    kernel32 = ctypes.windll.kernel32

    # already has a console?
    if kernel32.GetConsoleWindow():
        return

    # allocate new console
    if kernel32.AllocConsole() == 0:
        return

    # rebind stdio
    sys.stdout = open("CONOUT$", "w", buffering=1, encoding="utf-8", errors="replace")
    sys.stderr = open("CONOUT$", "w", buffering=1, encoding="utf-8", errors="replace")
    sys.stdin = open("CONIN$", "r", encoding="utf-8", errors="replace")


def _read_debugscreen_quick(path: str) -> int:
    """
    Read Settings.DebugScreen early so we can open console before logging.
    If config.ini isn't found yet, returns 0.
    """
    try:
        cfg = configparser.ConfigParser()
        if not cfg.read(path, encoding="utf-8"):
            return 0
        return int(cfg.get("Settings", "DebugScreen", fallback="0") or "0")
    except Exception:
        return 0


# =============================================================================
# Logging setup (file + console)
# =============================================================================
LOG_DIR = os.path.join(BASE_DIR, "LOG")
os.makedirs(LOG_DIR, exist_ok=True)

log_ts = datetime.now().strftime("%Y_%m_%d")
log_filename = os.path.join(LOG_DIR, f"GetSPSInvoices_{log_ts}.log")


def setup_logging() -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    fh = logging.FileHandler(log_filename, mode="a", encoding="utf-8")
    fh.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S"))

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S"))

    root.addHandler(fh)
    root.addHandler(ch)

    logging.info("=== Start run ===")
    logging.info(f"Frozen={_is_frozen()} | BASE_DIR={BASE_DIR} | CWD={os.getcwd()}")
    logging.info(f"CONFIG_PATH={CONFIG_PATH}")
    logging.info(f"LOG_FILE={log_filename}")


def log_info(msg: str) -> None:
    logging.info(msg)


def log_warn(msg: str) -> None:
    logging.warning(msg)


def log_error(msg: str) -> None:
    logging.error(msg)


# =============================================================================
# Small utils
# =============================================================================
def to_cents(amount_str: str) -> int:
    s = (amount_str or "0").strip().replace(",", ".")
    try:
        return int(round(float(s) * 100))
    except Exception:
        return 0


def mask_token(tok: str) -> str:
    tok = (tok or "").strip()
    if not tok:
        return ""
    if len(tok) <= 12:
        return tok[:3] + "***"
    return tok[:6] + "..." + tok[-4:]


def request_with_retry(
    fn,
    *,
    tries: int = 3,
    base_sleep: float = 1.0,
    what: str = "HTTP",
):
    last = None
    for i in range(1, tries + 1):
        try:
            return fn()
        except (ConnectionError, Timeout, HTTPError, RequestException) as e:
            last = e
            log_warn(f"{what} failed (try {i}/{tries}): {type(e).__name__}: {e}")
            if i < tries:
                time.sleep(base_sleep * i)
            else:
                raise last


def _is_uuid_like(s: str) -> bool:
    s = (s or "").strip()
    if len(s) != 36:
        return False
    return s.count("-") == 4 and all(c.isalnum() or c == "-" for c in s)


def _clip(s: Any, n: int) -> str:
    return ("" if s is None else str(s)).strip()[:n]


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(v)
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


# =============================================================================
# RAVYX_PO_STATUS (minimal writer - keeps your report populated)
# =============================================================================
def upsert_po_status(
    conn,
    *,
    f91: str,
    f27: str,
    step: str,
    vendor_name: str = "",
    status_txt: str = "SUCCESS",
    message: str = "",
    dept: Optional[int] = None,
    f1032: int = 0,
):
    """
    Uses MERGE to update/insert dbo.RAVYX_PO_STATUS.
    For SUCCESS, stores NULL in F1081 to keep report clean.
    """
    f91 = _clip(f91, 20)
    f27 = _clip(f27, 14)
    step = _clip(step, 40)
    vendor_name = _clip(vendor_name, 60)
    status_txt = _clip(status_txt, 60)

    msg_db = None if (status_txt or "").strip().upper() == "SUCCESS" else _clip(message, 5000)
    now = datetime.now()
    dept_i = None if dept is None else _safe_int(dept, 0)
    f1032_i = _safe_int(f1032, 0)

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
        f91,
        f27,
        now,
        step,
        status_txt,
        msg_db,
        vendor_name,
        dept_i,
        f1032_i,
        f1032_i,
        f91,
        step,
        f27,
        vendor_name,
        now,
        status_txt,
        msg_db,
        dept_i,
    )

    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    cur.close()


# =============================================================================
# Config parsing
# =============================================================================
def load_config(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    read_ok = cfg.read(path, encoding="utf-8")
    if not read_ok:
        raise FileNotFoundError(
            "config.ini not loaded.\n"
            f"Tried:\n"
            f"  1) {os.path.join(os.path.dirname(BASE_DIR), 'config.ini')}\n"
            f"  2) {os.path.join(BASE_DIR, 'config.ini')}\n"
        )
    return cfg


def load_unfi_vendors(cfg: configparser.ConfigParser) -> Dict[str, Dict[str, str]]:
    """
    Reads any section that starts with UNFI (ex: [UNFIGW], [UNFIRC], [UNFIVendor123])
    Requires at least SupplierId and StoreID.
    Returns dict keyed by SupplierId (F27) -> {supplier_id, supplier_name, account_number, store_id, section}
    """
    out: Dict[str, Dict[str, str]] = {}
    for sec in cfg.sections():
        if not sec.upper().startswith("UNFI"):
            continue
        if sec.upper() == "UNFI":
            continue  # generic auth/api section

        supplier_id = (cfg.get(sec, "SupplierId", fallback="") or "").strip()
        supplier_name = (cfg.get(sec, "SupplierName", fallback=sec) or "").strip()
        account_number = (cfg.get(sec, "AccountNumber", fallback="") or "").strip()
        store_id = (cfg.get(sec, "StoreID", fallback="") or "").strip()

        if not supplier_id:
            continue
        if not store_id or not store_id.isdigit():
            continue

        out[supplier_id] = {
            "supplier_id": supplier_id,
            "supplier_name": supplier_name,
            "account_number": account_number,
            "store_id": store_id,
            "section": sec,
        }
    return out


# =============================================================================
# Main job
# =============================================================================
def run_job(args) -> int:
    start = time.perf_counter()

    cfg = load_config(CONFIG_PATH)
    log_info(f"Config loaded | sections={cfg.sections()}")

    if "Settings" not in cfg:
        raise RuntimeError("config.ini missing [Settings]")

    SERVER_NAME = (cfg.get("Settings", "ServerName", fallback="") or "").strip()
    SQL_DRIVER = (cfg.get("Settings", "SQLDriver", fallback="SQL Server") or "SQL Server").strip()
    DB_NAME = (cfg.get("Settings", "DatabaseName", fallback="STORESQL") or "STORESQL").strip()
    STORE_NUMBER = _safe_int(cfg.get("Settings", "StoreNumber", fallback="0"), 0)

    if not SERVER_NAME:
        raise RuntimeError("Settings.ServerName is empty")

    # UNFI generic
    if "UNFI" not in cfg:
        raise RuntimeError("config.ini missing [UNFI] (generic UNFI auth/api settings)")

    UNFI_AUTH_URL = (cfg.get("UNFI", "AuthUrl", fallback="") or "").strip()
    UNFI_CLIENT_ID = (cfg.get("UNFI", "ClientId", fallback="") or "").strip()
    UNFI_USERNAME = (cfg.get("UNFI", "Username", fallback="") or "").strip()
    UNFI_PASSWORD = (cfg.get("UNFI", "Password", fallback="") or "").strip().strip('"').strip("'")
    UNFI_TIMEOUT_AUTH = _safe_int(cfg.get("UNFI", "TimeoutAuthSec", fallback="90"), 90)
    UNFI_TIMEOUT_GET = _safe_int(cfg.get("UNFI", "TimeoutGetSec", fallback="60"), 60)
    UNFI_API_BASE = (cfg.get("UNFI", "ApiBaseUrl", fallback="") or "").strip().rstrip("/")

    if not (UNFI_AUTH_URL and UNFI_CLIENT_ID and UNFI_USERNAME and UNFI_PASSWORD and UNFI_API_BASE):
        raise RuntimeError("UNFI settings incomplete (AuthUrl/ClientId/Username/Password/ApiBaseUrl)")

    # Invafresh
    if "GetSpsInvoice" not in cfg:
        raise RuntimeError("config.ini missing [GetSpsInvoice]")

    INV_BASE = (cfg.get("GetSpsInvoice", "BaseUrl", fallback="") or "").strip().rstrip("/")
    INV_USER = (cfg.get("GetSpsInvoice", "Username", fallback="") or "").strip()
    INV_PASS = (cfg.get("GetSpsInvoice", "Password", fallback="") or "").strip()

    if not (INV_BASE and INV_USER and INV_PASS):
        raise RuntimeError("GetSpsInvoice settings incomplete (BaseUrl/Username/Password)")

    INV_LOGIN_URL = f"{INV_BASE}/login"
    INV_RECEIPTS_URL = f"{INV_BASE}/backroom_products/receipt_transactions"

    # UNFI vendors
    unfi_vendors = load_unfi_vendors(cfg)
    if not unfi_vendors:
        raise RuntimeError("No UNFI vendor sections found. Add [UNFIxxx] with SupplierId+StoreID")

    if args.f27:
        if args.f27 not in unfi_vendors:
            raise RuntimeError(
                f"Vendor {args.f27} not found in UNFI vendor sections. Add section like [UNFIVendor...] SupplierId={args.f27}"
            )
        vendors_to_process = {args.f27: unfi_vendors[args.f27]}
    else:
        vendors_to_process = unfi_vendors

    log_info(f"SQL: {SERVER_NAME}\\{DB_NAME} (Driver={SQL_DRIVER}) StoreNumber(SMS)={STORE_NUMBER}")
    log_info(f"UNFI ApiBaseUrl={UNFI_API_BASE}")
    log_info(f"Invafresh BaseUrl={INV_BASE}")
    log_info(f"UNFI vendors configured: {', '.join(sorted(vendors_to_process.keys()))}")

    # --- SQL helpers ---
    def _get_conn() -> pyodbc.Connection:
        conn_str = f"DRIVER={{{SQL_DRIVER}}};SERVER={SERVER_NAME};DATABASE={DB_NAME};Trusted_Connection=yes"
        return pyodbc.connect(conn_str)

    def get_vendor_name(conn, f27: str) -> str:
        try:
            cur = conn.cursor()
            cur.execute("SELECT TOP 1 F334 FROM dbo.VENDOR_TAB WHERE F27 = ?", str(f27))
            row = cur.fetchone()
            cur.close()
            return (str(row[0]).strip() if row and row[0] is not None else "")
        except Exception:
            return ""

    def fetch_order_candidates(conn, f27_value: str) -> List[Tuple[str, str]]:
        """
        Candidates are CLOSED orders still in ORDER status, with GUID in F1254.
        Filters:
          - F1068='ORDER' and F1067='CLOSE'
          - F1254 not null/empty
          - skip if F2630='RTC' (already queued)
          - skip if F1254 contains 'SENT'
          - else must be uuid-like
        """
        sql = f"""
            SELECT F1032, F1254, ISNULL(F2630,'') AS F2630
            FROM [{DB_NAME}].[dbo].[REC_HDR]
            WHERE F27 = ?
              AND F1068 = 'ORDER'
              AND F1067 = 'CLOSE'
              AND F1254 IS NOT NULL
              AND LTRIM(RTRIM(CAST(F1254 AS varchar(200)))) <> ''
              AND ISNULL(F2630,'') <> 'RTC'
        """
        cur = conn.cursor()
        cur.execute(sql, (str(f27_value),))
        out: List[Tuple[str, str]] = []

        for po, f1254, f2630 in cur.fetchall():
            po_s = _clip(po, 20)
            f1254_s = ("" if f1254 is None else str(f1254)).strip()
            f2630_s = ("" if f2630 is None else str(f2630)).strip()

            if not po_s or not f1254_s:
                continue
            if f2630_s.upper() == "RTC":
                continue
            if "SENT" in f1254_s.upper():
                continue
            if not _is_uuid_like(f1254_s):
                continue

            out.append((po_s, f1254_s))

        cur.close()
        return out

    def update_rec_reg_from_unfi(conn, po_number: str, details: List[Dict[str, Any]]) -> Tuple[int, int]:
        """
        For each UNFI detail line:
          - match REC_REG by F1032 (PO) and F01 (GTIN)
          - only active item rows: F1067='ITEM' and ISNULL(F1069,0)=0
          - update:
              F64 = qty
              F38 = unit cost
              F65 = unit cost * qty
        Returns (updated_rows, missing_rows)
        """
        updated = 0
        missing = 0
        cur = conn.cursor()

        sel = f"""
            SELECT 1
            FROM [{DB_NAME}].[dbo].[REC_REG]
            WHERE F1032 = ?
              AND F01 = ?
              AND F1067 = 'ITEM'
              AND ISNULL(F1069,0) = 0
        """
        upd = f"""
            UPDATE [{DB_NAME}].[dbo].[REC_REG]
            SET
                F64 = ?,
                F65 = ?,
                F38 = ?
            WHERE F1032 = ?
              AND F01 = ?
              AND F1067 = 'ITEM'
              AND ISNULL(F1069,0) = 0
        """

        for it in details:
            gtin = (str(it.get("gtin", "") or "")).strip()
            if not gtin:
                continue

            qty = _safe_int(it.get("quantity", 0), 0)
            unit_cost = _safe_float(it.get("cost", 0), 0.0)
            line_total = unit_cost * qty

            cur.execute(sel, (po_number, gtin))
            if cur.fetchone() is None:
                missing += 1
                continue

            cur.execute(upd, (qty, line_total, unit_cost, po_number, gtin))
            if cur.rowcount:
                updated += int(cur.rowcount)

        conn.commit()
        cur.close()
        return updated, missing

    def set_hdr_ready_to_close(conn, po_number: str) -> None:
        """
        Queue for SMS close:
          - F1068='RECV'
          - F2630='RTC'
        Guard:
          - only if currently ORDER (prevents double-processing)
        """
        cur = conn.cursor()
        sql = f"""
            UPDATE [{DB_NAME}].[dbo].[REC_HDR]
            SET
                F1068 = 'RECV',
                F2630 = 'RTC'
            WHERE F1032 = ?
              AND F1068 = 'ORDER'
        """
        cur.execute(sql, (po_number,))
        conn.commit()
        cur.close()

    # --- Auth / HTTP ---
    def fetch_unfi_id_token(session: requests.Session) -> str:
        payload = {"client_id": UNFI_CLIENT_ID, "username": UNFI_USERNAME, "password": UNFI_PASSWORD}

        def do():
            return session.post(
                UNFI_AUTH_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=UNFI_TIMEOUT_AUTH,
            )

        resp = request_with_retry(do, tries=3, base_sleep=1, what="UNFI Auth")
        if resp.status_code != 200:
            raise RuntimeError(f"UNFI auth failed: {resp.status_code} {resp.text[:400]}")

        data = resp.json()
        tok = (((data.get("AuthenticationResult") or {}).get("IdToken")) or "").strip()
        if not tok:
            raise RuntimeError("UNFI auth response missing AuthenticationResult.IdToken")
        return tok

    def invafresh_login(session: requests.Session) -> str:
        payload = {"username": INV_USER, "password": INV_PASS}

        def do():
            return session.post(
                INV_LOGIN_URL,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=60,
            )

        resp = request_with_retry(do, tries=3, base_sleep=1, what="Invafresh Login")
        if resp.status_code != 200:
            raise RuntimeError(f"Invafresh login failed: {resp.status_code} {resp.text[:400]}")

        data = resp.json()
        tok = (data.get("access_token") or "").strip()
        if not tok:
            raise RuntimeError("Invafresh login did not return access_token.")
        return tok

    def build_unfi_get_order_url(store_id: str, order_guid: str) -> str:
        guid = (order_guid or "").strip()
        if not guid:
            raise ValueError("order_guid is empty")
        if not _is_uuid_like(guid):
            raise ValueError(f"F1254 value is not a GUID: {guid!r}")
        return f"{UNFI_API_BASE}/stores/{store_id}/orders/{guid}"

    def fetch_unfi_order(session: requests.Session, token: str, store_id: str, order_guid: str) -> Dict[str, Any]:
        url = build_unfi_get_order_url(store_id, order_guid)
        headers = {"Authorization": f"Bearer {token}"}

        def do():
            return session.get(url, headers=headers, timeout=UNFI_TIMEOUT_GET)

        log_info(f"UNFI GET url={url}")
        resp = request_with_retry(do, tries=3, base_sleep=1, what="UNFI GET Order")
        if resp.status_code != 200:
            raise RuntimeError(f"UNFI GET failed ({resp.status_code}): {resp.text[:500]}")
        return resp.json()

    def unfi_order_to_receipt_transactions(
        api_data: Dict[str, Any], *, po_override: str, store_number: int, conn: pyodbc.Connection
    ) -> Dict[str, Any]:
        details = api_data.get("details", []) or []

        invoice_number = po_override
        order_number = str(invoice_number)[:5] if invoice_number else ""
        date_str = datetime.now().strftime("%Y%m%d")

        _dept_cache: Dict[str, str] = {}

        def get_dept_num(gtin: str) -> str:
            gtin = (gtin or "").strip()
            if not gtin:
                return "UNKNOWN"
            if gtin in _dept_cache:
                return _dept_cache[gtin]

            query = f"""
                SELECT TOP 1
                    (SELECT SDP.F03 FROM [{DB_NAME}].[dbo].[SDP_TAB] SDP WHERE SDP.F04 = PST.F04) AS DeptNumber
                FROM [{DB_NAME}].[dbo].[POS_TAB] PST
                JOIN [{DB_NAME}].[dbo].[OBJ_TAB] OBJ ON PST.F01 = OBJ.F01
                WHERE PST.F01 = ?
            """
            cur = conn.cursor()
            cur.execute(query, (gtin,))
            row = cur.fetchone()
            cur.close()
            dept = str(row[0]).strip() if row and row[0] is not None else "UNKNOWN"
            _dept_cache[gtin] = dept
            return dept

        tx: List[Dict[str, Any]] = []
        for item in details:
            sku = str(item.get("gtin", "") or "").strip()
            if not sku:
                continue

            unit_cost = _safe_float(item.get("cost", 0), 0.0)
            qty = _safe_int(item.get("quantity", 0), 0)
            uom = (str(item.get("uom", "")).strip().lower() or "cs")
            dept_num = get_dept_num(sku)

            tx.append(
                {
                    "invoice_number": invoice_number,
                    "order_number": int(order_number) if order_number.isdigit() else None,
                    "external_order_number": invoice_number,
                    "delivery_or_invoice_date": date_str,
                    "store_number": int(store_number),
                    "department_number": dept_num,
                    "sku": sku,
                    "quantity": qty,
                    "quantity_unit_of_measure": uom,
                    "amount": to_cents(f"{unit_cost:.2f}"),
                }
            )

        return {"receipt_transactions": tx}

    def post_receipts_to_invafresh(session: requests.Session, inv_token: str, payload: Dict[str, Any]) -> requests.Response:
        headers = {"Authorization": f"Bearer {inv_token}", "Content-Type": "application/json"}

        def do():
            return session.post(INV_RECEIPTS_URL, headers=headers, data=json.dumps(payload), timeout=90)

        return request_with_retry(do, tries=3, base_sleep=1, what="Invafresh POST receipts")

    # -------------------------------------------------------------------------
    # Work
    # -------------------------------------------------------------------------
    unfi_sess = requests.Session()
    inv_sess = requests.Session()

    ok = 0
    failed = 0
    total_candidates = 0

    conn = _get_conn()
    try:
        log_info("UNFI: Connecting...")
        unfi_token = fetch_unfi_id_token(unfi_sess)
        log_info(f"UNFI token acquired (masked)={mask_token(unfi_token)}")

        log_info("Invafresh: Connecting...")
        inv_token = invafresh_login(inv_sess)
        log_info(f"Invafresh token acquired (masked)={mask_token(inv_token)}")

        for f27, vinfo in vendors_to_process.items():
            store_id = vinfo["store_id"]
            supplier_name = vinfo["supplier_name"]

            vendor_name_db = get_vendor_name(conn, f27) or supplier_name
            log_info(f"Vendor {f27} ({vendor_name_db}) -> UNFI store_id={store_id}")

            orders = fetch_order_candidates(conn, f27)
            total_candidates += len(orders)
            log_info(f"Candidates: {len(orders)} order(s) for vendor={f27}")

            if not orders:
                continue

            for po, guid in orders:
                try:
                    log_info(f"Processing PO={po} vendor={f27} guid={guid}")

                    # optional "started" ping
                    try:
                        upsert_po_status(
                            conn,
                            f91=po,
                            f27=f27,
                            step="GetSPSInvoices",
                            vendor_name=vendor_name_db,
                            status_txt="SUCCESS",
                            message="Processing started",
                            dept=None,
                            f1032=_safe_int(po, 0),
                        )
                    except Exception:
                        pass

                    api_data = fetch_unfi_order(unfi_sess, unfi_token, store_id, guid)
                    details = api_data.get("details", []) or []
                    if not details:
                        raise RuntimeError("UNFI order has no details lines")

                    # 1) Update REC_REG qty/costs
                    updated_rows, missing_rows = update_rec_reg_from_unfi(conn, po, details)
                    log_info(f"PO={po}: REC_REG updated_rows={updated_rows} missing_rows={missing_rows}")

                    # 2) Post receipts to Invafresh
                    payload = unfi_order_to_receipt_transactions(
                        api_data, po_override=po, store_number=STORE_NUMBER, conn=conn
                    )
                    line_count = len(payload.get("receipt_transactions", []))
                    if line_count <= 0:
                        raise RuntimeError("No receipt_transactions lines to post")

                    resp = post_receipts_to_invafresh(inv_sess, inv_token, payload)

                    if resp.status_code == 401:
                        log_warn("Invafresh returned 401, re-login once then retry")
                        inv_token = invafresh_login(inv_sess)
                        resp = post_receipts_to_invafresh(inv_sess, inv_token, payload)

                    if not (200 <= resp.status_code < 300):
                        raise RuntimeError(f"Invafresh POST failed ({resp.status_code}): {resp.text[:600]}")

                    # 3) Queue for SMS close: set RECV + RTC
                    set_hdr_ready_to_close(conn, po)

                    # 4) Report
                    try:
                        upsert_po_status(
                            conn,
                            f91=po,
                            f27=f27,
                            step="RTC",
                            vendor_name=vendor_name_db,
                            status_txt="SUCCESS",
                            message=f"Posted Invafresh OK | REC_REG updated={updated_rows} missing={missing_rows}",
                            dept=None,
                            f1032=_safe_int(po, 0),
                        )
                    except Exception:
                        pass

                    ok += 1
                    log_info(f"SUCCESS PO={po}: Posted + set F1068=RECV + F2630=RTC ({line_count} line(s))")

                except Exception as e:
                    failed += 1
                    log_error(f"FAILED PO={po}: {type(e).__name__}: {e}")

                    try:
                        upsert_po_status(
                            conn,
                            f91=po,
                            f27=f27,
                            step="GetSPSInvoices",
                            vendor_name=vendor_name_db,
                            status_txt="FAILED",
                            message=f"{type(e).__name__}: {e}",
                            dept=None,
                            f1032=_safe_int(po, 0),
                        )
                    except Exception:
                        pass

        dur = round(time.perf_counter() - start, 2)
        log_info(f"Done. OK={ok} Failed={failed} TotalCandidates={total_candidates} Duration={dur}s")
        logging.info("=== End run ===")
        return 0 if failed == 0 else 2

    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            unfi_sess.close()
        except Exception:
            pass
        try:
            inv_sess.close()
        except Exception:
            pass


# =============================================================================
# Main
# =============================================================================
def main():
    # Open DEV console early (only when frozen + DebugScreen=1)
    if _is_frozen() and _read_debugscreen_quick(CONFIG_PATH) == 1:
        ensure_console()
        print("=== DEBUG CONSOLE ENABLED (DebugScreen=1) ===")
        print(f"BASE_DIR={BASE_DIR}")
        print(f"CONFIG_PATH={CONFIG_PATH}")

    parser = argparse.ArgumentParser(description="Get SPS Invoices (UNFI -> Invafresh) - Scheduled task mode (no UI)")
    parser.add_argument(
        "--f27",
        default="",
        help="Optional: process only this UNFI vendor (must exist in UNFI vendor sections). If empty, processes all configured UNFI vendors.",
    )
    args = parser.parse_args()

    setup_logging()

    exit_code = 2
    try:
        exit_code = run_job(args)
    except Exception as e:
        log_error(f"Fatal error: {type(e).__name__}: {e}")
        logging.info("=== End run (fatal) ===")
        exit_code = 2

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
