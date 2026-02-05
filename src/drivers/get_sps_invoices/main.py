import argparse
import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import configparser
import pyodbc
import requests

from ui_getSPSInvoices import GetSpsInvoicesUI as VendorSendUI


# =============================================================================
# Path helpers 
# =============================================================================
def _is_frozen() -> bool:
    return getattr(sys, "frozen", False)


def _base_dir() -> str:
    return os.path.dirname(sys.executable) if _is_frozen() else os.path.dirname(os.path.abspath(__file__))


BASE_DIR = _base_dir()
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")


# =============================================================================
# Logging setup (file + console)
# =============================================================================
LOG_DIR = os.path.join(BASE_DIR, "Log")
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

    logging.info(f"Frozen={_is_frozen()} | BASE_DIR={BASE_DIR} | CWD={os.getcwd()}")
    logging.info(f"CONFIG_PATH={CONFIG_PATH}")
    logging.info(f"LOG_FILE={log_filename}")


# =============================================================================
# UI helper (summary-only UI, full details in log)
# =============================================================================
def ui_summary(q: Optional["queue.Queue"], level: str, msg: str, detail: str = "") -> None:
    lvl = (level or "INFO").upper()
    m = (msg or "").strip()
    d = (detail or "").strip()

    line = f"{m} | {d}" if d else m
    if lvl in ("ERROR", "CRITICAL"):
        logging.error(line)
    elif lvl in ("WARN", "WARNING"):
        logging.warning(line)
    else:
        logging.info(line)

    if q is not None:
        q.put((lvl, m, d))


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
    ui_q: Optional["queue.Queue"] = None,
    what: str = "HTTP",
):
    last = None
    for i in range(1, tries + 1):
        try:
            return fn()
        except Exception as e:
            last = e
            log_warn(f"{what} failed (try {i}/{tries}): {type(e).__name__}: {e}")
            if i < tries:
                if ui_q is not None:
                    ui_summary(ui_q, "WARN", f"{what} retrying...", f"Attempt {i+1}/{tries}")
                time.sleep(base_sleep * i)
            else:
                raise last


def _is_uuid_like(s: str) -> bool:
    s = (s or "").strip()
    if len(s) != 36:
        return False
    # very light validation (avoid adding uuid import)
    return (
        s.count("-") == 4
        and all(c.isalnum() or c == "-" for c in s)
    )


# =============================================================================
# Worker
# =============================================================================
def worker(args, ui_q: "queue.Queue") -> None:
    start = time.perf_counter()

    config = configparser.ConfigParser()
    read_ok = config.read(CONFIG_PATH, encoding="utf-8")
    log_info(f"Config loaded={bool(read_ok)} | sections={config.sections()}")

    if "Settings" not in config:
        ui_summary(ui_q, "ERROR", "Config error", "config.ini missing [Settings] or not loaded.")
        ui_summary(ui_q, "DONE", "Done", "Processed OK=0 | Failed=0 | Total=0")
        return

    # Settings
    SERVER_NAME = config["Settings"]["ServerName"]
    SQL_DRIVER = config["Settings"].get("SQLDriver", "SQL Server")
    DB_NAME = config["Settings"].get("DatabaseName", "STORESQL")
    STORE_NUMBER = int(config["Settings"].get("StoreNumber", "0") or "0")  # SMS store number (ex: 3)

    # UNFI
    if "UNFI" not in config:
        ui_summary(ui_q, "ERROR", "Config error", "config.ini missing [UNFI].")
        ui_summary(ui_q, "DONE", "Done", "Processed OK=0 | Failed=0 | Total=0")
        return

    UNFI_AUTH_URL = config["UNFI"]["AuthUrl"].strip()
    UNFI_CLIENT_ID = config["UNFI"]["ClientId"].strip()
    UNFI_USERNAME = config["UNFI"]["Username"].strip()
    UNFI_PASSWORD = config["UNFI"]["Password"].strip().strip('"')
    UNFI_TIMEOUT_AUTH = int(config["UNFI"].get("TimeoutAuthSec", "90"))

    UNFI_API_BASE = config["UNFI"]["ApiBaseUrl"].strip().rstrip("/")


    # GET uses the UNFI StoreID (ex: 127705).
    UNFI_ORDERS_STORE_ID = config["UNFI"].get("OrdersStoreId", "").strip().strip('"')

    if not UNFI_ORDERS_STORE_ID or not UNFI_ORDERS_STORE_ID.isdigit():
        ui_summary(
            ui_q,
            "ERROR",
            "Config error",
            "Missing/invalid [UNFI] OrdersStoreId (expected numeric like 127705) used for GET URL path.",
        )
        ui_summary(ui_q, "DONE", "Done", "Processed OK=0 | Failed=0 | Total=0")
        return

    # Invafresh
    if "GetSpsInvoice" not in config:
        ui_summary(ui_q, "ERROR", "Config error", "config.ini missing [GetSpsInvoice].")
        ui_summary(ui_q, "DONE", "Done", "Processed OK=0 | Failed=0 | Total=0")
        return

    INV_BASE = config["GetSpsInvoice"]["BaseUrl"].strip().rstrip("/")
    INV_USER = config["GetSpsInvoice"]["Username"].strip()
    INV_PASS = config["GetSpsInvoice"]["Password"].strip()

    INV_LOGIN_URL = f"{INV_BASE}/login"
    INV_RECEIPTS_URL = f"{INV_BASE}/backroom_products/receipt_transactions"

    log_info(f"SQL: {SERVER_NAME}\\{DB_NAME} (Driver={SQL_DRIVER}) StoreNumber(SMS)={STORE_NUMBER}")
    log_info(f"UNFI AuthUrl={UNFI_AUTH_URL}")
    log_info(f"UNFI ApiBaseUrl={UNFI_API_BASE} OrdersStoreId(GET path)={UNFI_ORDERS_STORE_ID!r}")
    log_info(f"Invafresh BaseUrl={INV_BASE}")

    # --- SQL helpers ---
    def _get_conn() -> pyodbc.Connection:
        conn_str = f"DRIVER={{{SQL_DRIVER}}};SERVER={SERVER_NAME};DATABASE={DB_NAME};Trusted_Connection=yes"
        return pyodbc.connect(conn_str)

    _dept_cache: Dict[str, Tuple[str, str]] = {}

    def get_department(upc: str) -> Tuple[str, str]:
        upc = (upc or "").strip()
        if not upc:
            return ("UNKNOWN", "UNKNOWN")
        if upc in _dept_cache:
            return _dept_cache[upc]

        query = f"""
            SELECT DISTINCT
                (SELECT SDP.F03 FROM [{DB_NAME}].[dbo].[SDP_TAB] SDP WHERE SDP.F04 = PST.F04) AS DeptNumber,
                (SELECT DPT.F238 FROM [{DB_NAME}].[dbo].[DEPT_TAB] DPT
                 WHERE DPT.F03 = (SELECT SDP.F03 FROM [{DB_NAME}].[dbo].[SDP_TAB] SDP WHERE SDP.F04 = PST.F04)) AS DeptName
            FROM [{DB_NAME}].[dbo].[POS_TAB] PST
            JOIN [{DB_NAME}].[dbo].[OBJ_TAB] OBJ ON PST.F01 = OBJ.F01
            WHERE PST.F01 = ?
        """
        conn = _get_conn()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(query, (upc,))
            row = cur.fetchone()
            if row and len(row) >= 2 and row[0] is not None:
                res = (str(row[0]), str(row[1]) if row[1] is not None else "")
            else:
                res = ("UNKNOWN", "UNKNOWN")
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass
            conn.close()

        _dept_cache[upc] = res
        return res

    def fetch_order_candidates(f27_value: str) -> List[Tuple[str, str]]:
        sql = f"""
            SELECT F1032, F1254
            FROM [{DB_NAME}].[dbo].[REC_HDR]
            WHERE F27 = ?
              AND F1068 = 'ORDER'
              AND F1067 = 'CLOSE'
              AND F1254 IS NOT NULL
        """
        conn = _get_conn()
        cur = None
        out: List[Tuple[str, str]] = []
        try:
            cur = conn.cursor()
            cur.execute(sql, (str(f27_value),))
            for po, guid in cur.fetchall():
                po_s = str(po).strip() if po else ""
                guid_s = str(guid).strip() if guid else ""
                if po_s and guid_s:
                    out.append((po_s, guid_s))
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass
            conn.close()
        return out

    def mark_order_received(po_number: str) -> None:
        sql = f"UPDATE [{DB_NAME}].[dbo].[REC_HDR] SET F1068 = 'RECV' WHERE F1032 = ?"
        conn = _get_conn()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(sql, (po_number,))
            conn.commit()
        finally:
            try:
                if cur:
                    cur.close()
            except Exception:
                pass
            conn.close()

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

        resp = request_with_retry(do, tries=3, base_sleep=1, ui_q=ui_q, what="UNFI Auth")
        if resp.status_code != 200:
            raise RuntimeError(f"UNFI auth failed: {resp.status_code} {resp.text[:400]}")
        return resp.json()["AuthenticationResult"]["IdToken"]

    def invafresh_login(session: requests.Session) -> str:
        payload = {"username": INV_USER, "password": INV_PASS}

        def do():
            return session.post(
                INV_LOGIN_URL,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=60,
            )

        resp = request_with_retry(do, tries=3, base_sleep=1, ui_q=ui_q, what="Invafresh Login")
        if resp.status_code != 200:
            raise RuntimeError(f"Invafresh login failed: {resp.status_code} {resp.text[:400]}")
        tok = resp.json().get("access_token")
        if not tok:
            raise RuntimeError("Invafresh login did not return access_token.")
        return tok

    # GET https://posapi.../stores/{OrdersStoreId}/orders/{guid}
    def build_unfi_get_order_url(order_guid: str) -> str:
        guid = (order_guid or "").strip()
        if not guid:
            raise ValueError("order_guid is empty")
     
        if not _is_uuid_like(guid):
            raise ValueError(f"F1254 value is not a GUID: {guid!r}")
        return f"{UNFI_API_BASE}/stores/{UNFI_ORDERS_STORE_ID}/orders/{guid}"

    def fetch_unfi_order(session: requests.Session, token: str, order_guid: str) -> Dict[str, Any]:
        url = build_unfi_get_order_url(order_guid)
        headers = {"Authorization": f"Bearer {token}"}

        def do():
            return session.get(url, headers=headers, timeout=60)

        log_info(f"UNFI GET url={url}")
        resp = request_with_retry(do, tries=3, base_sleep=1, ui_q=ui_q, what="UNFI GET Order")

        if resp.status_code != 200:
            raise RuntimeError(f"UNFI GET failed ({resp.status_code}): {resp.text[:500]}")

        return resp.json()

    def unfi_order_to_receipt_transactions(api_data: Dict[str, Any], po_override: str) -> Dict[str, Any]:
        details = api_data.get("details", []) or []

        invoice_number = po_override
        order_number = str(invoice_number)[:5] if invoice_number else ""
        date_str = datetime.now().strftime("%Y%m%d")

        tx = []
        for item in details:
            sku = str(item.get("gtin", "")).strip()
            cost = float(item.get("cost", 0) or 0)
            qty = int(item.get("quantity", 0) or 0)
            uom = (str(item.get("uom", "")).strip().lower() or "cs")

            dept_num, _ = get_department(sku)

            tx.append(
                {
                    "invoice_number": invoice_number,
                    "order_number": int(order_number) if order_number.isdigit() else None,
                    "external_order_number": invoice_number,
                    "delivery_or_invoice_date": date_str,
                    "store_number": int(STORE_NUMBER),
                    "department_number": dept_num,
                    "sku": sku,
                    "quantity": qty,
                    "quantity_unit_of_measure": uom,
                    "amount": to_cents(f"{cost:.2f}"),
                }
            )

        return {"receipt_transactions": tx}

    def post_receipts_to_invafresh(session: requests.Session, inv_token: str, payload: Dict[str, Any]) -> requests.Response:
        headers = {"Authorization": f"Bearer {inv_token}", "Content-Type": "application/json"}

        def do():
            return session.post(INV_RECEIPTS_URL, headers=headers, data=json.dumps(payload), timeout=90)

        return request_with_retry(do, tries=3, base_sleep=1, ui_q=ui_q, what="Invafresh POST receipts")

    # ---- Start job (UI summary) ----
    ui_summary(ui_q, "INFO", "Get SPS Invoices", f"Starting... (Vendor={args.f27})")

    unfi_sess = requests.Session()
    inv_sess = requests.Session()

    try:
        ui_summary(ui_q, "INFO", "UNFI", "Connecting...")
        unfi_token = fetch_unfi_id_token(unfi_sess)
        log_info(f"UNFI token acquired (masked)={mask_token(unfi_token)}")
        ui_summary(ui_q, "INFO", "UNFI", "Connected")

        ui_summary(ui_q, "INFO", "Invafresh", "Connecting...")
        inv_token = invafresh_login(inv_sess)
        log_info(f"Invafresh token acquired (masked)={mask_token(inv_token)}")
        ui_summary(ui_q, "INFO", "Invafresh", "Connected")

        orders = fetch_order_candidates(args.f27)
        log_info(f"Orders candidates from REC_HDR: {len(orders)} (F27={args.f27})")

        if not orders:
            ui_summary(ui_q, "DONE", "Done", "No orders found to process.")
            return

        ui_summary(ui_q, "INFO", "Orders", f"Found {len(orders)} order(s) to process")

        ok = 0
        failed = 0

        for po, guid in orders:
            try:
                ui_summary(ui_q, "INFO", "Processing", f"PO {po}")

                api_data = fetch_unfi_order(unfi_sess, unfi_token, guid)
                ui_summary(ui_q, "INFO", "UNFI", f"PO {po}: Order found")

                payload = unfi_order_to_receipt_transactions(api_data, po_override=po)
                line_count = len(payload.get("receipt_transactions", []))
                log_info(f"PO {po}: receipt_transactions lines={line_count}")

                if line_count <= 0:
                    ui_summary(ui_q, "WARN", "Skipped", f"PO {po}: No lines to post")
                    continue

                resp = post_receipts_to_invafresh(inv_sess, inv_token, payload)

                if resp.status_code == 401:
                    log_warn("Invafresh returned 401, re-login once then retry")
                    ui_summary(ui_q, "WARN", "Invafresh", "Session expired, reconnecting...")
                    inv_token = invafresh_login(inv_sess)
                    resp = post_receipts_to_invafresh(inv_sess, inv_token, payload)

                if not (200 <= resp.status_code < 300):
                    raise RuntimeError(f"Invafresh POST failed ({resp.status_code}): {resp.text[:600]}")

                mark_order_received(po)

                ok += 1
                ui_summary(ui_q, "SUCCESS", "Success", f"PO {po}: Posted + marked RECV ({line_count} line(s))")

            except Exception as e:
                failed += 1
                log_error(f"PO {po} failed: {type(e).__name__}: {e}")
                ui_summary(ui_q, "ERROR", "PO failed", f"PO {po}: {type(e).__name__}")

        dur = round(time.perf_counter() - start, 2)
        ui_summary(ui_q, "DONE", "Done", f"Processed OK={ok} | Failed={failed} | Total={len(orders)} | {dur}s")

    except Exception as e:
        log_error(f"Fatal error: {type(e).__name__}: {e}")
        ui_summary(ui_q, "ERROR", "Fatal error", type(e).__name__)
        ui_summary(ui_q, "DONE", "Done", "Processed OK=0 | Failed=1 | Total=1")

    finally:
        try:
            unfi_sess.close()
        except Exception:
            pass
        try:
            inv_sess.close()
        except Exception:
            pass


# =============================================================================
# Main (UI + thread)
# =============================================================================
def main():
    parser = argparse.ArgumentParser(description="Get SPS Invoices (UNFI -> Invafresh) with UI")
    parser.add_argument("--f27", default="3954", help="Vendor number in REC_HDR.F27 (ex: 3954 or 7313)")
    parser.add_argument("--autoclose", default="20", help="Auto-close seconds after done/error (default 20)")
    args = parser.parse_args()

    setup_logging()

    ui_q: "queue.Queue" = queue.Queue()
    ui = VendorSendUI(title="Get SPS Invoices", queue=ui_q, auto_close_seconds=int(args.autoclose))

    t = threading.Thread(target=worker, args=(args, ui_q), daemon=True)
    t.start()

    ui.run()


if __name__ == "__main__":
    main()
