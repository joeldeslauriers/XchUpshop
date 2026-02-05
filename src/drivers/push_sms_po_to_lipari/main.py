import os
import sys
import csv
import time
import shutil
import logging
import configparser
import threading
import queue
from datetime import datetime, timedelta
from pathlib import Path
from collections import namedtuple
from typing import Optional, Dict, Any, Tuple, List

import pyodbc


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


def get_sqi_args_optional() -> Tuple[str, int]:
    po = _strip_quotes(sys.argv[1]) if len(sys.argv) >= 2 else ""
    vendor_raw = _strip_quotes(sys.argv[2]) if len(sys.argv) >= 3 else ""
    vendor = 0

    if vendor_raw:
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
log_filename = os.path.join(LOG_DIR, f"pushSMSPOtoLipari_logs_{log_ts}.log")


class UIQueueHandler(logging.Handler):
    """
    Push WARNING/ERROR logs into the UI queue as SUMMARY events.
    """

    def __init__(self, q: "queue.Queue"):
        super().__init__()
        self.q = q

    def emit(self, record: logging.LogRecord) -> None:
        try:
            lvl = (record.levelname or "").upper()
            msg = record.getMessage()
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
# Logs purge 
# =============================================================================
def purge_logs(log_dir: str, days_to_keep: int) -> int:
    if not days_to_keep or days_to_keep <= 0:
        return 0

    cutoff = datetime.now() - timedelta(days=days_to_keep)
    deleted = 0
    p = Path(log_dir)
    if not p.exists():
        return 0

    patterns = [
        "pushSMSPOtoLipari_logs_*.log",
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
# Config
# =============================================================================
config = configparser.ConfigParser()
if not os.path.exists(CONFIG_PATH):
    raise FileNotFoundError(f"config.ini not found next to EXE/script: {CONFIG_PATH}")

config.read(CONFIG_PATH, encoding="utf-8")

LOG_PURGE_DAYS = config["Settings"].getint("LogPurge", fallback=30)

server_name = config["Settings"]["ServerName"]
sql_driver = config["Settings"].get("SQLDriver", "SQL Server")
database = config["Settings"].get("DatabaseName", "STORESQL")
server_ip = config["Settings"].get("FTPServerIP", "").strip()

# Lipari
LIPARI_LOCATION_ID = config.get("Lipari", "LipariLocationID", fallback="3").strip() or "3"
LIPARI_STORE_NUMBER_CONST = config.get("Lipari", "StoreNumberConst", fallback="12557000").strip() or "12557000"
LIPARI_PO_SUFFIX = config.get("Lipari", "PoSuffixFilter", fallback="10950").strip() or "10950"

# Destination share (Windows share)
DEFAULT_SHARE = fr"\\{server_ip}\c$\Vendor Files\Lipari\PO" if server_ip else ""
DEST_SHARE = config.get("Lipari", "DestShare", fallback=DEFAULT_SHARE).strip()
if not DEST_SHARE:
    raise RuntimeError("Lipari destination share is empty. Set [Settings] FTPServerIP or [Lipari] DestShare.")

# Always-backup folder for produced CSV
CSV_BACKUP_SUBDIR = config.get("Lipari", "CsvBackupSubdir", fallback="CSV").strip() or "CSV"
CSV_BACKUP_DIR = os.path.join(BASE_DIR, CSV_BACKUP_SUBDIR)
os.makedirs(CSV_BACKUP_DIR, exist_ok=True)

# Dept map from config.ini
LIPARI_DEPT_MAP: Dict[str, str] = {}
if "Lipari_Departments" in config:
    for k, v in config["Lipari_Departments"].items():
        LIPARI_DEPT_MAP[str(k).strip()] = str(v).strip()

connection_string = f"DRIVER={{{sql_driver}}};SERVER={server_name};DATABASE={database};Trusted_Connection=yes"

# =============================================================================
# Lipari marker in REC_HDR.F1254
# =============================================================================
LIPARI_SENT_MARKER = "SENT_LIPARI"


# =============================================================================
# Helpers 
# =============================================================================
def _safe_str(x: Any) -> str:
    return ("" if x is None else str(x)).strip()


def log_bad_item(po_number: str, row: Any, reason: str) -> str:
    """
    Logs a skipped item into the MAIN log only,
    and returns a short string usable for RAVYX_PO_STATUS.F1081.
    """
    upc = _safe_str(getattr(row, "PlumItemCode", ""))
    item = _safe_str(getattr(row, "LipariItemCode", ""))
    qty = _safe_str(getattr(row, "Quantity", ""))
    price = _safe_str(getattr(row, "Price", ""))

    logging.warning(
        f"PO {po_number}: Skipped line | Reason={reason} | UPC={upc or '?'} | "
        f"LipariItemCode={item or '?'} | QTY={qty or '?'} | Price={price or '?'}"
    )

    return f"UPC={upc or '?'} | LipariItemCode={item or '?'} | Reason={reason}"


# =============================================================================
# RAVYX_PO_STATUS helpers
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


def insert_ravyx_po_status(
    conn: pyodbc.Connection,
    *,
    po_number: str,
    vendor: int,
    vendor_name: Optional[str],
    status: str,  # SUCCESS / WARN / FAILED
    f1081_text: str,
    dept: Optional[int],
    rec_hdr_f91: Optional[str],
    process: str = "Order Export",
):
    runid = datetime.now().strftime("%y%m%d%H%M%S")
    now = datetime.now()

    f1081_val = _truncate_5000(f1081_text or "") or None
    vname = (vendor_name or "").strip() or None

    # Keep real REC_HDR.F91 only on SUCCESS/WARN; otherwise runid.
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


# =============================================================================
# NEW: REC_HDR.F1254 marker helpers
# =============================================================================
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


def set_f1254_marker(conn: pyodbc.Connection, po_number: str, marker: str) -> None:
    marker = (marker or "").strip()
    if not marker:
        raise ValueError("Marker is empty; cannot write F1254.")

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE [dbo].[REC_HDR]
        SET F1254 = ?
        WHERE F1032 = ?
        """,
        (marker, po_number),
    )
    conn.commit()
    logging.info(f"PO {po_number}: REC_HDR.F1254 set to marker '{marker}'.")


# =============================================================================
# SQL helpers
# =============================================================================
def get_po_numbers_today_legacy(conn: pyodbc.Connection) -> List[str]:
    q = """
        SELECT F1032
        FROM [dbo].[REC_HDR]
        WHERE CAST(F253 AS DATE) = CAST(GETDATE() AS DATE)
          AND F1067 = 'OPEN'
          AND F1068 = 'ORDER'
    """
    cur = conn.cursor()
    cur.execute(q)
    all_pos = [str(r[0]) for r in cur.fetchall() if r and r[0] is not None]
    filtered = [p for p in all_pos if p.endswith(str(LIPARI_PO_SUFFIX))]
    logging.info(f"Legacy scan: found {len(filtered)} PO(s) ending with {LIPARI_PO_SUFFIX} for today.")
    return filtered


def get_po_rows(conn: pyodbc.Connection, po_number: str):
    q = """
        SELECT
            REG.F1032 AS PurchaseOrder,
            HDR.F254  AS ShipDate,
            ?         AS StoreNumberConst,
            REG.F75   AS Quantity,
            'CS'      AS UoM,
            REG.F38   AS Price,
            REG.F26   AS LipariItemCode,
            REG.F01   AS PlumItemCode
        FROM [dbo].[REC_REG] REG
        JOIN [dbo].[REC_HDR] HDR ON REG.F1032 = HDR.F1032
        WHERE REG.F1032 = ?
        ORDER BY REG.F01
    """
    cur = conn.cursor()
    cur.execute(q, (LIPARI_STORE_NUMBER_CONST, po_number))
    cols = [c[0] for c in cur.description]
    Row = namedtuple("Row", cols)
    rows = [Row(*r) for r in cur.fetchall()]
    logging.info(f"PO {po_number}: fetched {len(rows)} line(s).")
    return rows


def set_po_closed(conn: pyodbc.Connection, po_number: str) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE [dbo].[REC_HDR] SET F1067 = 'CLOSE' WHERE F1032 = ?", (po_number,))
    conn.commit()
    logging.info(f"PO {po_number}: REC_HDR.F1067 set to 'CLOSE'.")


def force_keep_po_open(conn: pyodbc.Connection, po_number: str) -> None:
    cur = conn.cursor()
    cur.execute("UPDATE [dbo].[REC_HDR] SET F1067 = 'OPEN' WHERE F1032 = ?", (po_number,))
    conn.commit()
    logging.warning(f"PO {po_number}: forced OPEN (failure path).")


def get_dept_for_upc(conn: pyodbc.Connection, upc: str) -> Optional[str]:
    q = """
        SELECT TOP 1
            CASE
                WHEN POS.F04 = 1200 THEN '11'
                WHEN POS.F04 = 1300 THEN '12'
                ELSE CAST(DPT.F03 AS varchar(30))
            END AS DeptAdjusted
        FROM dbo.POS_TAB POS
        JOIN dbo.SDP_TAB DPT ON POS.F04 = DPT.F04
        WHERE POS.F01 = ?
    """
    cur = conn.cursor()
    cur.execute(q, (upc,))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    dept = str(row[0]).strip()
    return dept or None


def get_first_dept_from_order(conn: pyodbc.Connection, po_rows) -> Optional[int]:
    try:
        if not po_rows:
            return None
        upc = str(getattr(po_rows[0], "PlumItemCode", "") or "").strip()
        if not upc:
            return None

        d = get_dept_for_upc(conn, upc)
        if not d:
            return None

        d = d.strip()
        if not d.isdigit():
            return None
        return int(d)
    except Exception:
        return None


def map_store_number_from_upc(conn: pyodbc.Connection, upc: str) -> str:
    dept = get_dept_for_upc(conn, upc)
    if not dept:
        logging.info(f"Dept lookup: UPC={upc} — department not found.")
        return "UNKNOWN"

    mapped = LIPARI_DEPT_MAP.get(str(dept).strip())
    if mapped:
        logging.info(f"Dept mapping: UPC={upc} Dept={dept} → Store={mapped}")
        return mapped

    logging.info(f"Dept mapping: UPC={upc} Dept={dept} — no mapping in config; using UNKNOWN.")
    return "UNKNOWN"


# =============================================================================
# Payload builder
# =============================================================================
def build_csv_payload(
    conn: pyodbc.Connection,
    po_rows,
    po_number: str
) -> Tuple[Optional[List[List[Any]]], int, int, List[str]]:
    """
    Returns:
      payload (or None),
      valid_lines,
      skipped_lines,
      skipped_details[]  (strings to include in RAVYX_PO_STATUS.F1081)
    """
    logging.info(f"PO {po_number}: CSV build start ...")

    if not po_rows:
        logging.info(f"PO {po_number}: CSV build end — no rows.")
        return None, 0, 0, []

    payload: List[List[Any]] = []
    valid = 0
    skipped = 0
    skipped_details: List[str] = []

    for row in po_rows:
        if row.LipariItemCode is None or str(row.LipariItemCode).strip() == "":
            skipped += 1
            skipped_details.append(log_bad_item(po_number, row, "Missing LipariItemCode"))
            continue

        try:
            qty_int = int(row.Quantity)
        except Exception:
            skipped += 1
            skipped_details.append(log_bad_item(po_number, row, "Invalid Quantity (non-integer)"))
            continue

        if qty_int <= 0:
            skipped += 1
            skipped_details.append(log_bad_item(po_number, row, "Invalid Quantity (<=0)"))
            continue

        store_num_mapped = map_store_number_from_upc(conn, str(row.PlumItemCode))

        payload.append(
            [
                row.PurchaseOrder,
                row.ShipDate,
                store_num_mapped,
                qty_int,
                row.UoM,
                row.Price,
                row.LipariItemCode,
                row.PlumItemCode,
            ]
        )
        valid += 1

    logging.info(f"PO {po_number}: Payload summary — ValidLines={valid} SkippedLines={skipped}")
    logging.info(f"PO {po_number}: CSV build end.")
    return (payload if payload else None), valid, skipped, skipped_details


def _format_skipped_for_f1081(skipped_details: List[str], max_chars: int = 2000) -> str:
    """
    Create a compact 'SkippedItems:' section for F1081.
    """
    if not skipped_details:
        return ""

    parts = []
    total = 0
    for s in skipped_details:
        s = (s or "").strip()
        if not s:
            continue
        add = ("; " if parts else "") + s
        if total + len(add) > max_chars:
            parts.append("... (more skipped items)")
            break
        parts.append(s)
        total += len(add)

    return " | SkippedItems: " + "; ".join(parts)


# =============================================================================
# File helpers 
# =============================================================================
def _safe_makedirs(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def backup_csv(payload: List[List[Any]], po_number: str) -> str:
    _safe_makedirs(CSV_BACKUP_DIR)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{po_number}_{ts}.csv"
    path = os.path.join(CSV_BACKUP_DIR, filename)

    with open(path, mode="w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(payload)

    logging.info(f"PO {po_number}: CSV backup written. BackupPath='{path}'")
    return path


def test_share_connection(share_path: str) -> Tuple[bool, str]:
    try:
        if not share_path:
            return False, "Share path is empty."
        if not os.path.isdir(share_path):
            return False, f"Share not accessible (not a directory): {share_path}"
        _ = os.listdir(share_path)[:3]
        return True, "Share accessible"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def copy_backup_to_share(backup_path: str, po_number: str) -> str:
    filename = os.path.basename(backup_path)
    dest_path = os.path.join(DEST_SHARE, filename)

    ok, msg = test_share_connection(DEST_SHARE)
    if not ok:
        raise RuntimeError(msg)

    shutil.copy2(backup_path, dest_path)
    logging.info(f"PO {po_number}: CSV copied to share successfully. DestPath='{dest_path}'")
    return dest_path


def _is_share_unreachable_error(ex: Exception) -> bool:
    msg = str(ex) or ""
    needles = [
        "Share not accessible",
        "Share path is empty",
        "not a directory",
        "The network path was not found",
        "The specified network name is no longer available",
        "Access is denied",
        "Permission denied",
        "WinError 53",
        "WinError 67",
        "WinError 5",
        "WinError 64",
    ]
    return any(n.lower() in msg.lower() for n in needles)


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

    conn: Optional[pyodbc.Connection] = None
    po_arg = ""
    vendor_arg = 0

    totals = {
        "processed_pos": 0,
        "sent_pos": 0,
        "skipped_pos": 0,
        "valid_lines": 0,
        "skipped_lines": 0,
        "errors": 0,
    }

    try:
        logging.info("=== Start run (LipariPush) ===")
        logging.info(f"AppVersion={APP_VERSION} | Frozen={_is_frozen()} | BaseDir={BASE_DIR}")
        logging.info(f"ConfigPath={CONFIG_PATH}")
        logging.info(f"DestShare={DEST_SHARE}")
        logging.info(f"CsvBackupDir={CSV_BACKUP_DIR}")

        try:
            deleted = purge_logs(LOG_DIR, LOG_PURGE_DAYS)
            logging.info(f"Log purge: deleted {deleted} file(s) older than {LOG_PURGE_DAYS} day(s).")
        except Exception as e:
            logging.warning(f"Log purge failed: {e}")

        ui_summary("INFO", "Starting Lipari export…")
        ui_counters(sent=0, warns=0, errs=0)

        po_arg, vendor_arg = get_sqi_args_optional()
        conn = pyodbc.connect(connection_string)

        if po_arg:
            po_list = [str(po_arg)]
            logging.info(f"SQI mode: exporting only PO {po_arg} (Vendor={vendor_arg or 'N/A'})")
        else:
            po_list = get_po_numbers_today_legacy(conn)

        if not po_list:
            logging.info("No PO numbers found for Lipari.")
            ui_summary("INFO", "No PO to send.")
            ui_autoclose(10)
            logging.info("=== End run (LipariPush) ===")
            return 0

        for po in po_list:
            totals["processed_pos"] += 1
            logging.info(f"Processing PO {po} ...")
            ui_summary("INFO", f"Preparing PO {po}…")

            vendor_name = get_vendor_name(conn, vendor_arg) if vendor_arg else None
            rec_hdr_f91 = get_rec_hdr_f91(conn, str(po))

            # NEW: skip if already marked as sent
            existing_f1254 = get_rec_hdr_f1254(conn, str(po))
            if existing_f1254 and existing_f1254.strip().upper() == LIPARI_SENT_MARKER:
                msg = f"PO {po}: already sent (F1254={LIPARI_SENT_MARKER}). Skipping."
                logging.info(msg)
                ui_summary("WARN", msg)

                try:
                    insert_ravyx_po_status(
                        conn,
                        po_number=str(po),
                        vendor=vendor_arg or 0,
                        vendor_name=vendor_name,
                        status="WARN",
                        f1081_text=msg,
                        dept=None,
                        rec_hdr_f91=rec_hdr_f91,
                        process="Order Export",
                    )
                except Exception as e2:
                    logging.warning(f"PO {po}: unable to insert RAVYX_PO_STATUS (WARN skip): {e2}")

                totals["skipped_pos"] += 1
                continue

            po_rows = get_po_rows(conn, po)

            dept = get_first_dept_from_order(conn, po_rows)
            logging.info(f"PO {po}: Dept(F03) resolved from first REC_REG line: {dept!r}")

            payload, v_count, s_count, skipped_details = build_csv_payload(conn, po_rows, po)

            totals["valid_lines"] += v_count
            totals["skipped_lines"] += s_count
            ui_counters(sent=totals["sent_pos"], warns=totals["skipped_lines"], errs=totals["errors"])

            if not payload:
                totals["skipped_pos"] += 1
                msg = f"PO {po}: skipped — no valid lines."
                logging.warning(msg)
                ui_summary("WARN", msg)

                f1081 = msg + _format_skipped_for_f1081(skipped_details)

                try:
                    insert_ravyx_po_status(
                        conn,
                        po_number=str(po),
                        vendor=vendor_arg or 0,
                        vendor_name=vendor_name,
                        status="FAILED",
                        f1081_text=f1081,
                        dept=dept,
                        rec_hdr_f91=rec_hdr_f91,
                        process="Order Export",
                    )
                except Exception as e2:
                    logging.warning(f"PO {po}: unable to insert RAVYX_PO_STATUS (FAILED): {e2}")

                continue

            backup_path = ""
            try:
                ui_summary("INFO", f"Writing backup CSV for PO {po}…")
                backup_path = backup_csv(payload, po)

                ui_summary("INFO", f"Sending PO {po} to share…")
                dest_path = copy_backup_to_share(backup_path, po)

                # SUCCESS path:
                # - close PO
                # - mark as SENT in F1254
                set_po_closed(conn, po)
                set_f1254_marker(conn, po, LIPARI_SENT_MARKER)

                totals["sent_pos"] += 1
                ui_counters(sent=totals["sent_pos"], warns=totals["skipped_lines"], errs=totals["errors"])

                final_status = "WARN" if s_count > 0 else "SUCCESS"

                final_msg = f"Sent OK. Dest={dest_path} | Backup={backup_path}"
                final_msg += _format_skipped_for_f1081(skipped_details)

                try:
                    insert_ravyx_po_status(
                        conn,
                        po_number=str(po),
                        vendor=vendor_arg or 0,
                        vendor_name=vendor_name,
                        status=final_status,
                        f1081_text=final_msg,
                        dept=dept,
                        rec_hdr_f91=rec_hdr_f91,
                        process="Order Export",
                    )
                except Exception as e2:
                    logging.warning(f"PO {po}: unable to insert RAVYX_PO_STATUS ({final_status}): {e2}")

                dur = round(time.perf_counter() - start, 2)
                ui_summary("SUCCESS", f"PO {po} sent ({v_count} lines, {s_count} skipped). ({dur}s)")
                logging.info(f"PO {po}: success (valid={v_count}, skipped={s_count}).")

            except Exception as e:
                totals["skipped_pos"] += 1
                totals["errors"] += 1
                force_keep_po_open(conn, po)

                logging.exception(
                    f"PO {po}: send failed. {type(e).__name__}: {e} | Backup={backup_path or 'N/A'}"
                )

                if _is_share_unreachable_error(e):
                    f1081_short = "Shared Folder Unreachable"
                    ui_msg = "Shared Folder Unreachable"
                else:
                    f1081_short = _truncate_5000(f"{type(e).__name__}: {e}")
                    ui_msg = f"{type(e).__name__}: {e}"

                f1081_final = f1081_short + _format_skipped_for_f1081(skipped_details)

                try:
                    insert_ravyx_po_status(
                        conn,
                        po_number=str(po),
                        vendor=vendor_arg or 0,
                        vendor_name=vendor_name,
                        status="FAILED",
                        f1081_text=f1081_final,
                        dept=dept,
                        rec_hdr_f91=rec_hdr_f91,
                        process="Order Export",
                    )
                except Exception as e2:
                    logging.warning(f"PO {po}: unable to insert RAVYX_PO_STATUS (FAILED): {e2}")

                ui_summary("ERROR", f"Share error: {ui_msg}")
                ui_counters(sent=totals["sent_pos"], warns=totals["skipped_lines"], errs=totals["errors"])

        dur = round(time.perf_counter() - start, 2)
        logging.info(
            "Summary | "
            f"POsProcessed={totals['processed_pos']} | POsSent={totals['sent_pos']} | POsSkipped={totals['skipped_pos']} | "
            f"ValidLines={totals['valid_lines']} | SkippedLines={totals['skipped_lines']} | Errors={totals['errors']} | Duration={dur}s"
        )
        logging.info("=== End run (LipariPush) ===")

        ui_autoclose(15)
        return 0 if totals["sent_pos"] > 0 else 1

    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        if ui_q is not None:
            ui_q.put(("SUMMARY", "ERROR", f"Fatal error: {type(e).__name__}: {e}"))
            ui_q.put(("COUNTERS", "ERRORS", "1"))
            ui_q.put(("AUTOCLOSE", "20", ""))
        return 2

    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


# =============================================================================
# UI wrapper (PyInstaller-friendly)
# =============================================================================
def _import_vendor_ui():
    try:
        from ui_send_PO_Lipari import VendorSendUI  # type: ignore
        return VendorSendUI, None
    except Exception as e1:
        try:
            from drivers.push_sms_po_to_lipari.ui_send_PO_Lipari import VendorSendUI  # type: ignore
            return VendorSendUI, None
        except Exception as e2:
            return None, f"{type(e1).__name__}: {e1} | {type(e2).__name__}: {e2}"


def main_with_ui() -> int:
    ui_q: "queue.Queue" = queue.Queue()
    setup_logging(ui_q=ui_q)

    ui_q.put(("SUMMARY", "INFO", "Launching UI…"))

    VendorSendUI, err = _import_vendor_ui()
    if VendorSendUI is None:
        logging.warning(f"UI not available (import failed): {err}")
        return run_job(ui_q=None)

    ui = VendorSendUI(
        title="Send PO to Lipari",
        queue=ui_q,
        auto_close_seconds=0,
    )

    def worker():
        try:
            ui_q.put(("SUMMARY", "INFO", "UI started. Processing…"))
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


def wants_ui() -> bool:
    args = [a.lower() for a in sys.argv[1:]]
    if "--noui" in args:
        return False
    if "--ui" in args:
        return True
    return _is_frozen()


if __name__ == "__main__":
    if wants_ui():
        raise SystemExit(main_with_ui())

    setup_logging(ui_q=None)
    raise SystemExit(run_job(ui_q=None))
