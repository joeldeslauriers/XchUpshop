import csv
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set

import configparser
import pyodbc
import requests

# =============================================================================
# Version + Path helpers
# =============================================================================
def _is_frozen() -> bool:
    return getattr(sys, "frozen", False)


def _base_dir() -> str:
    return os.path.dirname(sys.executable) if _is_frozen() else os.path.dirname(os.path.abspath(__file__))


BASE_DIR = _base_dir()

# =============================================================================
# Config.ini lookup (shared first, then local)
# =============================================================================
def _find_config_path() -> str:
    base = BASE_DIR
    parent = os.path.dirname(base)
    candidates = [
        os.path.join(parent, "config.ini"),
        os.path.join(base, "config.ini"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return candidates[0]


CONFIG_PATH = _find_config_path()

# =============================================================================
# Logging (file + console)
# =============================================================================
LOG_DIR = os.path.join(BASE_DIR, "LOG")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"GetLipariInvoice_{datetime.now().strftime('%Y_%m_%d')}.log")


def setup_logging() -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%H:%M:%S")

    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    root.addHandler(fh)
    root.addHandler(ch)

    logging.info("=== Start run ===")
    logging.info(f"Frozen={_is_frozen()} | BASE_DIR={BASE_DIR} | CWD={os.getcwd()}")
    logging.info(f"CONFIG_PATH={CONFIG_PATH}")
    logging.info(f"LOG_FILE={LOG_FILE}")


def log_info(msg: str) -> None:
    logging.info(msg)


def log_warn(msg: str) -> None:
    logging.warning(msg)


def log_error(msg: str) -> None:
    logging.error(msg)


# =============================================================================
# Small utils
# =============================================================================
def _safe_int(v: Any, default: int = 0) -> int:
    """
    Safe int conversion that accepts:
      - int, float, Decimal
      - strings like "12", "12.0", "12.0000", "12,0000"
    """
    try:
        if v is None or v == "":
            return default
        s = str(v).strip().replace(",", ".")
        return int(round(float(s)))
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        s = str(v).strip().replace(",", ".")
        return float(s)
    except Exception:
        return default


def to_cents(amount: Any) -> int:
    s = ("" if amount is None else str(amount)).strip().replace(",", ".")
    try:
        return int(round(float(s) * 100))
    except Exception:
        return 0


def request_with_retry(fn, *, tries: int = 3, base_sleep: float = 1.0, what: str = "HTTP"):
    last = None
    for i in range(1, tries + 1):
        try:
            return fn()
        except Exception as e:
            last = e
            log_warn(f"{what} failed (try {i}/{tries}): {type(e).__name__}: {e}")
            if i < tries:
                time.sleep(base_sleep * i)
            else:
                raise last


# =============================================================================
# Config parsing
# =============================================================================
def load_config(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    ok = cfg.read(path, encoding="utf-8")
    if not ok:
        raise FileNotFoundError(f"config.ini not loaded. Path={path}")
    return cfg


def load_store_ranges(cfg: configparser.ConfigParser) -> List[Tuple[int, int, int]]:
    out: List[Tuple[int, int, int]] = []
    if "LipariStoreRanges" not in cfg:
        return out

    for k, v in cfg.items("LipariStoreRanges"):
        key = (k or "").strip()
        val = (v or "").strip()
        if not key or not val:
            continue

        store_number = _safe_int(val, 0)
        if store_number <= 0:
            continue

        if "-" not in key:
            continue

        a, b = key.split("-", 1)
        start = _safe_int(a, 0)
        end = _safe_int(b, 0)
        if start <= 0 or end <= 0 or end < start:
            continue

        out.append((start, end, store_number))

    return out


def map_location_to_store(location_id: int, ranges: List[Tuple[int, int, int]]) -> int:
    for start, end, store_num in ranges:
        if start <= location_id <= end:
            return store_num
    return 0


def pick_invafresh_section(cfg: configparser.ConfigParser) -> str:
    """
    Priority:
      1) [GetLipariInvoice]  (if you add it later)
      2) [GetSpsInvoice]
      3) [ImportOrders]
    """
    for section in ("GetLipariInvoice", "GetSpsInvoice", "ImportOrders"):
        if section in cfg:
            base = (cfg.get(section, "BaseUrl", fallback="") or "").strip()
            user = (cfg.get(section, "Username", fallback="") or "").strip()
            pwd = (cfg.get(section, "Password", fallback="") or "").strip()
            if base and user and pwd:
                return section
    raise RuntimeError("config.ini missing Invafresh login section: need [GetLipariInvoice] or [GetSpsInvoice] or [ImportOrders] with BaseUrl/Username/Password")


# =============================================================================
# SQL helpers
# =============================================================================
def sku_belongs_to_vendor(cur, sku: str, vendor_f27: str) -> bool:
    try:
        cur.execute("SELECT TOP 1 1 FROM dbo.COST_TAB WHERE F01 = ? AND F27 = ?", (sku, str(vendor_f27).strip()))
        return cur.fetchone() is not None
    except Exception as e:
        log_warn(f"Vendor validation query failed for SKU={sku} vendor={vendor_f27}: {type(e).__name__}: {e}")
        return False


def get_department_number(cur, sku: str) -> int:
    try:
        cur.execute(
            """
            SELECT TOP 1 DPT.F03
            FROM dbo.POS_TAB POS
            JOIN dbo.SDP_TAB DPT ON POS.F04 = DPT.F04
            WHERE POS.F01 = ?
            """,
            (sku,),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception as e:
        log_warn(f"Dept lookup failed for SKU={sku}: {type(e).__name__}: {e}")
        return 0


def get_rec_reg_before(cur, po_number: str, sku: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns (F19 pack, F75 cases, F64 units, F38 cost_per_case, F1140 cost_each) before update.
    """
    try:
        cur.execute(
            """
            SELECT TOP 1 F19, F75, F64, F38, F1140
            FROM dbo.REC_REG
            WHERE F1032 = ?
              AND F01 = ?
              AND F1067 = 'ITEM'
              AND ISNULL(F1069,0) = 0
            """,
            (po_number, sku),
        )
        row = cur.fetchone()
        if not row:
            return None, None, None, None, None
        f19 = None if row[0] is None else float(row[0])
        f75 = None if row[1] is None else float(row[1])
        f64 = None if row[2] is None else float(row[2])
        f38 = None if row[3] is None else float(row[3])
        f1140 = None if row[4] is None else float(row[4])
        return f19, f75, f64, f38, f1140
    except Exception:
        return None, None, None, None, None


def update_rec_reg_line_cs(cur, po_number: str, sku: str, qty_cases: int, case_cost: float) -> Tuple[bool, int, int, float, float]:
    """
    CSV is CS:
      - F19 = units per case (already in REC_REG)
      - F75 = cases
      - F64 = units = cases * F19
      - F38 = base cost per case
      - F1140 = unit cost = F38 / F19
      - F65 = total cost = F75 * F38
    Returns: (updated, pack, units, unit_cost, total_cost)
    """
    cur.execute(
        """
        SELECT TOP 1 F19
        FROM dbo.REC_REG
        WHERE F1032 = ?
          AND F01 = ?
          AND F1067 = 'ITEM'
          AND ISNULL(F1069,0) = 0
        """,
        (po_number, sku),
    )
    row = cur.fetchone()
    pack_raw = row[0] if row and row[0] is not None else 0
    pack = _safe_int(pack_raw, 0)
    if pack <= 0:
        pack = 1

    cases = int(qty_cases)
    units = cases * pack

    case_cost_f = float(case_cost)
    unit_cost = case_cost_f / float(pack) if pack > 0 else case_cost_f
    total_cost = case_cost_f * float(cases)

    cur.execute(
        """
        UPDATE dbo.REC_REG
        SET
            F75 = ?,      -- cases
            F64 = ?,      -- units
            F38 = ?,      -- cost per case
            F1140 = ?,    -- unit cost
            F65 = ?       -- total cost
        WHERE F1032 = ?
          AND F01 = ?
          AND F1067 = 'ITEM'
          AND ISNULL(F1069,0) = 0
        """,
        (cases, units, case_cost_f, float(unit_cost), float(total_cost), po_number, sku),
    )

    return ((cur.rowcount or 0) > 0), pack, units, float(unit_cost), float(total_cost)


def set_rec_hdr_rtc(cur, po_number: str) -> bool:
    try:
        cur.execute(
            """
            UPDATE dbo.REC_HDR
            SET F2630 = 'RTC'
            WHERE F1032 = ?
              AND ISNULL(F2630,'') <> 'RTC'
            """,
            (po_number,),
        )
        return (cur.rowcount or 0) > 0
    except Exception as e:
        log_warn(f"REC_HDR RTC update failed for PO={po_number}: {type(e).__name__}: {e}")
        return False


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    setup_logging()
    cfg = load_config(CONFIG_PATH)

    if "Settings" not in cfg:
        raise RuntimeError("config.ini missing [Settings]")

    server = (cfg.get("Settings", "ServerName", fallback="") or "").strip()
    db = (cfg.get("Settings", "DatabaseName", fallback="STORESQL") or "STORESQL").strip()
    driver = (cfg.get("Settings", "SQLDriver", fallback="ODBC Driver 17 for SQL Server") or "").strip()

    if not server:
        raise RuntimeError("Missing [Settings] ServerName")

    inv_section = pick_invafresh_section(cfg)
    inv_base = (cfg.get(inv_section, "BaseUrl", fallback="") or "").strip().rstrip("/")
    inv_user = (cfg.get(inv_section, "Username", fallback="") or "").strip()
    inv_pass = (cfg.get(inv_section, "Password", fallback="") or "").strip()

    auth_url = f"{inv_base}/login"
    post_url = f"{inv_base}/backroom_products/receipt_transactions"

    if "Lipari" not in cfg:
        raise RuntimeError("config.ini missing [Lipari]")

    input_dir = (cfg.get("Lipari", "InputDir", fallback=cfg.get("Lipari", "INPUT_DIR", fallback="")) or "").strip()
    archive_dir = (cfg.get("Lipari", "ArchiveDir", fallback=cfg.get("Lipari", "OUTPUT_DIR", fallback="")) or "").strip()

    if not input_dir:
        raise RuntimeError("Missing [Lipari] InputDir (or INPUT_DIR)")
    if not archive_dir:
        raise RuntimeError("Missing [Lipari] ArchiveDir (or OUTPUT_DIR)")

    default_uom = (cfg.get("Lipari", "DefaultUOM", fallback="cs") or "cs").strip().lower()
    fail_unknown_store = _safe_int(cfg.get("Lipari", "FailFileIfUnknownStore", fallback="1"), 1) == 1
    fail_missing_sku = _safe_int(cfg.get("Lipari", "FailFileIfMissingSku", fallback="1"), 1) == 1
    vendor_f27 = (cfg.get("Lipari", "VendorF27", fallback="10950") or "10950").strip()

    os.makedirs(archive_dir, exist_ok=True)
    store_ranges = load_store_ranges(cfg)

    log_info(f"SQL: {server}/{db} Driver={driver}")
    log_info(f"Invafresh section used: [{inv_section}] base={inv_base} user={inv_user}")
    log_info(f"Lipari: InputDir={input_dir}")
    log_info(f"Lipari: ArchiveDir={archive_dir}")
    log_info(f"Lipari: VendorF27={vendor_f27} DefaultUOM={default_uom}")
    log_info(f"Lipari: StoreRanges={len(store_ranges)} FailUnknownStore={int(fail_unknown_store)} FailMissingSku={int(fail_missing_sku)}")

    sess = requests.Session()

    def do_login():
        return sess.post(auth_url, json={"username": inv_user, "password": inv_pass}, timeout=60)

    log_info("Invafresh: login...")
    resp = request_with_retry(do_login, tries=3, base_sleep=1, what="Invafresh Login")
    if resp.status_code != 200:
        raise RuntimeError(f"Invafresh login failed: {resp.status_code} {resp.text[:400]}")
    token = (resp.json() or {}).get("access_token") or ""
    if not token:
        raise RuntimeError("Invafresh login did not return access_token")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    log_info("Invafresh: login OK")

    conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={db};Trusted_Connection=yes"
    log_info("SQL: connecting...")
    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    log_info("SQL: connection OK")

    processed_files = 0
    posted_files = 0
    failed_files = 0
    skipped_files = 0

    try:
        files = os.listdir(input_dir)
        log_info(f"Scanning folder: {input_dir} | files={len(files)}")

        for filename in files:
            if not filename.lower().endswith(".csv"):
                skipped_files += 1
                continue

            processed_files += 1
            input_csv_path = os.path.join(input_dir, filename)
            json_path = os.path.join(archive_dir, os.path.splitext(filename)[0] + ".json")
            archived_csv_path = os.path.join(archive_dir, filename)

            log_info(f"Processing file={filename}")

            transactions: List[Dict[str, Any]] = []
            line_total = 0

            skipped_empty_item = 0
            skipped_sku_not_found = 0
            skipped_vendor_mismatch = 0
            dept0_count = 0
            unknown_store = 0

            rec_updated = 0
            rec_missing = 0
            rec_changed = 0

            hdr_rtc_updated = 0
            hdr_rtc_skipped = 0

            must_fail_file = False
            fail_reason = ""

            pos_to_set_rtc: Set[str] = set()

            try:
                with open(input_csv_path, newline="", encoding="utf-8-sig") as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        line_total += 1

                        lipari_item = (row.get("LipariItem") or "").strip()
                        if not lipari_item:
                            skipped_empty_item += 1
                            continue

                        cur.execute("SELECT TOP 1 F01 FROM dbo.COST_TAB WHERE F26 = ?", (lipari_item,))
                        r = cur.fetchone()
                        if not r or r[0] is None:
                            skipped_sku_not_found += 1
                            msg = f"SKU not found for LipariItem={lipari_item} | file={filename} | line={line_total}"
                            log_warn(msg)
                            if fail_missing_sku:
                                must_fail_file = True
                                fail_reason = msg
                                break
                            continue

                        sku = str(r[0]).strip()

                        if vendor_f27 and not sku_belongs_to_vendor(cur, sku, vendor_f27):
                            skipped_vendor_mismatch += 1
                            log_warn(f"Vendor mismatch: SKU={sku} not linked to vendor F27={vendor_f27} | LipariItem={lipari_item} | file={filename} | line={line_total}")
                            continue

                        dept = get_department_number(cur, sku)
                        if dept == 0:
                            dept0_count += 1

                        location_id = _safe_int(row.get("StoreNumber"), 0)
                        store_number = map_location_to_store(location_id, store_ranges) if location_id else 0
                        if store_number <= 0:
                            unknown_store += 1
                            msg = f"Unknown store mapping for StoreNumber={location_id} | file={filename} | line={line_total}"
                            log_warn(msg)
                            if fail_unknown_store:
                                must_fail_file = True
                                fail_reason = msg
                                break

                        po_num = (row.get("PONumber") or "").strip()
                        inv_date = (row.get("InvoiceDate") or "").strip()

                        qty_cases = _safe_int(row.get("Quantity"), 0)
                        case_cost = _safe_float(row.get("Price"), 0.0)

                        before_f19, before_f75, before_f64, before_f38, before_f1140 = get_rec_reg_before(cur, po_num, sku)

                        updated, pack, new_units, new_unit_cost, new_total_cost = update_rec_reg_line_cs(cur, po_num, sku, qty_cases, case_cost)

                        if updated:
                            rec_updated += 1

                            changed = False
                            if before_f75 is not None and int(before_f75) != int(qty_cases):
                                changed = True
                            if before_f64 is not None and int(before_f64) != int(new_units):
                                changed = True
                            if before_f38 is not None and round(float(before_f38), 6) != round(float(case_cost), 6):
                                changed = True
                            if before_f1140 is not None and round(float(before_f1140), 6) != round(float(new_unit_cost), 6):
                                changed = True
                            if changed:
                                rec_changed += 1

                            log_info(
                                f"REC_REG updated PO={po_num} SKU={sku} "
                                f"Pack(F19)={pack} | "
                                f"F75(cases) {before_f75}->{qty_cases} | "
                                f"F64(units) {before_f64}->{new_units} | "
                                f"F38(case) {before_f38}->{case_cost} | "
                                f"F1140(each) {before_f1140}->{new_unit_cost:.6f} | "
                                f"F65(total) ->{new_total_cost:.2f}"
                            )

                            if po_num:
                                pos_to_set_rtc.add(po_num)
                        else:
                            rec_missing += 1
                            log_warn(f"REC_REG missing PO={po_num} SKU={sku} (no row updated)")

                        amount_cents = to_cents(case_cost)

                        try:
                            order_number = int(po_num) // 100000
                        except Exception:
                            order_number = 0

                        transactions.append(
                            {
                                "invoice_number": po_num,
                                "order_number": order_number if order_number > 0 else None,
                                "external_order_number": po_num,
                                "delivery_or_invoice_date": inv_date,
                                "store_number": int(store_number) if store_number > 0 else 0,
                                "department_number": int(dept) if dept > 0 else 0,
                                "sku": sku,
                                "quantity": int(qty_cases),
                                "quantity_unit_of_measure": default_uom,
                                "amount": int(amount_cents),
                            }
                        )

            except Exception as e:
                failed_files += 1
                log_error(f"Failed reading/parsing file={filename}: {type(e).__name__}: {e}")
                continue

            if must_fail_file:
                failed_files += 1
                log_error(f"File failed by rule: {filename} | {fail_reason}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue

            for po in sorted(pos_to_set_rtc):
                if set_rec_hdr_rtc(cur, po):
                    hdr_rtc_updated += 1
                    log_info(f"REC_HDR set RTC PO={po} (F2630='RTC')")
                else:
                    hdr_rtc_skipped += 1
                    log_info(f"REC_HDR RTC unchanged PO={po} (already RTC or not found)")

            try:
                conn.commit()
            except Exception as e:
                failed_files += 1
                log_error(f"SQL commit failed for file={filename}: {type(e).__name__}: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue

            log_info(
                f"File summary file={filename} lines={line_total} tx={len(transactions)} "
                f"skipped_empty_item={skipped_empty_item} skipped_sku_not_found={skipped_sku_not_found} "
                f"skipped_vendor_mismatch={skipped_vendor_mismatch} dept0={dept0_count} unknown_store={unknown_store} "
                f"REC_REG updated={rec_updated} missing={rec_missing} changed={rec_changed} "
                f"REC_HDR RTC_updated={hdr_rtc_updated} RTC_skipped={hdr_rtc_skipped}"
            )

            if not transactions:
                log_warn(f"No transactions built for file={filename}. Skipping POST.")
                continue

            output_data = {"receipt_transactions": transactions}

            try:
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(output_data, f, indent=2)
                log_info(f"JSON saved: {json_path} | tx={len(transactions)}")
            except Exception as e:
                log_warn(f"Could not write JSON for file={filename}: {type(e).__name__}: {e}")

            try:
                log_info(f"POST Invafresh: file={filename} tx={len(transactions)}")

                def do_post():
                    return sess.post(post_url, headers=headers, data=json.dumps(output_data), timeout=90)

                rpost = request_with_retry(do_post, tries=3, base_sleep=1, what="Invafresh POST")

                if rpost.status_code == 401:
                    log_warn("Invafresh returned 401, re-login once then retry")
                    resp2 = request_with_retry(do_login, tries=3, base_sleep=1, what="Invafresh Login(retry)")
                    if resp2.status_code != 200:
                        raise RuntimeError(f"Invafresh re-login failed: {resp2.status_code} {resp2.text[:400]}")
                    token2 = (resp2.json() or {}).get("access_token") or ""
                    if not token2:
                        raise RuntimeError("Invafresh re-login did not return access_token")
                    headers = {"Authorization": f"Bearer {token2}", "Content-Type": "application/json"}
                    rpost = request_with_retry(do_post, tries=3, base_sleep=1, what="Invafresh POST(retry401)")

                if not (200 <= rpost.status_code < 300):
                    raise RuntimeError(f"POST failed ({rpost.status_code}): {rpost.text[:800]}")

                posted_files += 1
                log_info(f"POST successful file={filename} status={rpost.status_code} body={rpost.text[:300]}")

                try:
                    shutil.move(input_csv_path, archived_csv_path)
                    log_info(f"Moved file to archive: {archived_csv_path}")
                except Exception as e:
                    log_warn(f"Unable to move CSV to archive for file={filename}: {type(e).__name__}: {e}")

            except Exception as e:
                failed_files += 1
                log_error(f"POST failed file={filename}: {type(e).__name__}: {e}")

        log_info(f"Done. processed_files={processed_files} posted_files={posted_files} failed_files={failed_files} skipped_files={skipped_files}")
        log_info("=== End run ===")
        return 0 if failed_files == 0 else 2

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        try:
            sess.close()
        except Exception:
            pass


if __name__ == "__main__":
    setup_logging()
    try:
        sys.exit(main())
    except Exception as e:
        log_error(f"Fatal error: {type(e).__name__}: {e}")
        log_info("=== End run (fatal) ===")
        sys.exit(2)