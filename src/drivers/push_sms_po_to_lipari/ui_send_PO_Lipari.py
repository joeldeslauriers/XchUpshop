# ui_send_PO_lipari.py
import tkinter as tk
from tkinter import ttk
from queue import Empty
from typing import Optional


class VendorSendUI:
    """
    Status window for "Send PO to Lipari".

    Queue payloads supported:
      - (msg, detail)                         # legacy
      - (level, msg, detail)                  # fallback level format
      - ("SUMMARY", level, msg)               # summary line (recommended)
      - ("DONE", "SUCCESS", msg)              # done success
      - ("ERROR", "FAILED", msg)              # done error
      - ("AUTOCLOSE", "20", "")               # start countdown N seconds
      - ("COUNTERS", key, value)              # set counters explicitly (ORDERS_SENT/WARNINGS/ERRORS)
    """

    def __init__(self, title: str = "Send PO to Lipari", queue=None, auto_close_seconds: int = 0):
        self.queue = queue
        self.auto_close_seconds = int(auto_close_seconds or 0)

        self.root = tk.Tk()
        self.root.title(title)

        self.root.geometry("760x440")
        self.root.minsize(760, 440)
        self.root.resizable(True, True)
        self.root.attributes("-topmost", True)

        self.msg_var = tk.StringVar(value="Starting...")
        self.detail_var = tk.StringVar(value="")

        # counters (business counters)
        self.orders_sent = 0
        self.errors_count = 0
        self.warn_count = 0
        self.count_var = tk.StringVar(value="Orders sent: 0 | Errors: 0 | Warnings: 0")

        # countdown state
        self._countdown_remaining = 0
        self._countdown_job = None

        # ===== Root grid config
        self.root.grid_rowconfigure(0, weight=0)
        self.root.grid_rowconfigure(1, weight=0)
        self.root.grid_rowconfigure(2, weight=0)
        self.root.grid_rowconfigure(3, weight=0)
        self.root.grid_rowconfigure(4, weight=1)
        self.root.grid_rowconfigure(5, weight=0)
        self.root.grid_columnconfigure(0, weight=1)

        # ===== Header
        ttk.Label(self.root, text=title, font=("Segoe UI", 12, "bold")).grid(
            row=0, column=0, pady=(10, 4), sticky="n"
        )

        ttk.Label(self.root, textvariable=self.msg_var, font=("Segoe UI", 10)).grid(
            row=1, column=0, sticky="ew", padx=16
        )

        self.detail_msg = tk.Message(
            self.root,
            textvariable=self.detail_var,
            font=("Segoe UI", 9),
            width=720,
            justify="center",
        )
        self.detail_msg.grid(row=2, column=0, sticky="ew", padx=16)

        # ===== Progress bar
        self.pb = ttk.Progressbar(self.root, mode="indeterminate")
        self.pb.grid(row=3, column=0, sticky="ew", padx=16, pady=(12, 8))
        self.pb.start(10)

        # ===== Messages area
        frame = ttk.Frame(self.root)
        frame.grid(row=4, column=0, sticky="nsew", padx=16, pady=(0, 8))
        frame.grid_rowconfigure(1, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        header = ttk.Frame(frame)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        header.grid_columnconfigure(0, weight=1)
        header.grid_columnconfigure(1, weight=1)

        ttk.Label(header, text="Messages:", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(header, textvariable=self.count_var, font=("Segoe UI", 9)).grid(
            row=0, column=1, sticky="e"
        )

        list_row = ttk.Frame(frame)
        list_row.grid(row=1, column=0, sticky="nsew")
        list_row.grid_rowconfigure(0, weight=1)
        list_row.grid_columnconfigure(0, weight=1)

        self.listbox = tk.Listbox(list_row, height=10)
        self.listbox.grid(row=0, column=0, sticky="nsew")

        sb = ttk.Scrollbar(list_row, orient="vertical", command=self.listbox.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self.listbox.configure(yscrollcommand=sb.set)

        # ===== Buttons
        btn_row = ttk.Frame(self.root)
        btn_row.grid(row=5, column=0, sticky="ew", padx=16, pady=(0, 12))
        btn_row.grid_columnconfigure(0, weight=1)
        btn_row.grid_columnconfigure(1, weight=1)

        self.clear_btn = ttk.Button(btn_row, text="Clear", command=self._clear_messages)
        self.clear_btn.grid(row=0, column=0, sticky="w")

        self.close_btn = ttk.Button(btn_row, text="Close", command=self.root.destroy, state="disabled")
        self.close_btn.grid(row=0, column=1, sticky="e")

        self.root.protocol("WM_DELETE_WINDOW", self._on_close_attempt)
        self.root.bind("<Configure>", self._on_resize)

    # -------------------------
    # Small helpers
    # -------------------------
    def _normalize_level(self, level: str) -> str:
        level = (level or "INFO").upper().strip()
        if level == "WARNING":
            return "WARN"
        if level in ("CRITICAL",):
            return "ERROR"
        return level

    def _on_resize(self, _evt=None):
        try:
            w = max(520, self.root.winfo_width() - 40)
            self.detail_msg.configure(width=w)
        except Exception:
            pass

    def _on_close_attempt(self):
        if str(self.close_btn["state"]) == "disabled":
            return
        self.root.destroy()

    def _clear_messages(self):
        self.listbox.delete(0, "end")
        self.orders_sent = 0
        self.errors_count = 0
        self.warn_count = 0
        self._countdown_remaining = 0
        if self._countdown_job is not None:
            try:
                self.root.after_cancel(self._countdown_job)
            except Exception:
                pass
            self._countdown_job = None
        self._refresh_counts()

    def _refresh_counts(self):
        base = f"Orders sent: {self.orders_sent} | Errors: {self.errors_count} | Warnings: {self.warn_count}"
        if self._countdown_remaining > 0:
            base += f" | Auto-close in {self._countdown_remaining}s"
        self.count_var.set(base)

    def _append_message(self, level: str, msg: str, detail: str = ""):
        level = self._normalize_level(level)
        line = f"[{level}] {msg}"
        if detail:
            line += f" | {detail}"
        self.listbox.insert("end", line)
        self.listbox.yview_moveto(1)

    def set(self, msg, detail=""):
        self.msg_var.set(msg or "")
        self.detail_var.set(detail or "")

    def _start_countdown(self, seconds: Optional[int] = None):
        if seconds is None:
            seconds = self.auto_close_seconds

        try:
            seconds = int(seconds or 0)
        except Exception:
            seconds = 0

        if seconds <= 0:
            return

        # reset countdown if already running
        if self._countdown_job is not None:
            try:
                self.root.after_cancel(self._countdown_job)
            except Exception:
                pass
            self._countdown_job = None

        self._countdown_remaining = seconds
        self._refresh_counts()

        def tick():
            if not self.root.winfo_exists():
                return

            self._countdown_remaining -= 1
            self._refresh_counts()

            if self._countdown_remaining <= 0:
                try:
                    self.root.destroy()
                except Exception:
                    pass
                return

            self._countdown_job = self.root.after(1000, tick)

        self._countdown_job = self.root.after(1000, tick)

    def done(self, msg="Done", detail=""):
        try:
            self.pb.stop()
        except Exception:
            pass
        try:
            self.pb.configure(mode="determinate", value=100)
        except Exception:
            pass
        self.set(msg, detail)
        self.close_btn.configure(state="normal")
        self._start_countdown()

    def error(self, msg="Error", detail=""):
        try:
            self.pb.stop()
        except Exception:
            pass
        self.set(msg, detail)
        self.close_btn.configure(state="normal")
        self._start_countdown()

    def _handle_counters(self, key: str, value: str):
        k = (key or "").strip().upper()
        v = 0
        try:
            v = int(str(value).strip())
        except Exception:
            v = 0

        if k in ("ORDERS_SENT", "SENT", "SUCCESS"):
            self.orders_sent = v
        elif k in ("WARNINGS", "WARNS", "WARN"):
            self.warn_count = v
        elif k in ("ERRORS", "ERRS", "ERROR"):
            self.errors_count = v

        self._refresh_counts()

    def _bump_from_level(self, level: str):
        level = self._normalize_level(level)
        if level in ("WARN",):
            self.warn_count += 1
        elif level in ("ERROR", "FAILED"):
            self.errors_count += 1
        self._refresh_counts()

    # -------------------------
    # Queue pump
    # -------------------------
    def pump_queue(self):
        if self.queue is not None:
            try:
                while True:
                    item = self.queue.get_nowait()

                    # legacy (msg, detail)
                    if isinstance(item, tuple) and len(item) == 2:
                        msg, detail = item
                        self.set(msg, detail)
                        self._append_message("INFO", msg, detail)
                        continue

                    # modern events (kind, a, b)
                    if isinstance(item, tuple) and len(item) == 3:
                        kind, a, b = item
                        kind = (kind or "INFO").upper().strip()

                        # SUMMARY (recommended)
                        if kind == "SUMMARY":
                            level = self._normalize_level(a or "INFO")
                            msg = b or ""
                            self.set(msg, "")
                            self._append_message(level, msg, "")
                            self._bump_from_level(level)
                            continue

                        # COUNTERS
                        if kind == "COUNTERS":
                            self._handle_counters(a, b)
                            continue

                        # AUTOCLOSE
                        if kind == "AUTOCLOSE":
                            self._start_countdown(seconds=a)
                            continue

                        # DONE
                        if kind == "DONE":
                            status = self._normalize_level(a or "")
                            msg = b or "Done"
                            self._append_message("SUCCESS" if status == "SUCCESS" else "INFO", msg, "")
                            # Optionnel: si tu veux un fallback quand DONE SUCCESS arrive sans COUNTERS
                            # if status == "SUCCESS" and self.orders_sent <= 0:
                            #     self.orders_sent = 1
                            self._refresh_counts()
                            self.done(msg, "")
                            continue

                        # ERROR
                        if kind == "ERROR":
                            status = self._normalize_level(a or "FAILED")
                            msg = b or "Error"
                            # bump + message
                            self.errors_count = max(self.errors_count, 0) + 1
                            self._refresh_counts()
                            self._append_message("ERROR", f"{status}", msg)
                            self.error(f"{status}", msg)
                            continue

                        # fallback: treat as (level, msg, detail)
                        level = self._normalize_level(kind)
                        msg = a or ""
                        detail = b or ""
                        self.set(msg, detail)
                        self._append_message(level, msg, detail)
                        self._bump_from_level(level)
                        continue

                    # fallback
                    self.set(str(item), "")
                    self._append_message("INFO", str(item), "")

            except Empty:
                pass

        self.root.after(100, self.pump_queue)

    def run(self):
        self.pump_queue()
        self.root.mainloop()
