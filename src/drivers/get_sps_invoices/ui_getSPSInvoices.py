import tkinter as tk
from tkinter import ttk
from queue import Empty


class GetSpsInvoicesUI:

    def __init__(self, title="Get SPS Invoices", queue=None, auto_close_seconds: int = 0):
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
        self.count_var = tk.StringVar(value="Success: 0 | Errors: 0 | Warnings: 0")

        self.success_count = 0
        self.errors_count = 0
        self.warn_count = 0

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
            self.root, textvariable=self.detail_var, font=("Segoe UI", 9),
            width=720, justify="center"
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
        self.success_count = 0
        self.errors_count = 0
        self.warn_count = 0
        self._refresh_counts()

    def _refresh_counts(self):
        base = f"Success: {self.success_count} | Errors: {self.errors_count} | Warnings: {self.warn_count}"
        if self._countdown_remaining > 0:
            base += f" | Auto-close in {self._countdown_remaining}s"
        self.count_var.set(base)

    def _append_message(self, level: str, msg: str, detail: str):
        level = (level or "INFO").upper()
        line = f"[{level}] {msg}"
        if detail:
            line += f" | {detail}"
        self.listbox.insert("end", line)
        self.listbox.yview_moveto(1)

        if level == "ERROR":
            self.errors_count += 1
        elif level == "WARN":
            self.warn_count += 1
        elif level in ("SUCCESS",):
            self.success_count += 1
        elif level == "INFO":
            if (msg or "").upper().startswith("SUCCESS"):
                self.success_count += 1

        self._refresh_counts()

    def set(self, msg, detail=""):
        self.msg_var.set(msg or "")
        self.detail_var.set(detail or "")

    def _start_countdown(self):
        if self.auto_close_seconds <= 0:
            return
        if self._countdown_job is not None:
            return

        self._countdown_remaining = self.auto_close_seconds
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
        self.pb.configure(mode="determinate", value=100)
        self.set(msg, detail)
        self.close_btn.configure(state="normal")
        self._start_countdown()

    def error(self, msg="Error", detail=""):
        try:
            self.pb.stop()
        except Exception:
            pass
        self.set(msg, detail)
        self._append_message("ERROR", msg, detail)
        self.close_btn.configure(state="normal")
        self._start_countdown()

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

                    # new (level, msg, detail)
                    if isinstance(item, tuple) and len(item) == 3:
                        level, msg, detail = item
                        level = (level or "INFO").upper()

                        self.set(msg, detail)

                        if level == "DONE":
                            self._append_message("INFO", msg, detail)
                            self.done(msg, detail)
                        elif level == "ERROR":
                            self._append_message("ERROR", msg, detail)
                            self.error(msg, detail)
                        else:
                            self._append_message(level, msg, detail)

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
