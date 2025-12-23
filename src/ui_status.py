import tkinter as tk
from tkinter import ttk
from queue import Empty


class StatusUI:
    """
    Small status window (Tkinter) that stays responsive while work runs in a background thread.
    Uses a Queue (thread-safe) to receive updates.

    Supported queue payloads:
      - (msg, detail)                         # legacy
      - (level, msg, detail)                  # new, level in {"INFO","WARN","ERROR","DONE"}
    """

    def __init__(self, title="Upshop Import", queue=None):
        self.queue = queue
        self.root = tk.Tk()
        self.root.title(title)
        self.root.resizable(False, False)
        self.root.geometry("680x360")  # bigger to show errors
        self.root.attributes("-topmost", True)

        self.msg_var = tk.StringVar(value="Starting...")
        self.detail_var = tk.StringVar(value="")
        self.count_var = tk.StringVar(value="Errors: 0 | Warnings: 0")

        self.errors_count = 0
        self.warn_count = 0

        ttk.Label(
            self.root, text="Upshop Order Import", font=("Segoe UI", 12, "bold")
        ).pack(pady=(10, 4))

        ttk.Label(self.root, textvariable=self.msg_var, font=("Segoe UI", 10)).pack()
        ttk.Label(self.root, textvariable=self.detail_var, font=("Segoe UI", 9)).pack()

        self.pb = ttk.Progressbar(self.root, mode="indeterminate")
        self.pb.pack(fill="x", padx=16, pady=(12, 8))
        self.pb.start(10)

        # ---- Errors / warnings area
        frame = ttk.Frame(self.root)
        frame.pack(fill="both", expand=True, padx=16, pady=(0, 8))

        header = ttk.Frame(frame)
        header.pack(fill="x", pady=(0, 4))

        ttk.Label(header, text="Messages (errors/warnings):", font=("Segoe UI", 9, "bold")).pack(side="left")
        ttk.Label(header, textvariable=self.count_var, font=("Segoe UI", 9)).pack(side="right")

        self.listbox = tk.Listbox(frame, height=10)
        self.listbox.pack(fill="both", expand=True)

        btn_row = ttk.Frame(self.root)
        btn_row.pack(fill="x", padx=16, pady=(0, 10))

        self.clear_btn = ttk.Button(btn_row, text="Clear", command=self._clear_messages)
        self.clear_btn.pack(side="left")

        self.close_btn = ttk.Button(btn_row, text="Close", command=self.root.destroy, state="disabled")
        self.close_btn.pack(side="right")

        self.root.protocol("WM_DELETE_WINDOW", self._on_close_attempt)

    def _on_close_attempt(self):
        # block close while still working (optional)
        if str(self.close_btn["state"]) == "disabled":
            return
        self.root.destroy()

    def _clear_messages(self):
        self.listbox.delete(0, "end")
        self.errors_count = 0
        self.warn_count = 0
        self._refresh_counts()

    def _refresh_counts(self):
        self.count_var.set(f"Errors: {self.errors_count} | Warnings: {self.warn_count}")

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

        self._refresh_counts()

    def set(self, msg, detail=""):
        self.msg_var.set(msg)
        self.detail_var.set(detail)

    def done(self, msg="Done", detail=""):
        self.pb.stop()
        self.pb.configure(mode="determinate", value=100)
        self.set(msg, detail)
        self.close_btn.configure(state="normal")

    def error(self, msg="Error", detail=""):
        self.pb.stop()
        self.set(msg, detail)
        # also push into list so it's not lost
        self._append_message("ERROR", msg, detail)
        self.close_btn.configure(state="normal")

    def pump_queue(self):
        """
        Pull UI updates from queue every 100ms.
        Accepts both old format (msg, detail) and new (level, msg, detail).
        """
        if self.queue is not None:
            try:
                while True:
                    item = self.queue.get_nowait()

                    # legacy: (msg, detail)
                    if isinstance(item, tuple) and len(item) == 2:
                        msg, detail = item
                        self.set(msg, detail)
                        continue

                    # new: (level, msg, detail)
                    if isinstance(item, tuple) and len(item) == 3:
                        level, msg, detail = item
                        level = (level or "INFO").upper()

                        # always update top status
                        self.set(msg, detail)

                        # record warnings/errors in list
                        if level in ("WARN", "ERROR"):
                            self._append_message(level, msg, detail)

                        # optional: if you send DONE through queue
                        if level == "DONE":
                            self.done(msg, detail)

                        continue

                    # fallback: unknown payload -> show it
                    self.set(str(item), "")

            except Empty:
                pass

        self.root.after(100, self.pump_queue)

    def run(self):
        """
        Start UI loop. Call pump_queue() before mainloop to enable queue updates.
        """
        self.pump_queue()
        self.root.mainloop()
