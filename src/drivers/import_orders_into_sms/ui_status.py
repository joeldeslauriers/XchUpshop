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

        # ✅ Better for DPI / long messages: allow resizing (at least vertically)
        self.root.geometry("720x420")
        self.root.minsize(720, 420)
        self.root.resizable(True, True)  # <--- key fix (was False, False)

        self.root.attributes("-topmost", True)

        self.msg_var = tk.StringVar(value="Starting...")
        self.detail_var = tk.StringVar(value="")
        self.count_var = tk.StringVar(value="Errors: 0 | Warnings: 0")

        self.errors_count = 0
        self.warn_count = 0

        # ===== Root grid config (buttons always visible)
        self.root.grid_rowconfigure(0, weight=0)  # title
        self.root.grid_rowconfigure(1, weight=0)  # msg
        self.root.grid_rowconfigure(2, weight=0)  # detail
        self.root.grid_rowconfigure(3, weight=0)  # progress
        self.root.grid_rowconfigure(4, weight=1)  # messages area (expands)
        self.root.grid_rowconfigure(5, weight=0)  # buttons
        self.root.grid_columnconfigure(0, weight=1)

        # ===== Header
        ttk.Label(
            self.root, text="Upshop Order Import", font=("Segoe UI", 12, "bold")
        ).grid(row=0, column=0, pady=(10, 4), sticky="n")

        ttk.Label(self.root, textvariable=self.msg_var, font=("Segoe UI", 10)).grid(
            row=1, column=0, sticky="ew", padx=16
        )

        # ✅ Use Message widget for wrapping (prevents layout weirdness + helps DPI)
        self.detail_msg = tk.Message(
            self.root,
            textvariable=self.detail_var,
            font=("Segoe UI", 9),
            width=680,          # wrap width
            justify="center"
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

        ttk.Label(header, text="Messages:", font=("Segoe UI", 9, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(header, textvariable=self.count_var, font=("Segoe UI", 9)).grid(
            row=0, column=1, sticky="e"
        )

        # Listbox + scrollbar
        list_row = ttk.Frame(frame)
        list_row.grid(row=1, column=0, sticky="nsew")
        list_row.grid_rowconfigure(0, weight=1)
        list_row.grid_columnconfigure(0, weight=1)

        self.listbox = tk.Listbox(list_row, height=10)
        self.listbox.grid(row=0, column=0, sticky="nsew")

        sb = ttk.Scrollbar(list_row, orient="vertical", command=self.listbox.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self.listbox.configure(yscrollcommand=sb.set)

        # ===== Buttons (ALWAYS visible)
        btn_row = ttk.Frame(self.root)
        btn_row.grid(row=5, column=0, sticky="ew", padx=16, pady=(0, 12))
        btn_row.grid_columnconfigure(0, weight=1)
        btn_row.grid_columnconfigure(1, weight=1)

        self.clear_btn = ttk.Button(btn_row, text="Clear", command=self._clear_messages)
        self.clear_btn.grid(row=0, column=0, sticky="w")

        self.close_btn = ttk.Button(btn_row, text="Close", command=self.root.destroy, state="disabled")
        self.close_btn.grid(row=0, column=1, sticky="e")

        self.root.protocol("WM_DELETE_WINDOW", self._on_close_attempt)

        # Keep wrap width correct when resizing
        self.root.bind("<Configure>", self._on_resize)

    def _on_resize(self, _evt=None):
        # Adjust message wrap width when window changes size
        try:
            w = max(500, self.root.winfo_width() - 40)
            self.detail_msg.configure(width=w)
        except Exception:
            pass

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

    # used by main.py to show success PO list (INFO lines)
    def add_message(self, level: str, msg: str, detail: str = ""):
        self._append_message(level, msg, detail)

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

                        self.set(msg, detail)

                        if level in ("WARN", "ERROR"):
                            self._append_message(level, msg, detail)
                            if level == "ERROR":
                                try:
                                    self.pb.stop()
                                except Exception:
                                    pass
                                self.close_btn.configure(state="normal")

                        if level == "DONE":
                            self.done(msg, detail)

                        continue

                    self.set(str(item), "")

            except Empty:
                pass

        self.root.after(100, self.pump_queue)

    def run(self):
        self.pump_queue()
        self.root.mainloop()
