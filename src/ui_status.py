import tkinter as tk
from tkinter import ttk
from queue import Empty


class StatusUI:
    """
    Small status window (Tkinter) that stays responsive while work runs in a background thread.
    Uses a Queue (thread-safe) to receive (msg, detail) updates.
    """

    def __init__(self, title="Upshop Import", queue=None):
        self.queue = queue
        self.root = tk.Tk()
        self.root.title(title)
        self.root.resizable(False, False)
        self.root.geometry("520x170")
        self.root.attributes("-topmost", True)

        self.msg_var = tk.StringVar(value="Starting...")
        self.detail_var = tk.StringVar(value="")

        ttk.Label(self.root, text="Upshop Order Import",
                  font=("Segoe UI", 12, "bold")).pack(pady=(10, 4))
        ttk.Label(self.root, textvariable=self.msg_var,
                  font=("Segoe UI", 10)).pack()
        ttk.Label(self.root, textvariable=self.detail_var,
                  font=("Segoe UI", 9)).pack()

        self.pb = ttk.Progressbar(self.root, mode="indeterminate")
        self.pb.pack(fill="x", padx=16, pady=12)
        self.pb.start(10)

        self.close_btn = ttk.Button(self.root, text="Close", command=self.root.destroy, state="disabled")
        self.close_btn.pack(pady=(0, 10))

        self.root.protocol("WM_DELETE_WINDOW", self._on_close_attempt)

    def _on_close_attempt(self):
        # block close while still working (optional)
        if str(self.close_btn["state"]) == "disabled":
            return
        self.root.destroy()

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
        self.close_btn.configure(state="normal")

    def pump_queue(self):
        """
        Pull UI updates from queue every 100ms.
        """
        if self.queue is not None:
            try:
                while True:
                    msg, detail = self.queue.get_nowait()
                    self.set(msg, detail)
            except Empty:
                pass

        self.root.after(100, self.pump_queue)

    def run(self):
        """
        Start UI loop. Call pump_queue() before mainloop to enable queue updates.
        """
        self.pump_queue()
        self.root.mainloop()
