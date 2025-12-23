# ui_status.py
import tkinter as tk
from tkinter import ttk
import threading

class StatusUI:
    def __init__(self, title="Upshop Import"):
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

        self.close_btn = ttk.Button(
            self.root, text="Close", command=self.root.destroy, state="disabled"
        )
        self.close_btn.pack(pady=(0, 10))

    def set(self, msg, detail=""):
        self.msg_var.set(msg)
        self.detail_var.set(detail)
        self.root.update_idletasks()

    def done(self, msg="Done", detail=""):
        self.pb.stop()
        self.pb.configure(mode="determinate", value=100)
        self.set(msg, detail)
        self.close_btn.configure(state="normal")

    def error(self, msg="Error", detail=""):
        self.pb.stop()
        self.set(msg, detail)
        self.close_btn.configure(state="normal")

    def run_background(self, func):
        def wrapper():
            try:
                func()
            except Exception as e:
                self.error("Import failed", str(e))
        threading.Thread(target=wrapper, daemon=True).start()
        self.root.mainloop()
