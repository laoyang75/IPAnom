#!/usr/bin/env python3
"""
RB20 v2.5 WebUI Launcher
A simple GUI to manage the WebUI service.
"""
import tkinter as tk
from tkinter import ttk, messagebox
import subprocess
import threading
import sys
import os
import webbrowser
import time
import signal

class LauncherApp:
    def __init__(self, root):
        self.root = root
        self.root.title("RB20 v2.5 Launcher")
        self.root.geometry("450x300")
        self.root.resizable(False, False)

        # Style colors (Light Theme)
        self.bg_color = "#ffffff"
        self.fg_color = "#333333"
        self.accent_blue = "#0066cc"
        self.accent_green = "#2da44e"
        self.accent_red = "#cf222e"
        self.card_bg = "#f6f8fa"
        self.border_color = "#d0d7de"

        self.root.configure(bg=self.bg_color)
        
        # Process management
        self.process = None
        self.port = 8000
        self.url = f"http://localhost:{self.port}"
        self.webui_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.setup_ui()
        self.update_status()

    def setup_ui(self):
        # Header
        header_frame = tk.Frame(self.root, bg=self.card_bg, height=60, bd=0)
        header_frame.pack(fill="x", side="top")
        header_frame.pack_propagate(False)

        tk.Label(
            header_frame, text="⬡ RB20 v2.5 WebUI", 
            font=("Inter", 16, "bold"), bg=self.card_bg, fg=self.fg_color
        ).pack(side="left", padx=20, pady=15)

        # Main Body
        main_frame = tk.Frame(self.root, bg=self.bg_color, padx=30, pady=25)
        main_frame.pack(fill="both", expand=True)

        # Status Row
        status_frame = tk.Frame(main_frame, bg=self.bg_color)
        status_frame.pack(fill="x", pady=(0, 20))
        
        tk.Label(status_frame, text="Service Status:", font=("Inter", 10), bg=self.bg_color, fg="#8b949e").pack(side="left")
        self.status_label = tk.Label(status_frame, text="STOPPED", font=("Inter", 10, "bold"), bg=self.bg_color, fg=self.accent_red)
        self.status_label.pack(side="left", padx=5)

        # Buttons
        btn_style = ttk.Style()
        btn_style.theme_use('default')
        btn_style.configure("TButton", font=("Inter", 10, "bold"), padding=10)

        self.start_btn = tk.Button(
            main_frame, text="🚀 Start Service", command=self.start_service,
            bg=self.accent_blue, fg="white", activebackground="#4c94e6",
            font=("Inter", 10, "bold"), bd=0, cursor="hand2", width=15
        )
        self.start_btn.pack(fill="x", pady=5)

        self.stop_btn = tk.Button(
            main_frame, text="🛑 Stop Service", command=self.stop_service,
            bg=self.accent_red, fg="white", activebackground="#e64c4c",
            font=("Inter", 10, "bold"), bd=0, cursor="hand2", width=15,
            state="disabled"
        )
        self.stop_btn.pack(fill="x", pady=5)

        self.browser_btn = tk.Button(
            main_frame, text="🌐 Open Browser", command=self.open_browser,
            bg="#21262d", fg=self.fg_color, activebackground="#30363d",
            font=("Inter", 10, "bold"), bd=1, highlightbackground=self.border_color,
            cursor="hand2", width=15
        )
        self.browser_btn.pack(fill="x", pady=(15, 5))

        # Footer
        footer = tk.Label(
            self.root, text=f"Port: {self.port} | Path: {self.webui_dir}", 
            font=("JetBrains Mono", 8), bg=self.bg_color, fg="#6e7681"
        )
        footer.pack(side="bottom", pady=10)

    def start_service(self):
        if self.process:
            return

        def run():
            try:
                # Use python3 and uvicorn
                cmd = [sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", str(self.port)]
                self.process = subprocess.Popen(
                    cmd, 
                    cwd=self.webui_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    preexec_fn=os.setsid if hasattr(os, 'setsid') else None
                )
                
                self.root.after(0, self.on_service_started)
                
                # Monitor output (optional, could log to a text area)
                for line in self.process.stdout:
                    print(f"[WebUI] {line.strip()}")
                
                self.process.wait()
                self.root.after(0, self.on_service_stopped)
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to start service: {e}"))
                self.root.after(0, self.on_service_stopped)

        threading.Thread(target=run, daemon=True).start()

    def on_service_started(self):
        self.status_label.config(text="RUNNING", fg=self.accent_green)
        self.start_btn.config(state="disabled", bg="#30363d")
        self.stop_btn.config(state="normal", bg=self.accent_red)
        time.sleep(1) # Wait for uvicorn to bind
        # self.open_browser() # Optional: Auto open browser

    def on_service_stopped(self):
        self.process = None
        self.status_label.config(text="STOPPED", fg=self.accent_red)
        self.start_btn.config(state="normal", bg=self.accent_blue)
        self.stop_btn.config(state="disabled", bg="#30363d")

    def stop_service(self):
        if not self.process:
            return
        
        try:
            if hasattr(os, 'killpg'):
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            else:
                self.process.terminate()
            
            self.process = None
            self.on_service_stopped()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to stop service: {e}")

    def open_browser(self):
        webbrowser.open(self.url)

    def update_status(self):
        # Could add more real-time health checks here
        self.root.after(2000, self.update_status)

if __name__ == "__main__":
    root = tk.Tk()
    app = LauncherApp(root)
    
    def on_closing():
        if app.process:
            if messagebox.askokcancel("Quit", "WebUI is still running. Stop it and quit?"):
                app.stop_service()
                root.destroy()
        else:
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()
