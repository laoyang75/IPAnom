#!/usr/bin/env python3
"""
RB20 v2.5 WebUI Launcher
A clean, modern GUI to manage the WebUI service using customtkinter.
"""
import customtkinter as ctk
import subprocess
import threading
import sys
import os
import webbrowser
import time
import signal
import socket
from tkinter import messagebox

# ── Theme: Light & Clean ──
ctk.set_appearance_mode("Light")
ctk.set_default_color_theme("blue")

# ── Constants ──
DEFAULT_PORT = 8721

# Clean day-mode palette
BG        = "#f5f5f7"    # light gray background
CARD_BG   = "#ffffff"    # white card
BORDER    = "#e0e0e0"    # subtle border
TEXT_PRI  = "#1d1d1f"    # primary text
TEXT_SEC  = "#86868b"    # secondary text
ACCENT    = "#0071e3"    # Apple blue
GREEN     = "#34c759"    # status green
RED       = "#ff3b30"    # status red
YELLOW    = "#ff9500"    # warning orange
HOVER_ACC = "#0056b3"


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("localhost", port)) == 0


def get_port_process(port: int) -> str:
    try:
        out = subprocess.check_output(
            f"lsof -ti:{port}", shell=True, text=True, stderr=subprocess.DEVNULL
        ).strip()
        if out:
            pid = out.split("\n")[0]
            name = subprocess.check_output(
                f"ps -p {pid} -o comm=", shell=True, text=True, stderr=subprocess.DEVNULL
            ).strip()
            return f"PID {pid} ({name})"
    except Exception:
        pass
    return ""


class LauncherApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("RB20 v2.5 可视化平台")
        self.geometry("460x480")
        self.resizable(False, False)
        self.configure(fg_color=BG)

        self.process = None
        self.port = DEFAULT_PORT
        self.url = f"http://localhost:{self.port}"
        self.webui_dir = os.path.dirname(os.path.abspath(__file__))

        self._build_ui()
        self._poll_status()

    # ────────────────────────────────────────────
    # UI
    # ────────────────────────────────────────────
    def _build_ui(self):
        # ── Header ──
        hdr = ctk.CTkFrame(self, fg_color="transparent")
        hdr.pack(fill="x", pady=(24, 4))

        ctk.CTkLabel(
            hdr, text="RB20 v2.5",
            font=ctk.CTkFont(size=24, weight="bold"),
            text_color=TEXT_PRI,
        ).pack()
        ctk.CTkLabel(
            hdr, text="可视化监控平台 · 服务管理器",
            font=ctk.CTkFont(size=12), text_color=TEXT_SEC,
        ).pack(pady=(2, 0))

        # ── Status Card ──
        card = ctk.CTkFrame(self, fg_color=CARD_BG, corner_radius=12,
                            border_width=1, border_color=BORDER)
        card.pack(fill="x", padx=28, pady=(16, 8))

        row = ctk.CTkFrame(card, fg_color="transparent")
        row.pack(fill="x", padx=18, pady=(14, 4))
        ctk.CTkLabel(row, text="服务状态", font=ctk.CTkFont(size=12),
                     text_color=TEXT_SEC).pack(side="left")
        self.status_label = ctk.CTkLabel(
            row, text="● 检测中…",
            font=ctk.CTkFont(size=12, weight="bold"), text_color=TEXT_SEC,
        )
        self.status_label.pack(side="right")

        row2 = ctk.CTkFrame(card, fg_color="transparent")
        row2.pack(fill="x", padx=18, pady=(0, 14))
        ctk.CTkLabel(row2, text="端口", font=ctk.CTkFont(size=12),
                     text_color=TEXT_SEC).pack(side="left")
        self.port_info = ctk.CTkLabel(
            row2, text=str(self.port),
            font=ctk.CTkFont(family="Menlo", size=12, weight="bold"),
            text_color=ACCENT,
        )
        self.port_info.pack(side="right")

        # ── Action Buttons — icons + flat style ──
        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.pack(fill="x", padx=28, pady=(8, 0))

        # Row 1: Start + Restart side by side
        row_top = ctk.CTkFrame(btns, fg_color="transparent")
        row_top.pack(fill="x", pady=(0, 6))
        row_top.columnconfigure(0, weight=3)
        row_top.columnconfigure(1, weight=2)

        self.start_btn = ctk.CTkButton(
            row_top, text="▶  启动", command=self.start_service,
            font=ctk.CTkFont(size=13, weight="bold"), height=40,
            fg_color=ACCENT, hover_color=HOVER_ACC, text_color="#ffffff",
            corner_radius=8,
        )
        self.start_btn.grid(row=0, column=0, sticky="ew", padx=(0, 4))

        self.restart_btn = ctk.CTkButton(
            row_top, text="↻  重启", command=self.restart_service,
            font=ctk.CTkFont(size=13, weight="bold"), height=40,
            fg_color="#f0f0f2", hover_color="#e4e4e7", text_color=TEXT_PRI,
            corner_radius=8, state="disabled",
        )
        self.restart_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0))

        # Row 2: Stop + Clean side by side
        row_mid = ctk.CTkFrame(btns, fg_color="transparent")
        row_mid.pack(fill="x", pady=6)
        row_mid.columnconfigure(0, weight=3)
        row_mid.columnconfigure(1, weight=2)

        self.stop_btn = ctk.CTkButton(
            row_mid, text="■  停止", command=self.stop_service,
            font=ctk.CTkFont(size=13, weight="bold"), height=40,
            fg_color="#f0f0f2", hover_color="#e4e4e7", text_color=TEXT_PRI,
            corner_radius=8, state="disabled",
        )
        self.stop_btn.grid(row=0, column=0, sticky="ew", padx=(0, 4))

        self.clean_btn = ctk.CTkButton(
            row_mid, text="⚠  清理端口", command=self.cleanup_port,
            font=ctk.CTkFont(size=13, weight="bold"), height=40,
            fg_color="#f0f0f2", hover_color="#e4e4e7", text_color=YELLOW,
            corner_radius=8, state="disabled",
        )
        self.clean_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0))

        # Row 3: Open browser — full width outline
        self.browser_btn = ctk.CTkButton(
            btns, text="🌐  在浏览器中打开", command=self.open_browser,
            font=ctk.CTkFont(size=13, weight="bold"), height=40,
            fg_color="transparent", border_width=1, border_color=BORDER,
            text_color=ACCENT, hover_color="#f0f0f2",
            corner_radius=8,
        )
        self.browser_btn.pack(fill="x", pady=6)

        # ── Footer ──
        self.footer = ctk.CTkLabel(
            self,
            text=os.path.basename(self.webui_dir),
            font=ctk.CTkFont(size=10), text_color=TEXT_SEC,
        )
        self.footer.pack(side="bottom", pady=10)

    # ────────────────────────────────────────────
    # Port management
    # ────────────────────────────────────────────
    def cleanup_port(self):
        proc = get_port_process(self.port)
        msg = f"端口 {self.port} 被占用"
        if proc:
            msg += f"\n占用进程: {proc}"
        msg += "\n\n确认要终止该进程吗？"

        if messagebox.askyesno("确认清理", msg):
            try:
                subprocess.run(f"lsof -ti:{self.port} | xargs kill -9",
                               shell=True, check=True)
                messagebox.showinfo("成功", f"端口 {self.port} 已释放")
                self._poll_status()
            except Exception as e:
                messagebox.showerror("错误", f"清理失败: {e}")

    # ────────────────────────────────────────────
    # Service lifecycle
    # ────────────────────────────────────────────
    def start_service(self):
        if self.process:
            return

        if port_in_use(self.port):
            proc = get_port_process(self.port)
            detail = f"\n占用进程: {proc}" if proc else ""
            messagebox.showwarning(
                "端口冲突",
                f"端口 {self.port} 已被占用。{detail}\n\n"
                f"请先停止占用进程，或使用「清理端口」按钮。"
            )
            return

        def run():
            try:
                cmd = [
                    sys.executable, "-m", "uvicorn",
                    "main:app", "--host", "0.0.0.0", "--port", str(self.port),
                ]
                self.process = subprocess.Popen(
                    cmd, cwd=self.webui_dir,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True,
                    preexec_fn=os.setsid if hasattr(os, "setsid") else None,
                )
                self.after(0, self._on_started)

                for line in self.process.stdout:
                    print(f"[WebUI] {line.strip()}")

                # Process ended — safely wait and clean up
                rc = self.process.wait() if self.process else None
                self.after(0, self._on_stopped)
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("错误", f"启动失败: {e}"))
                self.after(0, self._on_stopped)

        threading.Thread(target=run, daemon=True).start()

    def stop_service(self):
        proc = self.process
        if not proc:
            return
        try:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            else:
                proc.terminate()
            # Wait for the process to actually exit (up to 3s)
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
            # Now safe to clear
            self.process = None
            self._on_stopped()
        except Exception as e:
            self.process = None
            self._on_stopped()

    def restart_service(self):
        """Stop then start — runs in background to keep UI responsive."""
        def _do_restart():
            self.after(0, lambda: self.status_label.configure(
                text="↻ 重启中…", text_color=YELLOW))
            self.stop_service()
            time.sleep(1)  # brief pause for port release
            self.after(0, self.start_service)
        threading.Thread(target=_do_restart, daemon=True).start()

    def open_browser(self):
        webbrowser.open(self.url)

    # ────────────────────────────────────────────
    # State transitions
    # ────────────────────────────────────────────
    def _on_started(self):
        self.status_label.configure(text="● 运行中", text_color=GREEN)
        self.start_btn.configure(state="disabled", fg_color="#e4e4e7", text_color=TEXT_SEC)
        self.stop_btn.configure(state="normal", fg_color="#fff0f0", text_color=RED)
        self.restart_btn.configure(state="normal", fg_color="#f0f7ff", text_color=ACCENT)
        self.clean_btn.configure(state="disabled")
        self.port_info.configure(text=f"{self.port}  ✓", text_color=GREEN)

    def _on_stopped(self):
        self.process = None
        self.start_btn.configure(state="normal", fg_color=ACCENT, text_color="#ffffff")
        self.stop_btn.configure(state="disabled", fg_color="#f0f0f2", text_color=TEXT_SEC)
        self.restart_btn.configure(state="disabled", fg_color="#f0f0f2", text_color=TEXT_SEC)
        self.port_info.configure(text=str(self.port), text_color=ACCENT)
        self._poll_status()

    def _poll_status(self):
        if self.process:
            self.status_label.configure(text="● 运行中", text_color=GREEN)
            self.clean_btn.configure(state="disabled")
        else:
            if port_in_use(self.port):
                proc = get_port_process(self.port)
                tip = f"  ({proc})" if proc else ""
                self.status_label.configure(
                    text=f"⚠ 端口被占用{tip}", text_color=YELLOW
                )
                self.clean_btn.configure(state="normal", text_color=YELLOW)
                self.port_info.configure(text=f"{self.port}  ⚠", text_color=YELLOW)
            else:
                self.status_label.configure(text="● 已停止", text_color=RED)
                self.clean_btn.configure(state="disabled")
        self.after(3000, self._poll_status)


# ────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────
if __name__ == "__main__":
    app = LauncherApp()

    def on_closing():
        if app.process:
            if messagebox.askokcancel("退出", "服务仍在运行，确认停止并退出？"):
                app.stop_service()
                app.destroy()
        else:
            app.destroy()

    app.protocol("WM_DELETE_WINDOW", on_closing)
    app.mainloop()
