import tkinter as tk
from tkinter import messagebox
import paramiko
import threading
import time
import datetime
import random
import re

SERVER_IP = "172.17.0.38"
SERVER_USER = "root"
SERVER_PASS = "shuzilm##123"

class RemoteTimeApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(f"远程服务器时间管理 - {SERVER_IP}")
        # 居中显示窗口
        window_width = 520
        window_height = 480
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()
        x_cordinate = int((screen_width/2) - (window_width/2))
        y_cordinate = int((screen_height/2) - (window_height/2))
        self.geometry("{}x{}+{}+{}".format(window_width, window_height, x_cordinate, y_cordinate))
        
        self.ssh_client = None
        self.remote_time = None
        self.local_sync_time = None
        self.sync_running = False
        
        self.setup_ui()
        
        # 启动伪同步时钟线程
        self.sync_running = True
        self.clock_thread = threading.Thread(target=self.update_clock_loop, daemon=True)
        self.clock_thread.start()
        
        # 启动自动获取
        self.fetch_remote_time_bg()

    def setup_ui(self):
        # 顶部区域：当前时间
        frame_top = tk.LabelFrame(self, text="远程服务器当前时间", padx=10, pady=10)
        frame_top.pack(fill="x", padx=10, pady=10)
        
        self.lbl_time = tk.Label(frame_top, text="正在连接获取...", font=("Helvetica", 24, "bold"), fg="#0066cc")
        self.lbl_time.pack(pady=10)
        
        btn_frame1 = tk.Frame(frame_top)
        btn_frame1.pack(fill="x")
        
        self.btn_refresh = tk.Button(btn_frame1, text="手动刷新真实时间", command=self.fetch_remote_time_bg)
        self.btn_refresh.pack(side="left", padx=5)

        self.btn_ntp_on = tk.Button(btn_frame1, text="恢复网络同步(NTP)", command=self.enable_ntp_bg)
        self.btn_ntp_on.pack(side="right", padx=5)

        # 中部区域：修改时间
        frame_mid = tk.LabelFrame(self, text="修改服务器时间", padx=10, pady=10)
        frame_mid.pack(fill="x", padx=10, pady=10)

        # 目标日期和时间输入
        input_frame = tk.Frame(frame_mid)
        input_frame.grid(row=0, column=0, columnspan=2, sticky="w", pady=5)
        
        lbl_date = tk.Label(input_frame, text="目标时间:")
        lbl_date.pack(side="left", padx=(0, 5))
        
        self.entry_date = tk.Entry(input_frame, width=12)
        self.entry_date.pack(side="left", padx=5)
        self.entry_date.insert(0, datetime.datetime.now().strftime("%Y-%m-%d"))
        
        self.entry_time = tk.Entry(input_frame, width=10)
        self.entry_time.pack(side="left", padx=5)
        self.entry_time.insert(0, datetime.datetime.now().strftime("%H:%M:%S"))

        # 随机时间生成器
        lbl_rand = tk.Label(frame_mid, text="针对日期生成随机时间:")
        lbl_rand.grid(row=1, column=0, sticky="w", pady=5)
        
        rand_frame = tk.Frame(frame_mid)
        rand_frame.grid(row=1, column=1, sticky="w")
        
        self.btn_rand = tk.Button(rand_frame, text="生成随机工作时间", command=self.generate_random_time)
        self.btn_rand.pack(side="left", padx=5)
        
        # 执行更新区域
        update_frame = tk.Frame(frame_mid)
        update_frame.grid(row=2, column=0, columnspan=2, pady=15)
        
        self.btn_update = tk.Button(update_frame, text="确认更新远程时间", font=("Helvetica", 14, "bold"), fg="red", command=self.update_remote_time_bg)
        self.btn_update.pack(side="left", padx=5)
        
        self.lbl_status = tk.Label(update_frame, text="", font=("Helvetica", 12, "bold"))
        self.lbl_status.pack(side="left", padx=5)

        # 底部区域：日志
        frame_bottom = tk.LabelFrame(self, text="执行日志", padx=10, pady=5)
        frame_bottom.pack(fill="both", expand=True, padx=10, pady=10)
        
        self.txt_log = tk.Text(frame_bottom, height=6, state="disabled", bg="#f4f4f4", font=("Menlo", 12))
        self.txt_log.pack(fill="both", expand=True)

    def log(self, msg):
        self.txt_log.config(state="normal")
        now = datetime.datetime.now().strftime("%H:%M:%S")
        self.txt_log.insert("end", f"[{now}] {msg}\n")
        self.txt_log.see("end")
        self.txt_log.config(state="disabled")

    def execute_ssh_command(self, cmd, timeout=10):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            # timeout防止卡死
            ssh.connect(SERVER_IP, username=SERVER_USER, password=SERVER_PASS, timeout=timeout)
            stdin, stdout, stderr = ssh.exec_command(cmd)
            out = stdout.read().decode('utf-8').strip()
            err = stderr.read().decode('utf-8').strip()
            ssh.close()
            return True, out, err
        except Exception as e:
            return False, str(e), ""

    def fetch_remote_time_bg(self):
        self.lbl_time.config(text="正在获取...")
        threading.Thread(target=self._fetch_remote_time, daemon=True).start()

    def _fetch_remote_time(self):
        self.log(f"正在通过SSH连接 {SERVER_IP} 获取时间...")
        success, out, err = self.execute_ssh_command("date +'%Y-%m-%d %H:%M:%S'")
        if success and out:
            try:
                # 解析远程真实时间
                self.remote_time = datetime.datetime.strptime(out, "%Y-%m-%d %H:%M:%S")
                # 记录本地获取时的tick，用于伪同步计算
                self.local_sync_time = time.time()
                self.log(f"成功获取远端真实时间: {out}")
            except Exception as e:
                self.log(f"解析时间失败: {e}")
                self.lbl_time.config(text="解析失败")
        else:
            self.log(f"获取时间失败: {out} {err}")
            self.lbl_time.config(text="SSH连接失败")

    def enable_ntp_bg(self):
        if messagebox.askyesno("确认", "确定要开启服务器的NTP网络时间同步吗？\n开启后之前的手动修改将被系统自动覆盖。"):
            threading.Thread(target=self._enable_ntp, daemon=True).start()

    def _enable_ntp(self):
        self.log("正在开启 ntp 同步...")
        success, out, err = self.execute_ssh_command("timedatectl set-ntp yes")
        if success:
            self.log("成功开启 NTP 网络同步。")
            self._fetch_remote_time()
        else:
            self.log(f"开启失败: {out} {err}")

    def generate_random_time(self):
        # 随机生成工作时间：9:30-12:00 (9.5-12) 或 13:30-18:00 (13.5-18)
        # 用总秒数来控制概率，上午2.5小时，下午4.5小时
        morning_seconds = int(2.5 * 3600)  # 9000
        afternoon_seconds = int(4.5 * 3600) # 16200
        total_work_seconds = morning_seconds + afternoon_seconds
        
        r = random.randint(0, total_work_seconds - 1)
        if r < morning_seconds:
            # 上午 09:30:00 起
            base_hour = 9
            base_minute = 30
            offset_seconds = r
        else:
            # 下午 13:30:00 起
            base_hour = 13
            base_minute = 30
            offset_seconds = r - morning_seconds
            
        # 构造一个基础时间对象用于累加秒数
        base_time = datetime.datetime(2000, 1, 1, base_hour, base_minute)
        rand_time = base_time + datetime.timedelta(seconds=offset_seconds)
        
        # 只更新时间输入框
        self.entry_time.delete(0, 'end')
        self.entry_time.insert(0, rand_time.strftime("%H:%M:%S"))
        self.log(f"已生成随机工作时间: {rand_time.strftime('%H:%M:%S')}")

    def update_remote_time_bg(self):
        date_str = self.entry_date.get().strip()
        time_str = self.entry_time.get().strip()
        target = f"{date_str} {time_str}"
        
        # 校验格式
        if not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", target):
            messagebox.showerror("格式错误", "请确保日期(YYYY-MM-DD)和时间(HH:MM:SS)格式正确")
            return
        
        # 变色提示正在执行
        self.btn_update.config(state="disabled")
        self.lbl_status.config(text="更新中...", fg="orange")
        
        # 不再弹窗，直接后台线程执行
        threading.Thread(target=self._update_remote_time, args=(target,), daemon=True).start()

    def _update_remote_time(self, target):
        self.log("开始执行时间更新...")
        
        self.log("(1/3) 正在关闭 NTP...")
        success1, out1, err1 = self.execute_ssh_command("timedatectl set-ntp no")
        if not success1:
            self.log(f"关闭NTP可能失败(或已处于关闭): {out1} {err1}")
            # 不阻断，继续尝试修改时间
            
        self.log(f"(2/3) 正在设置时间为 {target} ...")
        success2, out2, err2 = self.execute_ssh_command(f"timedatectl set-time '{target}'")
        if not success2:
            self.log(f"❌ 设置时间失败: {out2} {err2}")
            self.after(0, lambda: self._update_ui_status("时间修改失败", "red"))
            return
            
        self.log("✅ 远程时间修改成功！")
        self.after(0, lambda: self.lbl_status.config(text="重启服务中...", fg="orange"))
        
        # 更新完时间后，重启 dovecot 服务以避免异常
        self.log("(3/3) 正在重启 dovecot 服务...")
        success3, out3, err3 = self.execute_ssh_command("systemctl restart dovecot && systemctl is-active dovecot", timeout=15)
        
        if success3 and out3 == "active":
            self.log("✅ Dovecot 服务已成功重启并正常运行。")
            self.after(0, lambda: self._update_ui_status("修改成功(服务已重启)", "green"))
        else:
            self.log(f"⚠️ Dovecot 服务重启或检测异常: {out3} {err3}")
            self.after(0, lambda: self._update_ui_status("修改成功(服务异常)", "orange"))

        # 最后自动刷新一次界面时间
        self._fetch_remote_time()


    def _update_ui_status(self, text, color):
        self.btn_update.config(state="normal")
        self.lbl_status.config(text=text, fg=color)
        # 5秒后清空状态文本，因为提示语较长，留久一点
        self.after(5000, lambda: self.lbl_status.config(text=""))

    def update_clock_loop(self):
        """伪同步时钟，每秒刷新一次本地界面上的时间，保持跟远端一样的流逝效果"""
        while self.sync_running:
            if self.remote_time and self.local_sync_time:
                # 计算自上次真实获取后，本地经过了多少秒
                elapsed = time.time() - self.local_sync_time
                # 累加到远端基准时间上
                current_pseudo_time = self.remote_time + datetime.timedelta(seconds=elapsed)
                self.lbl_time.config(text=current_pseudo_time.strftime("%Y-%m-%d %H:%M:%S"))
            time.sleep(1)

if __name__ == "__main__":
    app = RemoteTimeApp()
    app.mainloop()

