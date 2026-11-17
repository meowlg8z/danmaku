# -*- coding: utf-8 -*-
"""
日志 & 错误邮件发送模块

- 日志写入本地 logs/ 目录（按大小滚动，保留多个备份）
- 捕获未处理异常并发送邮件到指定邮箱
"""
import os
import sys
import traceback
import threading
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from logging.handlers import RotatingFileHandler
import logging


# —— 邮件配置 ——
EMAIL_CONFIG = {
    "smtp_server": "smtp.qq.com",
    "smtp_port": 465,
    "sender": "1428307726@qq.com",
    "password": "hplyjoyisvplgjcf",
    "receiver": "meowlg8z@outlook.com",
}


# —— 日志目录 ——
def _get_app_dir():
    """获取程序所在目录，兼容 PyInstaller 打包和源码运行"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    else:
        return os.path.dirname(os.path.abspath(__file__))


LOG_DIR = os.path.join(_get_app_dir(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "app.log")


# —— 初始化日志器 ——
logger = logging.getLogger("danmaku")
logger.setLevel(logging.DEBUG)

# 控制台处理器
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

# 文件处理器（单文件最大 2MB，保留 5 个备份）
file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
file_handler.setLevel(logging.DEBUG)

# 格式化
formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)


# —— 邮件发送功能 ——
_email_lock = threading.Lock()
_last_email_time = 0
EMAIL_COOLDOWN = 60


def send_error_email(error_msg: str, error_type: str = "异常"):
    """发送错误邮件"""
    global _last_email_time

    now = datetime.now().timestamp()
    if now - _last_email_time < EMAIL_COOLDOWN:
        return

    with _email_lock:
        now = datetime.now().timestamp()
        if now - _last_email_time < EMAIL_COOLDOWN:
            return

        try:
            config = EMAIL_CONFIG
            msg = MIMEMultipart()
            msg["From"] = config["sender"]
            msg["To"] = config["receiver"]
            msg["Subject"] = f"[弹幕助手] {error_type} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

            import platform
            body = f"""
            <h3>弹幕助手错误报告</h3>
            <p><strong>时间:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>错误类型:</strong> {error_type}</p>
            <p><strong>错误详情:</strong></p>
            <pre style="background:#f5f5f5;padding:10px;border-radius:5px;">{error_msg}</pre>
            <p><strong>运行环境:</strong></p>
            <ul>
                <li>系统: {platform.system()} {platform.release()}</li>
                <li>Python: {platform.python_version()}</li>
                <li>架构: {platform.architecture()[0]}</li>
            </ul>
            """
            msg.attach(MIMEText(body, "html", "utf-8"))

            if config["smtp_port"] == 465:
                server = smtplib.SMTP_SSL(config["smtp_server"], config["smtp_port"], timeout=10)
            else:
                server = smtplib.SMTP(config["smtp_server"], config["smtp_port"], timeout=10)
                server.starttls()

            server.login(config["sender"], config["password"])
            server.sendmail(config["sender"], [config["receiver"]], msg.as_string())
            server.quit()

            _last_email_time = now
            logger.info("错误邮件已发送")

        except Exception as e:
            logger.error(f"邮件发送失败: {e}")


# —— 全局异常钩子 ——
def global_exception_hook(exc_type, exc_value, exc_tb):
    """捕获未处理的异常"""
    error_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_tb))
    logger.error(f"未处理异常:\n{error_msg}")

    threading.Thread(
        target=send_error_email,
        args=(error_msg, exc_type.__name__),
        daemon=True
    ).start()


def thread_exception_hook(args):
    """捕获子线程未处理的异常"""
    error_msg = "".join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
    logger.error(f"线程异常 [{args.thread.name}]:\n{error_msg}")

    threading.Thread(
        target=send_error_email,
        args=(error_msg, f"线程异常 - {args.exc_type.__name__}"),
        daemon=True
    ).start()


def setup_exception_handling():
    """设置全局异常捕获"""
    sys.excepthook = global_exception_hook
    threading.excepthook = thread_exception_hook
    logger.info("日志系统已初始化")


# —— 便捷接口 ——
def get_logger():
    return logger