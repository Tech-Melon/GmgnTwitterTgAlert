#!/usr/bin/env python3
"""GmgnTwitterClaw 服务快捷控制面板。

用法: python ctl.py          → 交互式菜单
      python ctl.py start    → 直接启动
      python ctl.py stop     → 直接停止
      python ctl.py restart  → 直接重启
      python ctl.py status   → 查看状态
      python ctl.py log      → 实时日志
"""

import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

SERVICE_NAME = "gmgn-twitter-monitor.service"
BASE_DIR = Path(__file__).resolve().parent
APP_LOG_FILE = BASE_DIR / "twitter_monitor.log"
ENV_FILE = BASE_DIR / ".env"

# ──────────────────────────── 颜色工具 ────────────────────────────

def _c(code: int, text: str) -> str:
    return f"\033[{code}m{text}\033[0m"

def green(t: str) -> str:  return _c(32, t)
def red(t: str) -> str:    return _c(31, t)
def yellow(t: str) -> str: return _c(33, t)
def cyan(t: str) -> str:   return _c(36, t)
def bold(t: str) -> str:   return _c(1, t)
def dim(t: str) -> str:    return _c(2, t)

# ──────────────────────────── 核心操作 ────────────────────────────

def _run(cmd: list[str], *, replace: bool = False) -> int:
    """执行命令，replace=True 时用 os.execvp 替换当前进程（用于实时日志跟踪）。"""
    if replace:
        print(dim("(按 Ctrl+C 退出日志跟踪)\n"))
        os.execvp(cmd[0], cmd)
        return 0  # 不会执行到这里
    result = subprocess.run(cmd, check=False)
    return result.returncode


def do_start():
    print(cyan("▶ 正在启动服务..."))
    rc = _run(["sudo", "systemctl", "start", SERVICE_NAME])
    if rc == 0:
        print(green("✅ 服务已启动"))
        do_status()
    else:
        print(red(f"❌ 启动失败 (退出码: {rc})"))


def do_stop():
    print(yellow("⏹ 正在停止服务..."))
    rc = _run(["sudo", "systemctl", "stop", SERVICE_NAME])
    if rc == 0:
        print(green("✅ 服务已停止"))
    else:
        print(red(f"❌ 停止失败 (退出码: {rc})"))


def do_restart():
    print(cyan("🔄 正在重启服务..."))
    rc = _run(["sudo", "systemctl", "restart", SERVICE_NAME])
    if rc == 0:
        print(green("✅ 服务已重启"))
        do_status()
    else:
        print(red(f"❌ 重启失败 (退出码: {rc})"))


def do_status():
    print(cyan("\n📊 服务状态:\n"))
    _run(["sudo", "systemctl", "status", SERVICE_NAME, "--no-pager", "-l"])


def do_log_realtime():
    print(cyan("📜 实时 systemd 日志 (journalctl -f):"))
    _run(
        ["sudo", "journalctl", "-u", SERVICE_NAME, "-f", "--no-pager", "-o", "cat"],
        replace=True,
    )


def do_log_recent():
    try:
        n = input(cyan("  输入要查看的行数 [默认 50]: ")).strip()
        n = int(n) if n else 50
    except ValueError:
        n = 50
    print(cyan(f"\n📜 最近 {n} 条 systemd 日志:\n"))
    _run(["sudo", "journalctl", "-u", SERVICE_NAME, "--no-pager", "-o", "cat", "-n", str(n)])


def do_log_app():
    if not APP_LOG_FILE.exists():
        print(red(f"❌ 应用日志文件不存在: {APP_LOG_FILE}"))
        return
    try:
        n = input(cyan(f"  输入要查看的行数 [默认 80]: ")).strip()
        n = int(n) if n else 80
    except ValueError:
        n = 80
    print(cyan(f"\n📜 应用日志 (最后 {n} 行):\n"))
    _run(["tail", "-n", str(n), str(APP_LOG_FILE)])


def do_log_app_follow():
    """彩色实时日志 — 通过 journalctl 输出 stderr 中的 loguru 彩色日志。"""
    print(cyan("📜 实时彩色日志 (journalctl stderr):"))
    _run(
        ["sudo", "journalctl", "-u", SERVICE_NAME, "-f", "--no-pager",
         "-o", "cat", "-p", "0..7"],
        replace=True,
    )


def do_next_restart():
    """计算并显示 systemd 12h 定时重启的剩余时间。"""
    print(cyan("\n⏰ 12h 定时重启信息:\n"))
    try:
        # 获取服务激活时间戳
        result = subprocess.run(
            ["sudo", "systemctl", "show", SERVICE_NAME,
             "--property=ActiveEnterTimestamp", "--no-pager"],
            capture_output=True, text=True, check=False,
        )
        raw = result.stdout.strip()
        # 格式示例: ActiveEnterTimestamp=Fri 2026-04-11 02:30:00 CST
        if "=" not in raw or not raw.split("=", 1)[1].strip():
            print(yellow("  ⚠️ 无法获取服务启动时间（服务可能未运行）"))
            return

        ts_str = raw.split("=", 1)[1].strip()
        # systemd 时间戳去掉星期和时区，只解析日期+时间部分
        parts = ts_str.split()
        if len(parts) >= 3:
            dt_str = f"{parts[1]} {parts[2]}"
        else:
            dt_str = " ".join(parts)

        start_dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        next_restart_dt = start_dt + timedelta(hours=12)
        now = datetime.now()
        remaining = next_restart_dt - now

        print(f"  {'服务启动时间:':<12} {green(str(start_dt))}")
        print(f"  {'预计重启时间:':<12} {yellow(str(next_restart_dt))}")
        if remaining.total_seconds() > 0:
            h, rem = divmod(int(remaining.total_seconds()), 3600)
            m, s = divmod(rem, 60)
            print(f"  {'距离重启还有:':<12} {cyan(f'{h}h {m}m {s}s')}")
        else:
            print(f"  {red('⚠️ 已超过预计重启时间，服务可能正在/已完成重启')}")

    except Exception as e:
        print(red(f"  ❌ 获取重启信息失败: {e}"))


def do_enable():
    print(cyan("⚙️ 设置开机自启..."))
    _run(["sudo", "systemctl", "enable", SERVICE_NAME])
    print(green("✅ 已设置开机自启"))


def do_disable():
    print(yellow("⚙️ 取消开机自启..."))
    _run(["sudo", "systemctl", "disable", SERVICE_NAME])
    print(green("✅ 已取消开机自启"))


def do_env_backup():
    """给 .env 创建一个带时间戳的本地备份。"""
    if not ENV_FILE.exists():
        print(red(f"❌ 找不到 .env: {ENV_FILE}"))
        return

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_file = BASE_DIR / f".env.backup.{timestamp}"
    shutil.copy2(ENV_FILE, backup_file)
    print(green(f"✅ 已备份 .env -> {backup_file.name}"))


# ──────────────────────────── 菜单 ────────────────────────────

MENU_ITEMS = [
    ("启动服务",                    do_start),
    ("停止服务",                    do_stop),
    ("重启服务",                    do_restart),
    ("查看服务状态",                do_status),
    ("⏰ 查看下次 12h 重启时间",    do_next_restart),
    ("实时彩色日志 ✨",             do_log_app_follow),
    ("最近 N 条日志",               do_log_recent),
    ("应用日志 (纯文本文件)",       do_log_app),
    ("实时日志 (systemd 原始)",     do_log_realtime),
    ("备份 .env 配置",              do_env_backup),
    ("设置开机自启",                do_enable),
    ("取消开机自启",                do_disable),
]

# CLI 快捷命令映射
CLI_SHORTCUTS = {
    "start":   do_start,
    "stop":    do_stop,
    "restart": do_restart,
    "status":  do_status,
    "log":     do_log_realtime,
    "logs":    do_log_realtime,
    "env-backup": do_env_backup,
    "backup-env": do_env_backup,
}


def print_banner():
    print()
    print(bold(cyan("╔══════════════════════════════════════════╗")))
    print(bold(cyan("║   🐦 GmgnTwitterClaw 服务控制面板       ║")))
    print(bold(cyan("╚══════════════════════════════════════════╝")))
    print()


def print_menu():
    for i, (label, _) in enumerate(MENU_ITEMS, 1):
        print(f"  {bold(cyan(str(i)))}. {label}")
    print(f"  {bold(red('0'))}. 退出")
    print()


def interactive_loop():
    print_banner()
    while True:
        print_menu()
        try:
            choice = input(bold(f"  请选择 [0-{len(MENU_ITEMS)}]: ")).strip()
        except (KeyboardInterrupt, EOFError):
            print("\n" + dim("再见 👋"))
            break

        if choice == "0" or choice.lower() in ("q", "quit", "exit"):
            print(dim("再见 👋"))
            break

        try:
            idx = int(choice)
            if 1 <= idx <= len(MENU_ITEMS):
                print()
                MENU_ITEMS[idx - 1][1]()
                print()
            else:
                print(red("  ❌ 无效选项，请重新输入\n"))
        except ValueError:
            print(red("  ❌ 请输入数字\n"))


def main():
    # 支持 CLI 快捷命令: python ctl.py start/stop/restart/status/log
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd in CLI_SHORTCUTS:
            CLI_SHORTCUTS[cmd]()
        elif cmd in ("help", "-h", "--help"):
            print(__doc__)
        else:
            print(red(f"❌ 未知命令: {cmd}"))
            print(f"  可用命令: {', '.join(CLI_SHORTCUTS.keys())}")
            sys.exit(1)
    else:
        interactive_loop()


if __name__ == "__main__":
    main()
