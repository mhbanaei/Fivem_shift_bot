"""Shift Bot launcher.

Double-click this file (or run ``python start.py``). It:
  1. checks the virtual environment (recreates it if broken / missing pip),
  2. installs requirements.txt,
  3. runs bot.py.
"""

import os
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

BASE = Path(__file__).resolve().parent
VENV = BASE / ".venv"
if os.name == "nt":
    VENV_PY = VENV / "Scripts" / "python.exe"
else:
    VENV_PY = VENV / "bin" / "python"


def venv_py_ok() -> bool:
    """True if the venv exists and its pip actually works."""
    if not VENV_PY.exists():
        return False
    try:
        subprocess.run(
            [str(VENV_PY), "-m", "pip", "--version"],
            capture_output=True,
            check=True,
        )
        return True
    except (subprocess.CalledProcessError, OSError):
        return False


def fail(message: str):
    print(f"[X] {message}")
    print("    If the problem persists, run this command manually and paste the error:")
    print(f"    {sys.executable} -m venv .venv")
    input("Press Enter to close...")
    sys.exit(1)


def ensure_venv():
    print("[1/4] Checking virtual environment...")
    if venv_py_ok():
        print("     Environment is ready.")
        return

    print("[!] Virtual environment is missing or broken (no working pip) - recreating it...")
    if VENV.exists():
        print("[!] Removing old environment...")
        shutil.rmtree(VENV, ignore_errors=True)

    print("[!] Creating virtual environment... (this can take up to a minute)")
    result = subprocess.run([sys.executable, "-m", "venv", str(VENV)])
    if result.returncode != 0:
        fail("Failed to create the virtual environment.")

    if venv_py_ok():
        print("     Environment is ready.")
        return

    print("[!] pip is missing in the new environment - bootstrapping it...")
    result = subprocess.run([str(VENV_PY), "-m", "ensurepip", "--upgrade"])
    if result.returncode != 0:
        print("[!] ensurepip failed - downloading get-pip.py...")
        try:
            get_pip = BASE / "get-pip.py"
            urllib.request.urlretrieve("https://bootstrap.pypa.io/get-pip.py", get_pip)
            result = subprocess.run([str(VENV_PY), str(get_pip)])
            get_pip.unlink(missing_ok=True)
        except Exception as exc:
            fail(f"Could not install pip ({exc}). Check your internet connection.")

    if not venv_py_ok():
        fail("Could not install pip. Check your internet connection and try again.")
    print("     Environment is ready.")


def install_requirements():
    print("[2/4] Installing requirements...")
    req = BASE / "requirements.txt"
    result = subprocess.run(
        [str(VENV_PY), "-m", "pip", "install", "-r", str(req)]
    )
    if result.returncode != 0:
        print("[!] pip install failed - retrying once...")
        result = subprocess.run(
            [str(VENV_PY), "-m", "pip", "install", "-r", str(req)]
        )
    if result.returncode != 0:
        fail("Could not install requirements. Check your internet connection.")


def main():
    print("[*] Starting bot launcher...")
    ensure_venv()
    install_requirements()
    print("[3/4] Starting bot...")
    proc = subprocess.run([str(VENV_PY), str(BASE / "bot.py")])
    print()
    print("[x] Bot stopped. Press Enter to close...")
    input()
    sys.exit(proc.returncode or 0)


if __name__ == "__main__":
    main()
