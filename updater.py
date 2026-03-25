"""
Auto-updater for ServiceBox Proxy.
Checks GitHub Releases for a newer version on startup, downloads the new .exe,
and restarts itself.

On Windows you can't overwrite a running .exe, so the strategy is:
  1. Download new version as  <name>_update.exe
  2. Write a tiny .bat that waits for us to exit, swaps the files, relaunches
  3. Exit the current process — the .bat takes over
"""

import os
import sys
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import requests

from version import VERSION

GITHUB_REPO = "HexaFlow/servicebox-proxy"
CHECK_INTERVAL_MINUTES = 5


def _is_frozen() -> bool:
    """True when running as a PyInstaller bundle."""
    return getattr(sys, "frozen", False)


def _exe_path() -> Path:
    """Path to the current executable (only meaningful when frozen)."""
    return Path(sys.executable)


def _parse_version(tag: str) -> tuple:
    """'v1.2.3' or '1.2.3' -> (1, 2, 3)"""
    return tuple(int(x) for x in tag.lstrip("v").split("."))


def check_for_update() -> dict | None:
    """Return release info dict if a newer version exists, else None."""
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        remote_tag = data.get("tag_name", "")
        if not remote_tag:
            return None
        if _parse_version(remote_tag) <= _parse_version(VERSION):
            return None
        return data
    except Exception:
        return None


def _find_exe_asset(release: dict) -> dict | None:
    """Find the Windows .exe asset in the release."""
    for asset in release.get("assets", []):
        name = asset.get("name", "").lower()
        if name.endswith(".exe"):
            return asset
    return None


def apply_update(release: dict) -> bool:
    """Download new exe and schedule a swap-restart via a .bat script.
    Returns True if update was initiated (caller should exit).
    """
    if not _is_frozen():
        print(f"[updater] Nouvelle version {release['tag_name']} disponible "
              f"(actuelle: {VERSION}). Relancez depuis le .exe pour auto-update.")
        return False

    asset = _find_exe_asset(release)
    if not asset:
        print("[updater] Pas de .exe dans la release, update ignore.")
        return False

    download_url = asset["browser_download_url"]
    current_exe = _exe_path()
    update_exe = current_exe.with_name(current_exe.stem + "_update.exe")

    print(f"[updater] Telechargement {release['tag_name']} ...")
    try:
        resp = requests.get(download_url, timeout=120, stream=True)
        resp.raise_for_status()
        with open(update_exe, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        print(f"[updater] Echec du telechargement: {e}")
        return False

    # Write a .bat that:
    #   - waits for the old process to fully exit (retry until file is deletable)
    #   - deletes old exe
    #   - renames update exe
    #   - launches new exe in a persistent cmd window (stays open on crash)
    #   - deletes itself
    bat_path = current_exe.with_name("_update.bat")
    bat_content = f"""@echo off
echo [updater] Attente de la fermeture du processus...
timeout /t 3 /nobreak >nul

REM Retry deleting the old exe up to 10 times (in case process hasn't fully exited)
set retries=0
:retry_delete
del "{current_exe}" 2>nul
if exist "{current_exe}" (
    set /a retries+=1
    if %retries% GEQ 10 (
        echo [updater] ERREUR: impossible de supprimer l'ancien exe apres 10 tentatives
        pause
        exit /b 1
    )
    timeout /t 1 /nobreak >nul
    goto retry_delete
)

move "{update_exe}" "{current_exe}"
if errorlevel 1 (
    echo [updater] ERREUR: impossible de renommer le fichier
    pause
    exit /b 1
)

echo [updater] Lancement de la nouvelle version...
start "ServiceBox Proxy" cmd /k ""{current_exe}""
timeout /t 2 /nobreak >nul
del "%~f0"
"""
    with open(bat_path, "w") as f:
        f.write(bat_content)

    print(f"[updater] Lancement de la mise a jour → {release['tag_name']}")
    subprocess.Popen(
        ["cmd", "/c", str(bat_path)],
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    return True


def _background_check_loop():
    """Runs in a daemon thread, checks periodically and auto-applies updates."""
    while True:
        time.sleep(CHECK_INTERVAL_MINUTES * 60)
        release = check_for_update()
        if release:
            print(f"[updater] Mise a jour detectee: {release['tag_name']}. Application automatique...")
            if apply_update(release):
                print("[updater] Mise a jour lancee, arret du processus...")
                os._exit(0)  # Force exit — the .bat script will restart us


def start_update_checker():
    """Check once now, then periodically in background."""
    release = check_for_update()
    if release:
        if apply_update(release):
            # Update initiated — caller should sys.exit()
            return True
    # Start background periodic check
    t = threading.Thread(target=_background_check_loop, daemon=True)
    t.start()
    return False
