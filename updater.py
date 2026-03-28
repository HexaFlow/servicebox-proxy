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
import threading
import time
from pathlib import Path

import requests

from version import VERSION

GITHUB_REPO = "HexaFlow/servicebox-proxy"
CHECK_INTERVAL_MINUTES = 30  # 30 min to avoid GitHub rate limits (60 req/hr unauth)


def _is_frozen() -> bool:
    """True when running as a PyInstaller bundle."""
    return getattr(sys, "frozen", False)


def _exe_path() -> Path:
    """Path to the current executable (only meaningful when frozen)."""
    return Path(sys.executable)


def _parse_version(tag: str) -> tuple:
    """'v1.2.3' or '1.2.3' -> (1, 2, 3)"""
    return tuple(int(x) for x in tag.lstrip("v").split("."))


def _log(msg: str):
    """Print with [updater] prefix and flush to ensure visibility."""
    print(f"[updater] {msg}", flush=True)


def check_for_update() -> dict | None:
    """Return release info dict if a newer version exists, else None."""
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 403:
            _log("GitHub API rate limit atteint, prochaine verification dans 30 min")
            return None
        if resp.status_code != 200:
            _log(f"GitHub API erreur HTTP {resp.status_code}")
            return None
        data = resp.json()
        remote_tag = data.get("tag_name", "")
        if not remote_tag:
            _log("Pas de tag_name dans la release")
            return None
        local_ver = _parse_version(VERSION)
        remote_ver = _parse_version(remote_tag)
        if remote_ver <= local_ver:
            _log(f"Version a jour ({VERSION} >= {remote_tag})")
            return None
        _log(f"Nouvelle version disponible: {remote_tag} (actuelle: {VERSION})")
        return data
    except requests.exceptions.ConnectionError as e:
        _log(f"Pas de connexion internet: {e}")
        return None
    except Exception as e:
        _log(f"Erreur lors de la verification: {type(e).__name__}: {e}")
        return None


def _find_exe_asset(release: dict) -> dict | None:
    """Find the best Windows .exe asset in the release.

    Prefers the generic name (e.g. 'servicebox-proxy.exe') over versioned
    names (e.g. 'servicebox-proxy-3.0.0.exe') to match what users have on disk.
    """
    exe_assets = [
        a for a in release.get("assets", [])
        if a.get("name", "").lower().endswith(".exe")
    ]
    if not exe_assets:
        return None
    # Prefer shortest name (generic, no version number)
    exe_assets.sort(key=lambda a: len(a["name"]))
    return exe_assets[0]


def apply_update(release: dict) -> bool:
    """Download new exe and schedule a swap-restart via a .bat script.
    Returns True if update was initiated (caller should exit).
    """
    if not _is_frozen():
        _log(f"Nouvelle version {release['tag_name']} disponible "
             f"(actuelle: {VERSION}). Relancez depuis le .exe pour auto-update.")
        return False

    asset = _find_exe_asset(release)
    if not asset:
        _log("Pas de .exe dans la release, update ignore.")
        return False

    download_url = asset["browser_download_url"]
    current_exe = _exe_path()
    update_exe = current_exe.with_name(current_exe.stem + "_update.exe")
    log_file = current_exe.with_name("_update_log.txt")

    _log(f"Telechargement {release['tag_name']} depuis {asset['name']} ...")
    try:
        resp = requests.get(download_url, timeout=120, stream=True)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(update_exe, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
        _log(f"Telechargement termine: {downloaded} octets")
    except Exception as e:
        _log(f"Echec du telechargement: {e}")
        return False

    # Verify downloaded file is not empty / corrupt
    if not update_exe.exists() or update_exe.stat().st_size < 1_000_000:
        _log(f"Fichier telecharge trop petit ({update_exe.stat().st_size} octets), update annule")
        update_exe.unlink(missing_ok=True)
        return False

    # Write a .bat that:
    #   - logs everything to _update_log.txt for debugging
    #   - waits for the old process to fully exit (retry until file is deletable)
    #   - deletes old exe
    #   - renames update exe
    #   - launches new exe
    #   - deletes itself
    bat_path = current_exe.with_name("_update.bat")
    bat_content = f"""@echo off
echo [updater] Demarrage de la mise a jour... >> "{log_file}"
echo [updater] Date: %date% %time% >> "{log_file}"
echo [updater] Attente de la fermeture du processus... >> "{log_file}"
timeout /t 3 /nobreak >nul

REM Retry deleting the old exe up to 15 times
set retries=0
:retry_delete
del "{current_exe}" 2>nul
if exist "{current_exe}" (
    set /a retries+=1
    echo [updater] Tentative %retries%/15 de suppression... >> "{log_file}"
    if %retries% GEQ 15 (
        echo [updater] ERREUR: impossible de supprimer l'ancien exe apres 15 tentatives >> "{log_file}"
        exit /b 1
    )
    timeout /t 2 /nobreak >nul
    goto retry_delete
)

echo [updater] Ancien exe supprime. >> "{log_file}"
move "{update_exe}" "{current_exe}" >> "{log_file}" 2>&1
if errorlevel 1 (
    echo [updater] ERREUR: impossible de renommer le fichier >> "{log_file}"
    exit /b 1
)

echo [updater] Lancement de la nouvelle version... >> "{log_file}"
start "" "{current_exe}"
timeout /t 2 /nobreak >nul
del "%~f0"
"""
    with open(bat_path, "w") as f:
        f.write(bat_content)

    _log(f"Lancement de la mise a jour -> {release['tag_name']}")
    subprocess.Popen(
        ["cmd", "/c", str(bat_path)],
        creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
    )
    return True


def _background_check_loop():
    """Runs in a daemon thread, checks periodically and auto-applies updates."""
    while True:
        time.sleep(CHECK_INTERVAL_MINUTES * 60)
        _log(f"Verification des mises a jour... (v{VERSION})")
        release = check_for_update()
        if release:
            if apply_update(release):
                _log("Mise a jour lancee, arret du processus...")
                os._exit(0)  # Force exit — the .bat script will restart us


def start_update_checker():
    """Check once now, then periodically in background."""
    _log(f"Version actuelle: {VERSION}")
    release = check_for_update()
    if release:
        if apply_update(release):
            # Update initiated — caller should sys.exit()
            return True
    # Start background periodic check
    _log(f"Verification periodique activee (toutes les {CHECK_INTERVAL_MINUTES} min)")
    t = threading.Thread(target=_background_check_loop, daemon=True)
    t.start()
    return False
