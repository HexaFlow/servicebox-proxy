"""
Auto-updater for ServiceBox Proxy.
Checks GitHub Releases for a newer version on startup, downloads the new .exe,
and restarts itself.

On Windows you can't overwrite a running .exe, so the strategy is:
  1. Download new version as  <name>_update.exe
  2. Write a tiny .bat that waits for us to exit, swaps the files, relaunches
  3. Exit the current process — the .bat takes over
"""

import hashlib
import os
import sys
import subprocess
import threading
import time
from pathlib import Path

import requests

from version import VERSION

GITHUB_REPO = "HexaFlow/servicebox-proxy"
CHECK_INTERVAL_MINUTES = 1


def _is_frozen() -> bool:
    """True when running as a PyInstaller bundle."""
    return getattr(sys, "frozen", False)


def _exe_path() -> Path:
    """Path to the current executable (only meaningful when frozen)."""
    return Path(sys.executable)


def _log_file_path() -> Path:
    """Path to the persistent log file next to the exe."""
    if _is_frozen():
        return _exe_path().with_name("_update_log.txt")
    return Path("_update_log.txt")


def _parse_version(tag: str) -> tuple:
    """'v1.2.3' or '1.2.3' -> (1, 2, 3)"""
    return tuple(int(x) for x in tag.lstrip("v").split("."))


def _log(msg: str):
    """Print with [updater] prefix, flush, and also append to log file."""
    line = f"[updater] {msg}"
    print(line, flush=True)
    try:
        with open(_log_file_path(), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def check_for_update() -> dict | None:
    """Return release info dict if a newer version exists, else None."""
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 403:
            _log("GitHub API rate limit atteint")
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
    """Find the best Windows .exe asset in the release."""
    exe_assets = [
        a for a in release.get("assets", [])
        if a.get("name", "").lower().endswith(".exe")
    ]
    if not exe_assets:
        return None
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
    log_file = _log_file_path()

    expected_size = asset.get("size", 0)
    _log(f"Telechargement {release['tag_name']} depuis {asset['name']} (taille attendue: {expected_size})")
    try:
        resp = requests.get(download_url, timeout=120, stream=True)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "unknown")
        _log(f"Content-Type: {content_type}")
        if "text/html" in content_type:
            _log("ERREUR: recu du HTML au lieu du binaire")
            return False
        downloaded = 0
        with open(update_exe, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
        _log(f"Telechargement termine: {downloaded} octets")
    except Exception as e:
        _log(f"Echec du telechargement: {e}")
        return False

    # Verify downloaded file size matches expected
    actual_size = update_exe.stat().st_size if update_exe.exists() else 0
    _log(f"Taille sur disque: {actual_size} octets (attendu: {expected_size})")
    if actual_size < 1_000_000:
        _log("Fichier trop petit, update annule")
        update_exe.unlink(missing_ok=True)
        return False
    if expected_size and abs(actual_size - expected_size) > 1000:
        _log(f"ERREUR: taille incorrecte (diff: {actual_size - expected_size}), fichier corrompu")
        update_exe.unlink(missing_ok=True)
        return False

    # Compute SHA256
    sha = hashlib.sha256(open(update_exe, "rb").read()).hexdigest()
    _log(f"SHA256 telecharge: {sha}")

    # Verify against .sha256 file from release
    sha_asset = None
    for a in release.get("assets", []):
        if a.get("name", "").endswith(".sha256"):
            sha_asset = a
            break
    if sha_asset:
        try:
            sha_resp = requests.get(sha_asset["browser_download_url"], timeout=10)
            expected_sha = sha_resp.text.strip().upper()
            _log(f"SHA256 attendu:   {expected_sha}")
            if sha.upper() != expected_sha:
                _log("ERREUR: SHA256 ne correspond pas! Fichier corrompu.")
                update_exe.unlink(missing_ok=True)
                return False
            _log("SHA256 OK - fichier intact")
        except Exception as e:
            _log(f"Impossible de verifier SHA256: {e}")

    # Remove Windows "downloaded from internet" marker
    try:
        subprocess.run(
            ["powershell", "-Command", f"Unblock-File -Path '{update_exe}'"],
            timeout=5, capture_output=True,
        )
        _log("Unblock-File execute")
    except Exception:
        pass

    # Write a .bat that swaps the exe and relaunches
    # Key: redirect new exe's stderr to log file to capture crash errors
    bat_path = current_exe.with_name("_update.bat")
    bat_content = f"""@echo off
echo [bat] Demarrage de la mise a jour... >> "{log_file}"
echo [bat] Date: %date% %time% >> "{log_file}"
echo [bat] Attente de la fermeture du processus... >> "{log_file}"
timeout /t 3 /nobreak >nul

REM Retry deleting the old exe up to 15 times
set retries=0
:retry_delete
del "{current_exe}" 2>nul
if exist "{current_exe}" (
    set /a retries+=1
    echo [bat] Tentative %retries%/15 de suppression... >> "{log_file}"
    if %retries% GEQ 15 (
        echo [bat] ERREUR: impossible de supprimer l'ancien exe >> "{log_file}"
        exit /b 1
    )
    timeout /t 2 /nobreak >nul
    goto retry_delete
)

echo [bat] Ancien exe supprime. >> "{log_file}"

REM Unblock the update exe before renaming
powershell -Command "Unblock-File -Path '{update_exe}'" >nul 2>&1

move "{update_exe}" "{current_exe}" >> "{log_file}" 2>&1
if errorlevel 1 (
    echo [bat] ERREUR: impossible de renommer le fichier >> "{log_file}"
    exit /b 1
)

REM Unblock the renamed exe too
powershell -Command "Unblock-File -Path '{current_exe}'" >nul 2>&1

echo [bat] Fichier renomme OK. Lancement... >> "{log_file}"

REM Launch the new exe and capture any crash output to log
"{current_exe}" >> "{log_file}" 2>&1
echo [bat] Le processus s'est termine avec code: %errorlevel% >> "{log_file}"
echo [bat] Si vous voyez ceci, le proxy a crashe. >> "{log_file}"
pause
del "%~f0"
"""
    with open(bat_path, "w") as f:
        f.write(bat_content)

    _log(f"Lancement de la mise a jour -> {release['tag_name']}")
    # Launch bat in a VISIBLE window so user sees output
    subprocess.Popen(["cmd", "/c", str(bat_path)])
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
            return True
    _log(f"Verification periodique activee (toutes les {CHECK_INTERVAL_MINUTES} min)")
    t = threading.Thread(target=_background_check_loop, daemon=True)
    t.start()
    return False
