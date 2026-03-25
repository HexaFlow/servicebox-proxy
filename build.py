#!/usr/bin/env python3
"""Build the ServiceBox Proxy into a single .exe using PyInstaller."""

import subprocess
import sys
from version import VERSION

def main():
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--name", f"servicebox-proxy-{VERSION}",
        "--add-data", "version.py;.",
        "--hidden-import", "uvicorn.logging",
        "--hidden-import", "uvicorn.loops",
        "--hidden-import", "uvicorn.loops.auto",
        "--hidden-import", "uvicorn.protocols",
        "--hidden-import", "uvicorn.protocols.http",
        "--hidden-import", "uvicorn.protocols.http.auto",
        "--hidden-import", "uvicorn.protocols.websockets",
        "--hidden-import", "uvicorn.protocols.websockets.auto",
        "--hidden-import", "uvicorn.lifespan",
        "--hidden-import", "uvicorn.lifespan.on",
        "--hidden-import", "uvicorn.lifespan.off",
        "--hidden-import", "requests_ntlm",
        "--hidden-import", "ntlm_auth",
        "--console",
        "main.py",
    ]
    print(f"Building v{VERSION}...")
    subprocess.run(cmd, check=True)
    print(f"\nDone! Executable: dist/servicebox-proxy-{VERSION}.exe")


if __name__ == "__main__":
    main()
