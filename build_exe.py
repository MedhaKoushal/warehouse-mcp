import subprocess
import sys
from pathlib import Path

def build():
    root = Path(__file__).parent.resolve()
    server_script = root / "server.py"
    config_file = root / "config.json"

    print(f"[*] Building selleros-warehouse.exe from {server_script}...")

    cmd = [
        sys.executable,
        "-m", "PyInstaller",
        "--name", "selleros-warehouse",
        "--onefile",
        "--clean",
        "--noconfirm",
        "--add-data", f"{config_file};.",
        "--hidden-import", "installer",
        "--hidden-import", "apify_tools",
        "--hidden-import", "apify_gateway",
        "--hidden-import", "psycopg",
        "--hidden-import", "psycopg_binary",
        "--hidden-import", "psycopg_pool",
        "--hidden-import", "boto3",
        "--hidden-import", "botocore",
        "--hidden-import", "mcp",
        "--hidden-import", "mcp.server.fastmcp",
        "--hidden-import", "pydantic",
        "--hidden-import", "anyio",
        "--hidden-import", "starlette",
        "--hidden-import", "tkinter",
        "--hidden-import", "tkinter.ttk",
        str(server_script)
    ]

    print(f"[*] Running command: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(root))
    if result.returncode != 0:
        print("[-] Build failed!")
        sys.exit(result.returncode)

    dist_exe = root / "dist" / "selleros-warehouse.exe"
    if dist_exe.exists():
        print(f"[+] Build successful! Output executable: {dist_exe}")
        print(f"[+] File size: {dist_exe.stat().st_size / (1024 * 1024):.2f} MB")
    else:
        print("[-] Error: Output executable not found in dist/")
        sys.exit(1)

if __name__ == "__main__":
    build()
