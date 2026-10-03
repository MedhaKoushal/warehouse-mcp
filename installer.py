import os
import sys
import json
import shutil
import socket
import urllib.request
import subprocess
from pathlib import Path

INSTALL_DIR = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))) / "SellerOS" / "WarehouseMCP"
CLAUDE_CONFIG = Path(os.environ.get("APPDATA", str(Path.home() / "AppData" / "Roaming"))) / "Claude" / "claude_desktop_config.json"
ANTIGRAVITY_CONFIG = Path.home() / ".gemini" / "config" / "mcp_config.json"
CURSOR_CONFIG = Path.home() / ".cursor" / "mcp.json"

AWS_CLI_DEFAULT_PATH = r"C:\Program Files\Amazon\AWSCLIV2\aws.exe"
SSM_PLUGIN_DEFAULT_PATH = r"C:\Program Files\Amazon\SessionManagerPlugin\bin\session-manager-plugin.exe"

def log(msg: str):
    print(f"[*] {msg}")

def show_message_box(title: str, message: str, is_error: bool = False):
    """Shows a Windows alert / message box if GUI is available, otherwise console."""
    if os.name == "nt":
        try:
            import ctypes
            style = 0x10 if is_error else 0x40 # MB_ICONERROR or MB_ICONINFORMATION
            ctypes.windll.user32.MessageBoxW(0, message, title, style)
            return
        except Exception:
            pass
    print(f"[{'ERROR' if is_error else 'INFO'}] {title}: {message}")

def find_aws_cli() -> str | None:
    path = shutil.which("aws")
    if path:
        return path
    if os.path.exists(AWS_CLI_DEFAULT_PATH):
        return AWS_CLI_DEFAULT_PATH
    return None

def find_ssm_plugin() -> str | None:
    path = shutil.which("session-manager-plugin")
    if path:
        return path
    if os.path.exists(SSM_PLUGIN_DEFAULT_PATH):
        return SSM_PLUGIN_DEFAULT_PATH
    return None

def prompt_aws_credentials_gui() -> tuple[str, str] | None:
    """Displays a clean simple Tkinter dialog to prompt for AWS credentials."""
    try:
        import tkinter as tk
        from tkinter import ttk

        result = {}

        root = tk.Tk()
        root.title("SellerOS Warehouse - AWS Setup")
        root.geometry("450x260")
        root.resizable(False, False)

        # Center on screen
        root.update_idletasks()
        x = (root.winfo_screenwidth() - 450) // 2
        y = (root.winfo_screenheight() - 260) // 2
        root.geometry(f"+{x}+{y}")

        ttk.Label(
            root, 
            text="Enter AWS Credentials for SellerOS Data Warehouse", 
            font=("Segoe UI", 10, "bold")
        ).pack(pady=12)

        frame = ttk.Frame(root, padding=10)
        frame.pack(fill="x", padx=20)

        ttk.Label(frame, text="AWS Access Key ID:").grid(row=0, column=0, sticky="w", pady=6)
        entry_key = ttk.Entry(frame, width=38)
        entry_key.grid(row=0, column=1, pady=6)

        ttk.Label(frame, text="AWS Secret Access Key:").grid(row=1, column=0, sticky="w", pady=6)
        entry_secret = ttk.Entry(frame, width=38, show="*")
        entry_secret.grid(row=1, column=1, pady=6)

        def on_submit():
            key = entry_key.get().strip()
            secret = entry_secret.get().strip()
            if key and secret:
                result["key"] = key
                result["secret"] = secret
                root.destroy()

        ttk.Button(root, text="Save & Connect", command=on_submit).pack(pady=15)

        root.mainloop()
        if "key" in result and "secret" in result:
            return result["key"], result["secret"]
    except Exception as e:
        log(f"GUI credential prompt failed: {e}")
    return None

def check_or_setup_aws_credentials(profile_name: str = "selleros"):
    """Checks if AWS credentials exist for the profile, prompts GUI if missing."""
    aws_creds_file = Path.home() / ".aws" / "credentials"
    has_creds = False
    if aws_creds_file.exists():
        content = aws_creds_file.read_text(encoding="utf-8", errors="ignore")
        if f"[{profile_name}]" in content or "[default]" in content:
            has_creds = True

    if not has_creds:
        log(f"No AWS credentials found for profile [{profile_name}]. Prompting user...")
        creds = prompt_aws_credentials_gui()
        if creds:
            key, secret = creds
            aws_dir = Path.home() / ".aws"
            aws_dir.mkdir(parents=True, exist_ok=True)
            with open(aws_creds_file, "a", encoding="utf-8") as f:
                f.write(f"\n[{profile_name}]\naws_access_key_id = {key}\naws_secret_access_key = {secret}\n")
            
            config_file = aws_dir / "config"
            with open(config_file, "a", encoding="utf-8") as f:
                f.write(f"\n[profile {profile_name}]\nregion = us-east-1\noutput = json\n")
            log(f"Configured credentials for profile [{profile_name}].")
        else:
            log("No credentials provided. You may need to run 'aws configure --profile selleros' manually.")

def install_files(source_exe: Path, source_config: Path) -> Path:
    """Copies the executable and config.json to %LOCALAPPDATA%\\SellerOS\\WarehouseMCP."""
    INSTALL_DIR.mkdir(parents=True, exist_ok=True)
    target_exe = INSTALL_DIR / "selleros-warehouse.exe"
    target_config = INSTALL_DIR / "config.json"

    if source_exe.exists() and source_exe.resolve() != target_exe.resolve():
        if os.name == "nt" and target_exe.exists():
            my_pid = os.getpid()
            try:
                # Stop existing running instances to release file lock
                subprocess.run(
                    f'powershell -NoProfile -Command "Get-Process -Name selleros-warehouse -ErrorAction SilentlyContinue | Where-Object {{ $_.Id -ne {my_pid} }} | Stop-Process -Force"',
                    shell=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
            except Exception:
                pass

        try:
            shutil.copy2(source_exe, target_exe)
            log(f"Installed executable to: {target_exe}")
        except PermissionError:
            import time
            time.sleep(1)
            try:
                shutil.copy2(source_exe, target_exe)
                log(f"Installed executable to: {target_exe}")
            except Exception as e:
                log(f"Notice: Target executable was in use ({e}). Keeping existing installed version.")
        except Exception as e:
            log(f"Notice: {e}")
    elif target_exe.exists():
        log(f"Executable already at target: {target_exe}")
    elif source_exe.exists():
        shutil.copy2(source_exe, target_exe)
        log(f"Installed executable to: {target_exe}")
    else:
        raise FileNotFoundError(f"Source executable not found at: {source_exe}")

    if source_config.exists() and source_config.resolve() != target_config.resolve():
        try:
            shutil.copy2(source_config, target_config)
            log(f"Installed configuration to: {target_config}")
        except Exception as e:
            log(f"Notice: Could not overwrite config: {e}")

    return target_exe

def update_json_config(config_path: Path, server_name: str, exe_path: Path):
    """Injects or updates the MCP server configuration into a target JSON config file."""
    config_path.parent.mkdir(parents=True, exist_ok=True)
    data = {}
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = {}

    if not isinstance(data, dict):
        data = {}

    mcp_servers = data.setdefault("mcpServers", {})
    mcp_servers[server_name] = {
        "command": str(exe_path).replace("/", "\\"),
        "args": ["--server"]
    }

    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    log(f"Updated MCP configuration: {config_path}")

def find_all_claude_configs() -> list[Path]:
    """Finds all Claude Desktop configuration files across standard and packaged installations."""
    configs = set()
    appdata = Path(os.environ.get("APPDATA", ""))
    localappdata = Path(os.environ.get("LOCALAPPDATA", ""))
    userprofile = Path(os.environ.get("USERPROFILE", str(Path.home())))

    # 1. Standard AppData locations
    candidates = [
        appdata / "Claude" / "claude_desktop_config.json",
        localappdata / "Claude" / "claude_desktop_config.json",
        userprofile / "AppData" / "Roaming" / "Claude" / "claude_desktop_config.json",
        userprofile / "AppData" / "Local" / "Claude" / "claude_desktop_config.json",
    ]
    for c in candidates:
        if c.exists():
            configs.add(c.resolve())

    # 2. Packaged / MSIX / Windows Store installations (LocalCache)
    packages_dir = localappdata / "Packages"
    if packages_dir.exists():
        try:
            for f in packages_dir.glob("*Claude*/LocalCache/Roaming/Claude/claude_desktop_config.json"):
                configs.add(f.resolve())
            for f in packages_dir.glob("*Anthropic*/LocalCache/Roaming/Claude/claude_desktop_config.json"):
                configs.add(f.resolve())
        except Exception:
            pass

    # 3. Always include standard APPDATA location
    standard_config = (appdata / "Claude" / "claude_desktop_config.json").resolve()
    configs.add(standard_config)

    # 4. If any Claude package directory exists without the file yet, prepare it
    if packages_dir.exists():
        try:
            for pkg in packages_dir.glob("*Claude*"):
                target = pkg / "LocalCache" / "Roaming" / "Claude" / "claude_desktop_config.json"
                configs.add(target.resolve())
        except Exception:
            pass

    return sorted(list(configs))

def run_installation(current_exe: Path | None = None):
    """Main installation routine."""
    log("Starting SellerOS Warehouse MCP 1-Click Setup...")

    # 1. Check AWS CLI and SSM Plugin
    aws_path = find_aws_cli()
    if not aws_path:
        log("Notice: AWS CLI v2 not detected.")
    else:
        log(f"AWS CLI found: {aws_path}")

    ssm_path = find_ssm_plugin()
    if not ssm_path:
        log("Notice: AWS Session Manager Plugin not detected.")
    else:
        log(f"SSM Plugin found: {ssm_path}")

    # 2. Check AWS Credentials
    check_or_setup_aws_credentials("selleros")

    # 3. Locate source files
    if current_exe is None:
        if getattr(sys, "frozen", False):
            current_exe = Path(sys.executable)
        else:
            candidates = [
                Path(__file__).parent / "dist" / "selleros-warehouse.exe",
                Path(__file__).parent / "selleros-warehouse.exe",
                INSTALL_DIR / "selleros-warehouse.exe",
            ]
            for c in candidates:
                if c.exists():
                    current_exe = c
                    break
            if current_exe is None:
                current_exe = Path(__file__).parent / "dist" / "selleros-warehouse.exe"

    base_dir = current_exe.parent if current_exe.exists() else Path(__file__).parent
    source_config = base_dir / "config.json"
    if not source_config.exists():
        source_config = Path(__file__).parent / "config.json"

    # 4. Copy to local AppData
    installed_exe = install_files(current_exe, source_config)

    # 5. Configure Claude Desktop across all discovered locations
    claude_configs = find_all_claude_configs()
    configured_claude_paths = []
    for cfg_path in claude_configs:
        try:
            update_json_config(cfg_path, "selleros-warehouse", installed_exe)
            configured_claude_paths.append(str(cfg_path))
        except Exception as e:
            log(f"Notice: Failed to update {cfg_path}: {e}")

    # 6. Configure Antigravity IDE
    update_json_config(ANTIGRAVITY_CONFIG, "selleros-warehouse", installed_exe)

    # 7. Configure Cursor if .cursor directory exists
    if CURSOR_CONFIG.parent.exists():
        update_json_config(CURSOR_CONFIG, "selleros-warehouse", installed_exe)

    # 8. Friendly success notification
    claude_summary = "\n".join(f" - {p}" for p in configured_claude_paths)
    success_msg = (
        "SellerOS Warehouse MCP installed successfully!\n\n"
        f"Installed Executable:\n{installed_exe}\n\n"
        f"Configured Claude Desktop at:\n{claude_summary}\n\n"
        f"Configured Antigravity at:\n{ANTIGRAVITY_CONFIG}\n\n"
        "Please restart Claude Desktop or Antigravity to start querying."
    )
    log(success_msg)
    show_message_box("SellerOS Warehouse MCP - Setup Complete", success_msg)

if __name__ == "__main__":
    run_installation()
