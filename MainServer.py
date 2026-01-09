#!/usr/bin/env python3
import asyncio
import os
from pathlib import Path

import asyncssh

# ====== Static config (as requested) ======
SERVER_IP = "130.49.146.15"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# =========================================

# Where to store uploads on the server machine:
SFTP_ROOT = Path("./uploads").resolve()

# If your machine does NOT own SERVER_IP, bind will fail.
# Keep SERVER_IP static above; you can change only this bind host for local testing.
BIND_HOST = SERVER_IP  # try "0.0.0.0" for local test if needed


class LorettSFTPServer(asyncssh.SSHServer):
    """Single server class: SSH auth + connection logging.

    SFTP subsystem is provided via sftp_factory (function) below.
    """

    def connection_made(self, conn):
        peer = conn.get_extra_info("peername")
        print(f"[server] Connection from {peer}")

    def connection_lost(self, exc):
        if exc:
            print(f"[server] Connection error: {exc}")

    def password_auth_supported(self):
        return True

    def validate_password(self, username, password):
        return username == USERNAME and password == PASSWORD


def _ensure_host_key(path: Path) -> None:
    if path.exists():
        return
    print("[server] Generating host key...")
    key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
    path.write_text(key.export_private_key().decode())
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


def _sftp_factory(conn):
    # Use built-in SFTP server, restricted to SFTP_ROOT
    return asyncssh.SFTPServer(conn, chroot=str(SFTP_ROOT))


async def start_server():
    SFTP_ROOT.mkdir(parents=True, exist_ok=True)

    host_key_path = Path("ssh_host_rsa_key")
    _ensure_host_key(host_key_path)

    print(f"[server] SFTP ROOT: {SFTP_ROOT}")
    print(f"[server] Static config IP/PORT: {SERVER_IP}:{SERVER_PORT}")
    print(f"[server] Binding on: {BIND_HOST}:{SERVER_PORT}")
    print(f"[server] Credentials: {USERNAME} / {PASSWORD}")

    await asyncssh.create_server(
        LorettSFTPServer,
        BIND_HOST,
        SERVER_PORT,
        server_host_keys=[str(host_key_path)],
        sftp_factory=_sftp_factory,
    )

    print("[server] Server started, listening for connections...")


async def main():
    await start_server()
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[server] Server stopped")
