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


class MySFTPServer(asyncssh.SFTPServer):
    """Custom SFTP server that restricts access to SFTP_ROOT directory."""
    
    def __init__(self, conn):
        # Set root directory before calling parent __init__
        # This way asyncssh will automatically restrict all operations to this root
        root = str(SFTP_ROOT)
        super().__init__(conn, chroot=root)
    
    def format_user(self, uid):
        return str(uid)
    
    def format_group(self, gid):
        return str(gid)


class MySSHServer(asyncssh.SSHServer):
    """SSH server with password authentication."""
    
    def connection_made(self, conn):
        print(f"[server] Connection from {conn.get_extra_info('peername')}")
    
    def connection_lost(self, exc):
        if exc:
            print(f"[server] Connection error: {exc}")
    
    def password_auth_supported(self):
        return True
    
    def validate_password(self, username, password):
        if username == USERNAME and password == PASSWORD:
            return True
        return False


async def start_server():
    """Start the SFTP server."""
    # Ensure upload directory exists
    SFTP_ROOT.mkdir(parents=True, exist_ok=True)
    
    # Generate or load host keys
    host_key_path = Path("ssh_host_rsa_key")
    if not host_key_path.exists():
        print("[server] Generating host key...")
        key = asyncssh.generate_private_key('ssh-rsa', key_size=2048)
        host_key_path.write_text(key.export_private_key().decode())
    
    print(f"[server] SFTP ROOT: {SFTP_ROOT}")
    print(f"[server] Static config IP/PORT: {SERVER_IP}:{SERVER_PORT}")
    print(f"[server] Binding on: {BIND_HOST}:{SERVER_PORT}")
    print(f"[server] Credentials: {USERNAME} / {PASSWORD}")
    
    # Create server with custom SFTP handler
    await asyncssh.create_server(
        MySSHServer,
        BIND_HOST,
        SERVER_PORT,
        server_host_keys=[str(host_key_path)],
        sftp_factory=MySFTPServer,
    )
    
    print("[server] Server started, listening for connections...")


async def main():
    """Main server loop."""
    await start_server()
    
    # Keep server running
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n[server] Shutting down...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[server] Server stopped")
