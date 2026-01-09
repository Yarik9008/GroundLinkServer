#!/usr/bin/env python3
import asyncio
import os
import sys
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


class SimpleSFTPServer(asyncssh.SFTPServer):
    """
    Minimal async SFTP server mapping all operations into SFTP_ROOT directory.
    """
    
    def _to_local_path(self, path: str) -> Path:
        """Convert SFTP path like '/a/b.txt' to local path under SFTP_ROOT safely."""
        if not path:
            path = "."
        # normalize and prevent path traversal
        rel = os.path.normpath(path).lstrip("/\\")
        local = (SFTP_ROOT / rel).resolve()
        if not str(local).startswith(str(SFTP_ROOT)):
            raise PermissionError("Path traversal is not allowed")
        return local

    def listdir(self, path):
        """List directory contents."""
        try:
            local = self._to_local_path(path)
            return [item.name for item in local.iterdir()]
        except FileNotFoundError:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {path}")
        except PermissionError:
            raise asyncssh.SFTPPermissionDenied(f"Permission denied: {path}")

    def lstat(self, path):
        """Get file attributes without following symlinks."""
        try:
            local = self._to_local_path(path)
            stat_result = local.lstat()
            return asyncssh.SFTPAttrs.from_local(stat_result)
        except FileNotFoundError:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {path}")
        except PermissionError:
            raise asyncssh.SFTPPermissionDenied(f"Permission denied: {path}")

    def stat(self, path):
        """Get file attributes."""
        try:
            local = self._to_local_path(path)
            stat_result = local.stat()
            return asyncssh.SFTPAttrs.from_local(stat_result)
        except FileNotFoundError:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {path}")
        except PermissionError:
            raise asyncssh.SFTPPermissionDenied(f"Permission denied: {path}")

    def open(self, path, pflags, attrs):
        """Open a file."""
        try:
            local = self._to_local_path(path)
            local.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert SFTP flags to Python mode
            mode = ""
            if pflags & asyncssh.FXF_READ and pflags & asyncssh.FXF_WRITE:
                mode = "r+b"
            elif pflags & asyncssh.FXF_WRITE:
                mode = "wb"
            else:
                mode = "rb"
            
            if pflags & asyncssh.FXF_APPEND:
                mode = "ab"
            elif pflags & asyncssh.FXF_CREAT and pflags & asyncssh.FXF_EXCL:
                mode = "xb"
            
            return open(local, mode)
        except FileNotFoundError:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {path}")
        except FileExistsError:
            raise asyncssh.SFTPFailure(f"File exists: {path}")
        except PermissionError:
            raise asyncssh.SFTPPermissionDenied(f"Permission denied: {path}")

    def remove(self, path):
        """Remove a file."""
        try:
            local = self._to_local_path(path)
            local.unlink()
        except FileNotFoundError:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {path}")
        except PermissionError:
            raise asyncssh.SFTPPermissionDenied(f"Permission denied: {path}")

    def rename(self, oldpath, newpath):
        """Rename a file."""
        try:
            old_local = self._to_local_path(oldpath)
            new_local = self._to_local_path(newpath)
            new_local.parent.mkdir(parents=True, exist_ok=True)
            old_local.rename(new_local)
        except FileNotFoundError:
            raise asyncssh.SFTPNoSuchFile(f"No such file: {oldpath}")
        except PermissionError:
            raise asyncssh.SFTPPermissionDenied(f"Permission denied: {oldpath}")

    def mkdir(self, path, attrs):
        """Create a directory."""
        try:
            local = self._to_local_path(path)
            local.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            raise asyncssh.SFTPPermissionDenied(f"Permission denied: {path}")

    def rmdir(self, path):
        """Remove a directory."""
        try:
            local = self._to_local_path(path)
            local.rmdir()
        except FileNotFoundError:
            raise asyncssh.SFTPNoSuchFile(f"No such directory: {path}")
        except OSError:
            raise asyncssh.SFTPFailure(f"Directory not empty: {path}")
        except PermissionError:
            raise asyncssh.SFTPPermissionDenied(f"Permission denied: {path}")


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
    
    await asyncssh.create_server(
        MySSHServer,
        BIND_HOST,
        SERVER_PORT,
        server_host_keys=[str(host_key_path)],
        sftp_factory=SimpleSFTPServer,
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
