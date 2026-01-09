#!/usr/bin/env python3
import os
import socket
import threading
import traceback

import paramiko
from paramiko import AUTH_SUCCESSFUL
from paramiko.sftp_server import SFTPServerInterface, SFTPAttributes, SFTPHandle

# ====== Static config (as requested) ======
SERVER_IP = "130.49.146.15"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# =========================================

# Where to store uploads on the server machine:
SFTP_ROOT = os.path.abspath("./uploads")

# If your machine does NOT own SERVER_IP, bind will fail.
# Keep SERVER_IP static above; you can change only this bind host for local testing.
BIND_HOST = SERVER_IP  # try "0.0.0.0" for local test if needed


def ensure_dirs():
    os.makedirs(SFTP_ROOT, exist_ok=True)


def load_or_create_host_key(path="host_rsa.key", bits=2048):
    if os.path.exists(path):
        return paramiko.RSAKey.from_private_key_file(path)
    key = paramiko.RSAKey.generate(bits)
    key.write_private_key_file(path)
    return key


def _to_local_path(sftp_path: str) -> str:
    """
    Convert SFTP path like '/a/b.txt' to local path under SFTP_ROOT safely.
    """
    if not sftp_path:
        sftp_path = "."
    # normalize and prevent path traversal
    rel = os.path.normpath(sftp_path).lstrip("/\\")
    local = os.path.abspath(os.path.join(SFTP_ROOT, rel))
    if not local.startswith(SFTP_ROOT):
        raise PermissionError("Path traversal is not allowed")
    return local


class SimpleSFTPHandle(SFTPHandle):
    def __init__(self, flags, filename, fobj):
        super().__init__(flags)
        self.filename = filename
        self.readfile = fobj
        self.writefile = fobj


class SimpleSFTPServer(SFTPServerInterface):
    """
    Minimal SFTP server mapping all operations into SFTP_ROOT directory.
    Supports: listdir, stat/lstat, open (read/write), mkdir, rmdir, remove, rename.
    """

    def list_folder(self, path):
        local = _to_local_path(path)
        out = []
        for name in os.listdir(local):
            p = os.path.join(local, name)
            attr = SFTPAttributes.from_stat(os.stat(p))
            attr.filename = name
            out.append(attr)
        return out

    def stat(self, path):
        local = _to_local_path(path)
        return SFTPAttributes.from_stat(os.stat(local))

    def lstat(self, path):
        local = _to_local_path(path)
        return SFTPAttributes.from_stat(os.lstat(local))

    def open(self, path, flags, attr):
        local = _to_local_path(path)
        parent = os.path.dirname(local)
        os.makedirs(parent, exist_ok=True)

        # Convert POSIX open flags to Python mode
        # This is a simple mapping that works for typical uploads (write/create/truncate).
        import errno

        try:
            if flags & os.O_WRONLY:
                mode = "wb"
            elif flags & os.O_RDWR:
                mode = "r+b"
            else:
                mode = "rb"

            # create/truncate handling
            if flags & os.O_TRUNC:
                mode = "wb"
            if flags & os.O_APPEND:
                mode = "ab"

            # If file must exist but doesn't
            if not os.path.exists(local) and not (flags & os.O_CREAT):
                return errno.ENOENT

            fobj = open(local, mode)
            return SimpleSFTPHandle(flags, local, fobj)
        except PermissionError:
            return errno.EACCES
        except FileNotFoundError:
            return errno.ENOENT
        except Exception:
            return errno.EIO

    def remove(self, path):
        import errno
        try:
            os.remove(_to_local_path(path))
            return paramiko.SFTP_OK
        except FileNotFoundError:
            return errno.ENOENT
        except PermissionError:
            return errno.EACCES
        except Exception:
            return errno.EIO

    def rename(self, oldpath, newpath):
        import errno
        try:
            os.rename(_to_local_path(oldpath), _to_local_path(newpath))
            return paramiko.SFTP_OK
        except FileNotFoundError:
            return errno.ENOENT
        except PermissionError:
            return errno.EACCES
        except Exception:
            return errno.EIO

    def mkdir(self, path, attr):
        import errno
        try:
            os.makedirs(_to_local_path(path), exist_ok=True)
            return paramiko.SFTP_OK
        except PermissionError:
            return errno.EACCES
        except Exception:
            return errno.EIO

    def rmdir(self, path):
        import errno
        try:
            os.rmdir(_to_local_path(path))
            return paramiko.SFTP_OK
        except FileNotFoundError:
            return errno.ENOENT
        except OSError:
            return errno.ENOTEMPTY
        except PermissionError:
            return errno.EACCES
        except Exception:
            return errno.EIO


class SimpleServer(paramiko.ServerInterface):
    def __init__(self):
        self.event = threading.Event()

    def check_auth_password(self, username, password):
        if username == USERNAME and password == PASSWORD:
            return AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return "password"

    def check_channel_request(self, kind, chanid):
        # SFTP uses "session" channel
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_channel_subsystem_request(self, channel, name):
        # Client requests subsystem "sftp"
        if name == "sftp":
            return True
        return False


def handle_client(client_sock, host_key):
    transport = paramiko.Transport(client_sock)
    transport.add_server_key(host_key)
    server = SimpleServer()

    try:
        transport.start_server(server=server)

        chan = transport.accept(20)
        if chan is None:
            print("[server] No channel, closing")
            return

        # Start SFTP subsystem
        transport.set_subsystem_handler(
            "sftp",
            paramiko.SFTPServer,
            SimpleSFTPServer,
        )

        # Keep connection alive until transport ends
        while transport.is_active():
            transport.join(1)

    except (paramiko.SSHException, EOFError, ConnectionResetError) as e:
        # SSH handshake failed (port scanners, bots, bad clients) - log minimally
        print(f"[server] SSH handshake failed: {e.__class__.__name__}")
    except Exception:
        # Real errors (bugs in our code) - log full traceback
        print("[server] Unexpected error:")
        traceback.print_exc()
    finally:
        try:
            transport.close()
        except Exception:
            pass
        try:
            client_sock.close()
        except Exception:
            pass


def main():
    ensure_dirs()
    host_key = load_or_create_host_key()

    print(f"[server] SFTP ROOT: {SFTP_ROOT}")
    print(f"[server] Static config IP/PORT: {SERVER_IP}:{SERVER_PORT}")
    print(f"[server] Binding on: {BIND_HOST}:{SERVER_PORT}")
    print(f"[server] Credentials: {USERNAME} / {PASSWORD}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((BIND_HOST, SERVER_PORT))
    sock.listen(100)

    print("[server] Listening...")

    try:
        while True:
            client, addr = sock.accept()
            print(f"[server] Connection from {addr}")
            t = threading.Thread(target=handle_client, args=(client, host_key), daemon=True)
            t.start()
    finally:
        sock.close()


if __name__ == "__main__":
    main()
