#!/usr/bin/env python3
import asyncio
import errno
import os
import signal
import socket
import time
from pathlib import Path

import asyncssh

# ====== Static config (as requested) ======
SERVER_IP = "130.49.146.15"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# =========================================

# Performance tuning (large files / high throughput)
# Avoid frequent SSH rekey during multi-GB transfers.
REKEY_BYTES = 16 * 1024 * 1024 * 1024  # 16 GiB
# Disable compression for max throughput (and CPU savings)
COMPRESSION_ALGS = ["none"]
# Prefer fast ciphers (hardware AES if available, otherwise ChaCha20)
ENCRYPTION_ALGS = [
    "aes128-gcm@openssh.com",
    "aes256-gcm@openssh.com",
    "chacha20-poly1305@openssh.com",
    "aes128-ctr",
    "aes256-ctr",
]

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


def _iter_listen_inodes_for_port(port: int) -> set[str]:
    """Return socket inode strings for LISTEN sockets on TCP port."""

    def parse_proc_net(path: str) -> set[str]:
        inodes: set[str] = set()
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                # skip header
                next(f, None)
                for line in f:
                    parts = line.split()
                    if len(parts) < 10:
                        continue
                    local_addr = parts[1]          # ip:port in hex
                    state = parts[3]               # 0A == LISTEN
                    inode = parts[9]

                    if state != "0A":
                        continue
                    try:
                        _ip_hex, port_hex = local_addr.split(":")
                        if int(port_hex, 16) == port:
                            inodes.add(inode)
                    except Exception:
                        continue
        except FileNotFoundError:
            return set()
        return inodes

    return parse_proc_net("/proc/net/tcp") | parse_proc_net("/proc/net/tcp6")


def _pids_listening_on_port(port: int) -> set[int]:
    """Find PIDs which have LISTEN sockets on given TCP port."""
    inodes = _iter_listen_inodes_for_port(port)
    if not inodes:
        return set()

    target = {f"socket:[{inode}]" for inode in inodes}
    pids: set[int] = set()

    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        fd_dir = f"/proc/{pid}/fd"
        try:
            for fd in os.listdir(fd_dir):
                try:
                    link = os.readlink(os.path.join(fd_dir, fd))
                except OSError:
                    continue
                if link in target:
                    pids.add(pid)
                    break
        except (FileNotFoundError, PermissionError):
            continue

    return pids


def _pid_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read().split(b"\x00")
            parts = [p.decode("utf-8", errors="replace") for p in raw if p]
            return " ".join(parts) if parts else f"<pid {pid}>"
    except Exception:
        return f"<pid {pid}>"


def _force_free_tcp_port(port: int) -> None:
    """If port is occupied, terminate the owning process(es)."""
    pids = _pids_listening_on_port(port)
    if not pids:
        return

    self_pid = os.getpid()
    pids = {p for p in pids if p != self_pid}
    if not pids:
        return

    print(f"[server] Port {port} is busy. Stopping processes: {sorted(pids)}")
    for pid in sorted(pids):
        print(f"[server]  - PID {pid}: {_pid_cmdline(pid)}")

    # First try SIGTERM
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except PermissionError:
            raise PermissionError(f"No permission to stop pid {pid} (need sudo/root?)")

    deadline = time.time() + 2.0
    while time.time() < deadline:
        still = {pid for pid in pids if os.path.exists(f"/proc/{pid}")}
        if not still:
            break
        time.sleep(0.1)

    # Escalate to SIGKILL if needed
    still = {pid for pid in pids if os.path.exists(f"/proc/{pid}")}
    if still:
        print(f"[server] Forcing stop (SIGKILL): {sorted(still)}")
        for pid in still:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError:
                raise PermissionError(f"No permission to SIGKILL pid {pid} (need sudo/root?)")

        # brief wait for cleanup
        time.sleep(0.2)


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

    # Ensure the port is free before binding (kill any listener if needed).
    # This is intentionally aggressive as requested.
    await asyncio.to_thread(_force_free_tcp_port, SERVER_PORT)

    print(f"[server] SFTP ROOT: {SFTP_ROOT}")
    print(f"[server] Static config IP/PORT: {SERVER_IP}:{SERVER_PORT}")
    print(f"[server] Binding on: {BIND_HOST}:{SERVER_PORT}")
    print(f"[server] Credentials: {USERNAME} / {PASSWORD}")

    try:
        return await asyncssh.create_server(
            LorettSFTPServer,
            BIND_HOST,
            SERVER_PORT,
            server_host_keys=[str(host_key_path)],
            sftp_factory=_sftp_factory,
            rekey_bytes=REKEY_BYTES,
            compression_algs=COMPRESSION_ALGS,
            encryption_algs=ENCRYPTION_ALGS,
        )
    except OSError as e:
        # In case something raced us and grabbed the port again
        if getattr(e, "errno", None) == errno.EADDRINUSE:
            print(f"[server] Bind failed: address already in use. Retrying after forced cleanup...")
            await asyncio.to_thread(_force_free_tcp_port, SERVER_PORT)
            return await asyncssh.create_server(
                LorettSFTPServer,
                BIND_HOST,
                SERVER_PORT,
                server_host_keys=[str(host_key_path)],
                sftp_factory=_sftp_factory,
                rekey_bytes=REKEY_BYTES,
                compression_algs=COMPRESSION_ALGS,
                encryption_algs=ENCRYPTION_ALGS,
            )
        raise

    print("[server] Server started, listening for connections...")


async def main():
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # add_signal_handler isn't available on some platforms
            pass

    server = await start_server()
    print("[server] Server started, listening for connections...")

    try:
        await stop_event.wait()
    finally:
        # Graceful shutdown: close listener & active connections
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[server] Server stopped")
