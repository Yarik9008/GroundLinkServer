#!/usr/bin/env python3
"""
Минимальный пример Paramiko (SFTP server).

Источник: репозиторий Paramiko — https://github.com/paramiko/paramiko

Запуск (без аргументов):
    python3 MainServer.py

Файлы сохраняются в локальную папку `sftp_root/`.
"""

import logging
import os
import socket
import time
from pathlib import Path

import paramiko


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("sftp-server")

# ===== Статические настройки сервера =====
# Важно: сервер слушает на всех интерфейсах, а подключаться к нему можно по публичному IP ниже.
BIND_HOST = "0.0.0.0"
PUBLIC_HOST = "130.49.146.15"
PORT = 2222
USERNAME = "demo"
PASSWORD = "secret"
HOST_KEY_PATH = Path("sftp_server_rsa.key")
SFTP_ROOT = Path("sftp_root").resolve()
# ========================================

def ensure_host_key(path: Path) -> paramiko.PKey:
    """Читает host key с диска или генерирует новый RSA ключ."""
    if path.exists():
        logger.info("Используем существующий host key: %s", path)
        return paramiko.RSAKey(filename=str(path))

    logger.info("Host key не найден, генерируем новый RSA ключ...")
    key = paramiko.RSAKey.generate(2048)
    key.write_private_key_file(str(path))
    return key


class SimpleSSHServer(paramiko.ServerInterface):
    """Минимальный SSH сервер для SFTP (password-auth)."""

    def __init__(self, allowed_user: str, allowed_password: str):
        super().__init__()
        self.allowed_user = allowed_user
        self.allowed_password = allowed_password

    def check_auth_password(self, username: str, password: str) -> int:
        if username == self.allowed_user and password == self.allowed_password:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str) -> str:
        return "password"

    def check_channel_request(self, kind: str, chanid: int) -> int:
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_channel_subsystem_request(self, channel: paramiko.Channel, name: str) -> bool:
        ok = name == "sftp"
        logger.info("subsystem request: %s -> %s", name, "OK" if ok else "DENY")
        return ok


class SimpleSFTPServer(paramiko.SFTPServerInterface):
    """SFTP сервер, работающий в пределах локальной директории root."""

    def __init__(self, server, *l, **kw):
        super().__init__(server, *l, **kw)
        self.root = Path(kw.get("root", ".")).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _to_local(self, path: str) -> Path:
        local = (self.root / path.lstrip("/")).resolve()
        if not str(local).startswith(str(self.root)):
            raise paramiko.SFTPServerError(paramiko.SFTP_PERMISSION_DENIED)
        return local

    def list_folder(self, path):
        try:
            local = self._to_local(path)
            entries = []
            for child in local.iterdir():
                attr = paramiko.SFTPAttributes.from_stat(child.lstat())
                attr.filename = child.name
                entries.append(attr)
            return entries
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED

    def stat(self, path):
        try:
            return paramiko.SFTPAttributes.from_stat(self._to_local(path).stat())
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED

    def lstat(self, path):
        try:
            return paramiko.SFTPAttributes.from_stat(self._to_local(path).lstat())
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED

    def open(self, path, flags, attr):
        try:
            local = self._to_local(path)
            if flags & os.O_CREAT:
                local.parent.mkdir(parents=True, exist_ok=True)

            mode = self._flags_to_mode(flags)
            f = open(local, mode)

            handle = paramiko.SFTPHandle(flags)
            handle.filename = str(local)
            # Paramiko использует readfile/writefile внутри SFTPHandle для операций read/write.
            handle.readfile = f
            handle.writefile = f
            return handle
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED

    def remove(self, path):
        try:
            self._to_local(path).unlink()
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED
        return paramiko.SFTP_OK

    def rename(self, oldpath, newpath):
        try:
            old = self._to_local(oldpath)
            new = self._to_local(newpath)
            new.parent.mkdir(parents=True, exist_ok=True)
            old.rename(new)
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED
        return paramiko.SFTP_OK

    def mkdir(self, path, attr):
        try:
            self._to_local(path).mkdir(parents=True, exist_ok=True)
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED
        return paramiko.SFTP_OK

    def rmdir(self, path):
        try:
            self._to_local(path).rmdir()
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED
        return paramiko.SFTP_OK

    @staticmethod
    def _flags_to_mode(flags: int) -> str:
        readwrite = bool(flags & os.O_RDWR)
        write_only = bool(flags & os.O_WRONLY)
        append = bool(flags & os.O_APPEND)
        trunc = bool(flags & os.O_TRUNC)

        if readwrite:
            mode = "r+b"
        elif write_only:
            mode = "wb"
        else:
            mode = "rb"

        if trunc:
            mode = "w+b" if readwrite else "wb"
        if append:
            mode = "a+b" if readwrite else "ab"
        return mode


def handle_one_connection(
    client_socket: socket.socket,
    addr,
    host_key: paramiko.PKey,
    server: SimpleSSHServer,
) -> None:
    logger.info("Подключение: %s:%s", *addr)
    transport = paramiko.Transport(client_socket)
    transport.add_server_key(host_key)
    # Paramiko 4.x: SFTPServer ожидает sftp_si=<SFTPServerInterface>
    transport.set_subsystem_handler("sftp", paramiko.SFTPServer, sftp_si=SimpleSFTPServer, root=str(SFTP_ROOT))

    try:
        transport.start_server(server=server)
    except paramiko.SSHException as exc:
        logger.error("Ошибка запуска SSH server: %s", exc)
        return

    # Держим session-канал открытым, пока жив Transport:
    # SFTP подсистема будет запущена на этом канале.
    logger.info("Ожидаем session-канал...")
    channel = transport.accept(20)
    if channel is None:
        logger.warning("Канал не был открыт клиентом (timeout)")
        transport.close()
        return
    logger.info("Session-канал открыт, ждём SFTP запросы...")

    while transport.is_active():
        time.sleep(0.2)
    transport.close()


def run_server(
) -> None:
    SFTP_ROOT.mkdir(parents=True, exist_ok=True)
    host_key = ensure_host_key(HOST_KEY_PATH)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((BIND_HOST, PORT))
    sock.listen(100)

    logger.info("SFTP server слушает %s:%s (подключаться по %s:%s)", BIND_HOST, PORT, PUBLIC_HOST, PORT)
    logger.info("Логин: %s, пароль: %s", USERNAME, PASSWORD)
    logger.info("Root: %s", SFTP_ROOT)

    try:
        while True:
            client, addr = sock.accept()
            try:
                server = SimpleSSHServer(USERNAME, PASSWORD)
                handle_one_connection(client, addr, host_key, server)
            finally:
                try:
                    client.close()
                except Exception:
                    pass
    except KeyboardInterrupt:
        logger.info("Остановка сервера по Ctrl+C")
    finally:
        sock.close()


if __name__ == "__main__":
    run_server()