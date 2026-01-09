#!/usr/bin/env python3
"""
Простой SFTP‑сервер на Paramiko.

Особенности:
- Авторизация по логину/паролю (статически заданы в коде).
- Автогенерация host key, если он отсутствует.
- Хранилище файлов в локальной папке `sftp_root` (путь можно задать аргументом).

Пример запуска:
    python MainServer.py

Для подключения клиента можно использовать `MainClient.py` или любой SFTP‑клиент:
    sftp -P 2222 demo@130.49.146.15
"""

import argparse
import logging
import os
import socket
import threading
import time
from pathlib import Path

import paramiko


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("sftp-server")

# Статически заданные сетевые параметры и учетные данные
HOST = "130.49.146.15"
PORT = 2222
USERNAME = "demo"
PASSWORD = "secret"


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
    """Минимальный SSH сервер для SFTP."""

    def __init__(self, allowed_user: str, allowed_password: str):
        super().__init__()
        self.allowed_user = allowed_user
        self.allowed_password = allowed_password

    def check_auth_password(self, username, password):
        if username == self.allowed_user and password == self.allowed_password:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return "password"

    def check_channel_request(self, kind, chanid):
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_channel_subsystem_request(self, channel, name):
        return name == "sftp"


class LocalSFTPHandle(paramiko.SFTPHandle):
    """Обертка для работы с локальными файлами."""

    def __init__(self, fileobj):
        super().__init__()
        self._file = fileobj

    def read(self, offset, length):
        self._file.seek(offset)
        return self._file.read(length)

    def write(self, offset, data):
        self._file.seek(offset)
        self._file.write(data)
        self._file.flush()
        return len(data)

    def close(self):
        try:
            self._file.close()
        finally:
            return super().close()

    def stat(self):
        return paramiko.SFTPAttributes.from_stat(os.fstat(self._file.fileno()))


class SimpleSFTPServer(paramiko.SFTPServerInterface):
    """Простой SFTP сервер, работающий с локальной директорией root."""

    def __init__(self, server, *l, **kw):
        super().__init__(server, *l, **kw)
        self.root = Path(kw.get("root", ".")).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _to_local(self, path: str) -> Path:
        # Убираем ведущий слэш и нормализуем путь внутри root
        local = (self.root / path.lstrip("/")).resolve()
        if not str(local).startswith(str(self.root)):
            raise paramiko.SFTPServerError(paramiko.SFTP_PERMISSION_DENIED)
        return local

    def list_folder(self, path):
        try:
            local = self._to_local(path)
            entries = []
            for name in os.listdir(local):
                full_path = local / name
                attr = paramiko.SFTPAttributes.from_stat(full_path.lstat())
                attr.filename = name
                entries.append(attr)
            return entries
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED

    def stat(self, path):
        try:
            local = self._to_local(path)
            return paramiko.SFTPAttributes.from_stat(local.stat())
        except FileNotFoundError:
            return paramiko.SFTP_NO_SUCH_FILE
        except PermissionError:
            return paramiko.SFTP_PERMISSION_DENIED

    def lstat(self, path):
        try:
            local = self._to_local(path)
            return paramiko.SFTPAttributes.from_stat(local.lstat())
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
            fileobj = open(local, mode)
            return LocalSFTPHandle(fileobj)
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
        """Преобразует POSIX‑флаги в режим Python open()."""
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


def handle_client(client_socket, addr, host_key, root, username, password):
    logger.info("Новое подключение: %s:%s", *addr)
    transport = paramiko.Transport(client_socket)
    transport.add_server_key(host_key)
    transport.set_subsystem_handler("sftp", paramiko.SFTPServer, SimpleSFTPServer, root=root)

    server = SimpleSSHServer(username, password)
    try:
        transport.start_server(server=server)
    except paramiko.SSHException as exc:
        logger.error("Не удалось запустить SSH сервер: %s", exc)
        return

    # Ждем первого канала (не обязательно использовать)
    channel = transport.accept(20)
    if channel is None:
        logger.warning("Не удалось получить канал от %s:%s", *addr)
        transport.close()
        return

    try:
        while transport.is_active():
            if channel.recv_ready():
                channel.recv(1024)
            else:
                time.sleep(0.5)
    finally:
        transport.close()


def run_server(host: str, port: int, root: Path, username: str, password: str, host_key_path: Path):
    host_key = ensure_host_key(host_key_path)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(100)

    logger.info("SFTP сервер слушает %s:%s, root=%s", host, port, root)
    logger.info("Логин: %s, пароль: %s", username, password)

    try:
        while True:
            client, addr = sock.accept()
            thread = threading.Thread(
                target=handle_client,
                args=(client, addr, host_key, root, username, password),
                daemon=True,
            )
            thread.start()
    except KeyboardInterrupt:
        logger.info("Остановка сервера по Ctrl+C")
    finally:
        sock.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Простой SFTP сервер на Paramiko")
    parser.add_argument("--root", default="sftp_root", help="Каталог, в котором храним файлы")
    parser.add_argument("--host-key", default="sftp_server_rsa.key", help="Путь к host key (RSA)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_server(
        host=HOST,
        port=PORT,
        root=Path(args.root),
        username=USERNAME,
        password=PASSWORD,
        host_key_path=Path(args.host_key),
    )