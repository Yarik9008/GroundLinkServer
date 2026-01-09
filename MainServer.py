#!/usr/bin/env python3
import asyncio
from pathlib import Path

import asyncssh

# ====== Static config (как вы просили) ======
SERVER_IP = "130.49.146.15"   # если этот IP не на сервере — поставьте "0.0.0.0"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# ===========================================

UPLOAD_DIR = Path("./uploads").resolve()
HOST_KEY_PATH = Path("./ssh_host_key")  # создастся автоматически, если нет


class SimpleSSHServer(asyncssh.SSHServer):
    """Самый простой сервер: только парольная авторизация."""

    def password_auth_supported(self) -> bool:
        return True

    def validate_password(self, username: str, password: str) -> bool:
        return username == USERNAME and password == PASSWORD


def sftp_factory(conn):
    """
    Включаем SFTP и "запираем" пользователя в UPLOAD_DIR (chroot).
    Клиент видит этот каталог как корень "/".
    """
    return asyncssh.SFTPServer(conn, chroot=str(UPLOAD_DIR))


async def main():
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Генерация host key (если нет)
    if not HOST_KEY_PATH.exists():
        key = asyncssh.generate_private_key("ssh-rsa", key_size=2048)
        HOST_KEY_PATH.write_text(key.export_private_key().decode("utf-8"), encoding="utf-8")

    server = await asyncssh.create_server(
        SimpleSSHServer,
        SERVER_IP,
        SERVER_PORT,
        server_host_keys=[str(HOST_KEY_PATH)],
        sftp_factory=sftp_factory,

        # ======= SPEED / CPU tuning =======
        # Компрессия полностью OFF
        compression_algs=["none"],

        # Быстрые шифры (порядок = приоритет)
        encryption_algs=[
            "chacha20-poly1305@openssh.com",
            "aes128-gcm@openssh.com",
            "aes256-gcm@openssh.com",
            "aes128-ctr",
        ],
        # ==================================
    )

    print(f"[server] Listening on {SERVER_IP}:{SERVER_PORT}")
    print(f"[server] Upload dir (chroot): {UPLOAD_DIR}")
    print(f"[server] Credentials: {USERNAME} / {PASSWORD}")

    await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (OSError, asyncssh.Error) as e:
        print(f"[server] ERROR: {e}")
        raise
