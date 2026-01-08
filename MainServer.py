#!/usr/bin/env python3
"""
AsyncIO сервер для приема изображений от нескольких клиентов.

Протокол (v2, с возобновлением передачи):
  - client_name: uint32(len) + bytes(utf-8)
  - file_size:   uint64 (байты)
  - filename:    uint32(len) + bytes(utf-8)
  - upload_id:   uint32(len) + bytes(utf-8)  (стабильный id для resume)
  - server_offset_response: uint64 (сколько байт уже есть на сервере)
  - image_body:  (file_size - offset) байт
  - final_response: b"OK" или b"ER"
"""

import asyncio
import os
import socket
import struct
from datetime import datetime
from typing import Dict

from Logger import Logger

# Размер чанка для передачи данных (4 MB) - должен совпадать с размером на клиенте
CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB

# Буферы сокета (помогает на высоких скоростях / больших файлах)
SOCKET_BUF = 8 * 1024 * 1024  # 8 MB


def _set_socket_opts(sock: socket.socket) -> None:
    # TCP_NODELAY на всякий случай; при больших чанках эффект небольшой, но не мешает
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF)


async def _read_u32(reader: asyncio.StreamReader) -> int:
    data = await reader.readexactly(4)
    return struct.unpack("!I", data)[0]

async def _read_u64(reader: asyncio.StreamReader) -> int:
    data = await reader.readexactly(8)
    return struct.unpack("!Q", data)[0]


async def _read_string(reader: asyncio.StreamReader) -> str:
    n = await _read_u32(reader)
    data = await reader.readexactly(n)
    return data.decode("utf-8")

def _write_u64(writer: asyncio.StreamWriter, value: int) -> None:
    writer.write(struct.pack("!Q", int(value)))


def _safe_filename(name: str) -> str:
    # не даем клиенту писать по произвольным путям
    base = os.path.basename(name)
    # минимальная санитаризация
    return base.replace("/", "_").replace("\\", "_")


async def _receive_to_file(reader: asyncio.StreamReader, file_obj, size: int) -> None:
    remaining = size
    while remaining > 0:
        n = min(CHUNK_SIZE, remaining)
        try:
            chunk = await reader.readexactly(n)
        except asyncio.IncompleteReadError as e:
            if e.partial:
                file_obj.write(e.partial)
                remaining -= len(e.partial)
            raise ConnectionError("Соединение разорвано: клиент отключился")
        file_obj.write(chunk)
        remaining -= n


class ImageServer:
    def __init__(
        self,
        ip: str = "130.49.146.15",
        port: int = 8888,
        images_dir: str = "/root/lorett/GroundLinkMonitorServer/received_images",
        log_level: str = "info",
    ):
        self.ip = ip
        self.port = port
        self.images_dir = images_dir

        # Создаем директорию для логов
        logs_dir = "/root/lorett/GroundLinkMonitorServer/logs"
        os.makedirs(logs_dir, exist_ok=True)

        logger_config = {
            "log_level": log_level,
            "path_log": "/root/lorett/GroundLinkMonitorServer/logs/image_server_",
        }
        self.logger = Logger(logger_config)

        os.makedirs(self.images_dir, exist_ok=True)
        self._upload_locks: Dict[str, asyncio.Lock] = {}

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        sock = writer.get_extra_info("socket")
        if isinstance(sock, socket.socket):
            try:
                _set_socket_opts(sock)
            except Exception:
                # Опции могут быть недоступны на некоторых платформах/обертках
                pass

        try:
            self.logger.info(f"Подключен клиент: {peer}")

            client_name = await _read_string(reader)
            self.logger.info(f"Имя клиента: {client_name}")

            client_dir = os.path.join(self.images_dir, client_name)
            os.makedirs(client_dir, exist_ok=True)

            file_size = await _read_u64(reader)
            filename = _safe_filename(await _read_string(reader))
            upload_id = await _read_string(reader)

            self.logger.info(
                f"Клиент {client_name} ({peer}) отправляет файл: {filename}, size={file_size}, upload_id={upload_id}"
            )

            # Один и тот же upload_id может переподключаться — сериализуем по upload_id
            lock = self._upload_locks.get(upload_id)
            if lock is None:
                lock = asyncio.Lock()
                self._upload_locks[upload_id] = lock

            async with lock:
                done_path = os.path.join(client_dir, f"{upload_id}.done")
                part_path = os.path.join(client_dir, f"{upload_id}_{filename}.part")

                # Определяем сколько уже получено
                if os.path.exists(done_path):
                    existing = file_size
                else:
                    try:
                        existing = os.path.getsize(part_path)
                    except FileNotFoundError:
                        existing = 0

                # Если на диске больше, чем ожидается — сбрасываем (файл поменялся или upload_id ошибочный)
                if existing > file_size:
                    try:
                        with open(part_path, "wb"):
                            pass
                    except FileNotFoundError:
                        pass
                    existing = 0

                # Сообщаем клиенту оффсет, с которого продолжать
                self.logger.info(f"Resume: upload_id={upload_id} offset={existing}/{file_size}")
                _write_u64(writer, existing)
                await writer.drain()

                remaining = file_size - existing
                if remaining > 0:
                    # Дописываем с конца
                    with open(part_path, "r+b" if os.path.exists(part_path) else "wb", buffering=4 * 1024 * 1024) as f:
                        f.seek(existing)
                        await _receive_to_file(reader, f, remaining)

                # Завершено: если ещё не помечено как done — финализируем.
                # Важно: используем done-маркер, чтобы повторное подключение после обрыва
                # (когда клиент не получил OK) не приводило к повторной загрузке.
                if not os.path.exists(done_path):
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    final_name = f"{timestamp}_{filename}"
                    final_path = os.path.join(client_dir, final_name)
                    try:
                        os.replace(part_path, final_path)
                    except FileNotFoundError:
                        # Если part исчез — значит параллельная очистка; считаем ошибкой
                        raise
                    # Записываем маркер завершения
                    with open(done_path, "w", encoding="utf-8") as m:
                        m.write(final_name)
                else:
                    # Уже завершено ранее — читаем имя финального файла для лога (если есть)
                    final_name = "unknown"
                    try:
                        with open(done_path, "r", encoding="utf-8") as m:
                            final_name = m.read().strip() or "unknown"
                    except Exception:
                        pass
                    final_path = os.path.join(client_dir, final_name)

            self.logger.info(f"Файл сохранён: {final_path} ({file_size} байт)")

            writer.write(b"OK")
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, ConnectionError) as e:
            self.logger.error(f"Ошибка соединения с клиентом {peer}: {e}")
            try:
                writer.write(b"ER")
                await writer.drain()
            except Exception:
                pass
        except Exception as e:
            self.logger.error(f"Ошибка при работе с клиентом {peer}: {e}")
            try:
                writer.write(b"ER")
                await writer.drain()
            except Exception:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.handle_client,
            host=self.ip,
            port=self.port,
            backlog=socket.SOMAXCONN,
            limit=CHUNK_SIZE * 2,
        )

        # Настраиваем listening socket (recvbuf)
        for s in server.sockets or []:
            try:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, SOCKET_BUF)
                s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SOCKET_BUF)
            except Exception:
                pass

        addrs = ", ".join(str(s.getsockname()) for s in (server.sockets or []))
        self.logger.info(f"Сервер запущен на {addrs}")
        self.logger.info("Ожидание подключений...")

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(ImageServer().start())
