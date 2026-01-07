#!/usr/bin/env python3
"""
Сервер для приема изображений от нескольких клиентов
"""
import socket
import threading
import struct
import os
from datetime import datetime
from Logger import Logger

# Размер чанка для передачи данных (256 KB) - должен совпадать с размером на клиенте
# Увеличенный размер чанка повышает производительность передачи
CHUNK_SIZE = 262144  # 256 KB


class ImageServer:
    """Класс сервера для приема изображений от клиентов"""
    
    def __init__(self, ip="130.49.146.15", port=8888, images_dir="/root/lorett/GroundLinkMonitorServer/received_images", log_level="info"):
        """
        Инициализация сервера
        
        Args:
            ip: IP адрес сервера
            port: Порт сервера
            images_dir: Директория для сохранения изображений
            log_level: Уровень логирования (debug, info, warning, error, critical)
        """
        self.ip = ip
        self.port = port
        self.images_dir = images_dir
        self.server_socket = None
        self.running = False
        
        # Создаем директорию для логов
        logs_dir = "/root/lorett/GroundLinkMonitorServer/logs"
        os.makedirs(logs_dir, exist_ok=True)
        
        # Инициализация логгера
        logger_config = {
            'log_level': log_level,
            'path_log': '/root/lorett/GroundLinkMonitorServer/logs/image_server_'
        }
        self.logger = Logger(logger_config)
        
        # Создаем директорию для сохранения изображений
        os.makedirs(self.images_dir, exist_ok=True)
    
    def _receive_data(self, client_socket, size):
        """
        Получает указанное количество байт из сокета
        
        Args:
            client_socket: Сокет клиента
            size: Количество байт для получения
            
        Returns:
            bytes: Полученные данные
            
        Raises:
            ConnectionError: Если соединение разорвано
            socket.timeout: Если превышено время ожидания
        """
        data = b''
        while len(data) < size:
            try:
                # Читаем чанками - используем общую константу, синхронизированную с клиентом
                chunk = client_socket.recv(min(CHUNK_SIZE, size - len(data)))
                if not chunk:
                    raise ConnectionError("Соединение разорвано: клиент отключился")
                data += chunk
            except socket.timeout:
                raise ConnectionError(f"Таймаут при получении данных: получено {len(data)}/{size} байт")
            except socket.error as e:
                raise ConnectionError(f"Ошибка сокета при получении данных: {e}")
        return data
    
    def _receive_string(self, client_socket):
        """
        Получает строку из сокета (сначала длина, затем данные)
        
        Args:
            client_socket: Сокет клиента
            
        Returns:
            str: Полученная строка
        """
        # Получаем длину строки (4 байта)
        length_data = self._receive_data(client_socket, 4)
        length = struct.unpack('!I', length_data)[0]
        
        # Получаем саму строку
        string_data = self._receive_data(client_socket, length)
        return string_data.decode('utf-8')
    
    def _handle_client(self, client_socket, client_address):
        """
        Обрабатывает подключение клиента
        
        Args:
            client_socket: Сокет клиента
            client_address: Адрес клиента
        """
        try:
            # Устанавливаем таймауты для предотвращения зависания
            client_socket.settimeout(60.0)  # 60 секунд на операцию
            # Отключаем алгоритм Нейгла для немедленной отправки данных
            client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # Увеличиваем размер приемного буфера для повышения производительности
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1 MB
            # Увеличиваем размер отправного буфера
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1048576)  # 1 MB
            
            self.logger.info(f"Подключен клиент: {client_address}")
            
            # Получаем имя клиента
            client_name = self._receive_string(client_socket)
            self.logger.info(f"Имя клиента: {client_name}")
            
            # Создаем директорию для клиента
            client_dir = os.path.join(self.images_dir, client_name)
            os.makedirs(client_dir, exist_ok=True)
            
            # Получаем размер изображения
            size_data = self._receive_data(client_socket, 4)
            image_size = struct.unpack('!I', size_data)[0]
            
            self.logger.info(f"Клиент {client_name} ({client_address}) отправляет изображение размером {image_size} байт")
            
            # Получаем имя файла
            filename = self._receive_string(client_socket)
            self.logger.debug(f"Имя файла: {filename}")
            
            # Получаем само изображение
            image_data = self._receive_data(client_socket, image_size)
            
            # Сохраняем изображение в папку клиента
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            save_filename = f"{timestamp}_{filename}"
            save_path = os.path.join(client_dir, save_filename)
            
            with open(save_path, 'wb') as f:
                f.write(image_data)
            
            self.logger.info(f"Изображение сохранено: {save_path} ({len(image_data)} байт)")
            
            # Отправляем подтверждение клиенту
            client_socket.sendall(b'OK')
            
        except ConnectionError as e:
            self.logger.error(f"Ошибка соединения с клиентом {client_address}: {e}")
        except Exception as e:
            self.logger.error(f"Ошибка при работе с клиентом {client_address}: {e}")
        finally:
            try:
                client_socket.close()
                self.logger.debug(f"Соединение с клиентом {client_address} закрыто")
            except Exception as e:
                self.logger.debug(f"Ошибка при закрытии сокета {client_address}: {e}")
    
    def start(self):
        """Запускает сервер"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Устанавливаем таймаут на accept для возможности корректной остановки
        self.server_socket.settimeout(1.0)
        
        try:
            # Увеличиваем размер приемного буфера на серверном сокете
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576)  # 1 MB
            # Увеличиваем размер очереди подключений
            self.server_socket.bind((self.ip, self.port))
            self.server_socket.listen(10)  # Увеличено с 5 до 10
            self.running = True
            
            self.logger.info(f"Сервер запущен на {self.ip}:{self.port}")
            self.logger.info("Ожидание подключений...")
            
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    # Создаем новый поток для обработки клиента
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    client_thread.start()
                except socket.timeout:
                    # Таймаут на accept - это нормально, продолжаем цикл
                    continue
                except OSError:
                    # Сокет закрыт
                    break
                    
        except KeyboardInterrupt:
            self.logger.info("Сервер остановлен пользователем")
        except Exception as e:
            self.logger.error(f"Ошибка сервера: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Останавливает сервер"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
            self.logger.info("Сокет сервера закрыт")


if __name__ == "__main__":
    server = ImageServer()
    server.start()
