# Lorett Ground Link Monitor

Небольшой Python-скрипт для мониторинга станций приёма спутниковых данных.  
Скачивает логи, анализирует пролёты и отправляет ежедневный отчёт на email.

## Возможности
- Загрузка логов с серверов EUS
- Анализ SNR и определение пустых пролётов
- Генерация графиков (по дням и за 7 дней)
- Автоматическая отправка отчётов на email
- Работа по расписанию (00:00 UTC)

## Установка
```bash
pip install -r requirements.txt
```

## Использование

Текущая дата (UTC):
```bash
python3 LorettGroundLinkMonitor.py
```

Конкретная дата:
```bash
python3 LorettGroundLinkMonitor.py 20260107
```

Диапазон дат:
```bash
python3 LorettGroundLinkMonitor.py 20260101 20260107
```

Ежедневный запуск:
```bash
python3 LorettGroundLinkMonitor.py --scheduler
```

Статистика по коммерческим спутникам (список берётся из commercial_satellites в config.json), только станция:
```bash
python3 LorettGroundLinkMonitor.py --stat-commers R2.0S_Moscow
```

Статистика по коммерческим спутникам, станция + начало:
```bash
python3 LorettGroundLinkMonitor.py --stat-commers R2.0S_Moscow 20260101
```

Статистика по коммерческим спутникам, станция + начало + конец:
```bash
python3 LorettGroundLinkMonitor.py --stat-commers R2.0S_Moscow 20260101 20260110
```

Статистика по всем пролётам:
```bash
python3 LorettGroundLinkMonitor.py --stat-all R2.0S_Moscow 20260101 20260110
```

Отключить отправку email (для ручных запусков):
```bash
python3 LorettGroundLinkMonitor.py --off-email 20260110
```

Отправка письма только на debug-адрес из config.json (`email.debug_recipient`):
```bash
python3 LorettGroundLinkMonitor.py --debag-email
```

## Конфигурация
Все настройки находятся в `config.json`:
- станции (`name`, `bend` = `L`/`X`)
- email (SMTP, получатели)
- пороги SNR

## Systemd (сервис мониторинга)
Если скрипт установлен как systemd-сервис `lorett-monitor.service`, используйте:

Старт:
```bash
sudo systemctl start lorett-monitor.service
```

Остановка:
```bash
sudo systemctl stop lorett-monitor.service
```

Статус:
```bash
systemctl status lorett-monitor.service --no-pager
```

Перезапуск:
```bash
sudo systemctl restart lorett-monitor.service
```

## Логи
- `lorett_monitor.log`
- `journalctl` (при запуске через systemd)

## Мониторинг Telegram-канала
Скрипт `TelegramChannelWatcher.py` отслеживает новые сообщения в канале и печатает их в stdout.

Установите зависимости:
```bash
pip install -r requirements.txt
```

Переменные окружения:
- `TG_API_ID` и `TG_API_HASH` — данные приложения Telegram
- `TG_CHANNEL` — username или ID канала
- `TG_SESSION` — имя/путь сессии (опционально)
- `TG_BOT_TOKEN` — токен бота (опционально, если используете бота)
- `TG_OUTPUT` — путь к JSONL файлу (опционально)

Запуск:
```bash
TG_API_ID=12345 TG_API_HASH=xxxx TG_CHANNEL=@my_channel python3 TelegramChannelWatcher.py
```

Запуск с ботом (бот должен быть участником канала):
```bash
TG_API_ID=12345 TG_API_HASH=xxxx TG_CHANNEL=@my_channel TG_BOT_TOKEN=123:abc python3 TelegramChannelWatcher.py
```

## Лицензия
MIT
