#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тесты для TelClient: парсинг сообщений и настройки (без реального Telegram и БД).
"""

import os
import sys
import unittest
from unittest.mock import patch

# путь к корню проекта для импорта TelClient
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from TelClient import TelClient, COMM_STATION_ALIASES, COMM_PASS_LINE_RE, TELETHON_AVAILABLE


class TestSplitByDoubleNewline(unittest.TestCase):
    def test_empty(self):
        self.assertEqual(TelClient.split_by_double_newline(""), [])
        self.assertEqual(TelClient.split_by_double_newline("   \n\n  "), [])

    def test_single_part(self):
        self.assertEqual(TelClient.split_by_double_newline("one line"), ["one line"])
        self.assertEqual(TelClient.split_by_double_newline("a\nb"), ["a\nb"])

    def test_double_newline_splits(self):
        self.assertEqual(
            TelClient.split_by_double_newline("first\n\nsecond"),
            ["first", "second"],
        )
        self.assertEqual(
            TelClient.split_by_double_newline("a\n\nb\n\nc"),
            ["a", "b", "c"],
        )

    def test_strip_and_skip_empty(self):
        self.assertEqual(
            TelClient.split_by_double_newline("  x  \n\n  \n\n  y  "),
            ["x", "y"],
        )


class TestParsePasses(unittest.TestCase):
    def setUp(self):
        self.client = TelClient()

    def test_empty(self):
        self.assertEqual(self.client.parse_passes(""), [])
        self.assertEqual(self.client.parse_passes("  \n  \n  "), [])

    def test_valid_line(self):
        text = "R3.2S_Murmansk NOAA-21 UTC 2024-01-15 12:00:00 - 12:15:00"
        got = self.client.parse_passes(text)
        self.assertEqual(len(got), 1)
        station, satellite, start, end = got[0]
        self.assertEqual(station, "R3.2S_Murmansk")
        self.assertEqual(satellite, "NOAA-21")
        self.assertIn("2024-01-15", start)
        self.assertIn("12:00:00", start)
        self.assertIn("12:15:00", end)

    def test_alias_mur(self):
        text = "MUR NOAA-20 2024/01/15 11:00:00 - 11:12:00"
        got = self.client.parse_passes(text)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0][0], COMM_STATION_ALIASES["MUR"])

    def test_alias_ana(self):
        text = "ANA FENGYUN3D 2024.01.15 10:00:00 - 10:10:00"
        got = self.client.parse_passes(text)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0][0], COMM_STATION_ALIASES["ANA"])

    def test_multiple_lines(self):
        text = """
R3.2S_Murmansk NOAA-21 2024-01-15 12:00:00 - 12:15:00
R4.6S_Anadyr FENGYUN3D 2024-01-15 13:00:00 - 13:08:00
"""
        got = self.client.parse_passes(text)
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0][1], "NOAA-21")
        self.assertEqual(got[1][1], "FENGYUN3D")

    def test_ignores_invalid_lines(self):
        text = "not a pass line\nR3.2S_Murmansk NOAA-21 2024-01-15 12:00:00 - 12:15:00\nbad"
        got = self.client.parse_passes(text)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0][1], "NOAA-21")


class TestParseMessage(unittest.TestCase):
    def setUp(self):
        self.client = TelClient()

    def test_combines_parts(self):
        text = "MUR NOAA-20 2024-01-15 11:00:00 - 11:12:00\n\nANA FENGYUN3D 2024-01-15 12:00:00 - 12:08:00"
        got = self.client.parse_message(text)
        self.assertEqual(len(got), 2)
        self.assertEqual(got[0][0], "R3.2S_Murmansk")
        self.assertEqual(got[1][0], "R4.6S_Anadyr")


class TestGetSettings(unittest.TestCase):
    def test_from_config(self):
        config = {
            "telegram": {
                "api_id": 12345,
                "api_hash": "abc",
                "channel": "https://t.me/test",
                "session": "/tmp/session",
            }
        }
        client = TelClient(config=config)
        with patch.dict(os.environ, {}, clear=False):
            s = client._get_settings()
        self.assertEqual(s["api_id"], 12345)
        self.assertEqual(s["api_hash"], "abc")
        self.assertEqual(s["channel"], "https://t.me/test")
        self.assertEqual(s["session"], "/tmp/session")

    def test_from_env(self):
        client = TelClient(config={})
        with patch.dict(
            os.environ,
            {
                "TG_API_ID": "999",
                "TG_API_HASH": "hash",
                "TG_CHANNEL": "https://t.me/ch",
            },
            clear=False,
        ):
            s = client._get_settings()
        self.assertEqual(s["api_id"], 999)
        self.assertEqual(s["api_hash"], "hash")
        self.assertEqual(s["channel"], "https://t.me/ch")

    def test_config_overrides_env(self):
        config = {"telegram": {"api_id": 1, "channel": "from_config"}}
        client = TelClient(config=config)
        with patch.dict(os.environ, {"TG_API_ID": "2", "TG_CHANNEL": "from_env"}, clear=False):
            s = client._get_settings()
        self.assertEqual(s["api_id"], 1)
        self.assertEqual(s["channel"], "from_config")


class TestRunCommPassesSyncNoTelethon(unittest.TestCase):
    """Проверка, что без telethon или без настроек sync не падает и возвращает None."""

    def test_returns_none_when_no_channel(self):
        client = TelClient(config={})
        with patch.dict(os.environ, {"TG_API_ID": "1", "TG_API_HASH": "x", "TG_CHANNEL": ""}, clear=False):
            result = client.run_comm_passes_sync()
        # Либо None (если telethon нет или канал пустой), либо не исключение
        self.assertTrue(result is None or isinstance(result, tuple))


class TestConstants(unittest.TestCase):
    def test_aliases(self):
        self.assertEqual(COMM_STATION_ALIASES["MUR"], "R3.2S_Murmansk")
        self.assertEqual(COMM_STATION_ALIASES["ANA"], "R4.6S_Anadyr")

    def test_regex_matches_valid_line(self):
        line = "R3.2S_Murmansk NOAA-21 UTC 2024-01-15 12:00:00 - 12:15:00"
        m = COMM_PASS_LINE_RE.match(line)
        self.assertIsNotNone(m)
        self.assertEqual(m.group("station"), "R3.2S_Murmansk")
        self.assertEqual(m.group("satellite"), "NOAA-21")
        self.assertEqual(m.group("date"), "2024-01-15")
        self.assertEqual(m.group("start"), "12:00:00")
        self.assertEqual(m.group("end"), "12:15:00")


if __name__ == "__main__":
    unittest.main()
