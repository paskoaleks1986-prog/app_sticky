"""Разбор первой строки HTTP-запроса: проверка формата и извлечение ключа привязки."""

import re

# "GET /путь HTTP/1.1\r\n": метод заглавными буквами, путь без пробелов, версия 1.0 или 1.1
REQUEST_LINE_RE = re.compile(rb"^[A-Z]{1,16} \S{1,2000} HTTP/1\.[01]\r\n")

# Ключ: сегмент пути сразу после /ws/. Буквы, цифры, "_" и "-", не длиннее 64 символов.
KEY_RE = re.compile(rb"^[A-Z]+ /ws/([0-9A-Za-z_-]{1,64})[ ?/]")


def is_request_line(head: bytes) -> bool:
    """Похоже ли начало соединения на HTTP/1.x-запрос с полной первой строкой."""
    return REQUEST_LINE_RE.match(head) is not None


def extract_key(head: bytes) -> str | None:
    """Ключ привязки из первой строки запроса или None, если его там нет."""
    m = KEY_RE.match(head)
    return m.group(1).decode("ascii") if m else None
