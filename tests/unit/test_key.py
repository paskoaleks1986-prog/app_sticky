"""Разбор первой строки запроса: это HTTP? какой в ней ключ привязки?"""

import pytest

from uvicorn_sticky.key import extract_key, is_request_line


@pytest.mark.parametrize(
    "head, key",
    [
        (b"GET /ws/abc HTTP/1.1\r\nHost: x\r\n\r\n", "abc"),
        (b"GET /ws/abc?color=red HTTP/1.1\r\n", "abc"),  # query не мешает
        (b"GET /ws/abc/state HTTP/1.1\r\n", "abc"),  # вложенный путь
        (b"POST /ws/room-42_a HTTP/1.1\r\n", "room-42_a"),  # любой метод, дефис и _
        (b"GET /ws/" + b"k" * 64 + b" HTTP/1.1\r\n", "k" * 64),  # предельная длина
    ],
)
def test_key_found(head, key):
    assert extract_key(head) == key


@pytest.mark.parametrize(
    "head",
    [
        b"GET /docs HTTP/1.1\r\n",  # путь без ключа
        b"GET /ws/ HTTP/1.1\r\n",  # пустой ключ
        b"GET /ws/" + b"k" * 65 + b" HTTP/1.1\r\n",  # слишком длинный
        b"GET /ws/a.b HTTP/1.1\r\n",  # недопустимый символ
        b"GET /api/ws/abc HTTP/1.1\r\n",  # /ws/ не в начале пути
        b"",
    ],
)
def test_no_key(head):
    assert extract_key(head) is None


@pytest.mark.parametrize(
    "head",
    [
        b"GET / HTTP/1.1\r\nHost: x\r\n\r\n",
        b"POST /ws/abc HTTP/1.0\r\n",
        b"DELETE /a/b?c=d HTTP/1.1\r\n",
    ],
)
def test_looks_like_http(head):
    assert is_request_line(head)


@pytest.mark.parametrize(
    "head",
    [
        b"",
        b"HELLO\r\n\r\n",  # не HTTP
        b"GET /ws/abc",  # строка не дошла целиком
        b"get / HTTP/1.1\r\n",  # метод не заглавными
        b"GET / HTTP/2.0\r\n",  # HTTP/2 не поддерживаем
        b"\x16\x03\x01\x02\x00\x01\x00",  # начало TLS: кто-то пришёл по https
    ],
)
def test_not_http(head):
    assert not is_request_line(head)
