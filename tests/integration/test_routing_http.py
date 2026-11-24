"""Шаг 3: через роутер один ключ всегда попадает в один и тот же воркер Uvicorn."""

import json
import os
import signal
import socket
import urllib.request

from uvicorn_sticky.balancing import pick_worker

LABELS = ["W0", "W1", "W2"]


def test_same_key_always_same_worker(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    for key in ("aaa", "bbb", "ccc", "room-7"):
        answers = [r.get(f"/ws/{key}") for _ in range(10)]
        assert {a["worker"] for a in answers} == {pick_worker(key, LABELS)}
        # счётчик по ключу лежит в памяти одного процесса и растёт без пропусков
        assert [a["hits_for_key"] for a in answers] == list(range(1, 11))


def test_different_keys_use_all_workers(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    seen = {r.get(f"/ws/key-{i}")["worker"] for i in range(30)}
    assert seen == set(LABELS)


def test_requests_without_key_are_served(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    seen = {r.get("/health")["worker"] for _ in range(9)}
    assert seen == set(LABELS)  # по кругу, всеми воркерами


def test_post_body_arrives_intact(start_runner):
    """Роутер подсматривает только начало запроса: большое тело доходит до приложения целиком."""
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    body = os.urandom(300_000)
    req = urllib.request.Request(f"http://127.0.0.1:{r.port}/ws/aaa", data=body, method="POST")
    with urllib.request.urlopen(req, timeout=5) as resp:
        answer = json.load(resp)
    assert answer["received"] == len(body)
    assert answer["worker"] == pick_worker("aaa", LABELS)


def test_garbage_gets_400(start_runner):
    r = start_runner(workers=2)
    r.wait_for("на связи 2/2")
    with socket.create_connection(("127.0.0.1", r.port), timeout=3) as s:
        s.sendall(b"HELLO\r\n\r\n")
        assert s.recv(100).startswith(b"HTTP/1.1 400")


def test_killed_workers_key_moves_and_stays(start_runner):
    r = start_runner(workers=3, restart=False)
    r.wait_for("на связи 3/3")
    key = next(k for k in (f"k{i}" for i in range(100)) if pick_worker(k, LABELS) == "W1")
    assert r.get(f"/ws/{key}")["worker"] == "W1"

    before_kill = r.mark()  # при старте такая же строка могла уже мелькнуть
    os.kill(r.worker_pids()["W1"], signal.SIGKILL)
    r.wait_for("на связи 2/3: W0, W2", since=before_kill)

    answers = [r.get(f"/ws/{key}") for _ in range(6)]
    assert {a["worker"] for a in answers} == {pick_worker(key, ["W0", "W2"])}
    assert [a["hits_for_key"] for a in answers] == list(range(1, 7))  # состояние началось заново