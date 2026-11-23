"""Раннер поднимает настоящее приложение, и оно отвечает по HTTP."""
import json
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def get(port: int, path: str) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=2) as r:
        return json.load(r)


def start(app: str, port: int) -> subprocess.Popen:
    code = ("from uvicorn_sticky import StickyConfig, run; "
            f"run(StickyConfig(app={app!r}, port={port}, log_level='warning'))")
    return subprocess.Popen([sys.executable, "-c", code], cwd=ROOT,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def wait_until_up(proc: subprocess.Popen, port: int, path: str) -> dict:
    """Ждём первого ответа до 5 секунд. Если процесс умер, показываем, что он напечатал."""
    assert proc.stdout is not None

    for _ in range(50):
        if proc.poll() is not None:
            raise AssertionError(f"процесс завершился с кодом {proc.returncode}:\n{proc.stdout.read()}")
        try:
            return get(port, path)
        except urllib.error.HTTPError as e:      # сервер жив, но отвечает ошибкой
            proc.kill()
            raise AssertionError(f"сервер поднялся, но на {path} ответил {e.code}. "
                                 f"Проверь, что запущено приложение с маршрутом /ws/{{key}}") from None
        except OSError:                          # порт ещё не открыт
            time.sleep(0.1)
    proc.kill()
    raise AssertionError(f"сервер не поднялся за 5 секунд:\n{proc.stdout.read()}")


def test_runner_serves_example_app():
    port = free_port()
    proc = start("examples.fastapi_ex.main:app", port)
    try:
        first = wait_until_up(proc, port, "/ws/aaa")
        second = get(port, "/ws/aaa")
        assert first["worker"] == second["worker"]              # процесс один
        assert (first["hits_in_this_worker"], second["hits_in_this_worker"]) == (1, 2)
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
        out, _ = proc.communicate(timeout=10)
    assert proc.returncode == 0
    assert "[runner] сервер остановлен" in out and "Traceback" not in out
