"""Общие заготовки: запуск раннера в отдельном процессе и чтение его вывода."""

import json
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunnerProc:
    """Раннер, запущенный как отдельный процесс. Вывод копится в self.lines."""

    def __init__(self, app: str, workers: int) -> None:
        self.port = free_port()
        code = (
            "from uvicorn_sticky import StickyConfig, run; "
            f"run(StickyConfig(app={app!r}, workers={workers}, port={self.port}, log_level='warning'))"
        )
        self.proc = subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.lines: list[str] = []
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            self.lines.append(line.rstrip("\n"))

    @property
    def output(self) -> str:
        return "\n".join(self.lines)

    def mark(self) -> int:
        """Запомнить текущее место в выводе, чтобы потом искать только новые строки."""
        return len(self.lines)

    def wait_for(self, text: str, timeout: float = 10.0, since: int = 0) -> str:
        """Ждём строку вывода, содержащую text (начиная с места since). Возвращаем эту строку."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for line in self.lines[since:]:
                if text in line:
                    return line
            if self.proc.poll() is not None:
                break
            time.sleep(0.05)
        raise AssertionError(f"в выводе нет {text!r}. Вывод:\n{self.output}")

    def get(self, path: str) -> dict:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=3) as r:
            return json.load(r)

    def worker_pids(self) -> dict[str, int]:
        """Лейбл -> PID, из строк '[runner] воркер W0 запущен, pid 123'."""
        pids = {}
        for line in self.lines:
            if line.startswith("[runner] воркер ") and "запущен, pid" in line:
                parts = line.split()
                pids[parts[2]] = int(parts[-1])
        return pids

    def stop(self) -> int:
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        time.sleep(0.1)  # даём потоку дочитать вывод
        return self.proc.returncode


@pytest.fixture
def start_runner():
    started: list[RunnerProc] = []

    def _start(workers: int, app: str = "examples.fastapi_ex.main:app") -> RunnerProc:
        r = RunnerProc(app, workers)
        started.append(r)
        return r

    yield _start
    for r in started:
        r.stop()
