"""Шаг 2: раннер запускает N воркеров, выдаёт лейблы, роутер видит каждого."""

import os
import signal
import time
from pathlib import Path


def is_gone(pid: int) -> bool:
    """Процесса нет, либо он завершился и ждёт, пока его уберёт система (зомби)."""
    try:
        state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
    except (FileNotFoundError, ProcessLookupError):
        return True
    return state == "Z"


def test_all_workers_register(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3: W0, W1, W2")
    assert sorted(r.worker_pids()) == ["W0", "W1", "W2"]


def test_each_instance_knows_its_label(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    pids = r.worker_pids()
    for _ in range(40):
        answer = r.get("/ws/aaa")
        assert answer["total"] == 3
        assert answer["worker"] == f"W{answer['index']}"
        assert answer["pid"] == pids[answer["worker"]]  # лейбл совпадает с процессом


def test_killed_worker_disappears(start_runner):
    r = start_runner(workers=3, restart=False)
    r.wait_for("на связи 3/3")
    before_kill = r.mark()  # при старте такая же строка могла уже мелькнуть
    os.kill(r.worker_pids()["W1"], signal.SIGKILL)
    r.wait_for("на связи 2/3: W0, W2", since=before_kill)
    for _ in range(20):
        assert r.get("/ws/aaa")["worker"] in ("W0", "W2")  # оставшиеся работают


def test_clean_shutdown_leaves_no_processes(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    pids = r.worker_pids()
    assert r.stop() == 0
    assert "[runner] сервер остановлен" in r.output and "Traceback" not in r.output
    assert all(is_gone(pid) for pid in pids.values())  # процессов больше нет


def test_workers_exit_when_runner_is_killed(start_runner):
    """Раннер убит жёстко и не успел остановить воркеры: они замечают это сами и выходят."""
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    pids = r.worker_pids()
    r.proc.kill()
    r.proc.wait()
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline and not all(is_gone(pid) for pid in pids.values()):
        time.sleep(0.1)
    assert all(is_gone(pid) for pid in pids.values())