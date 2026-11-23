"""Шаг 2: раннер запускает N воркеров, выдаёт лейблы, роутер видит каждого."""
import os
import signal

import pytest


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
        assert answer["pid"] == pids[answer["worker"]]       # лейбл совпадает с процессом


def test_killed_worker_disappears(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    os.kill(r.worker_pids()["W1"], signal.SIGKILL)
    r.wait_for("на связи 2/3: W0, W2")
    for _ in range(20):
        assert r.get("/ws/aaa")["worker"] in ("W0", "W2")    # оставшиеся работают


def test_clean_shutdown_leaves_no_processes(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    pids = r.worker_pids()
    assert r.stop() == 0
    assert "[runner] сервер остановлен" in r.output and "Traceback" not in r.output
    for label, pid in pids.items():
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)                                   # процесса больше нет