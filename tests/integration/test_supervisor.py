"""Шаг 5: раннер следит за воркерами и поднимает упавшие с тем же лейблом."""

import asyncio
import json
import os
import signal

import websockets

from uvicorn_sticky.balancing import pick_worker

LABELS = ["W0", "W1", "W2"]


def key_of(label: str) -> str:
    """Ключ, который при трёх воркерах попадает в указанный."""
    return next(k for k in (f"k{i}" for i in range(200)) if pick_worker(k, LABELS) == label)


def kill_and_wait_for_return(r, label: str, sig=signal.SIGKILL) -> tuple[int, int]:
    """Убить воркер и дождаться, пока все снова будут на связи. Возвращает (старый pid, новый pid)."""
    old_pid = r.worker_pids()[label]
    mark = r.mark()
    os.kill(old_pid, sig)
    r.wait_for("на связи 2/3", since=mark)
    r.wait_for("на связи 3/3: W0, W1, W2", since=mark)
    return old_pid, r.worker_pids()[label]


def test_killed_worker_is_restarted_with_same_label(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    old_pid, new_pid = kill_and_wait_for_return(r, "W1")
    assert new_pid != old_pid
    assert "воркер W1 завершился (сигнал 9), перезапускаю" in r.output


def test_keys_return_to_restarted_worker(start_runner):
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    key = key_of("W1")
    before = r.get(f"/ws/{key}")
    assert before["worker"] == "W1"

    _, new_pid = kill_and_wait_for_return(r, "W1")

    after = [r.get(f"/ws/{key}") for _ in range(5)]
    assert {a["worker"] for a in after} == {"W1"}  # ключ вернулся к своему воркеру
    assert {a["pid"] for a in after} == {new_pid}  # но это уже новый процесс
    assert [a["hits_for_key"] for a in after] == [1, 2, 3, 4, 5]  # с чистой памятью


def test_worker_that_exits_on_its_own_is_restarted(start_runner):
    """Воркер вышел штатно (SIGTERM), хотя раннер его не останавливал: тоже поднимаем."""
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    old_pid, new_pid = kill_and_wait_for_return(r, "W2", sig=signal.SIGTERM)
    assert new_pid != old_pid


def test_restart_does_not_disturb_other_workers(start_runner):
    """Пока W1 падает и поднимается, соединения на W0 продолжают работать."""
    r = start_runner(workers=3)
    r.wait_for("на связи 3/3")
    room = key_of("W0")

    async def scenario():
        url = f"ws://127.0.0.1:{r.port}/ws/{room}"
        red = await websockets.connect(f"{url}?name=red")
        blue = await websockets.connect(f"{url}?name=blue")
        assert json.loads(await red.recv())["worker"] == "W0"
        await blue.recv()

        await asyncio.to_thread(kill_and_wait_for_return, r, "W1")

        await red.send("still here")
        msg = json.loads(await asyncio.wait_for(blue.recv(), 3))
        assert msg["text"] == "still here"
        await red.close()
        await blue.close()

    asyncio.run(scenario())


def test_restart_can_be_turned_off(start_runner):
    r = start_runner(workers=3, restart=False)
    r.wait_for("на связи 3/3")
    mark = r.mark()
    os.kill(r.worker_pids()["W1"], signal.SIGKILL)
    r.wait_for("воркер W1 завершился (сигнал 9), перезапуск отключён", since=mark)
    r.wait_for("на связи 2/3: W0, W2", since=mark)
    assert "перезапускаю" not in r.output


def test_crash_loop_is_stopped(start_runner):
    """Воркер падает снова и снова: после нескольких попыток раннер сдаётся и выходит с ошибкой."""
    r = start_runner(workers=1, app="tests.apps.crashy:app")
    r.wait_for("больше не перезапускаю", timeout=30)
    r.proc.wait(timeout=10)
    assert r.proc.returncode == 1
    assert r.output.count("перезапускаю (") == 5  # ровно пять попыток
    assert "все воркеры завершились" in r.output