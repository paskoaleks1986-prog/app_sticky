"""Шаг 4: WebSocket через роутер. Клиенты с одним ключом встречаются в одном воркере."""

import asyncio
import json
import os
import signal
import time

import pytest
import websockets

from uvicorn_sticky.balancing import pick_worker

APP = "examples.fastapi_ex.main:app"
LABELS = ["W0", "W1", "W2"]


async def join(port: int, room: str, name: str):
    """Подключиться к комнате и дождаться приветствия. Возвращает (соединение, приветствие)."""
    ws = await websockets.connect(f"ws://127.0.0.1:{port}/ws/{room}?name={name}")
    hello = json.loads(await asyncio.wait_for(ws.recv(), 3))
    assert hello["type"] == "hello"
    return ws, hello


async def recv(ws) -> dict:
    return json.loads(await asyncio.wait_for(ws.recv(), 3))


def test_two_clients_meet_in_one_worker(start_runner):
    r = start_runner(workers=3, app=APP)
    r.wait_for("на связи 3/3")

    async def scenario():
        red, red_hello = await join(r.port, "abc", "red")
        blue, blue_hello = await join(r.port, "abc", "blue")
        assert red_hello["worker"] == blue_hello["worker"] == pick_worker("abc", LABELS)
        await red.send("ha-ha")
        msg = await recv(blue)
        assert (msg["type"], msg["from"], msg["text"]) == ("msg", "red", "ha-ha")
        await red.close()
        await blue.close()

    asyncio.run(scenario())


def test_messages_flow_both_ways_in_order(start_runner):
    r = start_runner(workers=3, app=APP)
    r.wait_for("на связи 3/3")

    async def scenario():
        red, _ = await join(r.port, "chat", "red")
        blue, _ = await join(r.port, "chat", "blue")
        for i in range(20):
            await red.send(f"r{i}")
            await blue.send(f"b{i}")
        assert [(await recv(blue))["text"] for _ in range(20)] == [f"r{i}" for i in range(20)]
        assert [(await recv(red))["text"] for _ in range(20)] == [f"b{i}" for i in range(20)]
        await red.close()
        await blue.close()

    asyncio.run(scenario())


def test_every_room_delivers_and_all_workers_are_used(start_runner):
    """Много комнат одновременно: в каждой сообщение доходит, комнаты разложены по всем воркерам."""
    r = start_runner(workers=3, app=APP)
    r.wait_for("на связи 3/3")

    async def one_room(i: int) -> str:
        room = f"room-{i}"
        red, hello = await join(r.port, room, "red")
        blue, _ = await join(r.port, room, "blue")
        await red.send("ping")
        assert (await recv(blue))["text"] == "ping"
        await red.close()
        await blue.close()
        assert hello["worker"] == pick_worker(room, LABELS)
        return hello["worker"]

    async def scenario():
        return await asyncio.gather(*(one_room(i) for i in range(40)))

    assert set(asyncio.run(scenario())) == set(LABELS)


def test_after_worker_death_clients_reconnect_together(start_runner):
    r = start_runner(workers=3, app=APP, restart=False)
    r.wait_for("на связи 3/3")
    room = next(k for k in (f"k{i}" for i in range(100)) if pick_worker(k, LABELS) == "W1")

    async def scenario():
        red, hello = await join(r.port, room, "red")
        blue, _ = await join(r.port, room, "blue")
        assert hello["worker"] == "W1"

        before_kill = r.mark()
        os.kill(r.worker_pids()["W1"], signal.SIGKILL)
        for ws in (red, blue):  # оба соединения рвутся
            with pytest.raises(websockets.ConnectionClosed):
                await asyncio.wait_for(ws.recv(), 3)
        await asyncio.to_thread(r.wait_for, "на связи 2/3: W0, W2", 10.0, before_kill)

        red, red_hello = await join(r.port, room, "red")  # переподключаются
        blue, blue_hello = await join(r.port, room, "blue")
        assert red_hello["worker"] == blue_hello["worker"] == pick_worker(room, ["W0", "W2"])
        await red.send("still here")
        assert (await recv(blue))["text"] == "still here"
        await red.close()
        await blue.close()

    asyncio.run(scenario())


def test_shutdown_with_open_websockets_is_clean(start_runner):
    """Остановка при открытых соединениях: клиенты получают закрытие, раннер выходит быстро и без убийств."""
    r = start_runner(workers=2, app=APP)
    r.wait_for("на связи 2/2")

    async def scenario():
        clients = [(await join(r.port, f"room-{i}", "red"))[0] for i in range(6)]
        started = time.monotonic()
        code = await asyncio.to_thread(r.stop)
        took = time.monotonic() - started
        for ws in clients:
            with pytest.raises(websockets.ConnectionClosed):
                await asyncio.wait_for(ws.recv(), 3)
        return code, took

    code, took = asyncio.run(scenario())
    assert code == 0
    assert took < 5, f"остановка заняла {took:.1f} с"
    assert "убиваю" not in r.output and "Traceback" not in r.output