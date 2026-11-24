"""Служебный канал роутера: регистрация воркеров и отслеживание их смерти."""

import asyncio
import tempfile

from uvicorn_sticky.router import Router


def with_router(scenario, **router_kwargs):
    """Поднимаем роутер на временном Unix-сокете и выполняем сценарий."""

    async def main():
        path = tempfile.mkdtemp(prefix="us-") + "/c.sock"
        router = Router(control_path=path, **router_kwargs)
        task = asyncio.create_task(router.serve_control())
        await asyncio.wait_for(router.ready.wait(), 2)
        try:
            await scenario(router, path)
        finally:
            task.cancel()

    asyncio.run(main())


_open = []  # держим соединения, иначе сборщик мусора закроет их посреди теста


async def connect(path: str, label: str):
    reader, writer = await asyncio.open_unix_connection(path)
    _open.append(writer)
    writer.write(label.encode())
    await writer.drain()
    await asyncio.sleep(0.05)  # даём роутеру обработать
    return reader, writer


async def is_closed_by_router(reader) -> bool:
    try:
        return await asyncio.wait_for(reader.read(1), 0.5) == b""
    except TimeoutError:
        return False


def test_registers_workers():
    async def scenario(router, path):
        await connect(path, "W0")
        await connect(path, "W1")
        assert router.labels() == ["W0", "W1"]

    with_router(scenario)


def test_duplicate_label_rejected():
    async def scenario(router, path):
        first_reader, _ = await connect(path, "W0")
        second_reader, _ = await connect(path, "W0")
        assert await is_closed_by_router(second_reader)
        assert not await is_closed_by_router(first_reader)  # первый не пострадал
        assert router.labels() == ["W0"]

    with_router(scenario)


def test_bad_label_rejected():
    async def scenario(router, path):
        reader, _ = await connect(path, "bad label!")
        assert await is_closed_by_router(reader)
        assert router.labels() == []

    with_router(scenario)


def test_dead_worker_disappears():
    async def scenario(router, path):
        await connect(path, "W0")
        _, writer = await connect(path, "W1")
        writer.close()
        await asyncio.sleep(0.1)
        assert router.labels() == ["W0"]

    with_router(scenario)


def test_on_change_reports_every_change():
    seen: list[list[str]] = []

    async def scenario(router, path):
        _, writer = await connect(path, "W0")
        await connect(path, "W1")
        writer.close()
        await asyncio.sleep(0.1)

    with_router(scenario, on_change=lambda labels: seen.append(labels))
    assert seen == [["W0"], ["W0", "W1"], ["W1"]]
