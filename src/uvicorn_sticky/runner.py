"""
Раннер: главный процесс. Открывает порт, запускает роутер и воркеры, следит за ними, останавливает.
"""

import asyncio
import multiprocessing
import os
import shutil
import signal
import socket
import sys
import tempfile
from multiprocessing.process import BaseProcess

from .config import StickyConfig
from .router import Router
from .worker import worker_main

STOP_TIMEOUT = 10.0  # сколько ждём воркер после SIGTERM, прежде чем убить
STARTUP_TIMEOUT = 60.0  # сколько ждём, пока все воркеры выйдут на связь


def log(msg: str) -> None:
    print(f"[runner] {msg}", flush=True)


def run(config: StickyConfig) -> None:
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    code = asyncio.run(_supervise(config))
    log("сервер остановлен")
    if code:
        sys.exit(code)


def _bind(config: StickyConfig) -> socket.socket:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((config.host, config.port))
    listener.listen(2048)  # очередь копится уже сейчас, до готовности воркеров
    return listener


async def _supervise(config: StickyConfig) -> int:
    """Возвращает код выхода: 0 при штатной остановке, 1 если воркеры умерли сами."""
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    all_ready = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    run_dir = tempfile.mkdtemp(prefix="uvicorn-sticky-")  # создаётся с правами 0700
    control_path = os.path.join(run_dir, "ctl.sock")
    listener = _bind(config)

    def on_change(labels: list[str]) -> None:
        if stop.is_set():
            return  # идёт остановка: уход воркеров ожидаем
        log(f"на связи {len(labels)}/{config.workers}: {', '.join(labels) or 'никого'}")
        if len(labels) == config.workers:
            all_ready.set()

    router = Router(control_path=control_path, on_change=on_change)
    tasks = [asyncio.create_task(router.serve_control())]
    procs: list[BaseProcess] = []
    code = 0

    async def wait_until(event: asyncio.Event, timeout: float | None) -> bool:
        """Ждём событие, остановку или смерть всех воркеров. True, если событие наступило."""
        deadline = None if timeout is None else loop.time() + timeout
        while not (event.is_set() or stop.is_set()):
            if not any(p.is_alive() for p in procs):
                return False
            if deadline is not None and loop.time() > deadline:
                return False
            await asyncio.sleep(0.1)
        return event.is_set()

    try:
        await router.ready.wait()  # воркерам уже есть куда подключаться
        log(f"запускаю {config.app} на {config.host}:{config.port}, воркеров: {config.workers}")
        ctx = multiprocessing.get_context("spawn")  # чистый процесс, как у Uvicorn
        for index in range(config.workers):
            p = ctx.Process(
                target=worker_main, args=(config, index, control_path), name=config.label_for(index)
            )
            p.start()
            procs.append(p)
            log(f"воркер {p.name} запущен, pid {p.pid}")

        if (
            not await wait_until(all_ready, STARTUP_TIMEOUT)
            and not stop.is_set()
            and any(p.is_alive() for p in procs)
        ):
            log(
                f"не все воркеры вышли на связь за {STARTUP_TIMEOUT:.0f} с, начинаю приём с теми, что есть"
            )

        tasks.append(
            asyncio.create_task(router.serve_tcp(listener))
        )  # с этого момента клиенты обслуживаются
        await wait_until(stop, None)
        if not stop.is_set():
            log("все воркеры завершились")
            code = 1
    finally:
        stop.set()
        for p in procs:
            if p.is_alive():
                p.terminate()  # SIGTERM: Uvicorn завершается штатно
        for p in procs:
            await loop.run_in_executor(None, p.join, STOP_TIMEOUT)
            if p.is_alive():
                log(f"воркер {p.name} не остановился за {STOP_TIMEOUT:.0f} с, убиваю")
                p.kill()
                await loop.run_in_executor(None, p.join)
        for t in tasks:
            t.cancel()
        listener.close()
        shutil.rmtree(run_dir, ignore_errors=True)
    return code
