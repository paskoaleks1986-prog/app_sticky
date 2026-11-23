"""
Раннер: главный процесс. Запускает роутер и воркеры, следит за ними, останавливает.

Шаг 2: N воркеров с лейблами, каждый регистрируется у роутера. Соединения
между воркерами пока раздаёт ядро ОС (общий слушающий сокет), как в обычном Uvicorn.
"""
import asyncio
import os
import shutil
import signal
import socket
import sys
import tempfile
import multiprocessing

from multiprocessing.process import BaseProcess

from .config import StickyConfig
from .router import Router
from .worker import worker_main


STOP_TIMEOUT = 10.0          # сколько ждём воркер после SIGTERM, прежде чем убить


def log(msg: str) -> None:
    print(f"[runner] {msg}", flush=True)


def run(config: StickyConfig) -> None:
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    asyncio.run(_supervise(config))
    log("сервер остановлен")


def _bind(config: StickyConfig) -> socket.socket:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((config.host, config.port))
    listener.listen(2048)                       # очередь копится уже сейчас, до готовности воркеров
    listener.set_inheritable(True)
    return listener


async def _supervise(config: StickyConfig) -> None:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    run_dir = tempfile.mkdtemp(prefix="uvicorn-sticky-")       # создаётся с правами 0700
    control_path = os.path.join(run_dir, "ctl.sock")
    listener = _bind(config)

    def on_change(labels: list[str]) -> None:
        if stop.is_set():
            return                                             # идёт остановка: уход воркеров ожидаем
        log(f"на связи {len(labels)}/{config.workers}: {', '.join(labels) or 'никого'}")

    router = Router(control_path=control_path, on_change=on_change)
    control_task = asyncio.create_task(router.serve_control())
    procs: list[BaseProcess] = []
    try:
        await router.ready.wait()                              # воркерам уже есть куда подключаться
        log(f"запускаю {config.app} на {config.host}:{config.port}, воркеров: {config.workers}")
        ctx = multiprocessing.get_context("spawn")             # чистый процесс, как у Uvicorn
        for index in range(config.workers):
            p = ctx.Process(target=worker_main, args=(config, index, control_path, listener),
                            name=config.label_for(index))
            p.start()
            procs.append(p)
            log(f"воркер {p.name} запущен, pid {p.pid}")

        while not stop.is_set():
            if not any(p.is_alive() for p in procs):
                log("все воркеры завершились")
                break
            try:
                await asyncio.wait_for(stop.wait(), 0.2)
            except asyncio.TimeoutError:
                pass
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()                                  # SIGTERM: Uvicorn завершается штатно
        for p in procs:
            await loop.run_in_executor(None, p.join, STOP_TIMEOUT)
            if p.is_alive():
                log(f"воркер {p.name} не остановился за {STOP_TIMEOUT:.0f} с, убиваю")
                p.kill()
                await loop.run_in_executor(None, p.join)
        control_task.cancel()
        listener.close()
        shutil.rmtree(run_dir, ignore_errors=True)