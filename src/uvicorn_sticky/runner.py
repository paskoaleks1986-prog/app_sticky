"""
Раннер: главный процесс. Открывает порт, запускает роутер и воркеры, следит за ними, останавливает.
"""

import asyncio
import contextlib
import multiprocessing
import os
import shutil
import signal
import socket
import sys
import tempfile
from collections import deque
from dataclasses import dataclass, field
from multiprocessing.process import BaseProcess

from .config import StickyConfig
from .router import Router
from .worker import worker_main

STOP_TIMEOUT = 10.0  # сколько ждём воркер после SIGTERM, прежде чем убить
STARTUP_TIMEOUT = 60.0  # сколько ждём, пока все воркеры выйдут на связь
MAX_RESTARTS = 5  # столько перезапусков одного воркера допускаем...
RESTART_WINDOW = 60.0  # ...за это число секунд; больше — считаем, что он падает по кругу


def log(msg: str) -> None:
    print(f"[runner] {msg}", flush=True)


@dataclass
class Slot:
    """Место воркера: лейбл постоянный, а процесс на этом месте может смениться."""

    index: int
    label: str
    proc: BaseProcess
    was_ready: bool = False  # выходил ли воркер на этом месте на связь хотя бы раз
    given_up: bool = False  # больше не перезапускаем
    restarts: deque[float] = field(default_factory=deque)  # время последних перезапусков


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


def _how_it_ended(exitcode: int | None) -> str:
    if exitcode is not None and exitcode < 0:
        return f"сигнал {-exitcode}"
    return f"код {exitcode}"


async def _supervise(config: StickyConfig) -> int:
    """Возвращает код выхода: 0 при штатной остановке, 1 если воркеров не осталось."""
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    run_dir = tempfile.mkdtemp(prefix="uvicorn-sticky-")  # создаётся с правами 0700
    control_path = os.path.join(run_dir, "ctl.sock")
    listener = _bind(config)
    ctx = multiprocessing.get_context("spawn")  # чистый процесс, как у Uvicorn
    slots: list[Slot] = []

    def spawn(index: int) -> BaseProcess:
        label = config.label_for(index)
        proc = ctx.Process(target=worker_main, args=(config, index, control_path), name=label)
        proc.start()
        log(f"воркер {label} запущен, pid {proc.pid}")
        return proc

    def on_change(labels: list[str]) -> None:
        if stop.is_set():
            return  # идёт остановка: уход воркеров ожидаем
        for slot in slots:
            if slot.label in labels:
                slot.was_ready = True
        log(f"на связи {len(labels)}/{config.workers}: {', '.join(labels) or 'никого'}")

    def look_after(slot: Slot) -> None:
        """Если воркер на этом месте завершился, решаем: поднять заново или оставить."""
        if slot.given_up or slot.proc.is_alive():
            return
        ended = _how_it_ended(slot.proc.exitcode)
        slot.proc.join()  # забираем завершившийся процесс, чтобы не оставлять зомби

        if not slot.was_ready:
            log(f"воркер {slot.label} завершился ({ended}) при старте, не перезапускаю")
            slot.given_up = True
            return
        if not config.restart:
            log(f"воркер {slot.label} завершился ({ended}), перезапуск отключён")
            slot.given_up = True
            return

        now = loop.time()
        while slot.restarts and now - slot.restarts[0] > RESTART_WINDOW:
            slot.restarts.popleft()
        if len(slot.restarts) >= MAX_RESTARTS:
            log(
                f"воркер {slot.label} завершился ({ended}): упал {MAX_RESTARTS + 1} раз "
                f"за {RESTART_WINDOW:.0f} с, больше не перезапускаю"
            )
            slot.given_up = True
            return
        slot.restarts.append(now)
        log(f"воркер {slot.label} завершился ({ended}), перезапускаю ({len(slot.restarts)}/{MAX_RESTARTS})")
        slot.proc = spawn(slot.index)

    router = Router(control_path=control_path, on_change=on_change)
    tasks = [asyncio.create_task(router.serve_control())]
    code = 0
    try:
        await router.ready.wait()  # воркерам уже есть куда подключаться
        log(f"запускаю {config.app} на {config.host}:{config.port}, воркеров: {config.workers}")
        for index in range(config.workers):
            slots.append(Slot(index=index, label=config.label_for(index), proc=spawn(index)))

        serving = False
        started = loop.time()
        while not stop.is_set():
            for slot in slots:
                look_after(slot)
            if all(slot.given_up for slot in slots):
                log("все воркеры завершились")
                code = 1
                break
            if not serving:
                everyone_settled = all(slot.was_ready or slot.given_up for slot in slots)
                too_long = loop.time() - started > STARTUP_TIMEOUT
                if too_long and not everyone_settled:
                    log(f"не все воркеры вышли на связь за {STARTUP_TIMEOUT:.0f} с, начинаю приём с теми, что есть")
                if everyone_settled or too_long:
                    tasks.append(asyncio.create_task(router.serve_tcp(listener)))  # клиенты обслуживаются
                    serving = True
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), 0.1)  # пауза между проверками; остановка её прерывает
    finally:
        stop.set()
        procs = [slot.proc for slot in slots]
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