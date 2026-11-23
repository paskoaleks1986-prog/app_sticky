"""
Воркер: отдельный процесс, в котором Uvicorn выполняет приложение.

Шаг 2: воркер получает от раннера лейбл, сообщает его роутеру по служебному
каналу и обслуживает общий слушающий сокет (соединения пока раздаёт ядро ОС).
"""
import os
import socket
import sys
from dataclasses import dataclass

import uvicorn

from .config import StickyConfig


@dataclass(frozen=True, slots=True)
class WorkerInfo:
    label: str        # имя воркера, например "W2"
    index: int        # порядковый номер, с нуля
    total: int        # сколько воркеров запущено всего


_current: WorkerInfo | None = None
_control: socket.socket | None = None     # держим канал открытым: по его обрыву роутер узнаёт о смерти воркера


def current_worker() -> WorkerInfo | None:
    """Сведения о воркере, в котором выполняется этот код. None, если запуск был не через uvicorn-sticky."""
    return _current


def _set_current(info: WorkerInfo | None) -> None:
    global _current
    _current = info


def register(control_path: str, label: str) -> socket.socket:
    """Подключиться к роутеру и назвать свой лейбл."""
    ctl = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    ctl.connect(control_path)
    ctl.sendall(label.encode())
    return ctl


def worker_main(config: StickyConfig, index: int, control_path: str, listener: socket.socket) -> None:
    """Точка входа процесса-воркера. Вызывается раннером."""
    global _control
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)                   # приложение ищется от текущей папки, как у Uvicorn

    info = WorkerInfo(label=config.label_for(index), index=index, total=config.workers)
    _set_current(info)                            # до импорта приложения: оно может спросить current_worker()
    _control = register(control_path, info.label)

    server = uvicorn.Server(uvicorn.Config(config.app, log_level=config.log_level))
    server.run(sockets=[listener])                # блокирует, пока воркер не остановят