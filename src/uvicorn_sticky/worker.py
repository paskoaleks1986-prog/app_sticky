"""
Воркер: отдельный процесс, в котором Uvicorn выполняет приложение.

Воркер не слушает порт. Соединения ему передаёт роутер по служебному каналу,
и воркер отдаёт каждое из них Uvicorn так, будто принял его сам.
"""

import asyncio
import os
import socket
import sys
from dataclasses import dataclass
from typing import Any

import uvicorn

from .config import StickyConfig

STARTUP_FAILURE = 3  # тот же код выхода, что у Uvicorn при неудачном старте


@dataclass(frozen=True, slots=True)
class WorkerInfo:
    label: str  # имя воркера, например "W2"
    index: int  # порядковый номер, с нуля
    total: int  # сколько воркеров запущено всего


_current: WorkerInfo | None = None


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


class StickyServer(uvicorn.Server):
    """Сервер Uvicorn, который получает соединения от роутера вместо собственного порта."""

    def __init__(self, config: uvicorn.Config, control_path: str, label: str) -> None:
        super().__init__(config)
        self._control_path = control_path
        self._label = label
        self._control: socket.socket | None = None
        self._adopting: set[asyncio.Task] = set()

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        """Замена штатного старта: поднимаем приложение, но порт не открываем."""
        await self.lifespan.startup()
        if self.lifespan.should_exit:
            sys.exit(STARTUP_FAILURE)

        config = self.config

        def create_protocol() -> asyncio.Protocol:
            # Тот же объект-обработчик, который Uvicorn создаёт на каждое своё соединение.
            factory: Any = config.http_protocol_class
            return factory(
                config=config,
                server_state=self.server_state,
                app_state=self.lifespan.state,
            )

        self._create_protocol = create_protocol
        self.servers = []  # слушающих сокетов у воркера нет

        # Регистрируемся только теперь: приложение готово принимать запросы.
        self._control = register(self._control_path, self._label)
        self._control.setblocking(False)
        asyncio.get_running_loop().add_reader(self._control.fileno(), self._on_control_readable)
        self.started = True

    def _on_control_readable(self) -> None:
        """Роутер что-то прислал: забираем все пришедшие соединения."""
        assert self._control is not None
        while True:
            try:
                msg, fds, _, _ = socket.recv_fds(self._control, 1, 1)
            except BlockingIOError:
                return  # больше ничего нет
            except OSError:
                msg, fds = b"", []
            if not msg:  # канал закрыт: роутера больше нет
                self._close_control()
                self.should_exit = True  # Uvicorn штатно завершит работу
                return
            for fd in fds:
                self._adopt(fd)

    def _adopt(self, fd: int) -> None:
        """Отдать полученное соединение Uvicorn, как будто он принял его сам."""
        conn = socket.socket(fileno=fd)
        conn.setblocking(False)
        loop = asyncio.get_running_loop()
        task = loop.create_task(loop.connect_accepted_socket(self._create_protocol, sock=conn))
        self._adopting.add(task)
        task.add_done_callback(self._adopted)

    def _adopted(self, task: asyncio.Task) -> None:
        self._adopting.discard(task)
        if not task.cancelled() and task.exception() is not None:
            print(f"[{self._label}] не удалось принять соединение: {task.exception()!r}", flush=True)

    def _close_control(self) -> None:
        if self._control is not None:
            asyncio.get_running_loop().remove_reader(self._control.fileno())
            self._control.close()
            self._control = None

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        self._close_control()  # роутер сразу перестаёт слать нам новое
        await super().shutdown(sockets=sockets)


def worker_main(config: StickyConfig, index: int, control_path: str) -> None:
    """Точка входа процесса-воркера. Вызывается раннером."""
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)  # приложение ищется от текущей папки, как у Uvicorn

    info = WorkerInfo(label=config.label_for(index), index=index, total=config.workers)
    _set_current(info)  # до импорта приложения: оно может спросить current_worker()

    server = StickyServer(uvicorn.Config(config.app, log_level=config.log_level), control_path, info.label)
    server.run()  # блокирует, пока воркер не остановят
