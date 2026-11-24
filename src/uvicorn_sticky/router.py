"""
Роутер: принимает соединения клиентов и передаёт каждое нужному воркеру через
Unix-сокет (SCM_RIGHTS). Все соединения с одним ключом идут в один воркер.

Работает внутри главного процесса (раннера). Рассчитан на работу за reverse proxy,
который снимает TLS.
"""

import asyncio
import os
import socket
import struct
from collections.abc import Callable

from .balancing import pick_worker
from .config import LABEL_RE
from .key import extract_key, is_request_line

MAX_PEEK = 2048  # больше первой строки запроса читать незачем
PEEK_TIMEOUT = 5.0  # защита от slowloris
REGISTER_TIMEOUT = 3.0
MAX_PENDING = 1000  # соединений, ещё не переданных воркеру


def log(msg: str) -> None:
    print(f"[router] {msg}", flush=True)


def peer_uid(sock: socket.socket) -> int | None:
    """UID процесса на другом конце Unix-сокета (Linux)."""
    if not hasattr(socket, "SO_PEERCRED"):
        return None
    _pid, uid, _gid = struct.unpack(
        "3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
    )
    return uid


def reply(conn: socket.socket, status: str) -> None:
    try:
        conn.send(f"HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode())
    except OSError:
        pass  # клиент уже ушёл: отвечать некому


class Router:
    def __init__(
        self,
        control_path: str,
        on_change: Callable[[list[str]], None] | None = None,
        peek_timeout: float = PEEK_TIMEOUT,
    ) -> None:
        self.control_path = control_path
        self.on_change = on_change  # вызывается при появлении и пропаже воркера
        self.peek_timeout = peek_timeout
        self.workers: dict[str, socket.socket] = {}  # label -> control socket
        self.pending = 0
        self.ready = asyncio.Event()  # служебный сокет открыт, воркеры могут подключаться
        self._round_robin = 0
        self._tasks: set[asyncio.Task] = set()

    def labels(self) -> list[str]:
        return sorted(self.workers)

    def _changed(self) -> None:
        if self.on_change is not None:
            self.on_change(self.labels())

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)  # иначе сборщик мусора может удалить задачу
        task.add_done_callback(self._tasks.discard)

    # ---------- регистрация воркеров ----------

    async def serve_control(self) -> None:
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        old = os.umask(0o177)  # сокет создаётся с правами 0600
        try:
            srv.bind(self.control_path)
        finally:
            os.umask(old)
        srv.listen()
        srv.setblocking(False)
        self.ready.set()
        loop = asyncio.get_running_loop()
        try:
            while True:
                try:
                    ctl, _ = await loop.sock_accept(srv)
                except OSError as e:
                    log(f"control accept failed: {e}")
                    await asyncio.sleep(0.1)
                    continue
                self._spawn(self.register(ctl))
        finally:
            srv.close()

    async def register(self, ctl: socket.socket) -> None:
        loop = asyncio.get_running_loop()
        label = None
        shutting_down = False
        try:
            uid = peer_uid(ctl)
            if uid is not None and uid != os.getuid():
                log(f"rejected worker: foreign uid {uid}")
                return
            raw = await asyncio.wait_for(loop.sock_recv(ctl, 64), REGISTER_TIMEOUT)
            name = raw.decode("ascii", "replace").strip()
            if not LABEL_RE.match(name):
                log("rejected worker: bad label")
                return
            if name in self.workers:
                log(f"rejected worker: label {name} already taken")
                return
            label = name
            self.workers[label] = ctl
            log(f"worker registered: {label}")
            self._changed()
            # Воркер больше ничего не шлёт. Чтение вернётся, только когда он умрёт.
            await loop.sock_recv(ctl, 1)
        except asyncio.CancelledError:
            shutting_down = True  # роутер останавливается: это не смерть воркера
            raise
        except (TimeoutError, OSError) as e:
            log(f"worker channel error: {e!r}")
        finally:
            if label is not None and self.workers.get(label) is ctl:
                del self.workers[label]
                if not shutting_down:
                    log(f"worker gone: {label}")
                    self._changed()
            ctl.close()

    # ---------- выбор воркера ----------

    def choose(self, key: str | None) -> str:
        """Запрос с ключом идёт по хешу, без ключа — по кругу."""
        labels = self.labels()
        if key is not None:
            return pick_worker(key, labels)
        self._round_robin += 1
        return labels[self._round_robin % len(labels)]

    # ---------- приём клиентов ----------

    async def serve_tcp(self, listener: socket.socket) -> None:
        """Принимать клиентов на уже открытом слушающем сокете."""
        listener.setblocking(False)
        loop = asyncio.get_running_loop()
        while True:
            try:
                conn, _ = await loop.sock_accept(listener)
            except OSError as e:  # например, кончились дескрипторы (EMFILE)
                log(f"accept failed: {e}")
                await asyncio.sleep(0.1)
                continue
            if self.pending >= MAX_PENDING:  # не копим бесконечно недочитанные соединения
                conn.close()
                continue
            self.pending += 1
            self._spawn(self.dispatch(conn))

    async def dispatch(self, conn: socket.socket) -> None:
        handed_over = False
        try:
            try:
                head = await asyncio.wait_for(peek_request_line(conn), self.peek_timeout)
            except TimeoutError:
                return  # медленный клиент: молча закрываем
            if not is_request_line(head):
                reply(conn, "400 Bad Request")
                return
            if not self.workers:
                reply(conn, "503 Service Unavailable")
                return
            label = self.choose(extract_key(head))
            try:
                socket.send_fds(self.workers[label], [b"x"], [conn.fileno()])
                handed_over = True
            except BlockingIOError:  # воркер не успевает разбирать очередь
                reply(conn, "503 Service Unavailable")
            except OSError as e:  # воркер умер между выбором и отправкой
                log(f"send to {label} failed: {e}")
                reply(conn, "503 Service Unavailable")
        except OSError as e:
            log(f"dispatch error: {e}")
        finally:
            self.pending -= 1
            if not handed_over:
                discard_unread(conn)
            conn.close()  # после передачи закрываем только СВОЮ копию; у воркера своя


def discard_unread(conn: socket.socket) -> None:
    """
    Выбросить непрочитанные данные перед закрытием соединения, которое мы не передали воркеру.
    Если закрыть сокет с непрочитанными данными, ядро шлёт клиенту сброс (RST),
    и он может не успеть получить наш ответ 400 или 503.
    """
    try:
        while conn.recv(65536):
            pass
    except OSError:
        pass  # читать больше нечего (или клиент уже ушёл)


async def peek_request_line(conn: socket.socket) -> bytes:
    """Подсмотреть первую строку запроса, не забирая данные из сокета."""
    loop = asyncio.get_running_loop()
    while True:
        await wait_readable(loop, conn)
        data = conn.recv(MAX_PEEK, socket.MSG_PEEK)
        if not data or b"\r\n" in data or len(data) >= MAX_PEEK:
            return data
        # Данные остаются в сокете, и он всё время "готов к чтению".
        # Без паузы недосланная строка крутила бы цикл на 100 % CPU.
        await asyncio.sleep(0.02)


async def wait_readable(loop: asyncio.AbstractEventLoop, sock: socket.socket) -> None:
    fut = loop.create_future()
    loop.add_reader(sock.fileno(), lambda: fut.done() or fut.set_result(None))
    try:
        await fut
    finally:
        loop.remove_reader(sock.fileno())
