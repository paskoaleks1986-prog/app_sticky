"""
Router: держит TCP-порт, принимает соединения и передаёт их воркерам через
Unix-сокет (SCM_RIGHTS). Все соединения одной игры идут в один воркер.

Рассчитан на работу ЗА reverse proxy (Nginx снимает TLS), поэтому слушает 127.0.0.1.

Запуск: python router.py
"""
import asyncio
import hashlib
import os
import re
import socket
import stat
import struct
from collections.abc import Callable

from .config import LABEL_RE

HOST = "127.0.0.1"                 # не 0.0.0.0: снаружи доступен только Nginx
TCP_PORT = 8000
RUN_DIR = f"/tmp/ordo-router-{os.getuid()}"
CONTROL_PATH = f"{RUN_DIR}/ctl.sock"

PATH_RE = re.compile(rb"^GET /ws/([0-9a-zA-Z-]{1,64})[ ?]")
MAX_PEEK = 2048                    # больше первой строки запроса читать незачем
PEEK_TIMEOUT = 5.0                 # защита от slowloris
REGISTER_TIMEOUT = 3.0
MAX_PENDING = 1000                 # соединений, ещё не переданных воркеру


def log(msg: str) -> None:
    print(f"[router] {msg}", flush=True)


def prepare_run_dir() -> None:
    """Каталог для control-сокета: только наш, без доступа другим пользователям."""
    os.makedirs(RUN_DIR, mode=0o700, exist_ok=True)
    st = os.lstat(RUN_DIR)         # lstat: симлинк, подложенный в /tmp, не пройдёт
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
        raise SystemExit(f"unsafe run dir {RUN_DIR}: wrong type, owner or mode")
    try:
        os.unlink(CONTROL_PATH)
    except FileNotFoundError:
        pass


def peer_uid(sock: socket.socket) -> int | None:
    """UID процесса на другом конце Unix-сокета (Linux)."""
    if not hasattr(socket, "SO_PEERCRED"):
        return None
    _pid, uid, _gid = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
    return uid


def reply(conn: socket.socket, status: str) -> None:
    try:
        conn.send(f"HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".encode())
    except OSError:
        pass                       # клиент уже ушёл: отвечать некому


class Router:
    def __init__(self, control_path: str = CONTROL_PATH, host: str = HOST, port: int = TCP_PORT,
                 on_change: Callable[[list[str]], None] | None = None) -> None:
        self.control_path = control_path
        self.host = host
        self.port = port
        self.on_change = on_change                    # вызывается при появлении и пропаже воркера
        self.workers: dict[str, socket.socket] = {}   # label -> control socket
        self.pending = 0
        self.ready = asyncio.Event()                  # служебный сокет открыт, воркеры могут подключаться

    def labels(self) -> list[str]:
        return sorted(self.workers)

    def _changed(self) -> None:
        if self.on_change is not None:
            self.on_change(self.labels())

    # ---------- регистрация воркеров ----------

    async def serve_control(self) -> None:
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        old = os.umask(0o177)      # сокет создаётся с правами 0600
        try:
            srv.bind(self.control_path)
        finally:
            os.umask(old)
        srv.listen()
        srv.setblocking(False)
        self.ready.set()
        loop = asyncio.get_running_loop()
        while True:
            try:
                ctl, _ = await loop.sock_accept(srv)
            except OSError as e:
                log(f"control accept failed: {e}")
                await asyncio.sleep(0.1)
                continue
            asyncio.create_task(self.register(ctl))

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
            shutting_down = True               # роутер останавливается: это не смерть воркера
            raise
        except (asyncio.TimeoutError, OSError) as e:
            log(f"worker channel error: {e!r}")
        finally:
            if label is not None and self.workers.get(label) is ctl:
                del self.workers[label]
                if not shutting_down:
                    log(f"worker gone: {label}")
                    self._changed()
            ctl.close()

    # ---------- выбор воркера ----------

    def pick_worker(self, gid: str) -> str:
        """Rendezvous hashing: без таблицы gid -> воркер, значит нечему расти в памяти.
        Если воркер пропал, переезжают только его игры."""
        return max(self.workers, key=lambda w: hashlib.sha256(f"{w}|{gid}".encode()).digest())

    # ---------- приём клиентов ----------

    async def serve_tcp(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(512)
        srv.setblocking(False)
        loop = asyncio.get_running_loop()
        log(f"listening on {self.host}:{self.port}")
        while True:
            try:
                conn, _ = await loop.sock_accept(srv)
            except OSError as e:               # например, кончились дескрипторы (EMFILE)
                log(f"accept failed: {e}")
                await asyncio.sleep(0.1)
                continue
            if self.pending >= MAX_PENDING:    # не копим бесконечно недочитанные соединения
                conn.close()
                continue
            self.pending += 1
            asyncio.create_task(self.dispatch(conn))

    async def dispatch(self, conn: socket.socket) -> None:
        try:
            try:
                head = await asyncio.wait_for(peek_request_line(conn), PEEK_TIMEOUT)
            except asyncio.TimeoutError:
                return                          # медленный клиент: молча закрываем
            m = PATH_RE.match(head)
            if not m:
                reply(conn, "400 Bad Request")
                return
            if not self.workers:
                reply(conn, "503 Service Unavailable")
                return
            gid = m.group(1).decode()
            label = self.pick_worker(gid)
            try:
                socket.send_fds(self.workers[label], [b"x"], [conn.fileno()])
            except BlockingIOError:             # воркер не успевает разбирать очередь
                reply(conn, "503 Service Unavailable")
                return
            except OSError as e:                # воркер умер между выбором и отправкой
                log(f"send to {label} failed: {e}")
                reply(conn, "503 Service Unavailable")
                return
            log(f"gid={gid} -> {label}")
        except OSError as e:
            log(f"dispatch error: {e}")
        finally:
            self.pending -= 1
            conn.close()                        # закрываем СВОЮ копию; у воркера своя


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


async def main() -> None:
    prepare_run_dir()
    r = Router()
    try:
        await asyncio.gather(r.serve_control(), r.serve_tcp())
    finally:
        try:
            os.unlink(CONTROL_PATH)
        except OSError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass