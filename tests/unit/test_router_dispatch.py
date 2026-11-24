"""Роутер принимает клиента, выбирает воркер и передаёт ему соединение."""

import asyncio
import socket
import tempfile

from uvicorn_sticky.balancing import pick_worker
from uvicorn_sticky.router import Router


class FakeWorker:
    """Вместо настоящего воркера: регистрируется и отвечает своим лейблом на каждое соединение."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.ctl = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.task: asyncio.Task | None = None

    async def start(self, control_path: str) -> None:
        self.ctl.connect(control_path)
        self.ctl.sendall(self.label.encode())
        self.ctl.setblocking(False)
        self.task = asyncio.create_task(self._serve())
        await asyncio.sleep(0.05)

    async def _serve(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            ready = loop.create_future()
            loop.add_reader(self.ctl.fileno(), lambda f=ready: f.done() or f.set_result(None))
            try:
                await ready
            finally:
                loop.remove_reader(self.ctl.fileno())
            msg, fds, _, _ = socket.recv_fds(self.ctl, 1, 1)
            if not msg:
                return
            for fd in fds:
                conn = socket.socket(fileno=fd)
                conn.setblocking(False)
                request = await loop.sock_recv(conn, 65536)  # запрос должен прийти целиком
                body = f"{self.label}|{len(request)}".encode()
                await loop.sock_sendall(
                    conn, b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s" % (len(body), body)
                )
                conn.close()

    async def die(self) -> None:
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)  # сначала дожидаемся остановки
        self.ctl.close()
        await asyncio.sleep(0.1)


def with_router(scenario, workers=("W0", "W1", "W2"), **router_kwargs):
    async def main():
        path = tempfile.mkdtemp(prefix="us-") + "/c.sock"
        router = Router(control_path=path, **router_kwargs)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        tasks = [asyncio.create_task(router.serve_control())]
        await asyncio.wait_for(router.ready.wait(), 2)
        fakes = {label: FakeWorker(label) for label in workers}
        for f in fakes.values():
            await f.start(path)
        tasks.append(asyncio.create_task(router.serve_tcp(listener)))
        try:
            await scenario(port, fakes, router)
        finally:
            for t in tasks:
                t.cancel()
            for f in fakes.values():
                await f.die()
            listener.close()

    asyncio.run(main())


async def ask(port: int, raw: bytes, timeout: float = 2.0) -> bytes:
    """Отправить сырые байты и прочитать всё, что ответят, до закрытия соединения."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(raw)
    await writer.drain()
    try:
        return await asyncio.wait_for(reader.read(), timeout)
    finally:
        writer.close()


def request(path: str) -> bytes:
    return f"GET {path} HTTP/1.1\r\nHost: test\r\n\r\n".encode()


def worker_of(answer: bytes) -> str:
    return answer.rsplit(b"\r\n\r\n", 1)[1].decode().split("|")[0]


def test_same_key_goes_to_same_worker():
    async def scenario(port, fakes, router):
        for key in ("aaa", "bbb", "room-7"):
            seen = {worker_of(await ask(port, request(f"/ws/{key}"))) for _ in range(8)}
            assert seen == {pick_worker(key, list(fakes))}

    with_router(scenario)


def test_worker_receives_request_untouched():
    """Роутер только подсматривает: воркер получает запрос целиком, до последнего байта."""

    async def scenario(port, fakes, router):
        raw = request("/ws/aaa")
        answer = await ask(port, raw)
        assert answer.endswith(f"|{len(raw)}".encode())

    with_router(scenario)


def test_requests_without_key_are_spread_over_workers():
    async def scenario(port, fakes, router):
        seen = {worker_of(await ask(port, request("/health"))) for _ in range(9)}
        assert seen == set(fakes)

    with_router(scenario)


def test_garbage_gets_400():
    async def scenario(port, fakes, router):
        assert (await ask(port, b"HELLO\r\n\r\n")).startswith(b"HTTP/1.1 400")

    with_router(scenario)


def test_no_workers_gets_503():
    async def scenario(port, fakes, router):
        assert (await ask(port, request("/ws/aaa"))).startswith(b"HTTP/1.1 503")

    with_router(scenario, workers=())


def test_silent_client_is_dropped():
    """Клиент прислал половину строки и замолчал: роутер закрывает соединение по таймауту."""

    async def scenario(port, fakes, router):
        assert await ask(port, b"GET /ws/a", timeout=2.0) == b""

    with_router(scenario, peek_timeout=0.3)


def test_dead_workers_keys_move_to_survivors():
    async def scenario(port, fakes, router):
        labels = list(fakes)
        key = next(k for k in (f"k{i}" for i in range(100)) if pick_worker(k, labels) == "W1")
        assert worker_of(await ask(port, request(f"/ws/{key}"))) == "W1"
        await fakes["W1"].die()
        survivors = [x for x in labels if x != "W1"]
        seen = {worker_of(await ask(port, request(f"/ws/{key}"))) for _ in range(5)}
        assert seen == {pick_worker(key, survivors)}

    with_router(scenario)
