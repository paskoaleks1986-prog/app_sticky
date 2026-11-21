"""
Worker: регистрируется у роутера со своим лейблом, получает от него готовые
TCP-соединения (файловые дескрипторы) и обслуживает их.

Запуск: python worker.py W1
"""
import asyncio
import os
import socket
import sys

CONTROL_PATH = f"/tmp/ordo-router-{os.getuid()}/ctl.sock"
HEADER_TIMEOUT = 5.0
MAX_HEADERS = 100


async def handle(conn: socket.socket, label: str) -> None:
    writer = None
    try:
        reader, writer = await asyncio.open_connection(sock=conn)
        async with asyncio.timeout(HEADER_TIMEOUT):
            request_line = await reader.readline()   # данные на месте: роутер только подсматривал
            for _ in range(MAX_HEADERS):
                if (await reader.readline()) in (b"\r\n", b""):
                    break
        body = f"handled by {label}: {request_line.decode('latin-1').strip()}\n".encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
            + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            + body
        )
        await writer.drain()
    except (TimeoutError, OSError, ValueError) as e:
        print(f"[{label}] connection dropped: {e!r}", flush=True)
    finally:
        if writer is not None:
            writer.close()
        else:
            conn.close()                             # не теряем дескриптор при ранней ошибке


async def main(label: str) -> None:
    loop = asyncio.get_running_loop()
    ctl = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        ctl.connect(CONTROL_PATH)
        ctl.sendall(label.encode())
    except OSError as e:
        raise SystemExit(f"[{label}] cannot reach router: {e}")
    ctl.setblocking(False)
    print(f"[{label}] registered", flush=True)

    tasks: set[asyncio.Task] = set()
    while True:
        fut = loop.create_future()
        loop.add_reader(ctl.fileno(), lambda: fut.done() or fut.set_result(None))
        try:
            await fut
        finally:
            loop.remove_reader(ctl.fileno())
        try:
            msg, fds, _, _ = socket.recv_fds(ctl, 1, 1)
        except BlockingIOError:
            continue
        except OSError as e:
            print(f"[{label}] control channel error: {e}", flush=True)
            return
        if not msg:
            print(f"[{label}] router gone (or registration rejected)", flush=True)
            return
        for fd in fds:
            conn = socket.socket(fileno=fd)          # тот же TCP-сокет клиента
            conn.setblocking(False)
            t = asyncio.create_task(handle(conn, label))
            tasks.add(t)
            t.add_done_callback(tasks.discard)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python worker.py <label>")
    try:
        asyncio.run(main(sys.argv[1]))
    except KeyboardInterrupt:
        pass