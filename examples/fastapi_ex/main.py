"""
Пример приложения: обычные HTTP-запросы и WebSocket-комнаты в одном FastAPI.

Через библиотеку:   uvicorn-sticky examples.fastapi_ex.main:app --workers 4
Обычный Uvicorn:    uvicorn examples.fastapi_ex.main:app --workers 4

Всё состояние лежит в обычных переменных в памяти процесса. Через библиотеку запросы
с одним ключом приходят в один воркер, поэтому счётчики растут подряд, а участники
одной комнаты видят друг друга. Под обычным Uvicorn они расходятся по воркерам.
"""

import os
from collections import Counter
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect

from uvicorn_sticky import current_worker

info = current_worker()  # None, если запущено обычным Uvicorn
LABEL = info.label if info else f"pid{os.getpid()}"

# Обычные переменные в памяти ЭТОГО воркера. Другие воркеры их не видят.
hits = 0
hits_for_key: Counter[str] = Counter()
rooms: dict[str, dict[str, WebSocket]] = {}  # комната -> {имя: соединение}


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[{LABEL}] приложение запущено", flush=True)  # выполняется в каждом воркере
    yield
    print(f"[{LABEL}] приложение остановлено, обработало {hits}", flush=True)


app = FastAPI(lifespan=lifespan)


def who() -> dict:
    return {
        "worker": LABEL,
        "index": info.index if info else None,
        "total": info.total if info else None,
        "pid": os.getpid(),
    }


# ---------- обычные HTTP-запросы ----------


@app.get("/health")
async def health():
    return who()


@app.get("/ws/{key}")
async def by_key(key: str):
    global hits
    hits += 1
    hits_for_key[key] += 1
    return {**who(), "key": key, "hits_in_this_worker": hits, "hits_for_key": hits_for_key[key]}


@app.post("/ws/{key}")
async def upload(key: str, request: Request):
    body = await request.body()
    return {**who(), "key": key, "received": len(body)}


# ---------- WebSocket-комнаты ----------
# Подключение: ws://127.0.0.1:8000/ws/<комната>?name=<имя участника>


@app.websocket("/ws/{room}")
async def room_ws(ws: WebSocket, room: str, name: str):
    await ws.accept()
    members = rooms.setdefault(room, {})
    members[name] = ws
    await ws.send_json({"type": "hello", "worker": LABEL, "members": len(members)})
    try:
        while True:
            text = await ws.receive_text()
            others = [peer for peer_name, peer in members.items() if peer_name != name]
            if not others:
                # остальные участники подключены к ДРУГОМУ процессу: здесь их соединений нет
                await ws.send_json({"type": "lost", "worker": LABEL})
            for peer in others:
                await peer.send_json({"type": "msg", "from": name, "text": text, "worker": LABEL})
    except WebSocketDisconnect:
        pass
    finally:
        if members.get(name) is ws:  # удаляем только своё соединение
            del members[name]
            if not members:
                rooms.pop(room, None)