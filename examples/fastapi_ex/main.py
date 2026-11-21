"""Minimal FastAPI example app for uvicorn-sticky.

Run from the repo root:
    uvicorn examples.fastapi_app.main:app --workers 4

Every response includes the worker PID, so you can see which worker served the request.

1. HTTP CRUD:   GET/POST/PUT /items
2. WebSocket:   /ws/echo
3. Pseudo-LLM:  /ws/chat?session_id=<key>  (history is kept in this worker's memory)
"""

from __future__ import annotations

import asyncio
import os
import random

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

app = FastAPI(title="uvicorn-sticky FastAPI example")

WORKER_PID = os.getpid()


# --------------------------------------------------------------------------
# 1. HTTP: GET / POST (create) / PUT (update)
# --------------------------------------------------------------------------
class ItemIn(BaseModel):
    name: str
    price: float


class ItemOut(ItemIn):
    id: int
    worker_pid: int


# In-process storage: each worker has its own copy.
_items: dict[int, ItemIn] = {}
_next_id = 1


def _out(item_id: int, item: ItemIn) -> ItemOut:
    return ItemOut(id=item_id, worker_pid=WORKER_PID, **item.model_dump())


@app.get("/items")
async def list_items() -> list[ItemOut]:
    return [_out(i, it) for i, it in _items.items()]


@app.get("/items/{item_id}")
async def get_item(item_id: int) -> ItemOut:
    if item_id not in _items:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found (pid {WORKER_PID})")
    return _out(item_id, _items[item_id])


@app.post("/items", status_code=201)
async def create_item(item: ItemIn) -> ItemOut:
    global _next_id
    item_id = _next_id
    _next_id += 1
    _items[item_id] = item
    return _out(item_id, item)


@app.put("/items/{item_id}")
async def update_item(item_id: int, item: ItemIn) -> ItemOut:
    if item_id not in _items:
        raise HTTPException(status_code=404, detail=f"Item {item_id} not found (pid {WORKER_PID})")
    _items[item_id] = item
    return _out(item_id, item)


# --------------------------------------------------------------------------
# 2. WebSocket: echo
# --------------------------------------------------------------------------
@app.websocket("/ws/echo")
async def ws_echo(ws: WebSocket) -> None:
    await ws.accept()
    await ws.send_json({"event": "connected", "worker_pid": WORKER_PID})
    try:
        while True:
            text = await ws.receive_text()
            await ws.send_json({"echo": text, "worker_pid": WORKER_PID})
    except WebSocketDisconnect:
        pass


# --------------------------------------------------------------------------
# 3. Pseudo-LLM chat over WebSocket (token streaming + per-session history)
# --------------------------------------------------------------------------
# session_id -> list of {"role": ..., "content": ...}
# Lives only in this worker: if the client reconnects to another worker,
# the history is gone. This is exactly what uvicorn-sticky should prevent.
_sessions: dict[str, list[dict[str, str]]] = {}

_CANNED = [
    "That is an interesting question.",
    "Let me think about it step by step.",
    "Based on our conversation so far, I would say yes.",
    "I am a pseudo-LLM, so take this with a grain of salt.",
]


def _fake_answer(prompt: str, history_len: int) -> str:
    return (
        f"You said: '{prompt}'. {random.choice(_CANNED)} "
        f"(messages in this session: {history_len}, worker pid: {WORKER_PID})"
    )


@app.websocket("/ws/chat")
async def ws_chat(ws: WebSocket, session_id: str) -> None:
    await ws.accept()
    history = _sessions.setdefault(session_id, [])
    await ws.send_json(
        {
            "event": "session",
            "session_id": session_id,
            "restored_messages": len(history),
            "worker_pid": WORKER_PID,
        }
    )
    try:
        while True:
            prompt = await ws.receive_text()
            history.append({"role": "user", "content": prompt})

            answer = _fake_answer(prompt, len(history))
            # Stream the answer word by word, like a real LLM.
            for token in answer.split(" "):
                await ws.send_json({"event": "token", "data": token + " "})
                await asyncio.sleep(0.05)
            await ws.send_json({"event": "done", "worker_pid": WORKER_PID})

            history.append({"role": "assistant", "content": answer})
    except WebSocketDisconnect:
        pass


@app.get("/whoami")
async def whoami() -> dict[str, int]:
    return {"worker_pid": WORKER_PID}