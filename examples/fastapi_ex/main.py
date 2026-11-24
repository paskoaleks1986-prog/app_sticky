"""
Пример: HTTP-приложение, которое показывает, какой воркер ответил.

Через библиотеку:   uvicorn-sticky examples.fastapi_ex.main:app --workers 4
Обычный Uvicorn:    uvicorn examples.fastapi_ex.main:app --workers 4
"""

import os
from collections import Counter
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from uvicorn_sticky import current_worker

info = current_worker()  # None, если запущено обычным Uvicorn
LABEL = info.label if info else f"pid{os.getpid()}"

# Обычные переменные в памяти ЭТОГО воркера. Другие воркеры их не видят.
hits = 0
hits_for_key: Counter[str] = Counter()


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
