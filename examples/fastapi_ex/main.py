"""
Пример: HTTP-приложение, которое показывает, какой воркер ответил.

Через библиотеку:   uvicorn-sticky examples.fastapi_ex.main:app --workers 4
Обычный Uvicorn:    uvicorn examples.fastapi_ex.main:app --workers 4
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from uvicorn_sticky import current_worker

info = current_worker()                       # None, если запущено обычным Uvicorn
LABEL = info.label if info else f"pid{os.getpid()}"
hits = 0                                      # счётчик живёт в памяти ЭТОГО воркера


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[{LABEL}] приложение запущено", flush=True)      # выполняется в каждом воркере
    yield
    print(f"[{LABEL}] приложение остановлено, обработало {hits}", flush=True)


app = FastAPI(lifespan=lifespan)


@app.get("/ws/{key}")
async def who(key: str):
    global hits
    hits += 1
    return {
        "worker": LABEL,
        "index": info.index if info else None,
        "total": info.total if info else None,
        "pid": os.getpid(),
        "key": key,
        "hits_in_this_worker": hits,
    }