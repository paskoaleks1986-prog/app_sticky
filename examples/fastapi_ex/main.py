"""
Пример: обычный Uvicorn с несколькими воркерами, только HTTP.
Каждый воркер при старте получает лейбл и печатает его; GET показывает, кто ответил.

Запуск:  uvicorn main:app --port 8000 --workers 4
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

LABEL = f"W{os.getpid()}"          # пока лейбл = PID процесса
hits = 0                           # счётчик живёт в памяти ЭТОГО воркера


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[{LABEL}] воркер запущен", flush=True)      # выполняется в каждом воркере
    yield
    print(f"[{LABEL}] воркер остановлен, обработал {hits}", flush=True)


app = FastAPI(lifespan=lifespan)


@app.get("/ws/{key}")
async def who(key: str):
    global hits
    hits += 1
    return {"worker": LABEL, "key": key, "hits_in_this_worker": hits}