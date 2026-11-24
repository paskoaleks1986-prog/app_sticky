"""Приложение для тестов: нормально стартует, а через полсекунды процесс падает."""

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.get_running_loop().call_later(0.5, os._exit, 1)  # жёсткий выход, как при аварии
    yield


app = FastAPI(lifespan=lifespan)