"""
Runner: запускает ASGI-приложение через Uvicorn из нашего кода.

Шаг 1: один процесс, без роутера и без маршрутизации.
"""
import os
import sys

import uvicorn

from .config import StickyConfig


def run(config: StickyConfig) -> None:
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)            # как у Uvicorn: приложение ищется от текущей папки

    if config.workers > 1:
        print(f"[runner] workers={config.workers}: пока запускается один процесс "
              f"(несколько воркеров появятся на шаге 2)", flush=True)

    label = config.label_for(0)
    server = uvicorn.Server(uvicorn.Config(
        config.app, host=config.host, port=config.port, log_level=config.log_level,
    ))
    print(f"[runner] запускаю {config.app} на {config.host}:{config.port}, воркер {label}", flush=True)
    try:
        server.run()                       # блокирует, пока сервер не остановят
    except KeyboardInterrupt:
        pass                               # Ctrl+C: Uvicorn уже завершил работу штатно
    print("[runner] сервер остановлен", flush=True)
