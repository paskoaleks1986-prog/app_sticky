"""Команда: uvicorn-sticky пакет.модуль:app --workers 4"""
import argparse

from .config import StickyConfig
from .runner import run


def main() -> None:
    p = argparse.ArgumentParser(prog="uvicorn-sticky")
    p.add_argument("app", help='приложение в виде "пакет.модуль:переменная"')
    p.add_argument("--workers", type=int, default=1)
    a = p.parse_args()
    try:
        config = StickyConfig(app=a.app, workers=a.workers)
    except ValueError as e:
        p.error(str(e))
    run(config)
