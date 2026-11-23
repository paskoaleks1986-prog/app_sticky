"""Настройки запуска. Всё, кроме приложения и числа воркеров, задаётся здесь."""
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StickyConfig:
    app: str                      # "пакет.модуль:переменная", как у Uvicorn
    workers: int = 1
    host: str = "127.0.0.1"
    port: int = 8000
    label: str = "W{index}"       # шаблон имени воркера: W0, W1, ...
    log_level: str = "info"

    def __post_init__(self) -> None:
        module, sep, attr = self.app.partition(":")
        if not sep or not module or not attr or ":" in attr:
            raise ValueError(f'app должен быть вида "пакет.модуль:переменная", получено {self.app!r}')
        if self.workers < 1:
            raise ValueError(f"workers должен быть не меньше 1, получено {self.workers}")
        if not 1 <= self.port <= 65535:
            raise ValueError(f"port должен быть от 1 до 65535, получено {self.port}")
        if "{index}" not in self.label:
            raise ValueError(f'label должен содержать "{{index}}", получено {self.label!r}')

    def label_for(self, index: int) -> str:
        return self.label.format(index=index)
