"""Команда uvicorn-sticky: разбирает только приложение и --workers."""

import pytest

from uvicorn_sticky import cli


@pytest.fixture
def captured(monkeypatch):
    """Подменяем run(), чтобы сервер не запускался, и запоминаем конфиг."""
    seen = []
    monkeypatch.setattr(cli, "run", seen.append)
    return seen


def call(monkeypatch, *args):
    monkeypatch.setattr("sys.argv", ["uvicorn-sticky", *args])
    cli.main()


def test_app_only(monkeypatch, captured):
    call(monkeypatch, "pkg.mod:app")
    assert captured[0].app == "pkg.mod:app" and captured[0].workers == 1


def test_workers_flag(monkeypatch, captured):
    call(monkeypatch, "pkg.mod:app", "--workers", "4")
    assert captured[0].workers == 4


@pytest.mark.parametrize(
    "args",
    [
        ["a:b:c"],  # формат с несколькими двоеточиями
        ["pkg.mod:app", "--workers", "0"],  # неверное число воркеров
        ["pkg.mod:app", "--port", "9000"],  # лишний аргумент: порт задаётся в конфиге
        [],  # приложение не указано
    ],
)
def test_bad_arguments_exit_with_error(monkeypatch, captured, args):
    with pytest.raises(SystemExit) as e:
        call(monkeypatch, *args)
    assert e.value.code == 2 and captured == []
