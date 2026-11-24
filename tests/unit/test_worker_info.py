"""current_worker(): приложение узнаёт, в каком воркере оно работает."""

import dataclasses

import pytest

from uvicorn_sticky import WorkerInfo, current_worker
from uvicorn_sticky import worker as worker_module


@pytest.fixture(autouse=True)
def reset():
    yield
    worker_module._set_current(None)


def test_none_outside_runner():
    assert current_worker() is None


def test_returns_what_runner_set():
    worker_module._set_current(WorkerInfo(label="W2", index=2, total=4))
    info = current_worker()
    assert info is not None
    assert (info.label, info.index, info.total) == ("W2", 2, 4)


def test_info_is_immutable():
    info = WorkerInfo(label="W0", index=0, total=1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        info.label = "X"  # type: ignore[misc]
