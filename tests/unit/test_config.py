"""StickyConfig: значения по умолчанию и проверка входных данных."""
import pytest

from uvicorn_sticky import StickyConfig


def test_defaults():
    c = StickyConfig(app="pkg.mod:app")
    assert (c.workers, c.host, c.port, c.label) == (1, "127.0.0.1", 8000, "W{index}")


def test_label_for_uses_template():
    c = StickyConfig(app="pkg.mod:app", label="node-{index}")
    assert [c.label_for(i) for i in range(3)] == ["node-0", "node-1", "node-2"]


def test_config_is_immutable():
    c = StickyConfig(app="pkg.mod:app")
    with pytest.raises(AttributeError):
        c.port = 9000 # type: ignore[misc]


@pytest.mark.parametrize("app", ["", "nope", "pkg.mod:", ":app", "a:b:c", "examples:fastapi_ex:main:app"])
def test_bad_app_rejected(app):
    with pytest.raises(ValueError, match="app должен быть вида"):
        StickyConfig(app=app)


@pytest.mark.parametrize("workers", [0, -1])
def test_bad_workers_rejected(workers):
    with pytest.raises(ValueError, match="workers"):
        StickyConfig(app="pkg.mod:app", workers=workers)


@pytest.mark.parametrize("port", [0, -5, 65536])
def test_bad_port_rejected(port):
    with pytest.raises(ValueError, match="port"):
        StickyConfig(app="pkg.mod:app", port=port)


def test_label_without_index_rejected():
    with pytest.raises(ValueError, match="label"):
        StickyConfig(app="pkg.mod:app", label="worker")


@pytest.mark.parametrize("label", ["node {index}", "w/{index}", "x" * 40 + "{index}"])
def test_label_must_produce_valid_worker_name(label):
    with pytest.raises(ValueError, match="label"):
        StickyConfig(app="pkg.mod:app", label=label)