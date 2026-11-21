import uvicorn_sticky


def test_version_is_exposed() -> None:
    assert isinstance(uvicorn_sticky.__version__, str)
    assert uvicorn_sticky.__version__
