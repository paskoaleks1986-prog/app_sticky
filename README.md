# uvicorn-sticky

Route connections that share an affinity key to the same Uvicorn worker.

> Status: pre-alpha, work in progress.

## Install

From GitHub (until it is published on PyPI):

```bash
pip install "git+https://github.com/paskoaleks1986-prog/uvicorn-sticky.git"
```

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
make install      # pip install -e ".[dev]" + pre-commit hooks
make check        # ruff + mypy + pytest
make build        # sdist + wheel into dist/
```

## License

MIT
