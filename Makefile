.PHONY: venv install lint format typecheck test check build clean

venv:
	python3 -m venv .venv

install:
	pip install -e ".[dev]"
	pre-commit install

lint:
	ruff check .
	ruff format --check .

format:
	ruff check --fix .
	ruff format .

typecheck:
	mypy

test:
	pytest

check: lint typecheck test

build: clean
	python -m build
	twine check dist/*

clean:
	rm -rf build dist *.egg-info src/*.egg-info .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
