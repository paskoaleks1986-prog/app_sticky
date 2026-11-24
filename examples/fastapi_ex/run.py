# examples/fastapi_ex/run.py
from uvicorn_sticky import StickyConfig, run

run(
    StickyConfig(
        app="examples.fastapi_ex.main:app",
        workers=4,
        port=8000,
        label="W{index}",
    )
)
