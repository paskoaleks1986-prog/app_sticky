"""uvicorn-sticky: route connections sharing an affinity key to the same Uvicorn worker."""

from uvicorn_sticky.__about__ import __version__

from .config import StickyConfig
from .runner import run

__all__ = ["__version__",]
