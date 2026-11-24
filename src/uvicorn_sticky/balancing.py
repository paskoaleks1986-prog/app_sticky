"""Выбор воркера для ключа."""

import hashlib
from collections.abc import Iterable


def pick_worker(key: str, labels: Iterable[str]) -> str:
    """
    Rendezvous hashing: для ключа считаем "очки" каждого воркера и берём наибольшие.

    Один ключ всегда даёт один воркер, порядок лейблов не важен. Если воркер пропал,
    переезжают только его ключи; если он вернулся с тем же лейблом, ключи возвращаются.
    """
    best = max(labels, key=lambda w: hashlib.sha256(f"{w}|{key}".encode()).digest(), default=None)
    if best is None:
        raise ValueError("нет ни одного воркера")
    return best
