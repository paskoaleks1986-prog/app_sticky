"""Выбор воркера по ключу: постоянство, минимальный переезд, равномерность."""

from collections import Counter

import pytest

from uvicorn_sticky.balancing import pick_worker

LABELS = ["W0", "W1", "W2", "W3"]
KEYS = [f"key-{i}" for i in range(2000)]


def test_same_key_same_worker():
    assert {pick_worker("abc", LABELS) for _ in range(50)} == {pick_worker("abc", LABELS)}


def test_order_of_labels_does_not_matter():
    assert all(pick_worker(k, LABELS) == pick_worker(k, LABELS[::-1]) for k in KEYS[:200])


def test_only_dead_workers_keys_move():
    """Убрали W2: ключи остальных воркеров остаются на своих местах."""
    survivors = ["W0", "W1", "W3"]
    for k in KEYS:
        before = pick_worker(k, LABELS)
        after = pick_worker(k, survivors)
        if before != "W2":
            assert after == before


def test_returning_worker_gets_its_keys_back():
    """Воркер упал и поднялся с тем же лейблом: его ключи возвращаются к нему."""
    for k in KEYS[:300]:
        assert pick_worker(k, LABELS) == pick_worker(k, ["W0", "W1", "W3", "W2"])


def test_keys_spread_roughly_evenly():
    share = Counter(pick_worker(k, LABELS) for k in KEYS)
    assert set(share) == set(LABELS)
    for label, n in share.items():
        assert 0.20 < n / len(KEYS) < 0.30, f"{label} получил {n} из {len(KEYS)}"


def test_no_workers_is_an_error():
    with pytest.raises(ValueError):
        pick_worker("abc", [])
