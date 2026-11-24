"""
Тестер: поднимает пары игроков (red + blue) и проверяет, попали ли они в один воркер
и дошло ли сообщение от red до blue.

Запуск:  python bench/tester.py --stages 10,50,200
"""

import argparse
import asyncio
import json
import uuid
from collections import Counter

import websockets


async def play(base: str, sem: asyncio.Semaphore) -> dict:
    gid = str(uuid.uuid4())
    async with (
        sem,
        websockets.connect(f"{base}/ws/{gid}?name=red") as red,
        websockets.connect(f"{base}/ws/{gid}?name=blue") as blue,
    ):
        w_red = json.loads(await red.recv())["worker"]
        w_blue = json.loads(await blue.recv())["worker"]
        await red.send("ha-ha")
        # ответ придёт либо синему (доставлено), либо красному (соперника нет)
        got_blue = asyncio.create_task(blue.recv())
        got_red = asyncio.create_task(red.recv())
        done, pending = await asyncio.wait({got_blue, got_red}, timeout=3, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
        return {"gid": gid[:8], "red": w_red, "blue": w_blue, "delivered": got_blue in done}


async def stage(base: str, games: int, concurrency: int, show: int) -> None:
    sem = asyncio.Semaphore(concurrency)
    results = await asyncio.gather(*(play(base, sem) for _ in range(games)))

    together = sum(r["red"] == r["blue"] for r in results)
    delivered = sum(r["delivered"] for r in results)
    load = Counter(w for r in results for w in (r["red"], r["blue"]))

    print(f"\n=== {games} игр ({games * 2} соединений) ===")
    for r in results[:show]:
        mark = "OK  " if r["delivered"] else "LOST"
        print(f"  {mark} игра {r['gid']}  red -> {r['red']:<8} blue -> {r['blue']:<8}")
    if games > show:
        print(f"  ... ещё {games - show}")
    print(f"  в одном воркере: {together}/{games} ({together / games:.0%})")
    print(f"  сообщение дошло: {delivered}/{games} ({delivered / games:.0%})")
    print("  нагрузка: " + "  ".join(f"{w}={n}" for w, n in sorted(load.items())))


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="ws://127.0.0.1:8000")
    p.add_argument("--stages", default="10,50,200", help="число игр на каждом этапе")
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--show", type=int, default=6, help="сколько игр печатать построчно")
    a = p.parse_args()
    for n in map(int, a.stages.split(",")):
        await stage(a.url, n, a.concurrency, a.show)


if __name__ == "__main__":
    asyncio.run(main())