"""Minimal Django example views for uvicorn-sticky (plain Django, no DRF, no models).

Run from examples/django_app/:
    uvicorn project.asgi:application --workers 4

Every response includes the worker PID and global counters. Globals live in the
worker process: each worker has its own copy, which makes worker routing visible.

1. HTTP CRUD:   GET/POST /items, GET/PUT /items/<id>
2. Pseudo-LLM:  GET /chat/stream?session_id=<key>&prompt=<text>  (SSE token stream)
3. Counters:    GET /whoami
"""

from __future__ import annotations

import asyncio
import json
import os
import random

from collections.abc import AsyncIterator
from django.views.decorators.csrf import csrf_exempt
from django.http.response import HttpResponseBase

from django.http import (
    HttpRequest,
    HttpResponse,
    JsonResponse,
    StreamingHttpResponse,
)

WORKER_PID = os.getpid()

# ---------------------------------------------------------------- globals ----
REQUESTS_TOTAL = 0  # all requests served by this worker
ACTIVE_STREAMS = 0  # chat streams open right now in this worker
CLIENTS: set[str] = set()  # unique session_ids seen by this worker

ITEMS: dict[int, dict[str, object]] = {}
NEXT_ID = 1

SESSIONS: dict[str, list[dict[str, str]]] = {}  # session_id -> chat history


def _stats() -> dict[str, object]:
    return {
        "worker_pid": WORKER_PID,
        "requests_total": REQUESTS_TOTAL,
        "active_streams": ACTIVE_STREAMS,
        "unique_clients": len(CLIENTS),
    }


def _count() -> None:
    global REQUESTS_TOTAL
    REQUESTS_TOTAL += 1


def _parse(request: HttpRequest) -> dict[str, object] | None:
    try:
        data = json.loads(request.body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict) or "name" not in data or "price" not in data:
        return None
    return {"name": str(data["name"]), "price": float(data["price"])}


def _bad_request() -> JsonResponse:
    return JsonResponse({"error": "expected JSON {name, price}", **_stats()}, status=400)


def _not_found(item_id: int) -> JsonResponse:
    return JsonResponse({"error": f"item {item_id} not found", **_stats()}, status=404)


# ------------------------------------------------------------- 1. HTTP CRUD --
@csrf_exempt
async def items(request: HttpRequest) -> HttpResponse:
    """GET -> list, POST -> create."""
    global NEXT_ID
    _count()

    if request.method == "GET":
        data = [{"id": i, **it} for i, it in ITEMS.items()]
        return JsonResponse({"items": data, **_stats()})

    if request.method == "POST":
        item = _parse(request)
        if item is None:
            return _bad_request()
        item_id = NEXT_ID
        NEXT_ID += 1
        ITEMS[item_id] = item
        return JsonResponse({"id": item_id, **item, **_stats()}, status=201)

    return HttpResponse(status=405)


@csrf_exempt
async def item_detail(request: HttpRequest, item_id: int) -> HttpResponse:
    """GET -> read, PUT -> update."""
    _count()
    if item_id not in ITEMS:
        return _not_found(item_id)

    if request.method == "GET":
        return JsonResponse({"id": item_id, **ITEMS[item_id], **_stats()})

    if request.method == "PUT":
        item = _parse(request)
        if item is None:
            return _bad_request()
        ITEMS[item_id] = item
        return JsonResponse({"id": item_id, **item, **_stats()})

    return HttpResponse(status=405)


# ------------------------------------------------------- 2. Pseudo-LLM (SSE) --
_CANNED = [
    "That is an interesting question.",
    "Let me think about it step by step.",
    "Based on our conversation so far, I would say yes.",
    "I am a pseudo-LLM, so take this with a grain of salt.",
]


def _sse(event: str, data: object) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


async def chat_stream(request: HttpRequest) -> HttpResponseBase:
    """Stream a fake LLM answer word by word. History is kept per session_id in this worker."""
    _count()
    session_id = request.GET.get("session_id")
    prompt = request.GET.get("prompt", "")
    if not session_id:
        return JsonResponse({"error": "session_id is required", **_stats()}, status=400)

    CLIENTS.add(session_id)
    history = SESSIONS.setdefault(session_id, [])
    restored = len(history)
    history.append({"role": "user", "content": prompt})

    answer = (
        f"You said: '{prompt}'. {random.choice(_CANNED)} "
        f"(messages in this session: {len(history)}, worker pid: {WORKER_PID})"
    )

    async def events() -> AsyncIterator[bytes]:
        global ACTIVE_STREAMS
        ACTIVE_STREAMS += 1
        try:
            yield _sse("session", {"session_id": session_id, "restored_messages": restored})
            for token in answer.split(" "):
                yield _sse("token", token + " ")
                await asyncio.sleep(0.05)
            history.append({"role": "assistant", "content": answer})
            yield _sse("done", _stats())
        finally:
            ACTIVE_STREAMS -= 1

    response = StreamingHttpResponse(events(), content_type="text/event-stream")
    response["Cache-Control"] = "no-cache"
    return response


# ---------------------------------------------------------------- 3. Stats --
async def whoami(request: HttpRequest) -> JsonResponse:
    _count()
    return JsonResponse(_stats())