"""FastAPI application: registration, event ingest, backlog / dead-letter APIs."""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import db
from .dispatcher import Dispatcher


class SubscriptionIn(BaseModel):
    url: str = Field(min_length=1, examples=["http://receiver:9000/hook"])
    secret: str = Field(default="", description="Optional HMAC-SHA256 signing secret")


class EventIn(BaseModel):
    source: str = Field(min_length=1)
    sequence: int = Field(ge=0)
    event_id: str = Field(min_length=1)
    payload: Any | None = None


dispatcher = Dispatcher()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    dispatcher.start()
    try:
        yield
    finally:
        dispatcher.stop()


app = FastAPI(
    title="Sequential Webhook Delivery API",
    version="1.0.0",
    description="Per-source strictly ordered webhook delivery with buffering, "
    "idempotent ingestion, exponential backoff and a dead-letter queue.",
    lifespan=lifespan,
)


def _serialize_event(row: dict) -> dict:
    out = dict(row)
    if "payload_json" in out:
        try:
            out["payload"] = json.loads(out.pop("payload_json") or "null")
        except json.JSONDecodeError:
            out["payload"] = None
    out.pop("duplicate", None)
    return out


# --------------------------------------------------------------------------- health


@app.get("/health", tags=["system"])
def health() -> dict:
    return {"status": "ok"}


@app.get("/stats", tags=["system"])
def get_stats() -> dict:
    return db.stats()


# --------------------------------------------------------------------------- subscriptions


@app.post("/subscriptions", status_code=201, tags=["subscriptions"])
def create_subscription(body: SubscriptionIn) -> dict:
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(status_code=422, detail="url must be an http(s) URL")
    return db.create_subscription(body.url, body.secret)


@app.get("/subscriptions", tags=["subscriptions"])
def list_subscriptions(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    return {"items": db.list_subscriptions(limit, offset), "limit": limit, "offset": offset}


@app.get("/subscriptions/{subscription_id}", tags=["subscriptions"])
def get_subscription(subscription_id: int) -> dict:
    sub = db.get_subscription(subscription_id)
    if sub is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    return sub


@app.post("/subscriptions/{subscription_id}/deactivate", tags=["subscriptions"])
def deactivate_subscription(subscription_id: int) -> dict:
    sub = db.set_subscription_active(subscription_id, False)
    if sub is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    return sub


@app.post("/subscriptions/{subscription_id}/activate", tags=["subscriptions"])
def activate_subscription(subscription_id: int) -> dict:
    sub = db.set_subscription_active(subscription_id, True)
    if sub is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    return sub


# --------------------------------------------------------------------------- events


@app.post("/events", tags=["events"])
def ingest_event(body: EventIn) -> dict:
    try:
        event = db.ingest_event(
            source=body.source,
            sequence=body.sequence,
            event_id=body.event_id,
            payload=body.payload,
        )
    except db.SequenceConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    duplicate = event.get("duplicate", False)
    return {
        "event": _serialize_event(event),
        "duplicate": duplicate,
        "status": "duplicate" if duplicate else "accepted",
    }


@app.get("/events/{event_id}", tags=["events"])
def get_event(event_id: str) -> dict:
    event = db.get_event_by_event_id(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return _serialize_event(event)


# --------------------------------------------------------------------------- backlog


@app.get("/backlog", tags=["backlog"])
def get_backlog(
    subscription_id: int | None = Query(None),
    source: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    total, items = db.get_backlog(subscription_id, source, limit, offset)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


# --------------------------------------------------------------------------- dead letters


@app.get("/dead-letters", tags=["dead-letters"])
def list_dead_letters(
    subscription_id: int | None = Query(None),
    source: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    total, items = db.list_dead(subscription_id, source, limit, offset)
    return {"total": total, "limit": limit, "offset": offset, "items": items}


@app.get("/deliveries/{delivery_id}", tags=["dead-letters"])
def get_delivery(delivery_id: int) -> dict:
    item = db.get_delivery(delivery_id)
    if item is None:
        raise HTTPException(status_code=404, detail="delivery not found")
    return item


@app.post("/dead-letters/{delivery_id}/replay", tags=["dead-letters"])
def replay_dead_letter(delivery_id: int) -> dict:
    result = db.replay_delivery(delivery_id)
    if result is None:
        raise HTTPException(status_code=404, detail="delivery not found")
    if isinstance(result, str):
        raise HTTPException(
            status_code=409,
            detail=f"delivery is not dead (current status: {result})",
        )
    return {"status": "queued", "delivery": result}
