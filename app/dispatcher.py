"""Ordering-guaranteed delivery dispatcher.

A single poller loop periodically claims the *head* live delivery of each
(subscription, source) pair and hands it to a bounded thread pool. A pair is
never processed concurrently, a failing head is retried with exponential
backoff and blocks every later sequence of that pair, and once attempts are
exhausted the delivery moves to the dead-letter queue and the cursor advances
so later sequences can proceed.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx

from . import config, db

logger = logging.getLogger("webhook.dispatcher")


def backoff_delay(attempt_number: int) -> float:
    """Delay (seconds) after failed attempt ``attempt_number`` (1-based)."""
    delay = config.RETRY_BASE_DELAY_SECONDS * (2 ** (attempt_number - 1))
    return min(delay, config.RETRY_MAX_DELAY_SECONDS)


class Dispatcher:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pool = ThreadPoolExecutor(
            max_workers=config.DISPATCH_CONCURRENCY,
            thread_name_prefix="deliver",
        )
        self._inflight: set[str] = set()
        self._inflight_lock = threading.Lock()
        self._client = httpx.Client(timeout=config.DELIVERY_TIMEOUT_SECONDS)

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="dispatcher", daemon=True)
        self._thread.start()
        logger.info("dispatcher started")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        self._pool.shutdown(wait=False, cancel_futures=True)
        self._client.close()
        logger.info("dispatcher stopped")

    # ------------------------------------------------------------------ main loop

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception:  # pragma: no cover - defensive
                logger.exception("dispatcher poll failed")
            self._stop.wait(config.DISPATCH_INTERVAL_SECONDS)

    def _poll_once(self) -> None:
        candidates = db.claim_candidates(db.utcnow(), config.DISPATCH_CONCURRENCY * 4)
        free = config.DISPATCH_CONCURRENCY - len(self._inflight)
        if free <= 0:
            return
        for cand in candidates:
            if free <= 0:
                break
            key = f"{cand['subscription_id']}:{cand['source']}"
            with self._inflight_lock:
                if key in self._inflight:
                    continue
                self._inflight.add(key)
            free -= 1
            try:
                self._pool.submit(self._deliver, cand, key)
            except RuntimeError:
                with self._inflight_lock:
                    self._inflight.discard(key)

    # ------------------------------------------------------------------ delivery

    def _deliver(self, delivery: dict, inflight_key: str) -> None:
        try:
            sub = db.get_subscription(delivery["subscription_id"])
            if sub is None or not sub["active"]:
                return

            event = db.query_one(
                "SELECT * FROM events WHERE id = ?", (delivery["event_id"],)
            )
            if event is None:
                return
            event = dict(event)
            payload = json.loads(event.get("payload_json") or "null")
            # Always deliver an envelope carrying ordering metadata; the
            # caller-supplied body is available under "payload".
            body_obj = {
                "event_id": event["event_id"],
                "source": event["source"],
                "sequence": event["sequence"],
                "payload": payload,
            }
            body = json.dumps(body_obj, separators=(",", ":"), ensure_ascii=False).encode()

            attempt_no = int(delivery["attempts"]) + 1
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "webhook-sequencer/1.0",
                "X-Webhook-Event": str(event["event_id"]),
                "X-Webhook-Source": str(event["source"]),
                "X-Webhook-Sequence": str(event["sequence"]),
                "X-Webhook-Delivery": str(delivery["id"]),
                "X-Webhook-Attempt": str(attempt_no),
            }
            if sub.get("secret"):
                sig = hmac.new(
                    sub["secret"].encode(), body, hashlib.sha256
                ).hexdigest()
                headers["X-Webhook-Signature"] = f"sha256={sig}"

            status_code: int | None = None
            try:
                resp = self._client.post(sub["url"], content=body, headers=headers)
                status_code = resp.status_code
                ok = 200 <= resp.status_code < 300
                error = "" if ok else f"HTTP {resp.status_code}: {resp.text[:500]}"
            except httpx.HTTPError as exc:
                ok = False
                error = f"{type(exc).__name__}: {exc}"

            if ok:
                db.record_delivery_success(delivery["id"], attempt_no)
                logger.info(
                    "delivered id=%s sub=%s source=%s seq=%s attempt=%s",
                    delivery["id"], sub["id"], event["source"], event["sequence"], attempt_no,
                )
                return

            next_at: str | None = None
            if attempt_no < config.MAX_ATTEMPTS:
                delay = backoff_delay(attempt_no)
                next_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=delay)
                ).isoformat(timespec="milliseconds")
            result = db.record_delivery_failure(
                delivery["id"], attempt_no, status_code, error, next_at
            )
            if result == "dead":
                logger.warning(
                    "dead-lettered id=%s sub=%s source=%s seq=%s after %s attempts: %s",
                    delivery["id"], sub["id"], event["source"], event["sequence"],
                    attempt_no, error,
                )
            else:
                logger.info(
                    "delivery failed id=%s sub=%s source=%s seq=%s attempt=%s: %s",
                    delivery["id"], sub["id"], event["source"], event["sequence"],
                    attempt_no, error,
                )
        finally:
            with self._inflight_lock:
                self._inflight.discard(inflight_key)
