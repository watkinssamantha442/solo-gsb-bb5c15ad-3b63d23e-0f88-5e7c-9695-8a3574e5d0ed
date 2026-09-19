import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

# Test tuning must be set before the app/config modules are imported.
_tmpdir = tempfile.mkdtemp(prefix="wh-test-")
os.environ.setdefault("DATABASE_PATH", os.path.join(_tmpdir, "test.db"))
os.environ.setdefault("MAX_ATTEMPTS", "5")
os.environ.setdefault("RETRY_BASE_DELAY_SECONDS", "0.5")
os.environ.setdefault("RETRY_MAX_DELAY_SECONDS", "2")
os.environ.setdefault("DISPATCH_INTERVAL_SECONDS", "0.02")
os.environ.setdefault("DELIVERY_TIMEOUT_SECONDS", "2")
os.environ.setdefault("DISPATCH_CONCURRENCY", "8")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


class Receiver:
    """Minimal configurable webhook receiver running on a real socket."""

    def __init__(self, mode="ok", flaky_source=None):
        self.lock = threading.Lock()
        self.received = []  # list of (source, sequence, event_id, attempts)
        self.signatures = {}  # (source, sequence) -> signature header
        self.raw_bodies = {}  # (source, sequence) -> raw bytes
        self.mode = mode
        self.flaky_source = flaky_source
        self.flaky_failed = False
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def url(self):
        return f"http://127.0.0.1:{self.port}/hook"

    def _should_fail(self, body, headers):
        src, seq = body.get("source"), body.get("sequence")
        if self.mode == "fail":
            return True
        if self.mode == "fail_seq1" and seq == 1:
            return True
        if self.mode == "fail_source_seq1" and seq == 1 and src == self.flaky_source:
            return True
        if self.mode == "fail_once_source":
            if src == self.flaky_source and not self.flaky_failed:
                self.flaky_failed = True
                return True
        return False

    def _handler(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length) if length else b""
                try:
                    body = json.loads(raw or b"{}")
                except json.JSONDecodeError:
                    body = {}
                attempt = self.headers.get("X-Webhook-Attempt")
                if outer._should_fail(body, self.headers):
                    self.send_response(500)
                    self.send_header("Content-Length", "2")
                    self.end_headers()
                    self.wfile.write(b"no")
                    return
                with outer.lock:
                    outer.received.append(
                        (
                            body.get("source"),
                            body.get("sequence"),
                            body.get("event_id"),
                            int(attempt) if attempt else None,
                        )
                    )
                    sig = self.headers.get("X-Webhook-Signature")
                    if sig:
                        outer.signatures[(body.get("source"), body.get("sequence"))] = sig
                        outer.raw_bodies[(body.get("source"), body.get("sequence"))] = raw
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"ok")

        return Handler

    def sequences_for(self, source):
        with self.lock:
            return [seq for src, seq, _, _ in self.received if src == source]

    def attempts_for(self, source, sequence):
        with self.lock:
            return [att for src, seq, _, att in self.received if src == source and seq == sequence]


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def wait_until(fn, timeout=10.0, interval=0.02):
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return
        time.sleep(interval)
    raise AssertionError("condition not met before timeout")


@pytest.fixture
def receiver():
    rec = Receiver().start()
    try:
        yield rec
    finally:
        rec.stop()


@pytest.fixture
def subscribe(client):
    created = []

    def _make(rec, secret=""):
        r = client.post("/subscriptions", json={"url": rec.url(), "secret": secret})
        assert r.status_code == 201, r.text
        sub_id = r.json()["id"]
        created.append(sub_id)
        return sub_id

    yield _make

    # Deactivate so a later test's events are not fanned out to a receiver
    # whose port may have been recycled.
    for sub_id in created:
        client.post(f"/subscriptions/{sub_id}/deactivate")


def send_event(client, source, sequence, event_id, payload=None):
    body = {"source": source, "sequence": sequence, "event_id": event_id}
    if payload is not None:
        body["payload"] = payload
    r = client.post("/events", json=body)
    assert r.status_code == 200, r.text
    return r.json()
