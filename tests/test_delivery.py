import hashlib
import hmac
import json
import time

from conftest import Receiver, send_event, wait_until


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_out_of_order_buffered_and_delivered_in_sequence(client, receiver, subscribe):
    sub_id = subscribe(receiver)
    # Scrambled arrival order; cursor anchors at seq 1 (lowest observed).
    send_event(client, "t-order", 3, "t-order-3")
    send_event(client, "t-order", 1, "t-order-1")
    send_event(client, "t-order", 2, "t-order-2")

    wait_until(lambda: receiver.sequences_for("t-order") == [1, 2, 3])

    r = client.get(
        "/backlog", params={"subscription_id": sub_id, "source": "t-order"}
    )
    assert r.json()["total"] == 0


def test_duplicate_event_id_is_idempotent(client, receiver, subscribe):
    subscribe(receiver)
    send_event(client, "t-idem", 1, "t-dup-1", payload={"hello": "world"})
    wait_until(lambda: receiver.sequences_for("t-idem") == [1])

    r = client.post(
        "/events",
        json={
            "source": "t-idem",
            "sequence": 1,
            "event_id": "t-dup-1",
            "payload": {"different": True},
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["duplicate"] is True
    assert body["status"] == "duplicate"

    time.sleep(0.3)
    assert receiver.sequences_for("t-idem") == [1]  # delivered exactly once


def test_same_source_sequence_conflict_rejected(client, receiver, subscribe):
    subscribe(receiver)
    send_event(client, "t-conflict", 1, "t-conflict-a")
    r = client.post(
        "/events",
        json={"source": "t-conflict", "sequence": 1, "event_id": "t-conflict-b"},
    )
    assert r.status_code == 409


def test_failing_head_blocks_then_dead_letters_then_continues_and_replays(
    client, subscribe
):
    rec = Receiver(mode="fail_seq1").start()
    sub_id = subscribe(rec)
    try:
        send_event(client, "t-block", 1, "t-blk-1")
        send_event(client, "t-block", 2, "t-blk-2")
        send_event(client, "t-block", 3, "t-blk-3")

        # While seq 1 is failing/retrying, later sequences must not pass it.
        deadline = time.time() + 1.2
        while time.time() < deadline:
            assert 2 not in rec.sequences_for("t-block")
            assert 3 not in rec.sequences_for("t-block")
            time.sleep(0.02)

        def blk1_dead():
            r = client.get("/dead-letters", params={"source": "t-block"})
            return any(
                i["sequence"] == 1 and i["subscription_id"] == sub_id
                for i in r.json()["items"]
            )

        wait_until(blk1_dead, timeout=20)

        # Once dead, later sequences proceed, in order.
        wait_until(lambda: rec.sequences_for("t-block") == [2, 3])

        r = client.get("/dead-letters", params={"source": "t-block"})
        dead = [
            i
            for i in r.json()["items"]
            if i["sequence"] == 1 and i["subscription_id"] == sub_id
        ][0]
        assert dead["attempts"] == 5
        assert dead["status"] == "dead"
        assert dead["last_status"] == 500
        delivery_id = dead["id"]

        # Replay while the endpoint still rejects -> it returns to dead.
        r = client.post(f"/dead-letters/{delivery_id}/replay")
        assert r.status_code == 200
        wait_until(
            lambda: client.get(f"/deliveries/{delivery_id}").json()["status"]
            == "dead",
            timeout=20,
        )
        assert rec.sequences_for("t-block") == [2, 3]

        # Replay after the endpoint is fixed -> delivered, still in order.
        rec.mode = "ok"
        r = client.post(f"/dead-letters/{delivery_id}/replay")
        assert r.status_code == 200
        wait_until(lambda: 1 in rec.sequences_for("t-block"))
        assert (
            client.get(f"/deliveries/{delivery_id}").json()["status"] == "delivered"
        )

        # Replaying a live delivery is rejected.
        r = client.post(f"/dead-letters/{delivery_id}/replay")
        assert r.status_code == 409
    finally:
        rec.stop()


def test_transient_failure_is_retried_with_backoff(client, subscribe):
    rec = Receiver(mode="fail_once_source", flaky_source="t-flaky").start()
    subscribe(rec)
    try:
        t0 = time.time()
        send_event(client, "t-flaky", 1, "t-flaky-1")

        wait_until(lambda: rec.sequences_for("t-flaky") == [1])
        elapsed = time.time() - t0
        # First attempt fails; second attempt happens after base delay (0.5s).
        assert elapsed >= 0.3
        assert rec.attempts_for("t-flaky", 1) == [2]
    finally:
        rec.stop()


def test_sources_are_independent(client, subscribe):
    rec = Receiver(mode="fail_source_seq1", flaky_source="t-A").start()
    subscribe(rec)
    try:
        send_event(client, "t-A", 1, "t-A-1")
        send_event(client, "t-B", 1, "t-B-1")
        send_event(client, "t-B", 2, "t-B-2")

        # t-B flows while t-A is stuck.
        wait_until(lambda: rec.sequences_for("t-B") == [1, 2])
        time.sleep(0.4)
        assert rec.sequences_for("t-A") == []

        # t-A eventually dead-letters.
        wait_until(
            lambda: any(
                i["source"] == "t-A"
                for i in client.get(
                    "/dead-letters", params={"source": "t-A"}
                ).json()["items"]
            ),
            timeout=20,
        )
    finally:
        rec.stop()


def test_backlog_reports_buffered_state(client, subscribe):
    rec = Receiver(mode="fail").start()
    sub_id = subscribe(rec)
    try:
        send_event(client, "t-back", 1, "t-back-1")
        send_event(client, "t-back", 2, "t-back-2")
        send_event(client, "t-back", 3, "t-back-3")

        r = client.get(
            "/backlog", params={"subscription_id": sub_id, "source": "t-back"}
        )
        data = r.json()
        assert data["total"] >= 2
        by_seq = {i["sequence"]: i for i in data["items"]}
        assert by_seq[1]["buffered"] is False
        assert by_seq[1]["awaiting_sequence"] == 1
        assert by_seq[2]["buffered"] is True
        assert by_seq[3]["buffered"] is True
    finally:
        rec.stop()


def test_hmac_signature_header_is_sent(client, subscribe):
    rec = Receiver().start()
    subscribe(rec, secret="s3cret")
    try:
        send_event(client, "t-hmac", 1, "t-hmac-1", payload={"a": 1})
        wait_until(lambda: rec.sequences_for("t-hmac") == [1])

        sig = rec.signatures[("t-hmac", 1)]
        assert sig.startswith("sha256=")
        raw = rec.raw_bodies[("t-hmac", 1)]
        expected = hmac.new(b"s3cret", raw, hashlib.sha256).hexdigest()
        assert hmac.compare_digest(sig, f"sha256={expected}")
        assert json.loads(raw) == {
            "event_id": "t-hmac-1",
            "source": "t-hmac",
            "sequence": 1,
            "payload": {"a": 1},
        }
    finally:
        rec.stop()


def test_deactivate_stops_delivery_activate_resumes(client, subscribe):
    rec = Receiver().start()
    sub_id = subscribe(rec)
    try:
        client.post(f"/subscriptions/{sub_id}/deactivate")
        send_event(client, "t-pause", 1, "t-pause-1")
        time.sleep(0.4)
        assert rec.sequences_for("t-pause") == []

        client.post(f"/subscriptions/{sub_id}/activate")
        wait_until(lambda: rec.sequences_for("t-pause") == [1])
    finally:
        rec.stop()


def test_stats_endpoint(client):
    r = client.get("/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["subscriptions"] >= 1
    assert "events" in body
    assert set(body["deliveries"].keys()) == {
        "pending",
        "retrying",
        "delivered",
        "dead",
    }
