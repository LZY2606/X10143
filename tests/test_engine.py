from leasedebug.engine import ClientConfig, LeaseEngine
from leasedebug.state import check_invariants


def run_engine(events, ttl_ms=100):
    clients = {
        client_id: ClientConfig(client_id, clock_offset_ms=offset, latency_ms=latency)
        for client_id, offset, latency in [("c1", 100, 10), ("c2", -50, 10)]
    }
    engine = LeaseEngine(list(clients.values()), ttl_ms=ttl_ms)
    for event in events:
        kind = event[0]
        if kind == "part":
            _, client_id, start_ms, end_ms = event
            engine.schedule_partition(client_id=client_id, start_ms=start_ms, end_ms=end_ms)
        else:
            _, client_id, op, resource, send_ms, *rest = event
            engine.schedule_operation(
                client_id=client_id,
                op=op,
                resource=resource,
                send_ms=send_ms,
                **(rest[0] if rest else {}),
            )
    engine.run()
    assert check_invariants(engine)["ok"]
    return engine


def processed(engine, request_id):
    return engine.requests[request_id]["response"]


def test_simultaneous_arrivals_use_stable_order():
    engine = run_engine([
        ("op", "c1", "acquire", "r", 0, {"latency_ms": 10}),
        ("op", "c2", "acquire", "r", 0, {"latency_ms": 10}),
    ])
    assert processed(engine, "req-1")["status"] == "acquired"
    assert processed(engine, "req-2")["error_code"] == "lease_held"
    assert engine.requests["req-1"]["process_ms"] == 10
    assert engine.requests["req-2"]["process_ms"] == 10


def test_expiration_boundary_is_inclusive():
    before = run_engine([
        ("op", "c1", "acquire", "r", 0, {"ttl_ms": 100, "latency_ms": 0}),
        ("op", "c1", "renew", "r", 100, {"fence_token": 1, "latency_ms": 0}),
    ], ttl_ms=100)
    assert processed(before, "req-2")["error_code"] == "lease_expired"

    success = run_engine([
        ("op", "c1", "acquire", "r", 0, {"ttl_ms": 100, "latency_ms": 0}),
        ("op", "c1", "renew", "r", 99, {"fence_token": 1, "latency_ms": 0}),
    ], ttl_ms=100)
    assert processed(success, "req-2")["status"] == "renewed"


def test_late_old_renew_response_cannot_overwrite_newer_knowledge():
    engine = run_engine([
        ("op", "c1", "acquire", "r", 0, {"idem_key": "k1", "latency_ms": 0}),
        ("op", "c1", "renew", "r", 10, {"idem_key": "k2", "fence_token": 1, "latency_ms": 30}),
        ("op", "c1", "release", "r", 20, {"idem_key": "k3", "fence_token": 1, "latency_ms": 0}),
        ("op", "c1", "acquire", "r", 30, {"idem_key": "k4", "latency_ms": 0}),
    ])
    assert engine.requests["req-2"]["status"] == "stale_ignored"
    claim = engine.runtime_clients["c1"]["active"]["r"]
    assert claim["fence_token"] == 2
    stale_event = next(event for event in engine.timeline if event["type"] == "response_ignored_stale")
    assert stale_event["issue_seq"] < stale_event["latest_issue_seq"]


def test_release_after_expiry_is_idempotent_and_does_not_release_later_holder():
    engine = run_engine([
        ("op", "c1", "acquire", "r", 0, {"ttl_ms": 10, "latency_ms": 0}),
        ("op", "c1", "release", "r", 30, {"fence_token": 1, "idem_key": "old", "latency_ms": 0}),
        ("op", "c2", "acquire", "r", 40, {"ttl_ms": 50, "latency_ms": 0}),
        ("op", "c1", "release", "r", 50, {"fence_token": 1, "idem_key": "old", "latency_ms": 0}),
    ])
    assert processed(engine, "req-2")["status"] == "already_released"
    assert processed(engine, "req-3")["status"] == "acquired"
    assert processed(engine, "req-4")["status"] == "already_released"
    assert engine.leases["r"]["fence_token"] == 2
    assert engine.leases["r"]["holder_client_id"] == "c2"


def test_active_release_allows_reacquire():
    engine = run_engine([
        ("op", "c1", "acquire", "r", 0, {"ttl_ms": 100, "latency_ms": 0}),
        ("op", "c1", "release", "r", 20, {"fence_token": 1, "idem_key": "release-1", "latency_ms": 0}),
        ("op", "c2", "acquire", "r", 21, {"ttl_ms": 100, "latency_ms": 0}),
    ])
    assert processed(engine, "req-2")["status"] == "released"
    assert processed(engine, "req-3")["status"] == "acquired"
    assert processed(engine, "req-3")["fence_token"] == 2
    assert engine.leases["r"]["holder_client_id"] == "c2"


def test_partition_queue_uses_original_arrival_and_stable_order():
    engine = run_engine([
        ("part", "c1", 5, 50),
        ("op", "c1", "acquire", "r", 0, {"latency_ms": 10}),
        ("op", "c1", "read", "r", 20, {"latency_ms": 10}),
        ("op", "c1", "acquire", "other", 21, {"latency_ms": 9}),
    ])
    assert [engine.requests[f"req-{n}"]["process_ms"] for n in (1, 2, 3)] == [10, 30, 30]
    assert [engine.requests[f"req-{n}"]["response_delivery_ms"] for n in (1, 2, 3)] == [50, 50, 50]
    recovery = next(event for event in engine.timeline if event["type"] == "partition_recovered")
    assert recovery["queued_request_ids"] == ["req-1", "req-2", "req-3"]
    assert processed(engine, "req-1")["fence_token"] == 1
    assert processed(engine, "req-2")["holder_client_id"] == "c1"


def test_idempotent_retry_returns_same_result():
    engine = run_engine([
        ("op", "c1", "acquire", "r", 0, {"idem_key": "same", "latency_ms": 0}),
        ("op", "c1", "acquire", "r", 10, {"idem_key": "same", "latency_ms": 0}),
        ("op", "c1", "acquire", "r", 20, {"idem_key": "same", "latency_ms": 0}),
    ])
    assert [processed(engine, f"req-{n}")["status"] for n in (1, 2, 3)] == ["acquired", "acquired", "acquired"]
    assert [processed(engine, f"req-{n}")["idempotent_hit"] for n in (1, 2, 3)] == [False, True, True]
    assert [processed(engine, f"req-{n}")["fence_token"] for n in (1, 2, 3)] == [1, 1, 1]


def test_server_uses_server_time_not_client_wall_offset():
    engine = run_engine([
        ("op", "c1", "acquire", "r", 0, {"ttl_ms": 100, "latency_ms": 0}),
        ("op", "c1", "renew", "r", 99, {"fence_token": 1, "latency_ms": 0}),
    ], ttl_ms=100)
    assert processed(engine, "req-2")["status"] == "renewed"
    assert engine.client_wall_time("c1", 99) == 199


def test_snapshot_fork_branches_are_isolated(tmp_path):
    from leasedebug.store import Store

    store = Store(tmp_path / "data.json")
    exp = store.create_experiment(
        name="fork",
        ttl_ms=100,
        clients=[
            {"client_id": "c1", "clock_offset_ms": 0, "latency_ms": 0},
            {"client_id": "c2", "clock_offset_ms": 0, "latency_ms": 0},
        ],
    )
    experiment_id = exp["id"]
    store.add_operation(experiment_id, {"client_id":"c1","op":"acquire","resource":"r","send_ms":0})
    main = store.fast_forward(experiment_id)
    assert main["summary"]["final_fence_tokens"] == {"r": 1}

    store.seek(experiment_id, 0)
    store.fork(experiment_id, label="higher-latency", client_id="c2", latency_ms=20)
    store.add_operation(experiment_id, {"client_id":"c1","op":"release","resource":"r","send_ms":10,"fence_token":1})
    store.add_operation(experiment_id, {"client_id":"c2","op":"acquire","resource":"r","send_ms":10})
    forked = store.fast_forward(experiment_id)
    assert forked["server"]["resources"][0]["lease"]["holder_client_id"] == "c2"

    store.select_branch(experiment_id, "main")
    store.add_operation(experiment_id, {"client_id":"c2","op":"acquire","resource":"r","send_ms":10})
    main_again = store.fast_forward(experiment_id)
    assert main_again["server"]["requests"]["req-2"]["response"]["error_code"] == "lease_held"
    assert len(main_again["server"]["requests"]) == 2


def test_restart_recovery_and_export_import_are_deterministic(tmp_path):
    from leasedebug.store import Store

    path = tmp_path / "data.json"
    store = Store(path)
    exp = store.create_experiment(
        name="restart",
        ttl_ms=50,
        clients=[{"client_id": "c1", "clock_offset_ms": 7, "latency_ms": 5}],
    )
    experiment_id = exp["id"]
    store.add_operation(experiment_id, {"client_id":"c1","op":"acquire","resource":"r","send_ms":0})
    store.add_operation(experiment_id, {"client_id":"c1","op":"renew","resource":"r","send_ms":20,"fence_token":1})
    store.add_partition(experiment_id, {"client_id":"c1","start_ms":30,"end_ms":40})
    store.fast_forward(experiment_id)
    exported = store.export(experiment_id)

    restored = Store(path)
    restored_state = restored.state(experiment_id)
    imported = Store(tmp_path / "other.json")
    imported_exp = imported.import_experiment(exported)
    imported_state = imported.state(imported_exp["id"])

    for state in (restored_state, imported_state):
        assert state["summary"] == restored_state["summary"]
        assert state["summary"]["final_fence_tokens"] == {"r": 1}
        assert state["timeline"] == restored_state["timeline"]
        assert state["server"]["requests"] == restored_state["server"]["requests"]
        assert state["invariants"]["ok"]
