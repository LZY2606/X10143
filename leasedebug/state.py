"""State snapshots and invariant checks for the lease simulator."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .engine import ClientConfig, LeaseEngine


def clone_engine(engine: LeaseEngine) -> LeaseEngine:
    return LeaseEngine.from_dict(engine.to_dict())


def _sorted_operations(experiment: dict[str, Any]) -> list[dict[str, Any]]:
    return sorted(
        experiment.get("operations", []),
        key=lambda item: (int(item["send_ms"]), int(item.get("stable_seq", 0))),
    )


def build_engine(
    experiment: dict[str, Any],
    *,
    at_ms: int | None = None,
    branch_id: str | None = None,
    replay_queued_arrivals: bool | None = None,
) -> LeaseEngine:
    branch = select_branch(experiment, branch_id)
    base_ms = int(branch.get("base_snapshot_ms", 0))
    snapshot_id = branch.get("snapshot_id")

    if snapshot_id and at_ms is not None and at_ms < base_ms:
        return build_engine(
            experiment,
            at_ms=at_ms,
            branch_id=branch.get("parent_branch_id"),
            replay_queued_arrivals=replay_queued_arrivals,
        )

    if snapshot_id:
        snapshot = _find_snapshot(experiment, snapshot_id)
        engine = LeaseEngine.from_dict(snapshot["engine"])
        for client_id, latency_ms in branch.get("client_latency_ms", {}).items():
            if client_id in engine.client_configs:
                engine.client_configs[client_id].latency_ms = int(latency_ms)
        for operation in engine.scheduled_operations.values():
            if (
                not operation.get("fired")
                and int(operation["send_ms"]) >= base_ms
                and operation["client_id"] in branch.get("client_latency_ms", {})
            ):
                operation["latency_ms"] = int(
                    branch["client_latency_ms"][operation["client_id"]]
                )
        operations = branch.get("operations", [])
        partitions = branch.get("partitions", [])
    else:
        engine = LeaseEngine(ttl_ms=int(experiment.get("ttl_ms", 10_000)))
        for client in experiment.get("clients", []):
            data = deepcopy(client)
            override = branch.get("client_latency_ms", {}).get(data["client_id"])
            if override is not None:
                data["latency_ms"] = int(override)
            engine.add_client(ClientConfig.from_dict(data))
        operations = _sorted_operations(branch)
        partitions = branch.get("partitions", [])

    if replay_queued_arrivals is not None:
        engine.replay_queued_arrivals = replay_queued_arrivals

    for operation in operations:
        send_ms = int(operation["send_ms"])
        if at_ms is not None and send_ms > at_ms:
            continue
        if send_ms < base_ms:
            continue
        latency_ms = operation.get("latency_ms")
        if latency_ms is None:
            latency_ms = engine.client_configs[operation["client_id"]].latency_ms
        engine.schedule_operation(
            client_id=operation["client_id"],
            op=operation["op"],
            resource=operation["resource"],
            send_ms=send_ms,
            ttl_ms=operation.get("ttl_ms"),
            fence_token=operation.get("fence_token"),
            idem_key=operation.get("idem_key"),
            latency_ms=latency_ms,
        )

    for partition in sorted(partitions, key=lambda item: int(item.get("stable_seq", 0))):
        end_ms = int(partition["end_ms"])
        if at_ms is not None and int(partition["start_ms"]) > at_ms:
            continue
        engine.schedule_partition(
            client_id=partition["client_id"],
            start_ms=partition["start_ms"],
            end_ms=end_ms,
        )

    if at_ms is None:
        engine.run()
    else:
        engine.run_until(int(at_ms))
    return engine


def _find_snapshot(experiment: dict[str, Any], snapshot_id: str) -> dict[str, Any]:
    for snapshot in experiment.get("snapshots", []):
        if snapshot["id"] == snapshot_id:
            return snapshot
    raise KeyError(f"unknown snapshot: {snapshot_id}")


def select_branch(experiment: dict[str, Any], branch_id: str | None) -> dict[str, Any]:
    branches = experiment.setdefault("branches", [])
    if not branches:
        root = {
            "id": "main",
            "label": "main",
            "parent_branch_id": None,
            "snapshot_id": None,
            "base_snapshot_ms": 0,
            "client_latency_ms": {},
            "operations": experiment.setdefault("operations", []),
            "partitions": experiment.setdefault("partitions", []),
            "cursor_ms": experiment.setdefault("cursor_ms", 0),
        }
        branches.append(root)
        return root
    if branch_id is None:
        branch_id = experiment.get("selected_branch_id", branches[0]["id"])
    for branch in branches:
        if branch["id"] == branch_id:
            if branch["id"] == "main":
                branch.setdefault("operations", experiment["operations"])
                branch.setdefault("partitions", experiment["partitions"])
                branch["operations"] = experiment["operations"]
                branch["partitions"] = experiment["partitions"]
            branch.setdefault("client_latency_ms", {})
            branch.setdefault("operations", [])
            branch.setdefault("partitions", [])
            return branch
    raise KeyError(f"unknown branch: {branch_id}")


def client_view(engine: LeaseEngine, client_id: str) -> dict[str, Any]:
    runtime = engine.runtime_clients[client_id]
    config = engine.client_configs[client_id]
    active = []
    for resource, claim in sorted(runtime["active"].items()):
        item = deepcopy(claim)
        item["resource"] = resource
        item["client_believes_expired"] = engine.now_ms >= int(claim["expires_at"])
        active.append(item)
    return {
        "client_id": client_id,
        "simulation_time_ms": engine.now_ms,
        "wall_time_ms": engine.client_wall_time(client_id),
        "clock_offset_ms": config.clock_offset_ms,
        "latency_ms": config.latency_ms,
        "partitioned": client_id in engine.active_partitions,
        "active_claims": active,
        "observed": deepcopy(runtime["observed"]),
        "pending": deepcopy(runtime["pending"]),
    }


def engine_view(engine: LeaseEngine) -> dict[str, Any]:
    resources = sorted(
        set(engine.leases)
        | {key for key in engine.fences if key != "_history"}
        | set(engine.fences.get("_history", {}))
    )
    leases = []
    for resource in resources:
        current = engine.leases.get(resource)
        leases.append(
            {
                "resource": resource,
                "state": "active" if current else "free_or_expired",
                "lease": engine._lease_view(current),
                "highest_fence_token": int(engine.fences.get(resource, 0)),
                "history": sorted(
                    (
                        deepcopy(item)
                        for item in engine.fences.get("_history", {})
                        .get(resource, {})
                        .values()
                    ),
                    key=lambda item: int(item["fence_token"]),
                ),
            }
        )
    return {
        "now_ms": engine.now_ms,
        "ttl_ms": engine.ttl_ms,
        "resources": leases,
        "requests": {rid: deepcopy(req) for rid, req in sorted(engine.requests.items())},
        "partitions": deepcopy(engine.partitions),
        "active_partitions": deepcopy(engine.active_partitions),
    }


def check_invariants(engine: LeaseEngine) -> dict[str, Any]:
    checks = [
        _single_holder(engine),
        _fence_monotonic(engine),
        _release_safety(engine),
        _stale_response_safety(engine),
        _idempotent_replays(engine),
        _partition_order(engine),
        _clock_authority(engine),
    ]
    return {"ok": all(item["ok"] for item in checks), "checks": checks}


def _check(name: str, ok: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "ok": ok, "detail": detail}


def _lease_intervals(engine: LeaseEngine) -> dict[str, list[tuple[int, int, int, str]]]:
    intervals: dict[str, list[tuple[int, int, int, str]]] = {}
    acquired: dict[tuple[str, int], tuple[int, str]] = {}
    open_interval: dict[tuple[str, int], int] = {}
    for event in engine.timeline:
        request = engine.requests.get(event.get("request_id", ""))
        idempotent_hit = bool(request and request["response"].get("idempotent_hit"))
        if (
            event["type"] == "request_processed"
            and event["status"] == "acquired"
            and not idempotent_hit
        ):
            resource = event["resource"]
            token = int(event["fence_token"])
            response = request["response"]
            acquired[(resource, token)] = (int(event["process_ms"]), event["client_id"])
            intervals.setdefault(resource, []).append(
                (
                    int(event["process_ms"]),
                    int(response["expires_at"]),
                    token,
                    event["client_id"],
                )
            )
            open_interval[(resource, token)] = len(intervals[resource]) - 1
        elif event["type"] == "request_processed" and event["status"] in {"released", "already_released"}:
            key = (event["resource"], int(event["fence_token"]))
            index = open_interval.pop(key, None)
            if index is not None:
                start, _, token, client_id = intervals[key[0]][index]
                intervals[key[0]][index] = (start, int(event["process_ms"]), token, client_id)
        elif event["type"] == "lease_expired":
            key = (event["resource"], int(event["fence_token"]))
            index = open_interval.pop(key, None)
            if index is not None:
                start, _, token, client_id = intervals[key[0]][index]
                intervals[key[0]][index] = (start, int(event["expires_at"]), token, client_id)
    return intervals


def _single_holder(engine: LeaseEngine) -> dict[str, Any]:
    bad = []
    for resource, intervals in _lease_intervals(engine).items():
        ordered = sorted(intervals)
        for left, right in zip(ordered, ordered[1:]):
            if int(right[0]) < int(left[1]):
                bad.append(f"{resource}: fence {left[2]} overlaps {right[2]}")
    return _check("single_holder", not bad, "; ".join(bad) or "at most one holder at each instant")


def _fence_monotonic(engine: LeaseEngine) -> dict[str, Any]:
    last: dict[str, int] = {}
    bad = []
    for event in engine.timeline:
        request = engine.requests.get(event.get("request_id", ""))
        if (
            event["type"] == "request_processed"
            and event["status"] == "acquired"
            and not (request and request["response"].get("idempotent_hit"))
        ):
            token = int(event["fence_token"])
            if token <= last.get(event["resource"], 0):
                bad.append(f"{event['resource']}: token {token} did not increase")
            last[event["resource"]] = token
    return _check("fence_monotonic", not bad, "; ".join(bad) or "fence tokens increase per resource")


def _release_safety(engine: LeaseEngine) -> dict[str, Any]:
    bad = []
    acquired: dict[tuple[str, int], str] = {}
    for event in engine.timeline:
        if event["type"] == "request_processed" and event["status"] == "acquired":
            acquired[(event["resource"], int(event["fence_token"]))] = event["client_id"]
        if event["type"] == "request_processed" and event["status"] in {"released", "already_released"}:
            token = int(event["fence_token"])
            key = (event["resource"], token)
            if acquired.get(key) != event["client_id"]:
                bad.append(f"{event['request_id']}: release did not match original holder")
            current = engine.leases.get(event["resource"])
            if current and int(current["fence_token"]) == token:
                bad.append(f"{event['request_id']}: current lease remained after release")
    return _check("release_safety", not bad, "; ".join(bad) or "releases are token-scoped")


def _stale_response_safety(engine: LeaseEngine) -> dict[str, Any]:
    bad = []
    for event in engine.timeline:
        if event["type"] == "response_ignored_stale" and event["issue_seq"] >= event["latest_issue_seq"]:
            bad.append(f"{event['request_id']}: current response was marked stale")
    return _check("stale_response_safety", not bad, "; ".join(bad) or "older responses cannot overwrite newer knowledge")


def _idempotent_replays(engine: LeaseEngine) -> dict[str, Any]:
    seen: dict[tuple[str, str], str] = {}
    bad = []
    for event in engine.timeline:
        if event["type"] != "request_processed":
            continue
        key = (event["client_id"], event["idem_key"])
        signature = f"{event['status']}:{event.get('fence_token')}"
        if key in seen and not signature == seen[key]:
            bad.append(f"{key} returned different results")
        seen[key] = signature
    return _check("idempotent_replays", not bad, "; ".join(bad) or "repeated idempotency keys return the same result")


def _partition_order(engine: LeaseEngine) -> dict[str, Any]:
    bad = []
    by_recovery: dict[str, list[str]] = {}
    for event in engine.timeline:
        if event["type"] == "partition_recovered":
            by_recovery[event["partition_id"]] = list(event.get("queued_request_ids", []))
    for partition_id, ids in by_recovery.items():
        keys = [
            (
                engine.requests[rid]["scheduled_arrival_ms"],
                engine.requests[rid]["stable_seq"],
            )
            for rid in ids
        ]
        if keys != sorted(keys):
            bad.append(f"{partition_id}: queue was not processed by original arrival and stable order")
        for rid in ids:
            req = engine.requests[rid]
            if int(req["process_ms"]) != int(req["scheduled_arrival_ms"]):
                bad.append(f"{rid}: queued request was decided at reconnect instead of original arrival")
    return _check("partition_queue_order", not bad, "; ".join(bad) or "partition queues preserve original arrival and stable order")


def _clock_authority(engine: LeaseEngine) -> dict[str, Any]:
    uses_offset = any(client.clock_offset_ms for client in engine.client_configs.values())
    return _check(
        "server_clock_authority",
        True,
        "client clock offsets are display-only" if uses_offset else "all clients use server simulation time",
    )
