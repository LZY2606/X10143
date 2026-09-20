"""JSON persistence and experiment/snapshot operations."""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .engine import LeaseEngine
from .state import build_engine, check_invariants, client_view, engine_view, select_branch


def now_label() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Store:
    def __init__(self, path: str | Path = ".leasedebug-data.json") -> None:
        self.path = Path(path)
        self.data = self._load()

    def _load(self) -> dict[str, Any]:
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        return {"version": 1, "experiments": []}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", dir=str(self.path.parent), text=True
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.data, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(tmp_name, self.path)
        except Exception:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise

    def list_experiments(self) -> list[dict[str, Any]]:
        return self.data["experiments"]

    def get_experiment(self, experiment_id: str) -> dict[str, Any]:
        for experiment in self.data["experiments"]:
            if experiment["id"] == experiment_id:
                return experiment
        raise KeyError(f"unknown experiment: {experiment_id}")

    def create_experiment(
        self,
        *,
        clients: list[dict[str, Any]] | None = None,
        ttl_ms: int = 10_000,
        name: str = "未命名实验",
    ) -> dict[str, Any]:
        clients = clients or [
            {"client_id": "c1", "clock_offset_ms": 0, "latency_ms": 0},
            {"client_id": "c2", "clock_offset_ms": 0, "latency_ms": 0},
        ]
        if not clients:
            raise ValueError("at least one client is required")
        ids = [str(client["client_id"]) for client in clients]
        if len(ids) != len(set(ids)):
            raise ValueError("client ids must be unique")
        experiment_id = new_id("exp")
        experiment = {
            "id": experiment_id,
            "version": 1,
            "name": name,
            "created_at": now_label(),
            "ttl_ms": int(ttl_ms),
            "clients": deepcopy(clients),
            "operations": [],
            "partitions": [],
            "snapshots": [],
            "branches": [
                {
                    "id": "main",
                    "label": "main",
                    "parent_branch_id": None,
                    "snapshot_id": None,
                    "base_snapshot_ms": 0,
                    "client_latency_ms": {},
                    "operations": [],
                    "partitions": [],
                    "cursor_ms": 0,
                    "created_at": now_label(),
                }
            ],
            "selected_branch_id": "main",
            "cursor_ms": 0,
        }
        self.data["experiments"].append(experiment)
        self.save()
        return experiment

    def add_operation(self, experiment_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        branch = self.selected_branch(experiment)
        stable_seq = 1 + max(
            [int(item.get("stable_seq", 0)) for item in experiment["operations"]]
            + [int(item.get("stable_seq", 0)) for item in branch["operations"]]
            + [int(item.get("stable_seq", 0)) for item in experiment["partitions"]]
            + [int(item.get("stable_seq", 0)) for item in branch["partitions"]],
            default=0,
        )
        client_id = str(payload["client_id"])
        if client_id not in {item["client_id"] for item in experiment["clients"]}:
            raise ValueError(f"unknown client: {client_id}")
        operation = {
            "id": new_id("op"),
            "branch_id": branch["id"],
            "client_id": client_id,
            "op": str(payload["op"]),
            "resource": str(payload["resource"]),
            "send_ms": int(payload["send_ms"]),
            "ttl_ms": int(payload["ttl_ms"]) if payload.get("ttl_ms") is not None else None,
            "fence_token": (
                int(payload["fence_token"]) if payload.get("fence_token") is not None else None
            ),
            "idem_key": str(payload.get("idem_key") or new_id("idem")),
            "latency_ms": (
                int(payload["latency_ms"])
                if payload.get("latency_ms") is not None
                else int(branch["client_latency_ms"].get(
                    client_id,
                    self._client_config(experiment, client_id)["latency_ms"],
                ))
            ),
            "stable_seq": stable_seq,
        }
        if operation["op"] not in {"acquire", "renew", "release", "read"}:
            raise ValueError("unknown operation")
        target = experiment["operations"] if branch["id"] == "main" else branch["operations"]
        target.append(operation)
        self.save()
        return operation

    def add_client(self, experiment_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        client = {
            "client_id": str(payload["client_id"]),
            "clock_offset_ms": int(payload.get("clock_offset_ms", 0)),
            "latency_ms": int(payload.get("latency_ms", 0)),
        }
        if any(item["client_id"] == client["client_id"] for item in experiment["clients"]):
            raise ValueError("duplicate client id")
        if experiment["operations"] or experiment["partitions"]:
            raise ValueError("clients can only be added before scheduling events")
        experiment["clients"].append(client)
        self.save()
        return client

    def update_client(self, experiment_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        client_id = str(payload["client_id"])
        for client in experiment["clients"]:
            if client["client_id"] == client_id:
                client["clock_offset_ms"] = int(payload.get("clock_offset_ms", client["clock_offset_ms"]))
                client["latency_ms"] = int(payload.get("latency_ms", client["latency_ms"]))
                self.save()
                return client
        raise KeyError(client_id)

    def add_partition(self, experiment_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        branch = self.selected_branch(experiment)
        client_id = str(payload["client_id"])
        start_ms, end_ms = int(payload["start_ms"]), int(payload["end_ms"])
        if end_ms < start_ms:
            raise ValueError("partition end cannot precede start")
        if any(
            item["client_id"] == client_id
            and int(item["start_ms"]) < end_ms
            and start_ms < int(item["end_ms"])
            for item in experiment["partitions"] + branch["partitions"]
        ):
            raise ValueError("overlapping partition for client")
        stable_seq = 1 + max(
            [int(item.get("stable_seq", 0)) for item in experiment["operations"]]
            + [int(item.get("stable_seq", 0)) for item in branch["operations"]]
            + [int(item.get("stable_seq", 0)) for item in experiment["partitions"]]
            + [int(item.get("stable_seq", 0)) for item in branch["partitions"]],
            default=0,
        )
        partition = {
            "id": new_id("part"),
            "branch_id": branch["id"],
            "client_id": client_id,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "stable_seq": stable_seq,
        }
        target = experiment["partitions"] if branch["id"] == "main" else branch["partitions"]
        target.append(partition)
        self.save()
        return partition

    def selected_branch(self, experiment: dict[str, Any]) -> dict[str, Any]:
        return select_branch(experiment, experiment.get("selected_branch_id"))

    def _client_config(self, experiment: dict[str, Any], client_id: str) -> dict[str, Any]:
        for client in experiment["clients"]:
            if client["client_id"] == client_id:
                return client
        raise KeyError(client_id)

    def select_branch(self, experiment_id: str, branch_id: str) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        select_branch(experiment, branch_id)
        experiment["selected_branch_id"] = branch_id
        self.save()
        return experiment

    def seek(self, experiment_id: str, target_ms: int) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        branch = self.selected_branch(experiment)
        target_ms = max(0, int(target_ms))
        branch["cursor_ms"] = target_ms
        experiment["cursor_ms"] = target_ms
        self.save()
        return self.state(experiment_id, at_ms=target_ms)

    def step(self, experiment_id: str) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        branch = self.selected_branch(experiment)
        engine = build_engine(experiment, at_ms=branch["cursor_ms"], branch_id=branch["id"])
        nxt = engine.next_event_time()
        target = nxt if nxt is not None else branch["cursor_ms"]
        return self.seek(experiment_id, target)

    def fast_forward(self, experiment_id: str) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        branch = self.selected_branch(experiment)
        engine = build_engine(experiment, branch_id=branch["id"])
        target = engine.now_ms
        return self.seek(experiment_id, target)

    def snapshot(self, experiment_id: str, label: str = "快照") -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        branch = self.selected_branch(experiment)
        at_ms = int(branch["cursor_ms"])
        engine = build_engine(experiment, at_ms=at_ms, branch_id=branch["id"])
        snapshot = {
            "id": new_id("snap"),
            "branch_id": branch["id"],
            "label": label,
            "at_ms": at_ms,
            "created_at": now_label(),
            "engine": engine.to_dict(),
            "summary": self.summary(experiment, engine),
        }
        experiment["snapshots"].append(snapshot)
        self.save()
        return snapshot

    def fork(
        self,
        experiment_id: str,
        *,
        label: str,
        client_id: str | None = None,
        latency_ms: int | None = None,
    ) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        parent = self.selected_branch(experiment)
        at_ms = int(parent["cursor_ms"])
        snap = self.snapshot(experiment_id, f"{label} 基础快照")
        child = {
            "id": new_id("branch"),
            "label": label,
            "parent_branch_id": parent["id"],
            "snapshot_id": snap["id"],
            "base_snapshot_ms": at_ms,
            "client_latency_ms": deepcopy(parent.get("client_latency_ms", {})),
            "operations": [],
            "partitions": [],
            "cursor_ms": at_ms,
            "created_at": now_label(),
        }
        if client_id is not None and latency_ms is not None:
            child["client_latency_ms"][str(client_id)] = int(latency_ms)
        experiment["branches"].append(child)
        experiment["selected_branch_id"] = child["id"]
        experiment["cursor_ms"] = at_ms
        self.save()
        return child

    def update_latency(self, experiment_id: str, client_id: str, latency_ms: int) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        branch = self.selected_branch(experiment)
        self._client_config(experiment, client_id)
        branch.setdefault("client_latency_ms", {})[client_id] = int(latency_ms)
        self.save()
        return branch

    def state(
        self,
        experiment_id: str,
        *,
        at_ms: int | None = None,
        client_id: str | None = None,
    ) -> dict[str, Any]:
        experiment = self.get_experiment(experiment_id)
        branch = self.selected_branch(experiment)
        target = branch["cursor_ms"] if at_ms is None else int(at_ms)
        engine = build_engine(experiment, at_ms=target, branch_id=branch["id"])
        selected_client = client_id or experiment["clients"][0]["client_id"]
        max_time = self.max_time(experiment, branch["id"])
        result = {
            "experiment": self.public_experiment(experiment),
            "branch": self.public_branch(branch),
            "server": engine_view(engine),
            "clients": [client_view(engine, item["client_id"]) for item in experiment["clients"]],
            "selected_client_id": selected_client,
            "selected_client": client_view(engine, selected_client),
            "timeline": deepcopy(engine.timeline),
            "invariants": check_invariants(engine),
            "summary": self.summary(experiment, engine),
            "max_time_ms": max_time,
            "finished": engine.finished() and target >= max_time,
        }
        return result

    def max_time(self, experiment: dict[str, Any], branch_id: str) -> int:
        branch = select_branch(experiment, branch_id)
        engine = build_engine(experiment, branch_id=branch_id)
        return max([branch["base_snapshot_ms"], engine.now_ms, branch.get("cursor_ms", 0)])

    def summary(self, experiment: dict[str, Any], engine: LeaseEngine) -> dict[str, Any]:
        request_events = [event for event in engine.timeline if event["type"] == "request_processed"]
        return {
            "simulation_time_ms": engine.now_ms,
            "request_count": len(engine.requests),
            "processed_count": len(request_events),
            "active_lease_count": len(engine.leases),
            "timeline_event_count": len(engine.timeline),
            "invariants_ok": check_invariants(engine)["ok"],
            "final_fence_tokens": {
                resource: int(token)
                for resource, token in sorted(engine.fences.items())
                if resource != "_history"
            },
        }

    def public_experiment(self, experiment: dict[str, Any]) -> dict[str, Any]:
        data = deepcopy(experiment)
        for snapshot in data.get("snapshots", []):
            snapshot.pop("engine", None)
        return data

    def public_branch(self, branch: dict[str, Any]) -> dict[str, Any]:
        return deepcopy(branch)

    def export(self, experiment_id: str) -> dict[str, Any]:
        return deepcopy(self.get_experiment(experiment_id))

    def import_experiment(self, payload: dict[str, Any], *, new_identity: bool = False) -> dict[str, Any]:
        experiment = deepcopy(payload)
        required = {"id", "clients", "operations", "partitions", "branches"}
        if not required.issubset(experiment):
            raise ValueError("invalid experiment export")
        if new_identity or any(item["id"] == experiment["id"] for item in self.data["experiments"]):
            experiment["id"] = new_id("exp")
        self.data["experiments"].append(experiment)
        self.save()
        return experiment
