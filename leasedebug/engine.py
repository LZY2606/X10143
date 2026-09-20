"""Deterministic lease simulation driven by an integer virtual clock."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any


def _as_millis(value: int | float) -> int:
    return int(value)


@dataclass
class ClientConfig:
    client_id: str
    clock_offset_ms: int = 0
    latency_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "client_id": self.client_id,
            "clock_offset_ms": self.clock_offset_ms,
            "latency_ms": self.latency_ms,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClientConfig":
        return cls(
            client_id=str(data["client_id"]),
            clock_offset_ms=int(data.get("clock_offset_ms", 0)),
            latency_ms=int(data.get("latency_ms", 0)),
        )


class LeaseEngine:
    """Replayable, serializable event-sourced lease server simulation.

    Time is an integer millisecond virtual clock. Client clock offsets affect
    presentation only and never enter server-side authorization decisions.
    """

    def __init__(
        self,
        clients: list[ClientConfig] | list[dict[str, Any]] | None = None,
        ttl_ms: int = 10_000,
        now_ms: int = 0,
    ) -> None:
        self.now_ms = _as_millis(now_ms)
        self.ttl_ms = _as_millis(ttl_ms)
        self.client_configs: dict[str, ClientConfig] = {}
        self.fences: dict[str, int] = {}
        self.leases: dict[str, dict[str, Any]] = {}
        self.idem: dict[tuple[str, str], dict[str, Any]] = {}
        self.requests: dict[str, dict[str, Any]] = {}
        self.runtime_clients: dict[str, dict[str, Any]] = {}
        self.scheduled_operations: dict[str, dict[str, Any]] = {}
        self.partitions: dict[str, dict[str, Any]] = {}
        self.active_partitions: dict[str, str] = {}
        self.event_heap: list[dict[str, Any]] = []
        self.queued_requests: dict[str, list[str]] = {}
        self.held_responses: dict[str, list[str]] = {}
        self.timeline: list[dict[str, Any]] = []
        self.stable_seq = 0
        self.request_seq = 0
        self.operation_seq = 0
        self.partition_seq = 0
        self.replay_queued_arrivals = False

        for client in clients or []:
            self.add_client(client)

    def add_client(
        self,
        client: ClientConfig | dict[str, Any],
        *,
        clock_offset_ms: int | None = None,
        latency_ms: int | None = None,
    ) -> ClientConfig:
        if isinstance(client, ClientConfig):
            config = deepcopy(client)
        else:
            config = ClientConfig.from_dict(client)
        if clock_offset_ms is not None:
            config.clock_offset_ms = int(clock_offset_ms)
        if latency_ms is not None:
            config.latency_ms = int(latency_ms)
        if config.client_id in self.client_configs:
            raise ValueError(f"duplicate client: {config.client_id}")
        self.client_configs[config.client_id] = config
        self._init_runtime(config.client_id)
        self._log(
            "client_added",
            client_id=config.client_id,
            clock_offset_ms=config.clock_offset_ms,
            latency_ms=config.latency_ms,
        )
        return config

    def _init_runtime(self, client_id: str) -> None:
        self.runtime_clients[client_id] = {
            "client_id": client_id,
            "issue_seq": 0,
            "active": {},
            "observed": {},
            "pending": [],
        }
        self.queued_requests.setdefault(client_id, [])
        self.held_responses.setdefault(client_id, [])

    def client_wall_time(self, client_id: str, when_ms: int | None = None) -> int:
        when = self.now_ms if when_ms is None else int(when_ms)
        return when + self.client_configs[client_id].clock_offset_ms

    def schedule_operation(
        self,
        *,
        client_id: str,
        op: str,
        resource: str,
        send_ms: int,
        ttl_ms: int | None = None,
        fence_token: int | None = None,
        idem_key: str | None = None,
        latency_ms: int | None = None,
    ) -> dict[str, Any]:
        self._require_client(client_id)
        if op not in {"acquire", "renew", "release", "read"}:
            raise ValueError(f"unknown operation: {op}")
        send_ms = int(send_ms)
        self.operation_seq += 1
        event_id = f"op-{self.operation_seq}"
        event = {
            "id": event_id,
            "client_id": client_id,
            "op": op,
            "resource": resource,
            "send_ms": send_ms,
            "ttl_ms": int(ttl_ms) if ttl_ms is not None else None,
            "fence_token": int(fence_token) if fence_token is not None else None,
            "idem_key": idem_key or f"auto-{client_id}-{self.operation_seq}",
            "latency_ms": (
                int(latency_ms)
                if latency_ms is not None
                else self.client_configs[client_id].latency_ms
            ),
            "stable_seq": self._next_stable(),
            "fired": False,
        }
        self.scheduled_operations[event_id] = event
        self._push(
            self.event_heap,
            send_ms,
            event["stable_seq"],
            "scheduled_send",
            phase=1,
            operation_id=event_id,
        )
        return deepcopy(event)

    def schedule_partition(
        self,
        *,
        client_id: str,
        start_ms: int,
        end_ms: int,
    ) -> dict[str, Any]:
        self._require_client(client_id)
        start_ms, end_ms = int(start_ms), int(end_ms)
        if end_ms < start_ms:
            raise ValueError("partition end cannot precede start")
        self.partition_seq += 1
        partition_id = f"part-{self.partition_seq}"
        event = {
            "id": partition_id,
            "client_id": client_id,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "stable_seq": self._next_stable(),
        }
        self.partitions[partition_id] = event
        self._push(
            self.event_heap,
            start_ms,
            event["stable_seq"],
            "partition_start",
            phase=0,
            partition_id=partition_id,
        )
        self._push(
            self.event_heap,
            end_ms,
            event["stable_seq"],
            "partition_end",
            phase=4,
            partition_id=partition_id,
        )
        return deepcopy(event)

    def _next_stable(self) -> int:
        self.stable_seq += 1
        return self.stable_seq

    def _require_client(self, client_id: str) -> None:
        if client_id not in self.client_configs:
            raise ValueError(f"unknown client: {client_id}")

    def _push(
        self,
        heap: list[dict[str, Any]],
        when_ms: int,
        stable_seq: int,
        kind: str,
        phase: int = 2,
        **payload: Any,
    ) -> dict[str, Any]:
        item = {
            "when_ms": int(when_ms),
            "stable_seq": int(stable_seq),
            "phase": int(phase),
            "kind": kind,
            **deepcopy(payload),
        }
        heap.append(item)
        heap.sort(key=lambda entry: (entry["when_ms"], entry["phase"], entry["stable_seq"]))
        return item

    def next_event_time(self) -> int | None:
        if self.event_heap:
            return int(self.event_heap[0]["when_ms"])
        return None

    def finished(self) -> bool:
        return not self.event_heap

    def step(self) -> dict[str, Any] | None:
        if self.finished():
            return None
        event = self.event_heap.pop(0)
        self.now_ms = max(self.now_ms, int(event["when_ms"]))
        if event["kind"] == "scheduled_send":
            operation = self.scheduled_operations[event["operation_id"]]
            if not operation["fired"]:
                self._send_request(operation)
        elif event["kind"] == "partition_start":
            self._start_partition(event["partition_id"])
        elif event["kind"] == "partition_end":
            self._end_partition(event["partition_id"])
        elif event["kind"] == "request_arrival":
            self._arrive_request(event["request_id"])
        elif event["kind"] == "response_delivery":
            self._deliver_response(event["request_id"])
        return event

    def run_until(self, target_ms: int | None = None) -> None:
        while not self.finished():
            nxt = self.next_event_time()
            if nxt is None:
                break
            if target_ms is not None and nxt > int(target_ms):
                self.now_ms = max(self.now_ms, int(target_ms))
                break
            self.step()
        if target_ms is not None:
            self.now_ms = max(self.now_ms, int(target_ms))

    def run(self) -> None:
        self.run_until(None)

    def _send_request(self, operation: dict[str, Any]) -> None:
        operation["fired"] = True
        client_id = operation["client_id"]
        runtime = self.runtime_clients[client_id]
        runtime["issue_seq"] += 1
        issue_seq = runtime["issue_seq"]
        self.request_seq += 1
        request_id = f"req-{self.request_seq}"
        send_ms = int(operation["send_ms"])
        scheduled_arrival_ms = send_ms + int(operation["latency_ms"])
        fence_token = operation["fence_token"]
        if operation["op"] in {"renew", "release"} and fence_token is None:
            active = runtime["active"].get(operation["resource"])
            fence_token = active["fence_token"] if active else None

        request = {
            "id": request_id,
            "operation_id": operation["id"],
            "client_id": client_id,
            "op": operation["op"],
            "resource": operation["resource"],
            "ttl_ms": operation["ttl_ms"],
            "fence_token": int(fence_token) if fence_token is not None else None,
            "idem_key": operation["idem_key"],
            "latency_ms": int(operation["latency_ms"]),
            "send_ms": send_ms,
            "scheduled_arrival_ms": scheduled_arrival_ms,
            "arrival_ms": None,
            "process_ms": None,
            "nominal_response_ms": None,
            "response_delivery_ms": None,
            "issue_seq": issue_seq,
            "stable_seq": operation["stable_seq"],
            "status": "in_flight",
            "response": None,
            "queued_by_partition": False,
        }
        self.requests[request_id] = request
        runtime["pending"].append({"request_id": request_id, "issue_seq": issue_seq})
        self._log(
            "request_sent",
            request_id=request_id,
            operation_id=operation["id"],
            client_id=client_id,
            resource=request["resource"],
            op=request["op"],
            send_ms=send_ms,
            client_wall_ms=self.client_wall_time(client_id, send_ms),
            scheduled_arrival_ms=scheduled_arrival_ms,
            issue_seq=issue_seq,
            idem_key=request["idem_key"],
            fence_token=request["fence_token"],
            latency_ms=request["latency_ms"],
        )

        if self._partition_id_for(client_id, scheduled_arrival_ms):
            request["status"] = "queued_by_partition"
            request["queued_by_partition"] = True
            self.queued_requests[client_id].append(request_id)
            self._log(
                "request_queued",
                request_id=request_id,
                client_id=client_id,
                scheduled_arrival_ms=scheduled_arrival_ms,
                client_wall_ms=self.client_wall_time(client_id, scheduled_arrival_ms),
            )
        else:
            self._push(
                self.event_heap,
                scheduled_arrival_ms,
                request["stable_seq"],
                "request_arrival",
                phase=2,
                request_id=request_id,
            )

    def _partition_id_for(self, client_id: str, when_ms: int) -> str | None:
        partition_id = self.active_partitions.get(client_id)
        if not partition_id:
            return None
        partition = self.partitions[partition_id]
        if int(partition["start_ms"]) <= when_ms < int(partition["end_ms"]):
            return partition_id
        return None

    def _start_partition(self, partition_id: str) -> None:
        partition = self.partitions[partition_id]
        client_id = partition["client_id"]
        if client_id in self.active_partitions:
            raise ValueError(f"overlapping partition for client {client_id}")
        self.active_partitions[client_id] = partition_id
        self._log(
            "partition_started",
            partition_id=partition_id,
            client_id=client_id,
            start_ms=partition["start_ms"],
            end_ms=partition["end_ms"],
            client_wall_ms=self.client_wall_time(client_id, partition["start_ms"]),
        )

    def _end_partition(self, partition_id: str) -> None:
        partition = self.partitions[partition_id]
        client_id = partition["client_id"]
        if not self.replay_queued_arrivals:
            self._rebuild_until_partition_end(partition_id)
            return
        if self.active_partitions.get(client_id) != partition_id:
            return
        del self.active_partitions[client_id]
        request_ids = list(self.queued_requests.get(client_id, []))
        self.queued_requests[client_id] = []
        request_ids.sort(
            key=lambda rid: (
                self.requests[rid]["scheduled_arrival_ms"],
                self.requests[rid]["stable_seq"],
            )
        )
        for request_id in request_ids:
            request = self.requests[request_id]
            self._process_request(request, request["scheduled_arrival_ms"], queued=True)

        held = list(self.held_responses.get(client_id, []))
        self.held_responses[client_id] = []
        held.sort(
            key=lambda rid: (
                self.requests[rid]["nominal_response_ms"],
                self.requests[rid]["stable_seq"],
            )
        )
        for request_id in held:
            self._schedule_response_delivery(self.requests[request_id], queued=True)

        self._log(
            "partition_recovered",
            partition_id=partition_id,
            client_id=client_id,
            recovery_ms=partition["end_ms"],
            client_wall_ms=self.client_wall_time(client_id, partition["end_ms"]),
            queued_request_ids=request_ids,
            held_response_ids=held,
        )

    def _rebuild_until_partition_end(self, partition_id: str) -> None:
        target = self.partitions[partition_id]
        rebuilt = LeaseEngine(ttl_ms=self.ttl_ms)
        rebuilt.replay_queued_arrivals = True
        rebuilt.now_ms = 0
        for config in self.client_configs.values():
            rebuilt.add_client(config)
        for operation in sorted(
            self.scheduled_operations.values(), key=lambda item: item["stable_seq"]
        ):
            rebuilt.schedule_operation(
                client_id=operation["client_id"],
                op=operation["op"],
                resource=operation["resource"],
                send_ms=operation["send_ms"],
                ttl_ms=operation["ttl_ms"],
                fence_token=operation["fence_token"],
                idem_key=operation["idem_key"],
                latency_ms=operation["latency_ms"],
            )
        for partition in sorted(self.partitions.values(), key=lambda item: item["stable_seq"]):
            rebuilt.schedule_partition(
                client_id=partition["client_id"],
                start_ms=partition["start_ms"],
                end_ms=partition["end_ms"],
            )
        rebuilt.run_until(target["end_ms"])
        self.__dict__.update(rebuilt.__dict__)

    def _arrive_request(self, request_id: str) -> None:
        request = self.requests[request_id]
        client_id = request["client_id"]
        if self._partition_id_for(client_id, self.now_ms):
            request["status"] = "queued_by_partition"
            request["arrival_ms"] = None
            if request_id not in self.queued_requests[client_id]:
                self.queued_requests[client_id].append(request_id)
            self._log(
                "request_queued",
                request_id=request_id,
                client_id=client_id,
                scheduled_arrival_ms=request["scheduled_arrival_ms"],
                client_wall_ms=self.client_wall_time(
                    client_id, request["scheduled_arrival_ms"]
                ),
            )
            return
        self._process_request(request, self.now_ms, queued=False)

    def _process_request(
        self, request: dict[str, Any], process_ms: int, *, queued: bool
    ) -> None:
        request["arrival_ms"] = int(process_ms)
        request["process_ms"] = int(process_ms)
        if request["status"] == "queued_by_partition":
            request["queued_by_partition"] = True
        else:
            request["queued_by_partition"] = False
        self._log(
            "request_arrived",
            request_id=request["id"],
            client_id=request["client_id"],
            resource=request["resource"],
            op=request["op"],
            scheduled_arrival_ms=request["scheduled_arrival_ms"],
            arrival_ms=int(process_ms),
            queued=bool(queued or request["queued_by_partition"]),
        )
        response = self._handle_server_request(request, int(process_ms))
        request["response"] = response
        request["status"] = "processed"
        nominal_response_ms = int(process_ms) + int(request["latency_ms"])
        request["nominal_response_ms"] = nominal_response_ms
        self._log(
            "request_processed",
            request_id=request["id"],
            client_id=request["client_id"],
            resource=request["resource"],
            op=request["op"],
            process_ms=int(process_ms),
            nominal_response_ms=nominal_response_ms,
            status=response["status"],
            error_code=response.get("error_code"),
            fence_token=response.get("fence_token"),
            idem_key=request["idem_key"],
            idempotent_hit=response.get("idempotent_hit", False),
        )
        self._schedule_response_delivery(request, queued=queued)

    def _expire_current_lease(self, resource: str, now_ms: int) -> None:
        lease = self.leases.get(resource)
        if lease and now_ms >= int(lease["expires_at"]):
            expired = dict(lease)
            expired["state"] = "expired"
            self.leases.pop(resource, None)
            self.fences.setdefault("_history", {}).setdefault(resource, {})[
                int(lease["fence_token"])
            ] = expired
            self._log(
                "lease_expired",
                resource=resource,
                client_id=lease["holder_client_id"],
                fence_token=lease["fence_token"],
                expires_at=lease["expires_at"],
                process_ms=now_ms,
            )

    def _lease_history(self, resource: str) -> dict[int, dict[str, Any]]:
        return self.fences.setdefault("_history", {}).setdefault(resource, {})

    def _current_lease(self, resource: str, now_ms: int) -> dict[str, Any] | None:
        self._expire_current_lease(resource, now_ms)
        return self.leases.get(resource)

    def _handle_server_request(
        self, request: dict[str, Any], now_ms: int
    ) -> dict[str, Any]:
        resource = request["resource"]
        client_id = request["client_id"]
        idem_id = (client_id, request["idem_key"])
        if idem_id in self.idem:
            cached = deepcopy(self.idem[idem_id])
            cached["idempotent_hit"] = True
            return cached

        ttl_ms = int(request["ttl_ms"]) if request["ttl_ms"] is not None else self.ttl_ms
        response: dict[str, Any]
        current = self._current_lease(resource, now_ms)

        if request["op"] == "acquire":
            if current:
                response = self._error(
                    request,
                    now_ms,
                    "lease_held",
                    lease=self._lease_view(current),
                )
            else:
                token = int(self.fences.get(resource, 0)) + 1
                self.fences[resource] = token
                lease = {
                    "resource": resource,
                    "holder_client_id": client_id,
                    "fence_token": token,
                    "acquired_at": now_ms,
                    "expires_at": now_ms + ttl_ms,
                    "state": "active",
                }
                self.leases[resource] = lease
                response = {
                    "ok": True,
                    "status": "acquired",
                    "fence_token": token,
                    "resource": resource,
                    "holder_client_id": client_id,
                    "acquired_at": now_ms,
                    "expires_at": now_ms + ttl_ms,
                    "lease": self._lease_view(lease),
                    "server_time_ms": now_ms,
                    "idempotent_hit": False,
                }
        elif request["op"] == "renew":
            token = request["fence_token"]
            if token is None:
                response = self._error(request, now_ms, "missing_fence")
            elif current and int(current["fence_token"]) == int(token):
                if current["holder_client_id"] != client_id:
                    response = self._error(request, now_ms, "fence_mismatch")
                else:
                    current["expires_at"] = now_ms + ttl_ms
                    response = {
                        "ok": True,
                        "status": "renewed",
                        "fence_token": int(token),
                        "resource": resource,
                        "holder_client_id": client_id,
                        "acquired_at": current["acquired_at"],
                        "expires_at": current["expires_at"],
                        "lease": self._lease_view(current),
                        "server_time_ms": now_ms,
                        "idempotent_hit": False,
                    }
            else:
                response = self._error(
                    request,
                    now_ms,
                    "lease_expired" if not current else "fence_mismatch",
                    lease=self._lease_view(current) if current else None,
                )
        elif request["op"] == "release":
            token = request["fence_token"]
            history = self._lease_history(resource)
            if current and token is not None and int(current["fence_token"]) == int(token):
                if current["holder_client_id"] != client_id:
                    response = self._error(request, now_ms, "fence_mismatch")
                else:
                    released = dict(current)
                    released["state"] = "released"
                    released["released_at"] = now_ms
                    history[int(token)] = released
                    self.leases.pop(resource, None)
                    response = {
                        "ok": True,
                        "status": "released",
                        "fence_token": int(token),
                        "resource": resource,
                        "holder_client_id": None,
                        "server_time_ms": now_ms,
                        "idempotent_hit": False,
                    }
            elif token is not None and int(token) in history:
                record = history[int(token)]
                if record.get("holder_client_id") == client_id:
                    response = {
                        "ok": True,
                        "status": "already_released",
                        "fence_token": int(token),
                        "resource": resource,
                        "holder_client_id": None,
                        "server_time_ms": now_ms,
                        "idempotent_hit": False,
                    }
                else:
                    response = self._error(request, now_ms, "fence_mismatch")
            else:
                response = self._error(
                    request,
                    now_ms,
                    "unknown_fence" if token is not None else "missing_fence",
                    lease=self._lease_view(current) if current else None,
                )
        else:
            response = {
                "ok": True,
                "status": "read",
                "resource": resource,
                "holder_client_id": current["holder_client_id"] if current else None,
                "fence_token": current["fence_token"] if current else None,
                "expires_at": current["expires_at"] if current else None,
                "lease": self._lease_view(current) if current else None,
                "server_time_ms": now_ms,
                "idempotent_hit": False,
            }

        self.idem[idem_id] = deepcopy(response)
        return response

    def _error(
        self,
        request: dict[str, Any],
        now_ms: int,
        code: str,
        *,
        lease: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "ok": False,
            "status": "error",
            "error_code": code,
            "fence_token": request["fence_token"],
            "resource": request["resource"],
            "holder_client_id": lease["holder_client_id"] if lease else None,
            "expires_at": lease["expires_at"] if lease else None,
            "lease": self._lease_view(lease) if lease else None,
            "server_time_ms": now_ms,
            "idempotent_hit": False,
        }

    def _lease_view(self, lease: dict[str, Any] | None) -> dict[str, Any] | None:
        if lease is None:
            return None
        view = deepcopy(lease)
        view["active_at_server_time"] = self.now_ms < int(view["expires_at"])
        return view

    def _schedule_response_delivery(
        self, request: dict[str, Any], *, queued: bool
    ) -> None:
        client_id = request["client_id"]
        nominal = int(request["nominal_response_ms"])
        active = None if queued else self._partition_id_for(client_id, nominal)
        if active:
            request["status"] = "response_held"
            if request["id"] not in self.held_responses[client_id]:
                self.held_responses[client_id].append(request["id"])
            self._log(
                "response_held",
                request_id=request["id"],
                client_id=client_id,
                partition_id=active,
                nominal_response_ms=nominal,
                client_wall_ms=self.client_wall_time(client_id, nominal),
            )
            return
        delivery_ms = max(nominal, self.now_ms) if queued else nominal
        request["response_delivery_ms"] = delivery_ms
        request["status"] = "response_scheduled"
        self._log(
            "response_scheduled",
            request_id=request["id"],
            client_id=client_id,
            nominal_response_ms=nominal,
            delivery_ms=delivery_ms,
            client_wall_ms=self.client_wall_time(client_id, delivery_ms),
        )
        self._push(
            self.event_heap,
            delivery_ms,
            int(request["stable_seq"]),
            "response_delivery",
            phase=3,
            request_id=request["id"],
        )

    def _deliver_response(self, request_id: str) -> None:
        request = self.requests[request_id]
        client_id = request["client_id"]
        delivery_ms = self.now_ms
        active = self._partition_id_for(client_id, delivery_ms)
        if active:
            request["status"] = "response_held"
            if request_id not in self.held_responses[client_id]:
                self.held_responses[client_id].append(request_id)
            self._log(
                "response_held",
                request_id=request_id,
                client_id=client_id,
                partition_id=active,
                nominal_response_ms=request["nominal_response_ms"],
                client_wall_ms=self.client_wall_time(client_id, delivery_ms),
            )
            return

        request["response_delivery_ms"] = delivery_ms
        runtime = self.runtime_clients[client_id]
        runtime["pending"] = [
            item for item in runtime["pending"] if item["request_id"] != request_id
        ]
        observed = runtime["observed"].setdefault(
            request["resource"],
            {"latest_issue_seq": 0, "latest_request_id": None, "response": None},
        )
        stale = int(observed["latest_issue_seq"]) > int(request["issue_seq"])
        response = deepcopy(request["response"])
        if stale:
            request["status"] = "stale_ignored"
            response["ignored_as_stale"] = True
            self._log(
                "response_ignored_stale",
                request_id=request_id,
                client_id=client_id,
                resource=request["resource"],
                delivery_ms=delivery_ms,
                client_wall_ms=self.client_wall_time(client_id, delivery_ms),
                issue_seq=request["issue_seq"],
                latest_issue_seq=observed["latest_issue_seq"],
            )
        else:
            observed["latest_issue_seq"] = request["issue_seq"]
            observed["latest_request_id"] = request_id
            observed["response"] = response
            self._apply_client_response(request, response)
            request["status"] = "delivered"
        self._log(
            "response_delivered",
            request_id=request_id,
            client_id=client_id,
            resource=request["resource"],
            op=request["op"],
            delivery_ms=delivery_ms,
            client_wall_ms=self.client_wall_time(client_id, delivery_ms),
            issue_seq=request["issue_seq"],
            stale=stale,
            status=response.get("status"),
            fence_token=response.get("fence_token"),
        )

    def _apply_client_response(
        self, request: dict[str, Any], response: dict[str, Any]
    ) -> None:
        runtime = self.runtime_clients[request["client_id"]]
        resource = request["resource"]
        active = runtime["active"].get(resource)
        if response.get("ok") and response.get("status") in {"acquired", "renewed"}:
            runtime["active"][resource] = {
                "fence_token": response["fence_token"],
                "expires_at": response["expires_at"],
                "resource": resource,
                "request_id": request["id"],
            }
        elif response.get("ok") and response.get("status") in {"released", "already_released"}:
            if active and int(active["fence_token"]) == int(request["fence_token"]):
                runtime["active"].pop(resource, None)
        elif request["op"] in {"acquire", "renew"} and not response.get("ok"):
            if active and (
                request["fence_token"] is None
                or int(active["fence_token"]) == int(request["fence_token"])
            ):
                runtime["active"].pop(resource, None)

    def _log(self, event_type: str, **fields: Any) -> dict[str, Any]:
        entry = {"id": f"evt-{len(self.timeline) + 1}", "type": event_type, **fields}
        self.timeline.append(entry)
        return entry

    def to_dict(self) -> dict[str, Any]:
        return {
            "now_ms": self.now_ms,
            "ttl_ms": self.ttl_ms,
            "client_configs": {
                key: value.to_dict() for key, value in self.client_configs.items()
            },
            "fences": deepcopy(self.fences),
            "leases": deepcopy(self.leases),
            "idem": [
                [list(key), deepcopy(value)] for key, value in self.idem.items()
            ],
            "requests": deepcopy(self.requests),
            "runtime_clients": deepcopy(self.runtime_clients),
            "scheduled_operations": deepcopy(self.scheduled_operations),
            "partitions": deepcopy(self.partitions),
            "active_partitions": deepcopy(self.active_partitions),
            "event_heap": deepcopy(self.event_heap),
            "queued_requests": deepcopy(self.queued_requests),
            "held_responses": deepcopy(self.held_responses),
            "timeline": deepcopy(self.timeline),
            "stable_seq": self.stable_seq,
            "request_seq": self.request_seq,
            "operation_seq": self.operation_seq,
            "partition_seq": self.partition_seq,
            "replay_queued_arrivals": self.replay_queued_arrivals,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LeaseEngine":
        engine = cls(ttl_ms=int(data.get("ttl_ms", 10_000)))
        engine.now_ms = int(data.get("now_ms", 0))
        engine.client_configs = {
            key: ClientConfig.from_dict(value)
            for key, value in data.get("client_configs", {}).items()
        }
        for client_id in engine.client_configs:
            engine._init_runtime(client_id)
            engine.queued_requests.setdefault(client_id, [])
            engine.held_responses.setdefault(client_id, [])
        engine.fences = deepcopy(data.get("fences", {}))
        engine.leases = deepcopy(data.get("leases", {}))
        engine.idem = {
            (str(key[0]), str(key[1])): deepcopy(value)
            for key, value in data.get("idem", [])
        }
        engine.requests = deepcopy(data.get("requests", {}))
        engine.runtime_clients = deepcopy(data.get("runtime_clients", {}))
        engine.scheduled_operations = deepcopy(data.get("scheduled_operations", {}))
        engine.partitions = deepcopy(data.get("partitions", {}))
        engine.active_partitions = deepcopy(data.get("active_partitions", {}))
        engine.event_heap = deepcopy(data.get("event_heap", []))
        engine.queued_requests = deepcopy(data.get("queued_requests", {}))
        engine.held_responses = deepcopy(data.get("held_responses", {}))
        engine.timeline = deepcopy(data.get("timeline", []))
        engine.stable_seq = int(data.get("stable_seq", 0))
        engine.request_seq = int(data.get("request_seq", 0))
        engine.operation_seq = int(data.get("operation_seq", 0))
        engine.partition_seq = int(data.get("partition_seq", 0))
        engine.replay_queued_arrivals = bool(data.get("replay_queued_arrivals", False))
        for client_id in engine.client_configs:
            engine.queued_requests.setdefault(client_id, [])
            engine.held_responses.setdefault(client_id, [])
            engine.runtime_clients.setdefault(client_id, {})
        return engine
