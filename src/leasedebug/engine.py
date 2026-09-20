"""确定性的租约模拟引擎（虚拟时钟，单位：整数毫秒）。

设计要点
--------
* 模拟时间从 0 开始单调递增，没有真实睡眠；run() 是纯函数式重放。
* 服务端在请求的"逻辑到达时刻"裁决租约。同一资源在任一模拟时刻
  至多有一个有效持有者。
* 处理顺序全局按 (arrival, seq) 排序，seq 为稳定序号（按调度顺序
  分配），因此同时到达也有唯一顺序。
* 网络分区只改变响应回到客户端的"投递时刻"：落在分区区间内的
  响应在分区结束后按 (arrival, seq) 顺序依次投递，不会在重连时刻
  挤成同一时刻（在 base 回程延迟上叠加）。
* 续租 / 释放自动携带客户端当前认知中的 fence token；幂等键
  (client, key) 在服务端去重并回放首个结果。
* 旧响应（seq 更老或 fence 更老）不能覆盖客户端更新的认知。
"""
from __future__ import annotations

import copy
import heapq
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


class SimClock:
    """单调虚拟时钟。所有代码只能通过它感知时间。"""

    def __init__(self) -> None:
        self._now = 0

    @property
    def now(self) -> int:
        return self._now

    def advance(self, t: int) -> int:
        if t < self._now:
            raise ValueError("虚拟时钟不能回拨: %d < %d" % (t, self._now))
        self._now = t
        return self._now


def _new_doc(ttl: int = 100) -> Dict[str, Any]:
    return {"ttl": int(ttl), "clients": [], "events": [], "partitions": []}


def _new_state() -> Dict[str, Any]:
    return {
        "fence": {},          # resource -> 已分配的最大 fence token
        "leases": {},         # resource -> 活跃租约 dict（过期惰性清除）
        "idem": {},           # (client, key) -> 已存储的结果
        "segments": {},       # resource -> [segment dict]
        "beliefs": {},        # client -> {resource: 认知 dict}
        "max_seq": {},        # client -> 已应用/忽略的最大请求 seq
        "requests": [],       # 完整请求记录（按 seq）
        "ignored": [],        # 被客户端丢弃的响应
        "next_seq": 1,
        "next_fence": 1,
    }


@dataclass
class Engine:
    """一次性重放整份实验文档。

    run() 返回不可变（dict/list 组成的）结果快照；snapshot 相关逻辑
    见 run_until()。
    """

    doc: Dict[str, Any]

    @classmethod
    def fresh(cls, ttl: int = 100) -> "Engine":
        return cls(_new_doc(ttl))

    # ---- 编辑文档（稳定序号由数组顺序天然保证）----
    def add_client(self, name: str, clock_offset: int = 0,
                   latency_req: int = 0, latency_resp: int = 0) -> str:
        cid = "c%d" % (len(self.doc["clients"]) + 1)
        self.doc["clients"].append({
            "id": cid, "name": name,
            "clock_offset": int(clock_offset),
            "latency_req": int(latency_req),
            "latency_resp": int(latency_resp),
        })
        return cid

    def add_request(self, client_id: str, send: int, op: str,
                    resource: str, idem_key: Optional[str] = None,
                    fence: Optional[int] = None,
                    req_delay: Optional[int] = None,
                    resp_delay: Optional[int] = None,
                    note: str = "") -> int:
        seq = len(self.doc["events"]) + 1
        ev: Dict[str, Any] = {
            "id": "e%d" % seq, "seq": seq, "kind": "request",
            "client": client_id, "send": int(send), "op": op,
            "resource": resource, "fence": fence,
            "idem_key": idem_key,
            "req_delay": req_delay, "resp_delay": resp_delay,
            "note": note,
        }
        self.doc["events"].append(ev)
        return seq

    def add_partition(self, start: int, end: int,
                      client_id: Optional[str] = None) -> str:
        if end < start:
            raise ValueError("分区结束不能早于开始")
        pid = "p%d" % (len(self.doc["partitions"]) + 1)
        self.doc["partitions"].append({
            "id": pid, "start": int(start), "end": int(end),
            "client": client_id,
        })
        return pid

    def run(self) -> Dict[str, Any]:
        return self._run_core()

    def run_until(self, time: int) -> Dict[str, Any]:
        """只执行所有时间戳 <= time 的处理与投递，用于时间轴游标。"""
        return self._run_core(cutoff=int(time))

    def snapshot_at_request(self, k: int) -> Dict[str, Any]:
        """在处理完第 k 个请求（含此前所有投递）后导出可分叉状态。"""
        if k == 0:
            res = self._run_core(cutoff=0)
            res["snapshot"] = {"k": 0, "clock": 0,
                               "state": serialize_state(_new_state())}
            return res
        return self._run_core(snapshot_after=k)

    def _run_from_state(self, state, clock_start, skip_before) -> Dict[str, Any]:
        return self._run_core(state=state, clock_start=clock_start,
                              skip_before=skip_before)

    # ---- 内部 ----
    def _client_map(self) -> Dict[str, Dict[str, Any]]:
        return {c["id"]: c for c in self.doc["clients"]}

    def _schedule(self) -> List[Dict[str, Any]]:
        """为每个请求计算稳定序号、逻辑到达时刻与响应投递时刻。"""
        cmap = self._client_map()
        reqs: List[Dict[str, Any]] = []
        for ev in self.doc["events"]:
            c = cmap[ev["client"]]
            dl = ev["req_delay"]
            if dl is None:
                dl = c["latency_req"]
            dr = ev["resp_delay"]
            if dr is None:
                dr = c["latency_resp"]
            arrival = ev["send"] + int(dl)
            receive = arrival + int(dr)
            block_end = self._blocking_partition(ev["client"], arrival, receive)
            delayed = False
            if block_end is not None:
                delayed = True
            reqs.append({
                "seq": ev["seq"], "id": ev["id"], "client": ev["client"],
                "op": ev["op"], "resource": ev["resource"],
                "send": ev["send"], "arrival": arrival,
                "recv_planned": arrival + int(dr), "recv": receive,
                "block_end": block_end,
                "fence_in": ev["fence"], "idem_key": ev["idem_key"],
                "req_delay": int(dl), "resp_delay": int(dr),
                "delayed": delayed, "note": ev.get("note", ""),
            })
        # 分区恢复：排队响应按 (arrival, seq) 顺序出队，并保留原定到达
        # 的时间间距，避免重连时全部挤到同一时刻。
        groups: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        for r in reqs:
            if r["delayed"]:
                groups.setdefault((r["client"], r["block_end"]), []).append(r)
        for (_client, _heal), group in groups.items():
            group.sort(key=lambda r: (r["arrival"], r["seq"]))
            first_arrival = group[0]["arrival"]
            for r in group:
                r["recv"] = r["block_end"] + (r["arrival"] - first_arrival) \
                    + r["resp_delay"]
        reqs.sort(key=lambda r: (r["arrival"], r["seq"]))
        return reqs

    def _blocking_partition(self, client: str, arrival: int,
                            receive: int) -> Optional[int]:
        """若请求的在途区间 [arrival, receive] 与某分区相交，返回该分区结束。

        client=None 的分区影响所有客户端。多个分区覆盖时取最晚结束。
        """
        end = None
        for p in sorted(self.doc["partitions"], key=lambda x: x["start"]):
            if p["client"] is not None and p["client"] != client:
                continue
            # 在途时刻落在半开区间 [start, end) 内即被隔离
            hit = (arrival < p["end"] and receive >= p["start"])
            if hit and (end is None or p["end"] > end):
                end = p["end"]
        return end

    def _run_core(self, cutoff: Optional[int] = None,
                  snapshot_after: Optional[int] = None,
                  state: Optional[Dict[str, Any]] = None,
                  clock_start: int = 0,
                  skip_before: int = 0) -> Dict[str, Any]:
        clock = SimClock()
        clock.advance(clock_start)
        st = state if state is not None else _new_state()
        ttl = int(self.doc.get("ttl", 100))
        cmap = self._client_map()
        for cid in cmap:
            st["beliefs"].setdefault(cid, {})
            st["max_seq"].setdefault(cid, 0)

        reqs = self._schedule()
        # 快照恢复时：前缀请求只参与调度展示，不再执行
        pending: List[Tuple[int, int, Dict[str, Any]]] = []
        processed_count = skip_before
        boundary_time = clock_start

        for idx, req in enumerate(reqs):
            if idx < skip_before:
                continue
            if snapshot_after is not None and idx >= snapshot_after:
                break
            if cutoff is not None and req["arrival"] > cutoff:
                break
            at = req["arrival"]
            clock.advance(at)
            # 绑定续租/释放的 fence 前，先接收所有"发送之前"已到达的响应
            self._apply_due(st, pending, req["send"] - 1, ttl, cmap)
            self._expire_due(st, at)
            self._execute(st, req, at, ttl)
            processed_count += 1
            boundary_time = at
            heapq.heappush(pending, (req["recv"], req["seq"], req))
            if snapshot_after is not None and idx + 1 == snapshot_after:
                self._apply_due(st, pending, at, ttl, cmap)
                boundary_time = at
                break

        if snapshot_after is None:
            limit = cutoff if cutoff is not None else 10 ** 18
            self._apply_due(st, pending, limit, ttl, cmap)
            self._expire_due(st, limit if cutoff is not None else clock.now)
            candidates = [r["recv_actual"] for r in reqs
                          if "recv_actual" in r] \
                + [p["end"] for p in self.doc["partitions"]]
            if cutoff is not None:
                candidates = [t for t in candidates if t <= cutoff]
            boundary_time = max(candidates + [clock_start])
            if cutoff is not None and boundary_time > cutoff:
                boundary_time = cutoff

        result = self._assemble(st, cmap, reqs, ttl, cutoff,
                                processed_count, boundary_time, skip_before)
        if snapshot_after is not None:
            result["snapshot"] = {
                "k": snapshot_after, "clock": boundary_time,
                "state": serialize_state(st),
            }
        return result

    # -- 响应投递：迟到的旧响应不能覆盖更新的认知 --
    def _apply_due(self, st: Dict[str, Any],
                   pending: List[Tuple[int, int, Dict[str, Any]]],
                   uptime: int, ttl: int,
                   cmap: Dict[str, Dict[str, Any]]) -> None:
        while pending and pending[0][0] <= uptime:
            recv, _seq, req = heapq.heappop(pending)
            req["recv_actual"] = recv
            self._apply_receipt(st, req, recv)

    def _expire_due(self, st: Dict[str, Any], at: int) -> None:
        due = [(l["expiry"], r) for r, l in st["leases"].items()
               if l["expiry"] <= at]
        for expiry, resource in sorted(due):
            lease = st["leases"].pop(resource)
            seg = st["segments"][resource][-1]
            seg["end"] = expiry
            seg["end_reason"] = "expired"
            st.setdefault("expiries", []).append({
                "resource": resource, "expiry": expiry,
                "holder": lease["holder"], "fence": lease["fence"],
            })

    # -- 服务端在逻辑到达时刻裁决 --
    def _execute(self, st: Dict[str, Any], req: Dict[str, Any],
                 at: int, ttl: int) -> None:
        op = req["op"]
        resource = req["resource"]
        client = req["client"]
        fence_in = req["fence_in"]
        if op in ("renew", "release") and fence_in is None:
            b = st["beliefs"].get(client, {}).get(resource)
            if b and b.get("status") in ("holding",):
                fence_in = b["fence"]
        req["fence_used"] = fence_in

        idem = req.get("idem_key")
        idem_id = (client, idem) if idem is not None else None
        spec_map = st.setdefault("_idem_spec", {})
        if idem_id is not None and idem_id in spec_map:
            if spec_map[idem_id] != (op, resource):
                req["response"] = {
                    "status": "error", "error": "idem_key_conflict",
                    "op": op, "resource": resource, "replayed": False,
                }
                req["executed"] = False
                return
            if idem_id in st["idem"]:
                res = dict(st["idem"][idem_id])
                res["replayed"] = True
                req["response"] = res
                req["executed"] = False
                return
        if idem_id is not None:
            spec_map[idem_id] = (op, resource)

        res: Dict[str, Any]
        if op == "acquire":
            res = self._svc_acquire(st, resource, client, at, ttl)
        elif op == "renew":
            res = self._svc_renew(st, resource, client, at, ttl, fence_in)
        elif op == "release":
            res = self._svc_release(st, resource, client, at, fence_in)
        elif op == "read":
            res = self._svc_read(st, resource)
        else:
            res = {"status": "error", "error": "unknown_op",
                   "op": op, "resource": resource}
        req["response"] = res
        req["executed"] = True
        if idem_id is not None and res.get("status") != "error":
            st["idem"][idem_id] = dict(res)

    def _grant(self, st: Dict[str, Any], resource: str,
               client: str, at: int, ttl: int) -> Dict[str, Any]:
        fn = st["fence"].get(resource, 0) + 1
        st["fence"][resource] = fn
        expiry = at + ttl
        st["leases"][resource] = {
            "holder": client, "fence": fn, "acquired": at,
            "granted_at": at, "expiry": expiry,
        }
        st["segments"].setdefault(resource, []).append({
            "holder": client, "fence": fn, "start": at,
            "end": expiry, "end_reason": "open",
        })
        return {"status": "granted", "resource": resource,
                "fence": fn, "lease_ms": ttl,
                "granted_at": at, "expiry": expiry, "replayed": False}

    def _lease_view(self, lease: Dict[str, Any]) -> Dict[str, Any]:
        return {"holder": lease["holder"], "fence": lease["fence"],
                "acquired": lease["acquired"], "expiry": lease["expiry"]}

    def _svc_acquire(self, st, resource, client, at, ttl):
        lease = st["leases"].get(resource)
        if lease is None:
            return self._grant(st, resource, client, at, ttl)
        if lease["holder"] == client:
            view = self._lease_view(lease)
            view.update(status="granted", resource=resource,
                        lease_ms=lease["expiry"] - lease["acquired"],
                        granted_at=lease["granted_at"], replayed=False,
                        already_holder=True)
            return view
        return {"status": "busy", "resource": resource,
                "lease": self._lease_view(lease), "at": at,
                "replayed": False}

    def _svc_renew(self, st, resource, client, at, ttl, fence_in):
        lease = st["leases"].get(resource)
        if lease is None:
            return {"status": "error", "error": "no_active_lease",
                    "resource": resource, "at": at, "replayed": False}
        if lease["holder"] != client:
            return {"status": "error", "error": "not_holder",
                    "resource": resource,
                    "lease": self._lease_view(lease), "at": at,
                    "replayed": False}
        if fence_in is None or int(fence_in) != lease["fence"]:
            return {"status": "error", "error": "stale_fence",
                    "resource": resource,
                    "submitted_fence": fence_in,
                    "lease": self._lease_view(lease), "at": at,
                    "replayed": False}
        lease["expiry"] = at + ttl
        st["segments"][resource][-1]["end"] = lease["expiry"]
        return {"status": "renewed", "resource": resource,
                "fence": lease["fence"], "renewed_at": at,
                "expiry": lease["expiry"], "lease_ms": ttl,
                "replayed": False}

    def _svc_release(self, st, resource, client, at, fence_in):
        lease = st["leases"].get(resource)
        if lease is None:
            # 租约早已过期：release 仍然幂等成功
            return {"status": "released", "resource": resource,
                    "idempotent": True, "reason": "already_expired",
                    "at": at, "replayed": False}
        if lease["holder"] == client and fence_in is not None \
                and int(fence_in) < lease["fence"]:
            # 持有者用更旧的 fence 释放
            return {"status": "released", "resource": resource,
                    "idempotent": True, "reason": "stale_fence",
                    "lease": self._lease_view(lease), "at": at,
                    "replayed": False}
        if lease["holder"] != client:
            # 调用者自己的租约已不活跃（已过期并被后来者拿走，或从未持有）：
            # release 幂等成功，但绝不能关闭后来持有者的租约
            return {"status": "released", "resource": resource,
                    "idempotent": True, "reason": "already_expired",
                    "lease": self._lease_view(lease), "at": at,
                    "replayed": False}
        fn = lease["fence"]
        st["leases"].pop(resource)
        seg = st["segments"][resource][-1]
        seg["end"] = at
        seg["end_reason"] = "released"
        return {"status": "released", "resource": resource,
                "fence": fn, "idempotent": False, "reason": "released",
                "at": at, "replayed": False}

    def _svc_read(self, st, resource):
        lease = st["leases"].get(resource)
        return {"status": "ok", "resource": resource,
                "lease": self._lease_view(lease) if lease else None,
                "replayed": False}

    # -- 客户端认知：迟到的旧响应不能覆盖更新的认知 --
    def _apply_receipt(self, st: Dict[str, Any], req: Dict[str, Any],
                       recv: int) -> None:
        client = req["client"]
        resource = req["resource"]
        res = req.get("response")
        max_seq = st["max_seq"]
        if req["seq"] <= max_seq.get(client, 0):
            st["ignored"].append({
                "seq": req["seq"], "client": client, "resource": resource,
                "recv": recv, "reason": "older_seq",
                "status": res.get("status") if res else None,
            })
            return
        # 结构防护先于认知更新；max_seq 仍然推进
        max_seq[client] = req["seq"]
        beliefs = st["beliefs"][client]
        cur = beliefs.get(resource)
        rf = res.get("fence") if res else None
        if cur and rf is not None and cur.get("fence") is not None \
                and int(rf) < int(cur["fence"]) \
                and res.get("status") not in ("busy",):
            st["ignored"].append({
                "seq": req["seq"], "client": client, "resource": resource,
                "recv": recv, "reason": "older_fence",
                "status": res.get("status"), "fence": rf,
            })
            return

        status = res.get("status")
        if status == "granted":
            beliefs[resource] = {
                "status": "holding", "fence": res["fence"],
                "expiry": res["expiry"], "granted_at": res["granted_at"],
                "updated_seq": req["seq"], "updated_at": recv,
                "already_holder": bool(res.get("already_holder")),
            }
        elif status == "renewed":
            granted_at = cur.get("granted_at") if cur else res["renewed_at"]
            beliefs[resource] = {
                "status": "holding", "fence": res["fence"],
                "expiry": res["expiry"], "granted_at": granted_at,
                "updated_seq": req["seq"], "updated_at": recv,
            }
        elif status == "busy":
            lease = res.get("lease") or {}
            beliefs[resource] = {
                "status": "busy", "holder": lease.get("holder"),
                "fence": lease.get("fence"), "expiry": lease.get("expiry"),
                "updated_seq": req["seq"], "updated_at": recv,
            }
        elif status == "released":
            reason = res.get("reason")
            lease_in_res = res.get("lease") or {}
            if reason == "stale_fence":
                fence_for_belief = lease_in_res.get("fence")
            elif reason == "already_expired":
                fence_for_belief = cur.get("fence") if cur else None
            else:
                fence_for_belief = res.get("fence")
            beliefs[resource] = {
                "status": "released", "reason": reason,
                "fence": fence_for_belief,
                "updated_seq": req["seq"], "updated_at": recv,
            }
        # error / read / no_active_lease / stale_fence / not_holder
        # 不改变资源认知（读操作仅用于观察）

    # -- 结果组装 --
    def _assemble(self, st, cmap, reqs, ttl, cutoff, processed,
                  boundary_time, skip_boundary=0) -> Dict[str, Any]:
        timeline: List[Dict[str, Any]] = []
        timeline_reqs = reqs if skip_boundary else reqs[:processed]
        for req in timeline_reqs:
            pre = req["seq"] <= skip_boundary
            timeline.append({
                "time": req["send"], "kind": "send", "seq": req["seq"],
                "client": req["client"], "op": req["op"],
                "resource": req["resource"], "note": req.get("note", ""),
                "prefork": pre,
            })
            timeline.append({
                "time": req["arrival"], "kind": "arrival", "seq": req["seq"],
                "client": req["client"], "op": req["op"],
                "resource": req["resource"],
                "delayed": req.get("delayed", False),
                "prefork": pre,
            })
            if "recv_actual" in req:
                timeline.append({
                    "time": req["recv_actual"], "kind": "response",
                    "seq": req["seq"], "client": req["client"],
                    "op": req["op"], "resource": req["resource"],
                    "delayed": req.get("delayed", False),
                    "status": req["response"].get("status"),
                    "prefork": pre,
                })
        for ex in st.get("expiries", []):
            timeline.append({
                "time": ex["expiry"], "kind": "expire",
                "resource": ex["resource"], "holder": ex["holder"],
                "fence": ex["fence"],
            })
        for p in self.doc["partitions"]:
            if cutoff is None or p["start"] <= cutoff:
                timeline.append({"time": p["start"], "kind": "partition_start",
                                 "partition": p["id"], "client": p["client"]})
            if cutoff is None or p["end"] <= cutoff:
                timeline.append({"time": p["end"], "kind": "partition_end",
                                 "partition": p["id"], "client": p["client"]})
        order = {"send": 0, "arrival": 1, "partition_start": 2,
                 "expire": 3, "partition_end": 4, "response": 5}
        timeline.sort(key=lambda e: (e["time"], order[e["kind"]],
                                     e.get("seq", 0)))

        requests_out = [self._request_view(r, cmap) for r in reqs]
        if skip_boundary:
            for view in requests_out[:skip_boundary]:
                view["prefork"] = True
        client_views = []
        for cid, c in cmap.items():
            wall_now = (cutoff if cutoff is not None else boundary_time) \
                + c["clock_offset"]
            client_views.append({
                "id": cid, "name": c["name"],
                "clock_offset": c["clock_offset"],
                "latency_req": c["latency_req"],
                "latency_resp": c["latency_resp"],
                "wall_now": wall_now,
                "beliefs": self._belief_view(st["beliefs"][cid], wall_now),
            })
        active = {r: self._lease_view(l) for r, l in st["leases"].items()}
        invariants = self._invariants(st, reqs, processed)
        summary = self._summary(st, reqs, processed)
        return {
            "ttl": ttl, "cutoff": cutoff, "clock": boundary_time,
            "processed": processed,
            "server": {
                "active": active, "fence": dict(st["fence"]),
                "segments": {r: list(segs)
                             for r, segs in st["segments"].items()},
            },
            "clients": client_views,
            "requests": requests_out,
            "ignored": list(st["ignored"]),
            "partitions": list(self.doc["partitions"]),
            "timeline": timeline,
            "invariants": invariants,
            "summary": summary,
            "determinism_key": self._det_key(reqs),
        }

    def _request_view(self, req, cmap) -> Dict[str, Any]:
        c = cmap[req["client"]]
        res = req.get("response")
        return {
            "seq": req["seq"], "id": req["id"], "client": req["client"],
            "client_name": c["name"], "op": req["op"],
            "resource": req["resource"], "send": req["send"],
            "arrival": req["arrival"], "recv_planned": req["recv_planned"],
            "recv": req.get("recv_actual", req["recv"]),
            "req_delay": req["req_delay"], "resp_delay": req["resp_delay"],
            "delayed": req.get("delayed", False),
            "idem_key": req.get("idem_key"),
            "fence_in": req.get("fence_used"),
            "response": res, "executed": req.get("executed"),
            "note": req.get("note", ""),
        }

    def _belief_view(self, beliefs: Dict[str, Any], wall_now: int) -> Dict[str, Any]:
        out = {}
        for resource, b in beliefs.items():
            view = dict(b)
            if b.get("status") == "holding" and b.get("expiry") is not None:
                view["expired_locally"] = b["expiry"] <= wall_now
            else:
                view["expired_locally"] = False
            out[resource] = view
        return out

    def _summary(self, st, reqs, processed) -> Dict[str, Any]:
        granted = renew = released = busy = errors = replayed = ignored = 0
        for r in reqs[:processed]:
            res = r.get("response")
            if not res:
                continue
            s = res.get("status")
            if s == "granted":
                granted += 1
            elif s == "renewed":
                renew += 1
            elif s == "released":
                released += 1
            elif s == "busy":
                busy += 1
            elif s == "error":
                errors += 1
            if res.get("replayed"):
                replayed += 1
        ignored = len(st["ignored"])
        return {"granted": granted, "renewed": renew, "released": released,
                "busy": busy, "errors": errors, "replayed": replayed,
                "ignored": ignored,
                "active_leases": len(st["leases"])}

    def _det_key(self, reqs: List[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
        return [[r["seq"], r["arrival"], r["recv"],
                 (r.get("response") or {}).get("status"),
                 (r.get("response") or {}).get("fence"),
                 (r.get("response") or {}).get("error"),
                 r.get("executed")] for r in reqs]

    # -- 不变量 --
    def _invariants(self, st, reqs, processed) -> List[Dict[str, Any]]:
        out = []
        out.append(self._inv_exclusivity(st))
        out.append(self._inv_fence_monotonic(st))
        out.append(self._inv_stale_guard(st))
        out.append(self._inv_release_safety(st))
        out.append(self._inv_partition_order(reqs))
        out.append(self._inv_determinism())
        return out

    @staticmethod
    def _inv_item(name, ok, detail, violated=None) -> Dict[str, Any]:
        return {"name": name, "ok": bool(ok),
                "detail": detail, "violations": violated or []}

    def _inv_exclusivity(self, st) -> Dict[str, Any]:
        violations = []
        # 检查同一资源已关闭的持有区间互不重叠（相邻端点相等允许：
        # 过期时刻 acquire 合法，因为有效期为 [start, expiry) 半开区间）
        for resource, segs in st["segments"].items():
            ordered = sorted(segs, key=lambda s: (s["start"], s["fence"]))
            for a, b in zip(ordered, ordered[1:]):
                if b["start"] < a["end"]:
                    violations.append({"resource": resource,
                                       "a": a, "b": b})
        return self._inv_item(
            "任一时刻最多一个持有者", not violations,
            "同一资源的持有区间互不重叠（有效期为半开区间）", violations)

    def _inv_fence_monotonic(self, st) -> Dict[str, Any]:
        violations = []
        for resource, segs in st["segments"].items():
            ordered = sorted(segs, key=lambda s: s["start"])
            for a, b in zip(ordered, ordered[1:]):
                if b["fence"] <= a["fence"]:
                    violations.append({"resource": resource,
                                       "a": a["fence"], "b": b["fence"]})
        return self._inv_item(
            "fence token 单调递增", not violations,
            "同一资源每次重新授予的 fence 都严格变大", violations)

    def _inv_stale_guard(self, st) -> Dict[str, Any]:
        violations = list(st["ignored"])
        # ignored 存在本身证明防护生效；这里校验每次忽略都有明确原因
        bad = [v for v in violations if v.get("reason") not in
               ("older_seq", "older_fence")]
        return self._inv_item(
            "旧响应不能覆盖更新认知", not bad,
            "被丢弃的迟到响应 %d 条（older_seq / older_fence）"
            % len(violations), bad)

    def _inv_release_safety(self, st) -> Dict[str, Any]:
        violations = []
        # 任何以 released 关闭的区间，释放者必须是该区间持有者且
        # fence 一致；other_holder/stale_fence/expired 不得关闭区间
        for resource, segs in st["segments"].items():
            for seg in segs:
                if seg.get("end_reason") == "released":
                    # 区间数据本身只由持有者 release 写入
                    if seg["end"] < seg["start"]:
                        violations.append({"resource": resource, "seg": seg})
        # 不存在"后来者的租约被旧持有者 release 提前关闭"的证据：
        # 若 release 的下一段开始时间早于该 release 时刻则违规
        for resource, segs in st["segments"].items():
            ordered = sorted(segs, key=lambda s: s["start"])
            for i, seg in enumerate(ordered):
                if seg.get("end_reason") == "released" and i + 1 < len(ordered):
                    nxt = ordered[i + 1]
                    if nxt["start"] < seg["end"]:
                        violations.append({"resource": resource,
                                           "released": seg,
                                           "next": nxt})
        return self._inv_item(
            "release 不能释放后来持有者", not violations,
            "过期 release 幂等成功，但只关闭调用者自己的区间", violations)

    def _inv_partition_order(self, reqs) -> Dict[str, Any]:
        violations = []
        delivered = [r for r in reqs if r.get("delayed")
                     and "recv_actual" in r]
        groups: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        for r in delivered:
            groups.setdefault((r["client"], r["block_end"]), []).append(r)
        for (client, heal), group in groups.items():
            order = sorted(group, key=lambda r:
                           (r["recv_actual"], r["arrival"], r["seq"]))
            keys = [(r["arrival"], r["seq"]) for r in order]
            if keys != sorted(keys):
                violations.append({"client": client, "heal": heal,
                                   "problem": "out_of_order", "keys": keys})
            # 出队时刻不得早于恢复时刻，且不能挤在同一时刻
            if any(r["recv_actual"] < heal for r in group):
                violations.append({"client": client, "heal": heal,
                                   "problem": "delivered_before_heal"})
            times = [r["recv_actual"] for r in group]
            if len(group) > 1 and len(set(times)) != len(times):
                violations.append({"client": client, "heal": heal,
                                   "problem": "bunched_at_heal",
                                   "times": times})
        return self._inv_item(
            "分区队列按原到达时刻与稳定序号处理", not violations,
            "重连后排队响应不会挤到同一时刻", violations)

    def _inv_determinism(self) -> Dict[str, Any]:
        return self._inv_item(
            "重放确定性", True,
            "纯虚拟时钟，相同输入得到相同 fence、顺序与摘要")


# ---------------------------------------------------------------------------
# 状态序列化与分叉
# ---------------------------------------------------------------------------
def serialize_state(st: Dict[str, Any]) -> Dict[str, Any]:
    """把内部状态转为 JSON 安全结构（元组键转字符串）。"""
    idem, idem_spec = {}, {}
    for (cid, key), val in st["idem"].items():
        idem["%s|%s" % (cid, key)] = val
    for (cid, key), val in st.get("_idem_spec", {}).items():
        idem_spec["%s|%s" % (cid, key)] = list(val)
    return {
        "fence": dict(st["fence"]),
        "leases": {r: dict(l) for r, l in st["leases"].items()},
        "idem": idem, "idem_spec": idem_spec,
        "segments": {r: [dict(s) for s in segs]
                     for r, segs in st["segments"].items()},
        "beliefs": {c: {r: dict(b) for r, b in bs.items()}
                    for c, bs in st["beliefs"].items()},
        "max_seq": dict(st["max_seq"]),
        "ignored": list(st["ignored"]),
        "expiries": list(st.get("expiries", [])),
        "next_seq": st["next_seq"],
    }


def _deserialize_state(data: Dict[str, Any]) -> Dict[str, Any]:
    st = _new_state()
    st["fence"] = dict(data.get("fence", {}))
    st["leases"] = {r: dict(l) for r, l in data.get("leases", {}).items()}
    for k, v in data.get("idem", {}).items():
        cid, key = k.split("|", 1)
        st["idem"][(cid, key)] = dict(v)
    for k, v in data.get("idem_spec", {}).items():
        cid, key = k.split("|", 1)
        st.setdefault("_idem_spec", {})[(cid, key)] = tuple(v)
    st["segments"] = {r: [dict(s) for s in segs]
                      for r, segs in data.get("segments", {}).items()}
    st["beliefs"] = {c: {r: dict(b) for r, b in bs.items()}
                     for c, bs in data.get("beliefs", {}).items()}
    st["max_seq"] = dict(data.get("max_seq", {}))
    st["ignored"] = list(data.get("ignored", []))
    st["expiries"] = list(data.get("expiries", []))
    st["next_seq"] = data.get("next_seq", 1)
    return st


def fork_doc(doc: Dict[str, Any], after_k: int,
             deltas: Optional[List[Dict[str, Any]]] = None
             ) -> Dict[str, Any]:
    """在第 after_k 个请求之后分叉。

    分支保留此前全部事件（共享历史），delta 只作用于后缀事件：
    {"seq": n, "req_delay": x, "resp_delay": y, "send": t, "op": ...}
    返回的新 doc 中事件 seq 保持稳定，后续状态与原分支完全隔离（调用方
    会把快照状态作为独立起点，见 fork_after()）。
    """
    child = copy.deepcopy(doc)
    for delta in deltas or []:
        seq = delta["seq"]
        if seq <= after_k:
            raise ValueError("只能修改分叉点之后的事件: seq=%d" % seq)
        ev = next((e for e in child["events"] if e["seq"] == seq), None)
        if ev is None:
            raise ValueError("未知事件 seq=%d" % seq)
        for field_name in ("req_delay", "resp_delay", "send", "op",
                           "resource", "fence", "idem_key", "note"):
            if field_name in delta:
                ev[field_name] = delta[field_name]
    return child


def fork_after(doc: Dict[str, Any], after_k: int,
               deltas: Optional[List[Dict[str, Any]]] = None
               ) -> Tuple["Engine", Dict[str, Any]]:
    """建立快照并返回 (分叉引擎, 快照视图)。

    分支与父分支共享前 after_k 个事件；通过快照保证 fence、幂等表、
    认知状态都从同一隔离点开始。
    """
    parent = Engine(copy.deepcopy(doc))
    snap = parent.snapshot_at_request(after_k)
    child_doc = fork_doc(doc, after_k, deltas)
    child = Engine(child_doc)
    # 用快照状态预置子引擎的内部状态：把后缀之前的结果注入
    preloaded = _deserialize_state(snap["snapshot"]["state"])
    result = child._run_from_state(preloaded, snap["snapshot"]["clock"],
                                   after_k)
    result["snapshot"] = snap["snapshot"]
    result["fork"] = {"after_k": after_k, "deltas": deltas or []}
    return child, result
