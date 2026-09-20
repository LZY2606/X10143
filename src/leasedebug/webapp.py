"""HTTP 服务：REST JSON API + 静态单页。零第三方依赖。"""
from __future__ import annotations

import json
import os
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Tuple
from urllib.parse import urlparse

from .demo import build_demo_doc
from .engine import Engine, fork_after
from .store import Store

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _public_id(prefix: str) -> str:
    return prefix + uuid.uuid4().hex[:10]


class AppState:
    def __init__(self, db_path: str) -> None:
        self.store = Store(db_path)
        self.lock = threading.Lock()
        if not self.store.list_experiments():
            doc = build_demo_doc()
            self.store.create_experiment(_public_id("exp-"),
                                         "演示实验", doc)

    def close(self) -> None:
        self.store.close()


def _doc_engine(exp: Dict[str, Any]) -> Engine:
    return Engine(exp["doc"])


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "LeaseDebug/0.1"

    # --- 基础工具 ---
    def _send_json(self, obj: Any, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _exp(self, exp_id: str) -> Dict[str, Any]:
        exp = self.server.state.store.get_experiment(exp_id)
        if exp is None:
            raise _NotFound("实验不存在: " + exp_id)
        return exp

    def log_message(self, fmt, *args):  # 安静一点
        return

    # --- 路由 ---
    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_DELETE(self) -> None:
        self._dispatch("DELETE")

    def _dispatch(self, method: str) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            parts = [p for p in path.split("/") if p]
            with self.server.state.lock:
                self._route(method, parts)
        except _NotFound as exc:
            self._send_json({"error": str(exc)}, 404)
        except _BadRequest as exc:
            self._send_json({"error": str(exc)}, 400)
        except Exception as exc:  # noqa: BLE001 - 服务进程不能挂
            self._send_json({"error": "%s: %s" %
                             (type(exc).__name__, exc)}, 500)

    def _route(self, method: str, parts) -> None:
        store = self.server.state.store
        if method == "GET" and parts == ["api", "experiments"]:
            self._send_json({"experiments": store.list_experiments()})
            return
        if method == "POST" and parts == ["api", "experiments"]:
            body = self._read_json()
            engine = Engine.fresh(ttl=int(body.get("ttl", 100)))
            exp_id = _public_id("exp-")
            meta = store.create_experiment(
                exp_id, body.get("name", "未命名实验"), engine.doc)
            self._send_json(meta, 201)
            return
        if len(parts) >= 3 and parts[:2] == ["api", "experiments"]:
            self._route_experiment(method, parts[2:])
            return
        if method == "GET" and parts == ["api", "export"]:
            self._send_json(store.export_all())
            return
        if method == "POST" and parts == ["api", "import"]:
            counts = store.import_all(self._read_json())
            self._send_json({"imported": counts})
            return
        if method == "GET" and (parts == [] or parts == ["index.html"]):
            self._serve_static("index.html", "text/html; charset=utf-8")
            return
        if method == "GET" and len(parts) == 1:
            ctype = {
                "app.js": "application/javascript; charset=utf-8",
                "app.css": "text/css; charset=utf-8",
            }.get(parts[0], "application/octet-stream")
            self._serve_static(parts[0], ctype)
            return
        raise _NotFound("未知路径")

    # --- /api/experiments/<id>/... ---
    def _route_experiment(self, method, parts) -> None:
        store = self.server.state.store
        exp_id = parts[0]
        rest = parts[1:]
        if method == "GET" and not rest:
            self._send_json(self._exp(exp_id))
            return
        if method == "DELETE" and not rest:
            store.delete_experiment(exp_id)
            self._send_json({"deleted": exp_id})
            return
        if method == "POST" and rest == ["replay"]:
            exp = self._exp(exp_id)
            self._send_json(Engine(exp["doc"]).run())
            return
        if method == "POST" and rest == ["replay-until"]:
            body = self._read_json()
            exp = self._exp(exp_id)
            self._send_json(Engine(exp["doc"]).run_until(int(body["t"])))
            return
        if method == "POST" and rest == ["clients"]:
            body = self._read_json()
            exp = self._exp(exp_id)
            cid = Engine(exp["doc"]).add_client(
                body.get("name", "客户端"),
                int(body.get("clock_offset", 0)),
                int(body.get("latency_req", 0)),
                int(body.get("latency_resp", 0)))
            store.save_doc(exp_id, exp["doc"])
            self._send_json({"id": cid}, 201)
            return
        if method == "POST" and rest == ["requests"]:
            self._add_request(exp_id)
            return
        if method == "POST" and rest == ["partitions"]:
            self._add_partition(exp_id)
            return
        if method == "DELETE" and len(rest) == 2 and rest[0] == "events":
            exp = self._exp(exp_id)
            doc = exp["doc"]
            doc["events"] = [e for e in doc["events"] if e["id"] != rest[1]]
            doc["partitions"] = [p for p in doc["partitions"]
                                 if p["id"] != rest[1]]
            store.save_doc(exp_id, doc)
            self._send_json({"deleted": rest[1]})
            return
        if method == "GET" and rest == ["snapshots"]:
            self._exp(exp_id)
            self._send_json({"snapshots":
                             store.list_snapshots(exp_id)})
            return
        if method == "POST" and rest == ["snapshots"]:
            self._save_snapshot(exp_id)
            return
        if method == "POST" and rest == ["forks"]:
            self._create_fork(exp_id)
            return
        raise _NotFound("未知子路径: " + "/".join(rest))

    def _add_request(self, exp_id: str) -> None:
        body = self._read_json()
        for field in ("client", "send", "op", "resource"):
            if field not in body:
                raise _BadRequest("缺少字段 " + field)
        if body["op"] not in ("acquire", "renew", "release", "read"):
            raise _BadRequest("非法操作 " + str(body["op"]))
        store = self.server.state.store
        exp = self._exp(exp_id)
        engine = Engine(exp["doc"])
        seq = engine.add_request(
            body["client"], int(body["send"]), body["op"],
            str(body["resource"]),
            idem_key=body.get("idem_key") or None,
            fence=body.get("fence"),
            req_delay=body.get("req_delay"),
            resp_delay=body.get("resp_delay"),
            note=body.get("note", ""))
        store.save_doc(exp_id, exp["doc"])
        self._send_json({"seq": seq, "id": "e%d" % seq}, 201)

    def _add_partition(self, exp_id: str) -> None:
        body = self._read_json()
        store = self.server.state.store
        exp = self._exp(exp_id)
        engine = Engine(exp["doc"])
        pid = engine.add_partition(int(body["start"]), int(body["end"]),
                                   body.get("client"))
        store.save_doc(exp_id, exp["doc"])
        self._send_json({"id": pid}, 201)

    def _save_snapshot(self, exp_id: str) -> None:
        body = self._read_json()
        after_k = int(body.get("after_k", 0))
        exp = self._exp(exp_id)
        engine = Engine(exp["doc"])
        if after_k < 0 or after_k > len(exp["doc"]["events"]):
            raise _BadRequest("after_k 超出事件数")
        snap = engine.snapshot_at_request(after_k)
        snap_id = _public_id("snap-")
        payload = {"snapshot": snap["snapshot"], "doc": exp["doc"]}
        self.server.state.store.save_snapshot(
            snap_id, exp_id, body.get("label", "快照"), after_k, payload)
        self._send_json({"id": snap_id, "after_k": after_k}, 201)

    def _create_fork(self, exp_id: str) -> None:
        body = self._read_json()
        after_k = int(body.get("after_k", 0))
        deltas = body.get("deltas", [])
        exp = self._exp(exp_id)
        engine, result = fork_after(exp["doc"], after_k, deltas)
        child_id = _public_id("exp-")
        name = body.get("name") or ("%s（分叉@%d）" %
                                    (exp["name"], after_k))
        self.server.state.store.create_experiment(
            child_id, name, engine.doc, parent_id=exp_id,
            fork_after=after_k)
        self._send_json({"id": child_id,
                         "name": name, "replay": result}, 201)

    def _serve_static(self, filename: str, ctype: str) -> None:
        path = os.path.join(STATIC_DIR, filename)
        if not os.path.isfile(path):
            raise _NotFound(filename)
        with open(path, "rb") as fh:
            data = fh.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class _NotFound(Exception):
    pass


class _BadRequest(Exception):
    pass


def create_server(host: str, port: int, db_path: str) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.state = AppState(db_path)  # type: ignore[attr-defined]
    return server


def serve(host: str, port: int, db_path: str) -> Tuple[ThreadingHTTPServer, AppState]:
    server = create_server(host, port, db_path)
    return server, server.state  # type: ignore[attr-defined]
