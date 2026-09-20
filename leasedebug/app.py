"""HTTP server for the lease debugging workbench."""

from __future__ import annotations

import argparse
import json
import mimetypes
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
import sys

from .store import Store

STATIC_DIR = Path(__file__).parent / "static"


class LeaseDebugHandler(BaseHTTPRequestHandler):
    store = Store()

    def log_message(self, fmt: str, *args: Any) -> None:
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path) -> None:
        if not path.exists() or not path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/experiments":
            self._send_json(HTTPStatus.OK, self.store.list_experiments())
            return
        if parsed.path.startswith("/api/experiments/"):
            parts = [part for part in parsed.path.split("/") if part]
            experiment_id = parts[2]
            query = parse_qs(parsed.query)
            if len(parts) == 3:
                client_id = query.get("client", [None])[0]
                self._handle_store(self.store.state, experiment_id, client_id=client_id)
                return
            if len(parts) == 5 and parts[3] == "export":
                self._handle_store(self.store.export, experiment_id)
                return
        if parsed.path == "/":
            self._send_file(STATIC_DIR / "index.html")
            return
        if parsed.path.startswith("/static/"):
            name = Path(parsed.path.removeprefix("/static/")).name
            self._send_file(STATIC_DIR / name)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        parts = [part for part in parsed.path.split("/") if part]
        if parsed.path == "/api/experiments":
            self._with_body(lambda payload: self.store.create_experiment(
                clients=payload["clients"],
                ttl_ms=int(payload.get("ttl_ms", 10_000)),
                name=str(payload.get("name", "未命名实验")),
            ))
            return
        if len(parts) < 4 or parts[0] != "api" or parts[1] != "experiments":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        experiment_id, action = parts[2], parts[3]
        actions = {
            "operations": lambda payload: self.store.add_operation(experiment_id, payload),
            "partitions": lambda payload: self.store.add_partition(experiment_id, payload),
            "clients": lambda payload: self.store.add_client(experiment_id, payload),
            "client": lambda payload: self.store.update_client(experiment_id, payload),
            "step": lambda payload: self.store.step(experiment_id),
            "run": lambda payload: self.store.fast_forward(experiment_id),
            "seek": lambda payload: self.store.seek(experiment_id, int(payload["target_ms"])),
            "snapshots": lambda payload: self.store.snapshot(
                experiment_id, str(payload.get("label", "快照"))
            ),
            "fork": lambda payload: self.store.fork(
                experiment_id,
                label=str(payload.get("label", "分叉")),
                client_id=payload.get("client_id"),
                latency_ms=payload.get("latency_ms"),
            ),
            "latency": lambda payload: self.store.update_latency(
                experiment_id,
                str(payload["client_id"]),
                int(payload["latency_ms"]),
            ),
            "branch": lambda payload: self.store.select_branch(
                experiment_id, str(payload["branch_id"])
            ),
            "import": lambda payload: self.store.import_experiment(payload),
        }
        if action in actions:
            self._with_body(actions[action])
        else:
            self.send_error(HTTPStatus.NOT_FOUND)

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _with_body(self, callback: Any) -> None:
        try:
            payload = self._read_body()
            result = callback(payload)
            self._send_json(HTTPStatus.OK, result)
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def _handle_store(self, callback: Any, *args: Any, **kwargs: Any) -> None:
        try:
            self._send_json(HTTPStatus.OK, callback(*args, **kwargs))
        except KeyError as exc:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": str(exc)})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="租约调试台")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5224)
    parser.add_argument("--data", default=".leasedebug-data.json")
    args = parser.parse_args(argv)
    LeaseDebugHandler.store = Store(args.data)
    server = ThreadingHTTPServer((args.host, args.port), LeaseDebugHandler)
    print(f"租约调试台运行中: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
