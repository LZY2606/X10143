"""HTTP API 与静态页测试（回环端口，虚拟时钟，不访问外网）。"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import urllib.request

import pytest

from leasedebug.webapp import create_server


@pytest.fixture()
def server():
    tmp = tempfile.TemporaryDirectory()
    db = os.path.join(tmp.name, "test.db")
    srv = create_server("127.0.0.1", 0, db)
    port = srv.server_address[1]
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    base = "http://127.0.0.1:%d" % port
    yield base, srv
    srv.shutdown()
    srv.server_close()
    srv.state.close()
    tmp.cleanup()


def req(base, method, path, body=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    r = urllib.request.Request(base + path, data=data, headers=headers,
                               method=method)
    with urllib.request.urlopen(r) as resp:
        raw = resp.read().decode("utf-8")
        return resp.status, json.loads(raw) if raw else {}


def test_index_page_title(server):
    base, _ = server
    with urllib.request.urlopen(base + "/") as resp:
        html = resp.read().decode("utf-8")
    assert "租约调试台" in html


def test_full_workflow_via_api(server):
    base, _ = server
    # 启动时已有内置演示实验
    _, data = req(base, "GET", "/api/experiments")
    assert len(data["experiments"]) >= 1
    exp_id = data["experiments"][0]["id"]

    _, replay = req(base, "POST",
                    "/api/experiments/" + exp_id + "/replay")
    assert "timeline" in replay and "invariants" in replay

    # 游标重放
    _, mid = req(base, "POST",
                 "/api/experiments/" + exp_id + "/replay-until", {"t": 0})
    assert mid["clock"] == 0

    # 添加客户端与请求
    _, c = req(base, "POST",
               "/api/experiments/" + exp_id + "/clients",
               {"name": "测试端", "clock_offset": 5,
                "latency_req": 3, "latency_resp": 3})
    _, rq = req(base, "POST",
                "/api/experiments/" + exp_id + "/requests",
                {"client": c["id"], "send": 1000, "op": "acquire",
                 "resource": "res/new"})
    assert rq["seq"] >= 1

    # 添加分区
    _, p = req(base, "POST",
               "/api/experiments/" + exp_id + "/partitions",
               {"start": 1100, "end": 1200, "client": c["id"]})
    assert p["id"].startswith("p")

    # 快照 + 分叉
    _, snap = req(base, "POST",
                  "/api/experiments/" + exp_id + "/snapshots",
                  {"after_k": 2, "label": "检查点"})
    assert snap["after_k"] == 2
    _, fork = req(base, "POST",
                  "/api/experiments/" + exp_id + "/forks",
                  {"after_k": 2,
                   "deltas": [{"seq": 3, "req_delay": 50}],
                   "name": "延迟分支"})
    assert fork["id"] != exp_id
    assert "replay" in fork
    _, fr = req(base, "POST",
                "/api/experiments/" + fork["id"] + "/replay")
    assert all(i["ok"] for i in fr["invariants"])

    # 导出 / 导入一致性：导出里包含父实验的完整 doc
    _, blob = req(base, "GET", "/api/export")
    assert blob["format"] == "leasedebug-export"
    parent = next(e for e in blob["experiments"] if e["id"] == exp_id)
    from leasedebug.engine import Engine
    original = Engine(parent["doc"]).run()

    _, imported = req(base, "POST", "/api/import", blob)
    assert imported["imported"]["experiments"] == len(blob["experiments"])
    _, again = req(base, "POST",
                   "/api/experiments/" + exp_id + "/replay")
    assert again["determinism_key"] == original["determinism_key"]
    assert again["summary"] == original["summary"]


def test_bad_request_is_400(server):
    base, _ = server
    _, data = req(base, "GET", "/api/experiments")
    exp_id = data["experiments"][0]["id"]
    try:
        req(base, "POST",
            "/api/experiments/" + exp_id + "/requests",
            {"client": "c1", "op": "wat"})
        assert False, "应当 400"
    except urllib.error.HTTPError as e:
        assert e.code == 400
