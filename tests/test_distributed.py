"""分区队列、幂等、快照分叉、重启恢复、HTTP 层测试（均为虚拟时钟）。"""
from __future__ import annotations

import json
import os
import tempfile

import pytest

from leasedebug.engine import Engine, fork_after
from leasedebug.store import Store


def test_partition_queue_keeps_order_and_spacing():
    e = Engine.fresh(ttl=1000)
    a = e.add_client("A", latency_req=10, latency_resp=0)
    b = e.add_client("B", latency_req=10, latency_resp=0)
    e.add_partition(0, 100, a)
    # 三个请求在分区期间到达（30/31/32），服务端仍按原时刻裁决
    e.add_request(a, 20, "acquire", "r", idem_key="a1")
    e.add_request(a, 21, "read", "r")
    e.add_request(a, 22, "read", "r")
    # B 不受影响：30 时刻 A 已持有
    e.add_request(b, 20, "acquire", "r")
    r = e.run()

    a_reqs = [q for q in r["requests"] if q["client"] == a]
    # 恢复后投递时刻：100 + (arrival - 30)，保留 1ms 间距，不挤在一起
    assert [q["recv"] for q in a_reqs] == [100, 101, 102]
    assert all(q["delayed"] for q in a_reqs)
    # 服务端处理顺序仍按 (arrival, seq)
    assert a_reqs[0]["response"]["status"] == "granted"
    b_req = next(q for q in r["requests"] if q["client"] == b)
    assert b_req["response"]["status"] == "busy"
    assert b_req["recv"] == 30
    inv = next(i for i in r["invariants"]
               if i["name"].startswith("分区队列"))
    assert inv["ok"], inv["violations"]


def test_partition_requests_processed_at_original_arrival():
    # TTL 100：分区期间到达的续租按 50 裁决（成功），
    # 不能因为在 200 才出队而判它过期
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=0, latency_resp=0)
    e.add_request(a, 0, "acquire", "r", resp_delay=5)
    e.add_partition(10, 200, a)
    e.add_request(a, 50, "renew", "r")   # 逻辑到达 50，租约 [0,100)
    r = e.run()
    assert r["requests"][1]["response"]["status"] == "renewed"
    assert r["requests"][1]["recv"] >= 200  # 响应确实延迟到恢复后


def test_idempotent_resend_replays_first_result():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=0, latency_resp=0)
    b = e.add_client("B", latency_req=0, latency_resp=0)
    e.add_request(a, 0, "acquire", "r", idem_key="dup")
    e.add_request(a, 10, "release", "r")
    e.add_request(b, 15, "acquire", "r")
    # 同幂等键重送：必须回放首次结果 fence=1，而不是重新执行
    e.add_request(a, 20, "acquire", "r", idem_key="dup")
    r = e.run()
    replays = [q for q in r["requests"] if q["response"].get("replayed")]
    assert len(replays) == 1
    assert replays[0]["seq"] == 4
    assert replays[0]["response"]["fence"] == 1
    assert replays[0]["executed"] is False
    # fence 序列仍是 1（A）、2（B），回放没有制造新授予
    assert r["server"]["fence"] == {"r": 2}
    assert r["summary"]["replayed"] == 1

    # 幂等键冲突（同键不同操作）应报错
    e2 = Engine.fresh(ttl=100)
    a = e2.add_client("A", latency_req=0, latency_resp=0)
    e2.add_request(a, 0, "acquire", "r", idem_key="k")
    e2.add_request(a, 10, "renew", "r", idem_key="k")
    r2 = e2.run()
    assert r2["requests"][1]["response"]["error"] == "idem_key_conflict"


def test_snapshot_fork_isolation():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=5, latency_resp=5)
    b = e.add_client("B", latency_req=5, latency_resp=5)
    e.add_request(a, 0, "acquire", "r")                 # 1 共享
    e.add_request(a, 90, "renew", "r")                  # 2
    e.add_request(b, 100, "acquire", "r")               # 3

    parent = e.run()
    # 父分支：renew arrival 95 成功；B busy；fence 仅 1
    assert parent["requests"][1]["response"]["status"] == "renewed"
    assert parent["requests"][2]["response"]["status"] == "busy"
    assert parent["server"]["fence"] == {"r": 1}

    # 分叉：共享事件 1，把续租请求延迟改大到 30 -> arrival 120 已过期
    child, cr = fork_after(e.doc, 1,
                           deltas=[{"seq": 2, "req_delay": 30}])
    by_seq = {q["seq"]: q for q in cr["requests"]}
    assert by_seq[2]["arrival"] == 120
    assert by_seq[2]["response"]["error"] == "not_holder"
    assert by_seq[3]["response"]["status"] == "granted"
    assert by_seq[3]["response"]["fence"] == 2

    # 子分支隔离不影响父分支
    parent_again = Engine(e.doc).run()
    assert parent_again["determinism_key"] == parent["determinism_key"]
    assert parent_again["server"]["fence"] == {"r": 1}
    # 共享历史（快照 fence=1，acquired 一致）
    assert cr["snapshot"]["state"]["fence"] == {"r": 1}
    assert cr["fork"]["after_k"] == 1
    assert all(i["ok"] for i in cr["invariants"])


def _make_doc():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=4, latency_resp=6, clock_offset=12)
    b = e.add_client("B", latency_req=8, latency_resp=8)
    e.add_request(a, 0, "acquire", "res/db", idem_key="x")
    e.add_request(a, 80, "renew", "res/db")
    e.add_request(b, 90, "acquire", "res/db")
    e.add_partition(200, 260, a)
    e.add_request(a, 210, "renew", "res/db")
    return e.doc


def test_restart_recovery_from_sqlite():
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "state.db")
        store = Store(db)
        doc = _make_doc()
        store.create_experiment("exp-1", "恢复实验", doc)
        store.save_snapshot("snap-1", "exp-1", "检查点", 2,
                            {"k": 2})
        first = Engine(doc).run()
        store.close()

        # 重启：重新打开同一个 SQLite
        store2 = Store(db)
        exp = store2.get_experiment("exp-1")
        assert exp["name"] == "恢复实验"
        second = Engine(exp["doc"]).run()
        assert second["determinism_key"] == first["determinism_key"]
        assert second["summary"] == first["summary"]
        assert second["server"]["fence"] == first["server"]["fence"]
        snaps = store2.list_snapshots("exp-1")
        assert len(snaps) == 1 and snaps[0]["label"] == "检查点"
        store2.close()


def test_export_import_roundtrip_identical():
    with tempfile.TemporaryDirectory() as tmp:
        db1 = os.path.join(tmp, "a.db")
        s1 = Store(db1)
        doc = _make_doc()
        s1.create_experiment("exp-x", "导出实验", doc)
        original = Engine(doc).run()
        blob = s1.export_all()
        s1.close()

        db2 = os.path.join(tmp, "b.db")
        s2 = Store(db2)
        counts = s2.import_all(blob)
        assert counts["experiments"] == 1
        exp = s2.get_experiment("exp-x")
        imported = Engine(exp["doc"]).run()
        assert imported["determinism_key"] == original["determinism_key"]
        assert imported["summary"] == original["summary"]
        # fence token 逐个一致
        assert imported["server"]["fence"] == original["server"]["fence"]
        s2.close()

        # JSON 序列化稳定（导入导出文件本身可再次导入）
        text = json.dumps(blob, sort_keys=True)
        assert json.loads(text)["format"] == "leasedebug-export"
