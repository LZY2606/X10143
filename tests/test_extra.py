"""边角语义：旧 fence 释放、墙上时间只用于展示、游标与快照。"""
from __future__ import annotations

from leasedebug.engine import Engine


def test_holder_release_with_stale_fence_is_idempotent_and_safe():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=0, latency_resp=0)
    e.add_request(a, 0, "acquire", "r")
    # 当前 fence=1，故意提交更旧的 fence=0
    e.add_request(a, 20, "release", "r", fence=0)
    r = e.run()
    res = r["requests"][1]["response"]
    assert res["status"] == "released"
    assert res["idempotent"] is True
    assert res["reason"] == "stale_fence"
    # 租约没有被关闭
    assert r["server"]["active"]["r"]["fence"] == 1
    seg = r["server"]["segments"]["r"][0]
    assert seg["end_reason"] == "open"


def test_client_wall_clock_offset_is_display_only():
    e = Engine.fresh(ttl=100)
    # A 时钟快 50ms：它会更早"以为"租约过期，但服务端裁决不变
    a = e.add_client("A", clock_offset=50, latency_req=0, latency_resp=0)
    b = e.add_client("B", clock_offset=-30, latency_req=0, latency_resp=0)
    e.add_request(a, 0, "acquire", "r")
    e.add_request(b, 120, "read", "r")  # 推进模拟时钟到租约过期之后
    r = e.run()
    ca = r["clients"][0]
    cb = r["clients"][1]
    assert ca["wall_now"] == r["clock"] + 50
    assert cb["wall_now"] == r["clock"] - 30
    belief = ca["beliefs"]["r"]
    assert belief["status"] == "holding"
    assert belief["expired_locally"] is True   # 墙上 150 > expiry 100
    # 服务端视角：租约在 t=100 才过期
    assert r["server"]["active"] == {}


def test_cursor_run_until_is_prefix_of_full_run():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=5, latency_resp=5)
    b = e.add_client("B", latency_req=5, latency_resp=5)
    e.add_request(a, 0, "acquire", "r")
    e.add_request(a, 80, "renew", "r")
    e.add_request(b, 90, "acquire", "r")
    full = e.run()

    mid = e.run_until(10)  # 只有首个请求到达 5
    assert mid["processed"] == 1
    assert mid["server"]["active"]["r"]["fence"] == 1
    assert mid["clock"] <= 10
    # 游标位置是完整轨迹的前缀（摘要计数不减少）
    for key in ("granted", "renewed", "busy", "errors"):
        assert mid["summary"][key] <= full["summary"][key]
    # 任意中间时刻重放确定性
    assert e.run_until(10)["determinism_key"] == mid["determinism_key"]


def test_snapshot_at_zero_has_empty_state():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A")
    e.add_request(a, 10, "acquire", "r")
    snap = e.snapshot_at_request(0)
    assert snap["snapshot"]["k"] == 0
    assert snap["snapshot"]["state"]["fence"] == {}


def test_read_does_not_change_server_or_belief():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=0, latency_resp=0)
    e.add_request(a, 0, "read", "r")
    e.add_request(a, 5, "acquire", "r")
    r = e.run()
    assert r["requests"][0]["response"]["lease"] is None
    # read 不产生任何认知条目
    assert "r" not in r["clients"][0]["beliefs"] or \
        r["clients"][0]["beliefs"]["r"]["status"] == "holding"
    assert r["server"]["fence"] == {"r": 1}
