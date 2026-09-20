"""引擎测试：全部基于虚拟时钟（无真实睡眠、无真实网络）。"""
from __future__ import annotations

import json

import pytest

from leasedebug.engine import Engine, SimClock, fork_after


def test_virtual_clock_is_monotonic():
    clk = SimClock()
    assert clk.now == 0
    clk.advance(10)
    clk.advance(10)
    assert clk.now == 10
    clk.advance(999)
    assert clk.now == 999
    with pytest.raises(ValueError):
        clk.advance(1)


def test_acquire_renew_release_basic():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", clock_offset=0, latency_req=5, latency_resp=5)
    e.add_request(a, 0, "acquire", "r", idem_key="k1")
    e.add_request(a, 50, "renew", "r")
    e.add_request(a, 140, "release", "r")
    r = e.run()

    assert r["requests"][0]["response"]["status"] == "granted"
    assert r["requests"][0]["response"]["fence"] == 1
    assert r["requests"][1]["response"]["status"] == "renewed"
    assert r["requests"][1]["response"]["expiry"] == 155  # 55+100
    assert r["requests"][2]["response"]["status"] == "released"
    assert r["requests"][2]["response"]["reason"] == "released"
    assert r["server"]["active"] == {}
    seg = r["server"]["segments"]["r"][0]
    assert seg["start"] == 5 and seg["end"] == 145
    assert seg["end_reason"] == "released"
    assert all(i["ok"] for i in r["invariants"])


def test_simultaneous_arrival_tie_break_by_stable_seq():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=10)
    b = e.add_client("B", latency_req=10)
    e.add_request(a, 0, "acquire", "r")   # arrival 10, seq1
    e.add_request(b, 0, "acquire", "r")   # arrival 10, seq2
    r = e.run()

    arrivals = [(q["arrival"], q["seq"]) for q in r["requests"]]
    assert arrivals == [(10, 1), (10, 2)]
    assert r["requests"][0]["response"]["status"] == "granted"
    assert r["requests"][0]["response"]["fence"] == 1
    assert r["requests"][1]["response"]["status"] == "busy"
    # 再跑一次：稳定顺序与 fence 必须完全一致
    r2 = Engine(e.doc).run()
    assert r2["determinism_key"] == r["determinism_key"]


def test_expiry_boundary_is_half_open():
    # 租约有效期为 [acquire, acquire+ttl)，边界时刻 acquire 合法
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=0, latency_resp=0)
    b = e.add_client("B", latency_req=0, latency_resp=0)
    e.add_request(a, 0, "acquire", "r")     # [0,100)
    e.add_request(a, 99, "renew", "r")      # 99：未过期，续租成功
    e.add_request(b, 198, "acquire", "r")   # 续租后 [99,199)：busy
    e.add_request(b, 198, "release", "r")   # 198 仍过期不了别人
    r = e.run()
    assert r["requests"][1]["response"]["status"] == "renewed"
    assert r["requests"][2]["response"]["status"] == "busy"
    assert r["server"]["active"]["r"]["holder"] == a

    # 恰好在 expiry 时刻：已过期，可以重新授予
    e2 = Engine.fresh(ttl=100)
    a = e2.add_client("A", latency_req=0, latency_resp=0)
    b = e2.add_client("B", latency_req=0, latency_resp=0)
    e2.add_request(a, 0, "acquire", "r")
    e2.add_request(b, 100, "acquire", "r")  # 端点过期
    r2 = e2.run()
    assert r2["requests"][1]["response"]["status"] == "granted"
    assert r2["requests"][1]["response"]["fence"] == 2
    assert all(i["ok"] for i in r2["invariants"])


def test_late_old_renew_response_cannot_overwrite():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=0, latency_resp=0)
    e.add_request(a, 0, "acquire", "r", resp_delay=5)    # recv 5
    e.add_request(a, 50, "renew", "r", resp_delay=150)   # recv 200 旧
    e.add_request(a, 90, "renew", "r", resp_delay=5)     # recv 95 新
    r = e.run()

    assert r["requests"][1]["response"]["expiry"] == 150
    assert r["requests"][2]["response"]["expiry"] == 190
    ignored = [i for i in r["ignored"] if i["seq"] == 2]
    assert len(ignored) == 1 and ignored[0]["reason"] == "older_seq"
    belief = r["clients"][0]["beliefs"]["r"]
    assert belief["fence"] == 1 and belief["expiry"] == 190
    assert belief["updated_seq"] == 3


def test_release_then_reacquire_and_stale_release_safety():
    e = Engine.fresh(ttl=100)
    a = e.add_client("A", latency_req=0, latency_resp=0)
    b = e.add_client("B", latency_req=0, latency_resp=0)
    e.add_request(a, 0, "acquire", "r")
    e.add_request(a, 10, "release", "r")
    e.add_request(b, 15, "acquire", "r")      # fence 2
    # A 以为自己还持有：它的租约已在 10 被自己释放，
    # release 幂等成功但不能释放 B（reason=already_expired）
    e.add_request(a, 20, "release", "r")
    r = e.run()
    assert r["requests"][3]["response"]["reason"] == "already_expired"
    assert r["server"]["active"]["r"]["holder"] == b
    assert r["server"]["active"]["r"]["fence"] == 2

    # 过期后的 release 也幂等成功，且不影响后来者
    e2 = Engine.fresh(ttl=100)
    a = e2.add_client("A", latency_req=0, latency_resp=0)
    b = e2.add_client("B", latency_req=0, latency_resp=0)
    e2.add_request(a, 0, "acquire", "r")
    e2.add_request(b, 100, "acquire", "r")    # A 已过期，B 拿到 f2
    e2.add_request(a, 105, "release", "r")    # A 事后释放
    r2 = e2.run()
    assert r2["requests"][2]["response"]["reason"] == "already_expired"
    assert r2["server"]["active"]["r"]["holder"] == b
    assert all(i["ok"] for i in r2["invariants"])
