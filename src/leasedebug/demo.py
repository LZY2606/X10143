"""内置演示实验：覆盖大部分调试台能力。"""
from __future__ import annotations

from .engine import Engine


def build_demo_doc():
    e = Engine.fresh(ttl=100)
    a = e.add_client("客户端A", clock_offset=0, latency_req=4, latency_resp=6)
    b = e.add_client("客户端B", clock_offset=30, latency_req=8,
                     latency_resp=8)
    c = e.add_client("客户端C", clock_offset=-20, latency_req=2,
                     latency_resp=2)

    # --- 资源 res/db：正常获取 / 续租 / 忙等 ---
    e.add_request(a, 0, "acquire", "res/db", idem_key="a-db-1",
                  note="A 首次获取")
    e.add_request(b, 60, "read", "res/db", note="B 读取当前持有者")
    e.add_request(a, 80, "renew", "res/db", note="A 到期前续租")
    e.add_request(b, 90, "acquire", "res/db", note="B 抢锁 -> busy")
    e.add_request(a, 170, "renew", "res/db", note="A 再次续租")

    # --- 分区 [280,360)：A 与服务端隔离 ---
    e.add_partition(280, 360, a)
    e.add_request(a, 300, "renew", "res/db",
                  note="分区中的续租：到达时租约已过期，错误响应被延迟")
    e.add_request(b, 280, "acquire", "res/db", note="B 在 A 过期后获取 f2")
    e.add_request(c, 290, "acquire", "res/db",
                  note="C 再抢锁，看到 B 持有")
    e.add_request(c, 320, "read", "res/db", note="C 分区期间读取")

    # --- 旧认知不能释放后来持有者 ---
    e.add_request(a, 370, "release", "res/db",
                  note="A 仍以为自己持有 f1，释放幂等但不影响 B")
    e.add_request(b, 400, "release", "res/db", note="B 正常释放 f2")
    e.add_request(a, 410, "acquire", "res/db", note="释放后再获取，fence=3")

    # --- 幂等重送 ---
    e.add_request(c, 500, "acquire", "res/db", idem_key="c-db-retry",
                  note="C 首次抢锁（busy）")
    e.add_request(c, 560, "acquire", "res/db", idem_key="c-db-retry",
                  note="同一幂等键重送，回放首次结果")

    # --- 资源 res/2：两个请求同时到达，稳定序号决胜 ---
    e.add_request(b, 642, "acquire", "res/2", note="B 与 C 同时到达 #1")
    e.add_request(c, 648, "acquire", "res/2", note="B 与 C 同时到达 #2")

    # --- 资源 res/3：迟到的旧续租响应不能覆盖更新认知 ---
    e.add_request(a, 700, "acquire", "res/3", idem_key="a-3",
                  resp_delay=2, note="A 获取 res/3，响应很快")
    e.add_request(a, 720, "renew", "res/3", resp_delay=120,
                  note="旧续租，响应刻意延迟到新续租之后")
    e.add_request(a, 740, "renew", "res/3", resp_delay=2,
                  note="新续租先回到客户端")

    return e.doc
