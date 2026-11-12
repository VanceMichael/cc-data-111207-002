"""竞争队列 + 故障传输的本地演示。

场景：三家合作社共用一个出口和服务商主账号（凭据组 shared / isolated 均
为虚构标识）。rice 实时采集（权重 3）与 tea 历史补采（权重 1）竞争，
fruit 提交一笔带副作用的不可重放调用。故障注入：

- 外部挤占：值班同事手工重试，在调度器之外占用 shared 凭据组 2 条额度；
- fruit-tx-1 首次发送后连接断开（已发出、响应丢失）；
- 第 4 次发送触发服务端出口级暂停，Retry-After 为 HTTP 日期；
- 第 6 次发送后模拟进程崩溃，随后从日志恢复继续执行。

逻辑时钟拨快代替真实等待，Source 为无外部请求的本地服务。
运行：python3 demo.py
"""

import json
import tempfile
from email.utils import formatdate
from pathlib import Path
from urllib.parse import urlencode

from scheduler import ManualClock, Request, Response, Scheduler, TransportError
from source import Source


class SourceTransport:
    """把调度器请求映射到本地 Source（无外部请求）。"""

    def __init__(self, source, clock):
        self.source = source
        self.clock = clock

    def __call__(self, request, timeout=None):
        self.source.now = self.clock.time()
        path = "/quote?" + urlencode({"tenant": request.tenant})
        status, headers, body = self.source.handle(
            path, {"X-Credential": request.credential})
        return Response(status, dict(headers), body)


class FaultyTransport:
    """故障注入：发送后断连、出口级暂停（HTTP 日期）、进程崩溃。"""

    def __init__(self, inner, clock, source):
        self.inner = inner
        self.clock = clock
        self.source = source
        self.sends = 0
        self.drops = {}          # request_id -> 剩余断连次数
        self.egress_pause = None  # (第几次发送触发, 暂停秒数)
        self._pause_until = None
        self.crash_at = None      # 第几次发送后模拟进程崩溃

    def __call__(self, request, timeout=None):
        self.sends += 1
        if self._pause_until is not None and self.clock.time() >= self._pause_until:
            self.source.cooldown = None
            self._pause_until = None
        if self.egress_pause and self.sends == self.egress_pause[0]:
            self._pause_until = self.clock.time() + self.egress_pause[1]
            self.source.cooldown = (
                "egress", formatdate(self._pause_until, usegmt=True))
        if self.drops.get(request.id, 0) > 0:
            self.drops[request.id] -= 1
            raise TransportError("连接在发送后断开，响应丢失", sent=True)
        response = self.inner(request, timeout=timeout)
        if self.crash_at is not None and self.sends == self.crash_at:
            raise RuntimeError("模拟进程崩溃：请求已发出，响应尚未保存")
        return response


def fmt_reasons(reasons):
    parts = []
    for reason in reasons:
        if reason["kind"] == "queue":
            parts.append(f"队首阻塞<{reason['ahead']}>")
        else:
            when = reason.get("until", reason.get("free_at"))
            parts.append(f"{reason['kind']}:{reason['scope']}/{reason['identity']}→{when:.0f}")
    return ", ".join(parts) if parts else "就绪"


def print_waiting(sched, title):
    print(f"\n[{title}] t={sched.clock.time():.0f} 等待原因：")
    for rid, info in sched.waiting().items():
        print(f"  {rid:<10} {fmt_reasons(info['reasons'])}")


def print_records(records, start=1):
    for index, record in enumerate(records, start=start):
        outcome = record["outcome"]
        detail = ""
        if outcome == "throttled":
            detail = (f"{record['scope']} 冷却至 {record['until']:.0f}"
                      f"（{record['rule']}）")
        elif outcome == "unknown":
            detail = "已发出未保存，停放待人工核对"
        elif outcome == "interrupted":
            detail = "进程崩溃时已发出未保存，预留保留"
        elif record.get("detail"):
            detail = record["detail"]
        usage = record.get("usage")
        quota = "  ".join(f"{scope}={value}" for scope, value in usage.items()) \
            if usage else "（恢复前记录）"
        print(f"  #{index:<2} t={record['at']:>4.0f} {record['id']:<10} "
              f"{record['tenant']}/{record['credential']:<9} "
              f"{outcome:<11} {detail:<28} 额度: {quota}")


def main():
    fixture = json.loads((Path(__file__).parent / "fixtures/source.json")
                         .read_text(encoding="utf-8"))
    clock = ManualClock(0.0)
    source = Source(fixture)
    transport = FaultyTransport(SourceTransport(source, clock), clock, source)

    # 外部挤占：同事手工重试，在调度器之外占用 shared 凭据组 2 条额度
    for _ in range(2):
        source.history.append({"at": 0.0, "egress": "all",
                               "credential": "shared", "tenant": "manual"})

    state = tempfile.mkdtemp(prefix="quota-demo-")
    sched = Scheduler(fixture["limits"], transport, clock, state_dir=state)

    sched.submit(Request("rice-rt-1", "rice", "shared", weight=3))
    sched.submit(Request("rice-rt-2", "rice", "shared", weight=3))
    for i in range(1, 6):
        sched.submit(Request(f"tea-bf-{i}", "tea", "shared", weight=1))
    sched.submit(Request("fruit-tx-1", "fruit", "isolated",
                         weight=1, replayable=False))

    transport.drops["fruit-tx-1"] = 1      # 发送后断连
    transport.egress_pause = (4, 15.0)     # 第 4 次发送触发出口级暂停 15 秒
    transport.crash_at = 6                 # 第 6 次发送后模拟进程崩溃

    print("=== 分层额度调度演示（逻辑时钟，不真实等待）===")
    print("提交：rice 实时×2（权重3） tea 补采×5（权重1） fruit 副作用×1（不可重放）")
    print("故障：外部挤占 shared 凭据 2 条 / fruit 发送后断连 / "
          "第4次发送出口级暂停(HTTP日期) / 第6次发送后进程崩溃\n")

    print("[第一段] 前 3 次发送：")
    for _ in range(3):
        sched.step()
    print_records(sched.records)
    print_waiting(sched, "凭据组冷却在租户间共享")

    try:
        sched.run_until_idle()
    except RuntimeError as error:
        print(f"\n[崩溃] t={clock.time():.0f} {error}")
    print("\n[第一段] 崩溃前的发送：")
    print_records(sched.records[3:], start=4)
    sched.close()

    print("\n=== 进程重启：从日志恢复待执行项与额度预留 ===")
    sched = Scheduler(fixture["limits"], transport, clock, state_dir=state)
    for rid in ["rice-rt-1", "rice-rt-2", "tea-bf-1", "tea-bf-2", "fruit-tx-1"]:
        print(f"  {rid:<10} → {sched.status(rid)['state']}")
    print(f"  崩溃时 in-flight 的 tea-bf-1 预留保留："
          f"tenant/tea 已用 {sched.tracker.used('tenant', 'tea', clock.time())}/2")
    print_waiting(sched, "恢复后")

    sched.run_until_idle()
    sched.abandon("fruit-tx-1", "已发出未保存，人工核对后放弃")

    print("\n[第二段] 恢复后的发送（含重建的中断记录）：")
    print_records(sched.records[5:], start=6)

    print("\n=== 汇总 ===")
    for tenant in ["rice", "tea", "fruit"]:
        done = [r["id"] for r in sched.records
                if r["tenant"] == tenant and r["outcome"] == "done"]
        print(f"  {tenant:<6} 完成 {len(done)} 笔: {', '.join(done) or '无'}")
    print("  fruit-tx-1 状态:", sched.status("fruit-tx-1")["state"],
          f"（{sched.status('fruit-tx-1')['error']}）")
    print("  公平性：rice 实时 2 笔最迟 t=25 完成；tea 补采 5 笔至 t=45 "
          "每个窗口持续推进，未被高权重饿死。")
    sched.close()


if __name__ == "__main__":
    main()
