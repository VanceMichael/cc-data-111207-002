import json
import tempfile
import unittest
from email.utils import formatdate
from pathlib import Path

from scheduler import (ManualClock, Request, Response, Scheduler,
                       TransportError, TransportTimeout, parse_retry_after)

LIMITS = {
    "egress": {"capacity": 4, "window": 10},
    "credential": {"capacity": 3, "window": 10},
    "tenant": {"capacity": 2, "window": 10},
}

FAIR_LIMITS = {
    "egress": {"capacity": 4, "window": 10},
    "credential": {"capacity": 99, "window": 10},
    "tenant": {"capacity": 99, "window": 10},
}


class FakeTransport:
    """按脚本回放响应或异常，记录每次调用。"""

    def __init__(self, script=None):
        self.script = list(script or [])
        self.calls = []

    def __call__(self, request, timeout=None):
        self.calls.append((request.id, timeout))
        if self.script:
            action = self.script.pop(0)
            if callable(action):
                return action(request)
            if isinstance(action, Exception):
                raise action
            return action
        return Response(200, {}, b"{}")


def make(transport, limits=None, clock=None, state_dir=None, **kwargs):
    return Scheduler(limits or LIMITS, transport, clock or ManualClock(0.0),
                     state_dir=state_dir, **kwargs)


class RetryAfterTests(unittest.TestCase):
    def test_seconds_and_http_date(self):
        delay, rule = parse_retry_after("30", now=100.0, default=10, maximum=3600)
        self.assertEqual((delay, rule), (30.0, "seconds"))
        moment = formatdate(160.0, usegmt=True)
        delay, rule = parse_retry_after(moment, now=100.0, default=10, maximum=3600)
        self.assertEqual((delay, rule), (60.0, "http-date"))

    def test_past_and_zero_mean_no_wait(self):
        self.assertEqual(parse_retry_after("0", 100.0, 10, 3600)[0], 0.0)
        past = formatdate(50.0, usegmt=True)
        delay, rule = parse_retry_after(past, now=100.0, default=10, maximum=3600)
        self.assertEqual(delay, 0.0)
        self.assertIn("past", rule)

    def test_malformed_and_absent_fall_back_to_default(self):
        self.assertEqual(parse_retry_after("soon", 0.0, 10, 3600),
                         (10.0, "malformed→default"))
        self.assertEqual(parse_retry_after(None, 0.0, 10, 3600),
                         (10.0, "absent→default"))
        self.assertEqual(parse_retry_after("-5", 0.0, 10, 3600),
                         (10.0, "malformed→default"))

    def test_far_future_is_capped(self):
        delay, rule = parse_retry_after("99999", 0.0, 10, maximum=3600)
        self.assertEqual((delay, rule), (3600.0, "seconds(capped)"))


class LayeredQuotaTests(unittest.TestCase):
    def test_three_layers_all_required(self):
        transport = FakeTransport()
        sched = make(transport)
        for rid, tenant in [("r1", "rice"), ("t1", "tea"), ("f1", "fruit"),
                            ("r2", "rice"), ("t2", "tea"), ("r3", "rice")]:
            credential = "isolated" if tenant == "fruit" else "shared"
            sched.submit(Request(rid, tenant, credential))
        for _ in range(4):
            self.assertIsNotNone(sched.step())
        # 第一窗口：r1,t1,f1,r2 发出后，egress 4/4、shared 3/3、rice 2/2 全满
        self.assertEqual([r["id"] for r in sched.records],
                         ["r1", "t1", "f1", "r2"])
        self.assertIsNone(sched.step())
        waiting = sched.waiting()
        scopes = {reason["scope"] for reason in waiting["t2"]["reasons"]}
        self.assertIn("egress", scopes)
        self.assertIn("credential", scopes)
        self.assertIn("tenant", {r["scope"] for r in waiting["r3"]["reasons"]})
        sched.run_until_idle()
        self.assertEqual([r["at"] for r in sched.records],
                         [0.0] * 4 + [10.0] * 2)
        self.assertTrue(all(sched.states[rid] == "done"
                            for rid in ["r1", "t1", "f1", "r2", "t2", "r3"]))

    def test_fixture_requests_submit_directly(self):
        fixture = json.loads((Path(__file__).parents[1]
                              / "fixtures/source.json").read_text("utf-8"))
        sched = make(FakeTransport(), limits=fixture["limits"])
        for entry in fixture["requests"]:
            sched.submit(entry)
        sched.run_until_idle()
        self.assertEqual([r["id"] for r in sched.records], ["r1", "r2", "r3"])
        self.assertTrue(all(s == "done" for s in sched.states.values()))


class CooldownTests(unittest.TestCase):
    def test_credential_cooldown_shared_across_tenants(self):
        transport = FakeTransport([
            Response(429, {"Retry-After": "30", "X-Quota-Scope": "credential"}),
        ])
        sched = make(transport)
        sched.submit(Request("r1", "rice", "shared"))
        sched.submit(Request("t1", "tea", "shared"))
        sched.submit(Request("f1", "fruit", "isolated"))
        sched.step()  # r1 被 429，shared 凭据组冷却 30 秒
        waiting = sched.waiting()
        for rid in ["r1", "t1"]:
            self.assertEqual(waiting[rid]["reasons"][0]["kind"], "cooldown")
            self.assertEqual(waiting[rid]["reasons"][0]["until"], 30.0)
        self.assertEqual(waiting["f1"]["reasons"], [])  # isolated 不受影响
        sched.step()  # f1 照常发出
        self.assertIsNone(sched.step())
        sched.run_until_idle()
        by_id = {}
        for record in sched.records:
            by_id.setdefault(record["id"], []).append(record["at"])
        self.assertEqual(by_id["f1"], [0.0])
        self.assertEqual(by_id["r1"], [0.0, 30.0])
        self.assertEqual(by_id["t1"], [30.0])

    def test_egress_pause_with_http_date_blocks_everything(self):
        clock = ManualClock(1000.0)
        transport = FakeTransport([
            Response(429, {"Retry-After": formatdate(1020.0, usegmt=True),
                           "X-Quota-Scope": "egress"}),
        ])
        sched = make(transport, clock=clock)
        sched.submit(Request("r1", "rice", "shared"))
        sched.submit(Request("f1", "fruit", "isolated"))
        record = sched.step()
        self.assertEqual(record["rule"], "http-date")
        self.assertEqual(record["until"], 1020.0)
        waiting = sched.waiting()
        self.assertEqual(waiting["f1"]["reasons"][0]["scope"], "egress")
        sched.run_until_idle()
        self.assertEqual([r["at"] for r in sched.records],
                         [1000.0, 1020.0, 1020.0])

    def test_429_refunds_local_reservation(self):
        transport = FakeTransport([
            Response(429, {"Retry-After": "30", "X-Quota-Scope": "tenant"}),
        ])
        sched = make(transport)
        sched.submit(Request("r1", "rice", "shared"))
        sched.step()
        self.assertEqual(sched.tracker.used("tenant", "rice", 0.0), 0)
        self.assertEqual(sched.tracker.used("egress", "all", 0.0), 0)
        sched.run_until_idle()
        self.assertEqual(sched.tracker.used("tenant", "rice", 30.0), 1)

    def test_past_retry_after_retries_without_waiting(self):
        clock = ManualClock(1000.0)
        transport = FakeTransport([
            Response(429, {"Retry-After": formatdate(500.0, usegmt=True),
                           "X-Quota-Scope": "tenant"}),
        ])
        sched = make(transport, clock=clock)
        sched.submit(Request("r1", "rice", "shared"))
        sched.run_until_idle()
        self.assertEqual([r["at"] for r in sched.records], [1000.0, 1000.0])
        self.assertEqual(sched.records[0]["rule"], "http-date(past→0)")


class FailureSemanticsTests(unittest.TestCase):
    def test_non_replayable_drop_parks_unknown_and_keeps_reservation(self):
        transport = FakeTransport([TransportError("drop", sent=True)])
        sched = make(transport)
        sched.submit(Request("f1", "fruit", "isolated", replayable=False))
        sched.step()
        sched.run_until_idle()
        self.assertEqual(sched.states["f1"], "unknown")
        self.assertEqual(len(transport.calls), 1)  # 没有自动重发
        self.assertEqual(sched.tracker.used("tenant", "fruit", 0.0), 1)
        sched.abandon("f1", "人工核对后放弃")
        self.assertEqual(sched.states["f1"], "failed")

    def test_unknown_can_be_resumed_by_operator(self):
        transport = FakeTransport([TransportError("drop", sent=True)])
        sched = make(transport)
        sched.submit(Request("f1", "fruit", "isolated", replayable=False))
        sched.step()
        sched.resume("f1")  # 人工与服务商核对后显式重发
        sched.run_until_idle()
        self.assertEqual(sched.states["f1"], "done")
        self.assertEqual(len(transport.calls), 2)

    def test_replayable_drop_retries_and_keeps_both_reservations(self):
        transport = FakeTransport([TransportError("drop", sent=True)])
        sched = make(transport)
        sched.submit(Request("r1", "rice", "shared"))
        sched.run_until_idle()
        self.assertEqual(sched.states["r1"], "done")
        self.assertEqual(len(transport.calls), 2)
        # 已发出未保存的那次不退还：窗口内计 2 条
        self.assertEqual(sched.tracker.used("tenant", "rice", 0.0), 2)

    def test_failure_before_send_refunds(self):
        transport = FakeTransport([TransportError("connect refused", sent=False)])
        sched = make(transport)
        sched.submit(Request("r1", "rice", "shared"))
        sched.run_until_idle()
        self.assertEqual(sched.states["r1"], "done")
        self.assertEqual(sched.tracker.used("tenant", "rice", 0.0), 1)

    def test_timeout_counts_as_sent_and_retries_replayable(self):
        transport = FakeTransport([TransportTimeout()])
        sched = make(transport)
        sched.submit(Request("r1", "rice", "shared", timeout=5))
        sched.run_until_idle()
        self.assertEqual(sched.states["r1"], "done")
        self.assertEqual(transport.calls[0], ("r1", 5))
        self.assertEqual(sched.tracker.used("tenant", "rice", 0.0), 2)

    def test_max_attempts_stops_retrying(self):
        transport = FakeTransport([TransportError("drop", sent=True)] * 5)
        sched = make(transport, max_attempts=3)
        sched.submit(Request("r1", "rice", "shared"))
        sched.run_until_idle()
        self.assertEqual(sched.states["r1"], "failed")
        self.assertEqual(len(transport.calls), 3)


class CancelTests(unittest.TestCase):
    def test_cancel_pending(self):
        transport = FakeTransport()
        sched = make(transport)
        sched.submit(Request("r1", "rice", "shared"))
        sched.submit(Request("t1", "tea", "shared"))
        self.assertTrue(sched.cancel("r1"))
        sched.run_until_idle()
        self.assertEqual([c[0] for c in transport.calls], ["t1"])
        self.assertEqual(sched.states["r1"], "cancelled")
        self.assertFalse(sched.cancel("r1"))

    def test_cancel_in_flight_discards_result_but_keeps_reservation(self):
        sched = None

        def cancel_during_flight(request):
            sched.cancel(request.id)
            return Response(200, {}, b"{}")

        sched = make(FakeTransport([cancel_during_flight]))
        sched.submit(Request("r1", "rice", "shared"))
        record = sched.step()
        self.assertEqual(record["outcome"], "cancelled")
        self.assertEqual(sched.states["r1"], "cancelled")
        self.assertIsNone(sched.result("r1"))
        self.assertEqual(sched.tracker.used("tenant", "rice", 0.0), 1)


class FairnessTests(unittest.TestCase):
    def test_weighted_fairness_and_no_starvation(self):
        transport = FakeTransport()
        sched = make(transport, limits=FAIR_LIMITS)
        for i in range(1, 7):
            sched.submit(Request(f"rt-{i}", "realtime", "shared", weight=3))
        for i in range(1, 7):
            sched.submit(Request(f"bf-{i}", "backfill", "shared", weight=1))
        sched.run_until_idle()
        sequence = [r["id"] for r in sched.records]
        self.assertEqual(sequence, ["rt-1", "bf-1", "rt-2", "rt-3",
                                    "rt-4", "bf-2", "rt-5", "rt-6",
                                    "bf-3", "bf-4", "bf-5", "bf-6"])
        times = {r["id"]: r["at"] for r in sched.records}
        # 高权重实时流量前两个窗口内完成（份额 3:1）
        self.assertTrue(all(times[f"rt-{i}"] <= 10.0 for i in range(1, 7)))
        # 低权重补采每个窗口都有进展，不被饿死
        backfill_times = [times[f"bf-{i}"] for i in range(1, 7)]
        self.assertEqual(backfill_times, [0.0, 10.0, 20.0, 20.0, 20.0, 20.0])
        self.assertTrue(all(s == "done" for s in sched.states.values()))

    def test_weight_ratio_over_time(self):
        transport = FakeTransport()
        sched = make(transport, limits=FAIR_LIMITS)
        for i in range(8):
            sched.submit(Request(f"a-{i}", "tenant-a", "shared", weight=1))
            sched.submit(Request(f"b-{i}", "tenant-b", "shared", weight=3))
        sched.run_until_idle()
        first_eight = [r["id"] for r in sched.records[:8]]
        self.assertEqual(sum(1 for rid in first_eight if rid.startswith("a")), 2)
        self.assertEqual(sum(1 for rid in first_eight if rid.startswith("b")), 6)


class RestartTests(unittest.TestCase):
    def test_restart_recovers_pending_and_keeps_inflight_reservation(self):
        with tempfile.TemporaryDirectory() as state:
            clock = ManualClock(0.0)
            transport = FakeTransport([Response(200, {}, b"{}"), RuntimeError("崩溃")])
            sched = make(transport, clock=clock, state_dir=state)
            sched.submit(Request("r1", "rice", "shared"))
            sched.submit(Request("f1", "fruit", "isolated", replayable=False))
            sched.submit(Request("t1", "tea", "shared"))
            sched.step()  # r1 完成
            with self.assertRaises(RuntimeError):
                sched.step()  # f1 已发出未保存时进程崩溃
            sched.close()

            sched2 = make(FakeTransport(), clock=clock, state_dir=state)
            self.assertEqual(sched2.states["r1"], "done")
            self.assertEqual(sched2.states["f1"], "unknown")   # 副作用调用不自动重发
            self.assertEqual(sched2.states["t1"], "pending")   # 待执行项恢复
            # 崩溃时已发出的 f1 预留保留，不简单退还
            self.assertEqual(sched2.tracker.used("tenant", "fruit", 0.0), 1)
            self.assertEqual(sched2.tracker.used("egress", "all", 0.0), 2)
            sched2.run_until_idle()
            self.assertEqual(sched2.states["t1"], "done")
            self.assertEqual(sched2.states["f1"], "unknown")
            sched2.abandon("f1")
            self.assertEqual(sched2.states["f1"], "failed")
            sched2.close()

    def test_restart_requeues_replayable_inflight(self):
        with tempfile.TemporaryDirectory() as state:
            clock = ManualClock(0.0)
            sched = make(FakeTransport([RuntimeError("崩溃")]),
                         clock=clock, state_dir=state)
            sched.submit(Request("r1", "rice", "shared"))
            with self.assertRaises(RuntimeError):
                sched.step()
            sched.close()

            transport = FakeTransport()
            sched2 = make(transport, clock=clock, state_dir=state)
            self.assertEqual(sched2.states["r1"], "pending")
            self.assertEqual(sched2.tracker.used("tenant", "rice", 0.0), 1)
            sched2.run_until_idle()
            self.assertEqual(sched2.states["r1"], "done")
            self.assertEqual(len(transport.calls), 1)
            # 崩溃前预留 + 重发预留，窗口内共 2 条
            self.assertEqual(sched2.tracker.used("tenant", "rice", 0.0), 2)
            sched2.close()

    def test_restart_restores_cooldown_and_results(self):
        with tempfile.TemporaryDirectory() as state:
            clock = ManualClock(0.0)
            transport = FakeTransport([
                Response(200, {}, b'{"value": 17}'),
                Response(429, {"Retry-After": "50",
                               "X-Quota-Scope": "credential"}),
            ])
            sched = make(transport, clock=clock, state_dir=state)
            sched.submit(Request("r1", "rice", "shared"))
            sched.submit(Request("t1", "tea", "shared"))
            sched.step()
            sched.step()
            sched.close()

            sched2 = make(FakeTransport(), clock=clock, state_dir=state)
            self.assertEqual(sched2.states["r1"], "done")
            self.assertEqual(sched2.result("r1").body, b'{"value": 17}')
            waiting = sched2.waiting()
            self.assertEqual(waiting["t1"]["reasons"][0],
                             {"kind": "cooldown", "scope": "credential",
                              "identity": "shared", "until": 50.0})
            sched2.run_until_idle()
            self.assertEqual([r["at"] for r in sched2.records
                              if r["id"] == "t1" and r["outcome"] == "done"],
                             [50.0])
            sched2.close()


if __name__ == "__main__":
    unittest.main()
