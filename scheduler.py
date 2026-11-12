"""分层额度调度器：出口 / 凭据组 / 租户三层约束，可嵌入采集进程。

- 发送必须同时满足三层滑动窗口额度（本地镜像服务端 (now-window, now] 计数）；
- 429 按响应的 X-Quota-Scope 作用域冷却，同凭据的租户共享凭据组冷却，
  出口级暂停阻塞全部租户；Retry-After 支持秒数与 HTTP 日期，缺失、格式
  错误、已在过去的值都有确定规则（见 parse_retry_after）；
- 重试只针对允许重放的请求：发送后断连（sent=True）的副作用调用不自动
  重发、不退还额度，停放为 unknown 等待人工决定；
- 排队取消、执行超时、进程退出恢复都与额度预留协调：已发出但响应尚未
  保存的请求，其预留不简单退还；
- 公平性：各租户按虚拟完成时间选择，每次发送推进 1/weight，高权重获得
  更高份额，但低权重不会被无限饿死。

时间与传输均可注入：clock 需提供 time()/sleep()；transport 是
(request, timeout=...) -> Response 的可调用对象，失败时抛 TransportError。
"""

from __future__ import annotations

import json
import os
import re
import time as time_module
from dataclasses import asdict, dataclass, field
from datetime import timezone
from email.utils import parsedate_to_datetime
from itertools import count
from pathlib import Path


# ---------------------------------------------------------------- 时钟

class SystemClock:
    """真实时钟。"""

    def time(self):
        return time_module.time()

    def sleep(self, seconds):
        time_module.sleep(seconds)


class ManualClock:
    """逻辑时钟：sleep 直接拨快，本地样例不必真等。"""

    def __init__(self, now=0.0):
        self.now = float(now)

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += float(seconds)


# ---------------------------------------------------------------- 传输

@dataclass
class Response:
    status: int
    headers: dict = field(default_factory=dict)
    body: bytes = b""

    def header(self, name):
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return None


class TransportError(Exception):
    """传输失败。sent=True 表示连接在发送后断开，请求可能已到达服务端。"""

    def __init__(self, message, sent):
        super().__init__(message)
        self.sent = bool(sent)


class TransportTimeout(TransportError):
    """执行超时：按“可能已发出”处理（sent 默认 True），不退还额度。"""

    def __init__(self, message="执行超时", sent=True):
        super().__init__(message, sent)


# ---------------------------------------------------------------- Retry-After

_SECONDS = re.compile(r"\d+")


def parse_retry_after(value, now, default, maximum):
    """把 Retry-After 解析为等待秒数，规则确定：

    - 缺失或格式错误 → default（调用方传入，一般为该作用域窗口）；
    - 非负整数 → 秒数；HTTP 日期（无时区按 UTC）→ 与 now 的差；
    - 秒数为 0 或日期已在过去 → 0（不等待）；
    - 超过 maximum → 截断到 maximum。

    返回 (等待秒数, 规则说明)。
    """
    if value is None:
        return float(default), "absent→default"
    text = str(value).strip()
    if _SECONDS.fullmatch(text):
        delay, rule = float(int(text)), "seconds"
    else:
        try:
            moment = parsedate_to_datetime(text)
            if moment is None:
                raise ValueError(text)
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            delay, rule = moment.timestamp() - float(now), "http-date"
        except Exception:
            return float(default), "malformed→default"
    if delay <= 0:
        return 0.0, rule + "(past→0)"
    if delay > maximum:
        return float(maximum), rule + "(capped)"
    return float(delay), rule


# ---------------------------------------------------------------- 额度与冷却

class QuotaTracker:
    """本地镜像服务端 (now-window, now] 的滑动窗口计数。

    只记录确定消耗的额度：429 与发送前失败会退还；已发出未保存响应
    与发送后断连的预留保留。
    """

    def __init__(self, limits):
        self.limits = {
            scope: {"capacity": int(cfg["capacity"]), "window": float(cfg["window"])}
            for scope, cfg in limits.items()
        }
        self.events = []  # (ts, scope, identity)

    def record(self, ts, identities):
        token = []
        for scope, identity in identities.items():
            event = (float(ts), scope, identity)
            self.events.append(event)
            token.append(event)
        return token

    def refund(self, token):
        for event in token:
            try:
                self.events.remove(event)
            except ValueError:
                pass

    def used(self, scope, identity, now):
        window = self.limits[scope]["window"]
        return sum(
            1 for ts, s, i in self.events
            if s == scope and i == identity and now - window < ts <= now
        )

    def free_at(self, scope, identity, now):
        """该作用域出现空位的最早时间；当前有空位则返回 now。"""
        limit = self.limits[scope]
        times = sorted(
            ts for ts, s, i in self.events
            if s == scope and i == identity and now - limit["window"] < ts <= now
        )
        if len(times) < limit["capacity"]:
            return float(now)
        return times[-limit["capacity"]] + limit["window"]


class Cooldowns:
    """按 (作用域, 标识) 记录的冷却截止时间；同凭据的租户天然共享。"""

    def __init__(self):
        self.until = {}

    def set(self, scope, identity, moment):
        key = (scope, identity)
        self.until[key] = max(self.until.get(key, 0.0), float(moment))

    def until_ts(self, scope, identity):
        return self.until.get((scope, identity), 0.0)


# ---------------------------------------------------------------- 请求

@dataclass
class Request:
    id: str
    tenant: str
    credential: str
    weight: float = 1.0        # 实时采集给大权重，补采给小权重
    replayable: bool = True    # 带副作用的调用置 False，绝不自动重发
    timeout: float | None = None
    params: dict = field(default_factory=dict)


TERMINAL = {"done", "failed", "cancelled"}


# ---------------------------------------------------------------- 日志

class Journal:
    """追加式 JSONL 日志：进程退出后据此恢复待执行项与额度预留。"""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")

    def log(self, **event):
        self._fh.write(json.dumps(event, ensure_ascii=False) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self):
        self._fh.close()

    @staticmethod
    def read(path):
        path = Path(path)
        if not path.exists():
            return []
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]


# ---------------------------------------------------------------- 调度器

class Scheduler:
    """可嵌入采集进程的分层额度调度器。

    接口：submit 提交 / cancel 取消 / step 与 run_until_idle 驱动执行 /
    waiting 查看等待原因 / status 与 result 查询结果 / resume 与 abandon
    处理 unknown。传入相同 state_dir 重新构造即可在进程退出后恢复。
    """

    def __init__(self, limits, transport, clock=None, state_dir=None,
                 default_cooldown=None, max_cooldown=3600.0, max_attempts=8):
        self.transport = transport
        self.clock = clock or SystemClock()
        self.tracker = QuotaTracker(limits)
        self.cooldowns = Cooldowns()
        self.default_cooldown = default_cooldown  # None → 用对应作用域的窗口
        self.max_cooldown = float(max_cooldown)
        self.max_attempts = int(max_attempts)
        self.requests = {}
        self.states = {}       # id -> pending/in_flight/done/failed/cancelled/unknown
        self.queues = {}       # tenant -> [id]，租户内先进先出
        self.results = {}
        self.errors = {}
        self.attempts = {}     # 传输错误次数（429 不计）
        self.tenant_vt = {}    # 租户虚拟完成时间，每次发送推进 1/weight
        self.order = {}
        self.records = []      # 每次发送的记录：时间、租户、额度归属、结局
        self.cancel_requested = set()
        self._seq = count(1)
        self._sends = {}
        self.journal = None
        if state_dir is not None:
            self.journal = Journal(Path(state_dir) / "journal.jsonl")
            self._recover()

    # ---------------- 提交 / 取消 / 查询 ----------------

    def submit(self, request):
        if isinstance(request, dict):
            request = Request(**request)
        if request.id in self.requests:
            raise ValueError(f"重复的请求 id: {request.id}")
        if request.weight <= 0:
            raise ValueError("weight 必须为正数")
        self.requests[request.id] = request
        self.states[request.id] = "pending"
        self.order[request.id] = next(self._seq)
        self.attempts[request.id] = 0
        self._sends[request.id] = 0
        self.queues.setdefault(request.tenant, []).append(request.id)
        self.tenant_vt.setdefault(
            request.tenant, min(self.tenant_vt.values(), default=0.0))
        self._log(t="submit", id=request.id, ts=self.clock.time(),
                  req=asdict(request))
        return request.id

    def cancel(self, request_id):
        """排队中 → 直接移除；飞行中 → 标记，响应到达后丢弃，预留不退。"""
        state = self.states.get(request_id)
        if state == "pending":
            self._dequeue(request_id)
            self.states[request_id] = "cancelled"
            self._log(t="cancelled", id=request_id, ts=self.clock.time())
            return True
        if state == "in_flight":
            self.cancel_requested.add(request_id)
            self._log(t="cancel_requested", id=request_id, ts=self.clock.time())
            return True
        return False

    def waiting(self):
        """每个排队请求的等待原因（quota/cooldown/queue）与预计就绪时间。"""
        now = self.clock.time()
        report = {}
        for tenant, queue in self.queues.items():
            for position, rid in enumerate(queue):
                req = self.requests[rid]
                reasons = self._blockers(req, now)
                if position > 0:
                    reasons = reasons + [{"kind": "queue", "ahead": queue[0]}]
                timed = [r[key] for r in reasons for key in ("until", "free_at")
                         if key in r]
                report[rid] = {
                    "tenant": tenant,
                    "state": self.states[rid],
                    "reasons": reasons,
                    "ready_at": max(timed) if timed else now,
                }
        return report

    def status(self, request_id):
        return {
            "state": self.states[request_id],
            "attempts": self.attempts[request_id],
            "error": self.errors.get(request_id),
            "has_result": request_id in self.results,
        }

    def result(self, request_id):
        return self.results.get(request_id)

    def resume(self, request_id):
        """人工核对后把 unknown 请求重新排队（含不可重放请求的显式决定）。"""
        if self.states.get(request_id) != "unknown":
            raise ValueError(f"{request_id} 不在 unknown 状态")
        req = self.requests[request_id]
        self.states[request_id] = "pending"
        self.queues.setdefault(req.tenant, []).append(request_id)
        self._log(t="resumed", id=request_id, ts=self.clock.time())

    def abandon(self, request_id, reason="人工放弃"):
        """人工核对后放弃 unknown 请求；额度预留不退（可能已消耗）。"""
        if self.states.get(request_id) != "unknown":
            raise ValueError(f"{request_id} 不在 unknown 状态")
        self.states[request_id] = "failed"
        self.errors[request_id] = reason
        self._log(t="failed", id=request_id, ts=self.clock.time(), reason=reason)

    # ---------------- 驱动执行 ----------------

    def step(self):
        """尝试发送一个请求；没有可发送的返回 None。返回发送记录。"""
        now = self.clock.time()
        picked = self._pick(now)
        if picked is None:
            return None
        tenant, rid = picked
        req = self.requests[rid]
        self._dequeue(rid)
        self.states[rid] = "in_flight"
        identities = self._identities(req)
        token = self.tracker.record(now, identities)
        self.tenant_vt[tenant] += 1.0 / req.weight
        self._sends[rid] += 1
        attempt = self._sends[rid]
        self._log(t="sent", id=rid, ts=now, identities=identities, attempt=attempt)
        record = {"id": rid, "tenant": tenant, "credential": req.credential,
                  "at": now, "identities": dict(identities), "attempt": attempt}
        try:
            response = self.transport(req, timeout=req.timeout)
        except TransportError as error:
            record.update(outcome="transport_error", status=None,
                          detail=str(error))
            self._after_transport_error(req, token, error, now, record)
        else:
            self._after_response(req, token, response, now, record)
        record["usage"] = {
            scope: f"{self.tracker.used(scope, identity, now)}"
                   f"/{self.tracker.limits[scope]['capacity']}"
            for scope, identity in identities.items()
        }
        self.records.append(record)
        return record

    def run_until_idle(self, max_sends=1000):
        """驱动直到没有待执行项；等待时通过注入的 clock 休眠。"""
        sends = 0
        while True:
            record = self.step()
            if record is not None:
                sends += 1
                if sends > max_sends:
                    raise RuntimeError(
                        f"发送次数超过 max_sends={max_sends}，仍有待执行项")
                continue
            if not any(self.queues.values()):
                return
            wait = self._next_wait()
            if wait is None:
                return
            self.clock.sleep(max(wait, 0.0))

    def next_ready_at(self):
        """下一个可能有请求就绪的时间；没有待执行项时返回 None。"""
        wait = self._next_wait()
        if wait is None:
            return None
        return self.clock.time() + max(wait, 0.0)

    def close(self):
        if self.journal is not None:
            self.journal.close()

    # ---------------- 响应与故障结局 ----------------

    def _after_response(self, req, token, response, now, record):
        rid = req.id
        record["status"] = response.status
        if response.status == 429:
            # 服务端约定 429 不消耗额度：退还本地预留，按作用域冷却。
            self.tracker.refund(token)
            scope = (response.header("X-Quota-Scope") or "").lower()
            if scope not in self.tracker.limits:
                scope = "egress"  # 缺失或未知作用域 → 按出口级处理（最保守）
            identity = self._identities(req)[scope]
            delay, rule = parse_retry_after(
                response.header("Retry-After"), now,
                default=self._default_cooldown(scope), maximum=self.max_cooldown)
            until = now + delay
            self.cooldowns.set(scope, identity, until)
            self._log(t="throttled", id=rid, ts=now, attempt=record["attempt"],
                      scope=scope, identity=identity, until=until, rule=rule)
            self._requeue_front(req)  # 429 是明确拒绝，任何请求都可安全重排
            record.update(outcome="throttled", scope=scope, until=until, rule=rule)
            return
        if rid in self.cancel_requested:
            self.cancel_requested.discard(rid)
            self.states[rid] = "cancelled"
            self._log(t="finished", id=rid, ts=now, attempt=record["attempt"],
                      status=response.status, outcome="cancelled")
            record.update(outcome="cancelled")
            return
        if 200 <= response.status < 300:
            self.states[rid] = "done"
            self.results[rid] = response
            body = response.body
            if isinstance(body, (bytes, bytearray)):
                body = bytes(body).decode("utf-8", errors="replace")
            self._log(t="finished", id=rid, ts=now, attempt=record["attempt"],
                      status=response.status, outcome="done", body=body)
            record.update(outcome="done")
        else:
            self.states[rid] = "failed"
            self.errors[rid] = f"HTTP {response.status}"
            self._log(t="finished", id=rid, ts=now, attempt=record["attempt"],
                      status=response.status, outcome="failed")
            record.update(outcome="failed")

    def _after_transport_error(self, req, token, error, now, record):
        rid = req.id
        if not error.sent:
            self.tracker.refund(token)  # 发送前失败：未占用服务端额度
        outcome = "retry"
        if rid in self.cancel_requested:
            self.cancel_requested.discard(rid)
            self.states[rid] = "cancelled"
            outcome = "cancelled"
        elif error.sent and not req.replayable:
            # 已发出但结果未知的副作用调用：不自动重发，不退还额度。
            self.states[rid] = "unknown"
            outcome = "unknown"
        else:
            self.attempts[rid] += 1
            if self.attempts[rid] >= self.max_attempts:
                self.states[rid] = "failed"
                self.errors[rid] = f"传输错误达到 {self.max_attempts} 次"
                outcome = "failed"
            else:
                self._requeue_front(req)
        self._log(t="transport_error", id=rid, ts=now, attempt=record["attempt"],
                  sent=error.sent, outcome=outcome, detail=str(error))
        record["outcome"] = outcome

    # ---------------- 崩溃恢复 ----------------

    def _recover(self):
        now = self.clock.time()
        inflight = {}      # rid -> (ts, identities)，已发送未结
        cancel_req = set()
        for ev in Journal.read(self.journal.path):
            t = ev["t"]
            if t == "submit":
                req = Request(**ev["req"])
                self.requests[req.id] = req
                self.order[req.id] = next(self._seq)
                self.attempts[req.id] = 0
                self._sends[req.id] = 0
                self.states[req.id] = "pending"
                self.queues.setdefault(req.tenant, []).append(req.id)
                self.tenant_vt.setdefault(req.tenant, 0.0)
                continue
            rid = ev.get("id")
            if rid is None or rid not in self.requests:
                continue
            req = self.requests[rid]
            if t == "sent":
                self._dequeue(rid)
                self.states[rid] = "in_flight"
                self._sends[rid] += 1
                inflight[rid] = (ev["ts"], ev["identities"])
            elif t == "throttled":
                inflight.pop(rid, None)  # 429 不消耗额度，无预留
                if ev["until"] > now:
                    self.cooldowns.set(ev["scope"], ev["identity"], ev["until"])
                self.states[rid] = "pending"
                self.queues.setdefault(req.tenant, []).insert(0, rid)
            elif t == "finished":
                ts, identities = inflight.pop(rid, (None, None))
                if identities is not None:
                    self.tracker.record(ts, identities)
                outcome = ev["outcome"]
                self.states[rid] = outcome
                if outcome == "done":
                    self.results[rid] = Response(
                        ev["status"], {}, str(ev.get("body", "")).encode("utf-8"))
                elif outcome == "failed":
                    self.errors[rid] = f"HTTP {ev['status']}"
            elif t == "transport_error":
                ts, identities = inflight.pop(rid, (None, None))
                if ev["sent"] and identities is not None:
                    self.tracker.record(ts, identities)  # 可能已发出：保留预留
                outcome = ev["outcome"]
                if outcome == "retry":
                    self.attempts[rid] += 1
                    self.states[rid] = "pending"
                    self.queues.setdefault(req.tenant, []).insert(0, rid)
                elif outcome == "failed":
                    self.attempts[rid] += 1
                    self.states[rid] = "failed"
                    self.errors[rid] = f"传输错误达到 {self.max_attempts} 次"
                else:
                    self.states[rid] = outcome  # unknown / cancelled
            elif t == "cancel_requested":
                cancel_req.add(rid)
            elif t == "cancelled":
                self._dequeue(rid)
                self.states[rid] = "cancelled"
            elif t == "resumed":
                self._dequeue(rid)
                self.states[rid] = "pending"
                self.queues.setdefault(req.tenant, []).append(rid)
            elif t == "failed":
                self._dequeue(rid)
                self.states[rid] = "failed"
                self.errors[rid] = ev.get("reason", "")
        # 崩溃时仍在飞行中：已发出未保存响应，预留保留，不简单退还。
        for rid, (ts, identities) in inflight.items():
            self.tracker.record(ts, identities)
            req = self.requests[rid]
            if rid in cancel_req:
                self.states[rid] = "cancelled"
            elif req.replayable:
                self.attempts[rid] += 1
                if self.attempts[rid] >= self.max_attempts:
                    self.states[rid] = "failed"
                    self.errors[rid] = f"传输错误达到 {self.max_attempts} 次"
                else:
                    self.states[rid] = "pending"
                    self.queues.setdefault(req.tenant, []).insert(0, rid)
            else:
                self.states[rid] = "unknown"  # 副作用调用不自动重发
        self._rebuild_records()

    def _rebuild_records(self):
        sent = {}
        for ev in Journal.read(self.journal.path):
            t = ev["t"]
            rid = ev.get("id")
            if t == "sent":
                req = self.requests[rid]
                key = (rid, ev["attempt"])
                sent[key] = {
                    "id": rid, "tenant": req.tenant, "credential": req.credential,
                    "at": ev["ts"], "identities": dict(ev["identities"]),
                    "attempt": ev["attempt"],
                }
                self.records.append(sent[key])
            elif rid is None:
                continue
            elif t == "throttled" and (rid, ev["attempt"]) in sent:
                sent[(rid, ev["attempt"])].update(
                    outcome="throttled", status=429, scope=ev["scope"],
                    until=ev["until"], rule=ev["rule"])
            elif t == "finished" and (rid, ev["attempt"]) in sent:
                sent[(rid, ev["attempt"])].update(
                    outcome=ev["outcome"], status=ev["status"])
            elif t == "transport_error" and (rid, ev["attempt"]) in sent:
                sent[(rid, ev["attempt"])].update(
                    outcome=ev["outcome"], status=None, detail=ev.get("detail"))
        for record in self.records:
            record.setdefault("outcome", "interrupted")  # 已发出未保存响应
            record.setdefault("status", None)

    # ---------------- 内部 ----------------

    def _identities(self, req):
        return {"egress": "all", "credential": req.credential,
                "tenant": req.tenant}

    def _default_cooldown(self, scope):
        if self.default_cooldown is not None:
            return float(self.default_cooldown)
        return self.tracker.limits[scope]["window"]

    def _log(self, **event):
        if self.journal is not None:
            self.journal.log(**event)

    def _dequeue(self, rid):
        queue = self.queues.get(self.requests[rid].tenant, [])
        if rid in queue:
            queue.remove(rid)

    def _requeue_front(self, req):
        self.states[req.id] = "pending"
        self.queues.setdefault(req.tenant, []).insert(0, req.id)

    def _blockers(self, req, now):
        reasons = []
        for scope, identity in self._identities(req).items():
            until = self.cooldowns.until_ts(scope, identity)
            if until > now:
                reasons.append({"kind": "cooldown", "scope": scope,
                                "identity": identity, "until": until})
            free = self.tracker.free_at(scope, identity, now)
            if free > now:
                reasons.append({"kind": "quota", "scope": scope,
                                "identity": identity, "free_at": free})
        return reasons

    def _pick(self, now):
        best = None
        for tenant, queue in self.queues.items():
            if not queue:
                continue
            rid = queue[0]
            if self._blockers(self.requests[rid], now):
                continue
            key = (self.tenant_vt[tenant], self.order[rid])
            if best is None or key < best[0]:
                best = (key, tenant, rid)
        return None if best is None else (best[1], best[2])

    def _next_wait(self):
        now = self.clock.time()
        waits = []
        for tenant, queue in self.queues.items():
            if not queue:
                continue
            blockers = self._blockers(self.requests[queue[0]], now)
            if not blockers:
                return 0.0
            waits.append(max(b[key] for b in blockers
                             for key in ("until", "free_at") if key in b) - now)
        return min(waits) if waits else None
