"""执行账本：唯一可能产生上游费用的付费执行、幂等重放与崩溃恢复。

边界约定：

* ``execute`` 只接受计划引用与计划哈希；不接受 ``confirmed``/``--yes`` 之类的
  自报确认，也不接受动态产品、金额或追加主体。可信宿主在 Skill 层完成业务范围
  确认，服务端不再保留第二道人工/CLI 审批。
* 每个租户的计划最多产生一个运行；重放同一 ``plan_id``+``plan_hash`` 返回同一
  运行，不会重复派发。
* 每次上游调用前，以 SQLite ``BEGIN IMMEDIATE`` 事务登记幂等键与调用数；同一
  请求键最多派发一次。派发标记为 ``inflight`` 后才允许发出网络请求，因此崩溃
  后可区分“尚未派发”与“是否计费未知”。
* ``billing_unknown`` 保留账本状态、终止运行，绝不自动重发。``get`` 只读本地
  账本与快照，不访问上游。
* ``recover`` 仅用于服务启动（构造 ``Engine`` 时会调用一次）；``close`` 优雅停止
  后台 worker，未完成运行保持可恢复状态。
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from collections.abc import Mapping
from typing import Any

from server.common import ROUTES, Fault, digest, later, now, pack, uid

LOG = logging.getLogger('portrait.execution')

# ---------------------------------------------------------------------------
# Schema.  Store 初始化时与 planning.DDL 一起执行；表名均有 execution_ 前缀。
# ---------------------------------------------------------------------------

DDL = '''
CREATE TABLE IF NOT EXISTS execution_runs(
 id TEXT PRIMARY KEY,
 tenant TEXT NOT NULL,
 plan_id TEXT NOT NULL,
 plan_hash TEXT NOT NULL,
 stage TEXT NOT NULL,
 roster_id TEXT NOT NULL,
 plan_snapshot_id TEXT NOT NULL,
 snapshot_id TEXT NOT NULL REFERENCES snapshots(id),
 state TEXT NOT NULL,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 started_at TEXT,
 finished_at TEXT,
 expires_at TEXT NOT NULL,
 max_calls INTEGER NOT NULL,
 complete INTEGER NOT NULL DEFAULT 0,
 products_total INTEGER NOT NULL DEFAULT 0,
 products_completed INTEGER NOT NULL DEFAULT 0,
 subjects_total INTEGER NOT NULL DEFAULT 0,
 subjects_completed INTEGER NOT NULL DEFAULT 0,
 pages_written INTEGER NOT NULL DEFAULT 0,
 records_written INTEGER NOT NULL DEFAULT 0,
 error_code TEXT,
 error_message TEXT,
 error_retryable INTEGER NOT NULL DEFAULT 0,
 error_retry_after INTEGER,
 trace_id TEXT,
 result_json TEXT,
 worker_owner TEXT,
 worker_heartbeat TEXT,
 UNIQUE(tenant, plan_id)
);
CREATE INDEX IF NOT EXISTS execution_runs_state ON execution_runs(tenant,state,updated_at);

CREATE TABLE IF NOT EXISTS execution_dispatches(
 id TEXT PRIMARY KEY,
 tenant TEXT NOT NULL,
 run_id TEXT NOT NULL REFERENCES execution_runs(id),
 product_id TEXT NOT NULL,
 params_key TEXT NOT NULL,
 request_key TEXT NOT NULL,
 state TEXT NOT NULL,
 response TEXT,
 acquired_at TEXT,
 source_dispatch_id TEXT,
 expires_at TEXT,
 error_code TEXT,
 error_message TEXT,
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 UNIQUE(run_id, product_id, params_key)
);
CREATE INDEX IF NOT EXISTS execution_dispatches_reuse
 ON execution_dispatches(tenant,product_id,params_key,state,expires_at);
CREATE INDEX IF NOT EXISTS execution_dispatches_request
 ON execution_dispatches(tenant,request_key,state);
CREATE INDEX IF NOT EXISTS execution_dispatches_run ON execution_dispatches(run_id,state);
'''

RUN_STATES = ('queued', 'running', 'partial', 'succeeded', 'failed', 'cancelled')
TERMINAL_STATES = frozenset(('succeeded', 'failed', 'cancelled'))
NONTERMINAL_STATES = frozenset(('queued', 'running'))
PAGE_VIEWS = ('identities', 'candidates', 'subjects', 'evidence', 'coverage')

#: 只有这些错误码可以写入 Run.error（与 mcp_contract.RUN_ERROR_CODES 一致）。
RUN_ERROR_CODES = frozenset((
    'invalid_argument', 'plan_hash_mismatch', 'plan_expired', 'plan_not_executable',
    'limit_exceeded', 'out_of_scope', 'roster_incomplete', 'no_applicable_subjects',
    'snapshot_not_found', 'snapshot_expired', 'cursor_invalid', 'run_not_found',
    'conflict', 'already_running', 'upstream_unavailable', 'partial_failure',
    'billing_unknown', 'cancelled', 'internal_error',
))

#: 计入调用硬帽的派发状态。reused 复用本地已取得数据，不占上游额度。
_USED_CALL_STATES = "('done','failed','inflight','uncertain','reserved')"

_PLAN_RE = re.compile(r'^plan_[A-Za-z0-9_-]{8,64}$')
_HASH_RE = re.compile(r'^sha256:[0-9a-f]{64}$')
_RUN_RE = re.compile(r'^run_[A-Za-z0-9_-]{8,64}$')
_LEASE_STALE_SECONDS = 120

#: 本进程内仍在执行的 (owner, run_id)；用于避免启动恢复抢走同进程健康 worker。
_ACTIVE: set[tuple[str, str]] = set()
_ACTIVE_LOCK = threading.Lock()


def _new_owner() -> str:
    """owner 携带 PID，供跨进程启动恢复判断原执行器是否仍存活。"""
    return f'{os.getpid()}.{uid("worker")}'


def _owner_pid(owner: Any) -> int | None:
    if not isinstance(owner, str):
        return None
    head = owner.split('.', 1)[0]
    return int(head) if head.isdigit() else None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _is_active(owner: Any, run_id: str) -> bool:
    if not isinstance(owner, str):
        return False
    with _ACTIVE_LOCK:
        return (owner, run_id) in _ACTIVE


def _set_active(owner: str, run_id: str, active: bool) -> None:
    with _ACTIVE_LOCK:
        if active:
            _ACTIVE.add((owner, run_id))
        else:
            _ACTIVE.discard((owner, run_id))


def _owner_busy(owner: Any, run_id: str, heartbeat: Any) -> bool:
    """另一个仍健康的执行器是否持有该运行；同进程空闲 owner 不算忙。"""
    if not isinstance(owner, str) or not owner:
        return False
    if _is_active(owner, run_id):
        return True
    pid = _owner_pid(owner)
    if pid is None or pid == os.getpid() or not _pid_alive(pid):
        return False
    return isinstance(heartbeat, str) and heartbeat > later(-_LEASE_STALE_SECONDS)


def _clean_message(message: Any, fallback: str = '运行未能完成。') -> str:
    """脱敏错误的最后一道显示约束：单行、定长、非空。"""
    text = message if isinstance(message, str) else ''
    text = ' '.join(text.replace('\x00', ' ').split())
    return (text or fallback)[:500]


def _map_error(code: Any, message: Any) -> tuple[str, str]:
    """把网关/快照错误归一到契约枚举，避免把凭据或原始报文带出去。"""
    text = _clean_message(message)
    if isinstance(code, str) and code in RUN_ERROR_CODES and code not in ('billing_unknown',):
        return code, text
    if code == 'billing_unknown':
        return 'billing_unknown', '上游请求是否计费未知；已保留派发记录，不会自动重发。'
    if code in ('configuration_error',):
        return 'upstream_unavailable', '服务未配置可用的上游调用；未发生付费请求。'
    if code in ('upstream_denied', 'upstream_rejected'):
        return 'partial_failure', '上游拒绝了本次查询；请检查产品订阅或范围。'
    if code in ('snapshot_incomplete',):
        return 'partial_failure', text
    return 'internal_error', '运行因内部错误终止；请联系服务运营并提供追踪号。'


def _obj(value: Any, fallback_code: str = 'invalid_argument') -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except ValueError:
            raise Fault(fallback_code, '计划内容损坏，无法执行。') from None
        if isinstance(parsed, dict):
            return parsed
    raise Fault(fallback_code, '计划内容损坏，无法执行。')


def _positive_int(value: Any) -> int | None:
    return value if type(value) is int and value >= 1 else None


def _collection_parent(plan: dict) -> str:
    """Collector 的父快照：resolution 用 args.snapshot_id 重放决策，其余用阶段快照。"""
    args = plan.get('args') if isinstance(plan.get('args'), dict) else {}
    parent = args.get('snapshot_id')
    if isinstance(parent, str) and parent:
        return parent
    return plan['snapshot_id']


def _scope_subject_count(scope_plan: dict) -> int:
    scope = scope_plan.get('scope') if isinstance(scope_plan.get('scope'), dict) else {}
    for key in ('unique_subject_count', 'subject_count'):
        value = scope.get(key)
        if type(value) is int and value >= 0:
            return value
    value = scope.get('row_count')
    return value if type(value) is int and value >= 0 else 0


class Engine:
    """付费执行引擎：每租户每计划最多一个运行，调用数以本地账本为准。"""

    def __init__(self, store, gateway):
        self.store = store
        self.gateway = gateway
        self.settings = store.settings
        self._lock = threading.RLock()
        self._workers: dict[str, threading.Thread] = {}
        self._stop_events: dict[str, threading.Event] = {}
        self._tokens: set[str] = set()
        self._closed = False
        # 服务启动即恢复中断的运行；recover 只处理本租户且仅在本服务 live 时派发。
        self.recover()

    # -- public API ---------------------------------------------------------

    def execute(self, tenant: str, args: dict) -> dict:
        """唯一可能产生费用的入口；同一计划幂等返回同一运行。"""
        if self._closed:
            raise Fault('internal_error', '执行引擎已关闭，不能开始新的执行。')
        request = self._execute_args(args)
        plan_id, plan_hash = request['plan_id'], request['plan_hash']
        created: tuple[str, str, str] | None = None
        run_id = None
        with self.store.tx(write=True) as db:
            plan = _load_plan(db, tenant, plan_id)
            if plan['plan_hash'] != plan_hash:
                raise Fault('plan_hash_mismatch', '计划哈希与计划内容不一致，拒绝执行。')
            existing = db.execute(
                'SELECT * FROM execution_runs WHERE tenant=? AND plan_id=?',
                (tenant, plan_id),
            ).fetchone()
            if existing is not None:
                if existing['plan_hash'] != plan_hash:
                    raise Fault('plan_hash_mismatch', '已有运行绑定了其他计划哈希。')
                run_id = existing['id']
            else:
                if not self.settings.live:
                    raise Fault(
                        'upstream_unavailable',
                        '服务未启用真实上游（live=false）；本次未发生任何付费调用。',
                    )
                terms = _plan_gate(self.store, db, tenant, plan)
                parent_id = _collection_parent(plan)
                snapshot_id = self.store.new_snapshot(
                    db, tenant, plan['roster_id'], parent_id=parent_id, sealed=False
                )
                run_id = uid('run')
                token = _new_owner()
                created = (run_id, token, snapshot_id)
                ts = now()
                db.execute(
                    '''INSERT INTO execution_runs(
                         id,tenant,plan_id,plan_hash,stage,roster_id,
                         plan_snapshot_id,snapshot_id,state,created_at,updated_at,
                         expires_at,max_calls,products_total,subjects_total,
                         worker_owner,worker_heartbeat)
                       VALUES(?,?,?,?,?,?,?,?,'queued',?,?,?,?,?,?,?,?)''',
                    (
                        run_id, tenant, plan_id, plan_hash, plan['stage'],
                        plan['roster_id'], parent_id, snapshot_id, ts, ts,
                        terms['expires_at'], terms['max_calls'],
                        len(terms['product_ids']), terms['subjects_total'],
                        token, ts,
                    ),
                )
        if created is not None:
            run_id, token, _ = created
            with self._lock:
                self._tokens.add(token)
            self._start_worker(run_id, token)
        else:
            self._ensure_worker(tenant, run_id)
        return self.get(tenant, {'run_id': run_id})

    def get(self, tenant: str, args: dict) -> dict:
        """读取运行状态；只读本地账本，绝不触发上游请求或恢复。"""
        run_id = self._run_args(args)
        with self.store.tx() as db:
            row = db.execute(
                'SELECT * FROM execution_runs WHERE id=? AND tenant=?', (run_id, tenant)
            ).fetchone()
            if row is None:
                raise Fault('run_not_found', '运行不存在或不属于当前租户。')
            result = None
            if row['result_json']:
                try:
                    result = json.loads(row['result_json'])
                except (TypeError, ValueError):
                    raise Fault('internal_error', '运行结果损坏，无法读取。') from None
                if not isinstance(result, dict):
                    raise Fault('internal_error', '运行结果损坏，无法读取。')
            return self._view(db, row, result)

    def recover(self) -> None:
        """服务启动时的账本恢复；仅本租户，且仅在本服务启用上游时派发。"""
        if self._closed or not self.settings.live:
            return
        tenant = self.settings.tenant
        starts: list[tuple[str, str]] = []
        with self.store.tx(write=True) as db:
            rows = db.execute(
                "SELECT * FROM execution_runs WHERE tenant=? AND state IN ('queued','running') "
                'ORDER BY created_at', (tenant,)
            ).fetchall()
            for row in rows:
                if self._local_worker_alive(row['id']) or _owner_busy(
                        row['worker_owner'], row['id'], row['worker_heartbeat']):
                    continue
                try:
                    start = self._recover_one(db, row)
                except Fault as exc:
                    code, message = _map_error(exc.code, exc.message)
                    self._finish_tx(db, row, 'failed', False, code, message)
                    continue
                except Exception:
                    trace = uid('trace')
                    LOG.error('运行恢复失败 run=%s trace=%s', row['id'], trace)
                    self._finish_tx(
                        db, row, 'failed', False, 'internal_error',
                        '运行恢复失败；请联系服务运营并提供追踪号。', trace,
                    )
                    continue
                if start is not None:
                    starts.append(start)
        for run_id, token in starts:
            with self._lock:
                self._tokens.add(token)
            self._start_worker(run_id, token)

    def close(self) -> None:
        """优雅停止 worker；未派发的请求回到可恢复状态，已派发的不伪造结果。"""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            events = list(self._stop_events.values())
            threads = [(rid, t) for rid, t in self._workers.items() if t.is_alive()]
        for event in events:
            event.set()
        deadline = time.monotonic() + 15.0
        for _, thread in threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            thread.join(remaining)
        with self._lock:
            self._tokens.clear()

    # -- argument validation ------------------------------------------------

    @staticmethod
    def _execute_args(args: Any) -> dict:
        if not isinstance(args, dict) or set(args) != {'plan_id', 'plan_hash'}:
            raise Fault(
                'invalid_argument',
                '执行参数只允许 plan_id、plan_hash；不接受自报确认、金额或动态范围。',
            )
        plan_id, plan_hash = args['plan_id'], args['plan_hash']
        if (not isinstance(plan_id, str) or not _PLAN_RE.fullmatch(plan_id)
                or not isinstance(plan_hash, str) or not _HASH_RE.fullmatch(plan_hash)):
            raise Fault('invalid_argument', '计划引用或哈希格式无效。')
        return {'plan_id': plan_id, 'plan_hash': plan_hash}

    @staticmethod
    def _run_args(args: Any) -> str:
        if not isinstance(args, dict) or set(args) != {'run_id'}:
            raise Fault('invalid_argument', '读取运行只接受 run_id。')
        run_id = args['run_id']
        if not isinstance(run_id, str) or not _RUN_RE.fullmatch(run_id):
            raise Fault('invalid_argument', '运行引用格式无效。')
        return run_id

    # -- run rendering ------------------------------------------------------

    def _view(self, db, row, result: dict | None) -> dict:
        state = row['state'] if row['state'] in RUN_STATES else 'failed'
        calls_used = _usage(db, row['id'])
        error = _error_view(row)
        view = {
            'run_id': row['id'],
            'plan_id': row['plan_id'],
            'plan_hash': row['plan_hash'],
            'state': state,
            # 契约明确 partial 不属于 succeeded/failed/cancelled 三个终止状态，
            # 但它已停止派发且不能靠原计划上限续跑（is_resumable=false）。
            'is_terminal': state in TERMINAL_STATES,
            'is_resumable': state in NONTERMINAL_STATES,
            'created_at': row['created_at'],
            'updated_at': row['updated_at'],
            'progress': {
                'products_total': _clamp(row['products_total'], 0, 43),
                'products_completed': _clamp(row['products_completed'], 0, 43),
                'subjects_total': _clamp(row['subjects_total'], 0, 100_000),
                'subjects_completed': _clamp(row['subjects_completed'], 0, 100_000),
                'pages_written': _clamp(row['pages_written'], 0, 10_000_000),
                'records_written': _clamp(row['records_written'], 0, 100_000_000),
            },
            'usage': {
                'calls_used': _clamp(calls_used, 0, 100_000),
                'max_calls': _clamp(row['max_calls'], 1, 100_000),
            },
        }
        if state in ('succeeded', 'partial') and result is None:
            raise Fault('internal_error', '运行结果缺失，无法读取。')
        if result is not None:
            view['result'] = result
        if state == 'failed' and error is None:
            error = {'code': 'internal_error', 'message': '运行失败但没有可读原因。',
                     'retryable': False}
        if error is not None:
            view['error'] = error
        return view

    # -- worker lifecycle ---------------------------------------------------

    def _local_worker_alive(self, run_id: str) -> bool:
        with self._lock:
            thread = self._workers.get(run_id)
            return bool(thread and thread.is_alive())

    def _owner_is_local(self, owner: str | None) -> bool:
        if not owner:
            return False
        with self._lock:
            return owner in self._tokens

    def _start_worker(self, run_id: str, token: str) -> bool:
        with self._lock:
            if self._closed:
                return False
            thread = self._workers.get(run_id)
            if thread is not None and thread.is_alive():
                return False
            stop = threading.Event()
            worker = threading.Thread(
                target=self._work,
                args=(run_id, token, stop),
                name='portrait-' + run_id[-12:],
                daemon=True,
            )
            self._workers[run_id] = worker
            self._stop_events[run_id] = stop
            self._tokens.add(token)
        _set_active(token, run_id, True)
        worker.start()
        return True

    def _ensure_worker(self, tenant: str, run_id: str) -> None:
        """幂等重放时按需恢复本进程对应运行；不会绕过计划有效期或调用上限。"""
        if self._closed or not self.settings.live or self._local_worker_alive(run_id):
            return
        claim = None
        with self.store.tx(write=True) as db:
            row = db.execute(
                'SELECT * FROM execution_runs WHERE id=? AND tenant=?', (run_id, tenant)
            ).fetchone()
            if row is None or row['state'] not in NONTERMINAL_STATES:
                return
            owner = row['worker_owner']
            if not self._owner_is_local(owner) and _owner_busy(
                    owner, run_id, row['worker_heartbeat']):
                return
            token = _new_owner()
            claimed = db.execute(
                'UPDATE execution_runs SET worker_owner=?, worker_heartbeat=?, updated_at=? '
                "WHERE id=? AND state IN ('queued','running') AND worker_owner IS ?",
                (token, now(), now(), run_id, owner),
            )
            if claimed.rowcount != 1:
                return
            claim = token
        if claim is not None:
            with self._lock:
                self._tokens.add(claim)
            self._start_worker(run_id, claim)

    def _work(self, run_id: str, token: str, stop: threading.Event) -> None:
        try:
            plan = self._claim(run_id, token, stop)
            if plan is None:
                return
            self._collect(run_id, token, stop, plan)
        except Fault as exc:
            stopped = stop.is_set() or self._closed
            # 只有明确“未派发/确定未计费”的中断才回到可恢复状态；计费未知必须终止。
            if exc.code == 'cancelled' or (
                    stopped and exc.code in ('upstream_unavailable', 'configuration_error')):
                self._requeue(run_id, token)
            elif exc.code == 'limit_exceeded':
                # 调用量硬帽触顶是预期停止：有已购结果则交 partial。
                self._finish(run_id, token, 'partial', False, 'limit_exceeded', exc.message)
            else:
                code, message = _map_error(exc.code, exc.message)
                self._finish(run_id, token, 'failed', False, code, message)
        except Exception:
            trace = uid('trace')
            LOG.error('运行失败 run=%s trace=%s', run_id, trace)
            self._finish(
                run_id, token, 'failed', False, 'internal_error',
                '运行因内部错误终止；请联系服务运营并提供追踪号。', trace,
            )
        finally:
            _set_active(token, run_id, False)
            with self._lock:
                self._tokens.discard(token)
                if self._workers.get(run_id) is threading.current_thread():
                    self._workers.pop(run_id, None)
                    self._stop_events.pop(run_id, None)

    def _claim(self, run_id: str, token: str, stop: threading.Event) -> dict | None:
        with self.store.tx(write=True) as db:
            row = db.execute('SELECT * FROM execution_runs WHERE id=?', (run_id,)).fetchone()
            if row is None or row['worker_owner'] != token:
                return None
            if row['state'] not in NONTERMINAL_STATES:
                return None
            if stop.is_set() or self._closed:
                self._requeue_tx(db, row, token)
                return None
            if not self.settings.live:
                return None
            snapshot = db.execute(
                'SELECT * FROM snapshots WHERE id=? AND tenant=?',
                (row['snapshot_id'], row['tenant']),
            ).fetchone()
            if snapshot is None:
                raise Fault('snapshot_not_found', '运行结果快照不存在，不能继续执行。')
            if snapshot['sealed']:
                raise Fault('internal_error', '运行结果快照状态异常，不能继续写入。')
            if snapshot['expires_at'] <= now():
                raise Fault('snapshot_expired', '运行结果快照已过期，不能继续执行。')
            plan = _load_plan(db, row['tenant'], row['plan_id'])
            if plan['plan_hash'] != row['plan_hash']:
                raise Fault('plan_hash_mismatch', '计划已发生变化，不能继续执行。')
            _plan_gate(self.store, db, row['tenant'], plan)
            ts = now()
            db.execute(
                'UPDATE execution_runs SET state=\'running\', started_at=COALESCE(started_at,?), '
                'worker_heartbeat=?, updated_at=? WHERE id=? AND worker_owner=?',
                (ts, ts, ts, run_id, token),
            )
            return plan

    def _collect(self, run_id: str, token: str, stop: threading.Event, plan: dict) -> None:
        from server.collector import collect

        with self.store.tx() as db:
            row = db.execute(
                'SELECT snapshot_id FROM execution_runs WHERE id=?', (run_id,)
            ).fetchone()
        if row is None:
            return
        snapshot_id = row['snapshot_id']
        complete = collect(
            self.store,
            plan['tenant'],
            plan,
            snapshot_id,
            lambda product_id, params: self._fetch(run_id, token, stop, plan, product_id, params),
        )
        self._finish(run_id, token, 'succeeded' if complete else 'partial', bool(complete))

    # -- the only paid dispatch path ---------------------------------------

    def _fetch(self, run_id: str, token: str, stop: threading.Event, plan: dict,
               product_id: str, params: Any) -> tuple[dict, str]:
        if self._closed or stop.is_set():
            raise Fault('cancelled', '执行正在停止；本次请求未派发。')
        if not self.settings.live:
            raise Fault(
                'upstream_unavailable',
                '服务未启用真实上游（live=false）；本次请求未派发。',
            )
        scope = plan.get('scope_plan', {}).get('scope')
        products = scope.get('product_ids') if isinstance(scope, dict) else None
        if (not isinstance(product_id, str) or not isinstance(products, list)
                or product_id not in products):
            raise Fault('out_of_scope', '请求的产品不在本计划冻结范围内，拒绝派发。')
        if not isinstance(params, Mapping):
            raise Fault('invalid_argument', '上游参数必须是 JSON 对象。')
        params = dict(params)
        clean = params
        validate = getattr(self.gateway, 'validate_call', None)
        if callable(validate):
            validated = validate(product_id, params)
            if isinstance(validated, dict):
                clean = validated
        params_key = digest({'product_id': product_id, 'params': clean})
        # 跨运行（同租户、同名单、同产品、同参数）的幂等请求键；仅用于审计与复用。
        request_key = digest([plan['tenant'], plan['roster_id'], product_id, params_key])
        ts = now()
        replay: tuple[str, str] | None = None
        reservation: str | None = None
        with self.store.tx(write=True) as db:
            run = db.execute(
                'SELECT * FROM execution_runs WHERE id=?', (run_id,)
            ).fetchone()
            if run is None or run['worker_owner'] != token:
                raise Fault('conflict', '运行已由其他执行器接管，停止派发。')
            if run['state'] not in NONTERMINAL_STATES:
                raise Fault('conflict', '运行已不在可执行状态。')
            terms = _plan_gate(self.store, db, run['tenant'], plan)
            _require_snapshot_writable(db, run['snapshot_id'], run['tenant'])

            existing = db.execute(
                'SELECT * FROM execution_dispatches WHERE run_id=? AND product_id=? AND params_key=?',
                (run_id, product_id, params_key),
            ).fetchone()
            if existing is not None:
                if existing['state'] in ('done', 'reused'):
                    replay = (existing['response'], existing['acquired_at'])
                elif existing['state'] in ('inflight', 'uncertain'):
                    raise Fault(
                        'billing_unknown',
                        '同一请求已发出且计费状态未知；已保留派发记录，不会自动重发。',
                    )
                elif existing['state'] == 'failed':
                    code, message = _map_error(existing['error_code'], existing['error_message'])
                    raise Fault(code, message)
                elif existing['state'] == 'reserved':
                    raise Fault('conflict', '同一请求正在由其他执行器派发，拒绝重复发送。')
                # abandoned：上次中断发生在派发之前，派发名额已释放，可重新登记。
            if replay is None:
                source = _find_reusable(db, run, product_id, params_key)
                if source is not None and source['response'] is not None:
                    if existing is not None:
                        # 上次中断发生在派发前：把该 abandoned 行直接改成复用记录。
                        updated = db.execute(
                            "UPDATE execution_dispatches SET state='reused', "
                            'response=?, acquired_at=?, '
                            'source_dispatch_id=?, expires_at=?, error_code=NULL, '
                            "error_message=NULL, updated_at=? WHERE id=? AND state='abandoned'",
                            (
                                source['response'], source['acquired_at'], source['id'],
                                source['expires_at'], ts, existing['id'],
                            ),
                        )
                        if updated.rowcount != 1:
                            raise Fault('conflict', '派发记录已被其他执行器改变，拒绝复用。')
                    else:
                        db.execute(
                            '''INSERT INTO execution_dispatches(
                                 id,tenant,run_id,product_id,params_key,request_key,state,
                                 response,acquired_at,source_dispatch_id,expires_at,
                                 created_at,updated_at)
                               VALUES(?,?,?,?,?,?,'reused',?,?,?,?,?,?)''',
                            (
                                uid('disp'), run['tenant'], run_id, product_id,
                                params_key, request_key, source['response'], source['acquired_at'],
                                source['id'], source['expires_at'], ts, ts,
                            ),
                        )
                    replay = (source['response'], source['acquired_at'])
            if replay is None:
                active = _active_duplicate(db, run_id, run['tenant'], request_key)
                if active is not None:
                    if active['state'] in ('inflight', 'uncertain'):
                        raise Fault(
                            'billing_unknown',
                            '同一请求在其他运行中已发出且计费状态未知；不会重复派发。',
                        )
                    raise Fault(
                        'conflict',
                        '同一请求正由其他运行派发；本次参数不重复发送，请稍后复用其结果。',
                    )
            if replay is None:
                if existing is not None:  # abandoned
                    _check_budget(db, run, terms, product_id)
                    db.execute(
                        "UPDATE execution_dispatches SET state='reserved', error_code=NULL, "
                        "error_message=NULL, updated_at=? WHERE id=? AND state='abandoned'",
                        (ts, existing['id']),
                    )
                    reservation = existing['id']
                else:
                    _check_budget(db, run, terms, product_id)
                    reservation = uid('disp')
                    db.execute(
                        '''INSERT INTO execution_dispatches(
                             id,tenant,run_id,product_id,params_key,request_key,state,
                             created_at,updated_at)
                           VALUES(?,?,?,?,?,?,'reserved',?,?)''',
                        (
                            reservation, run['tenant'], run_id, product_id,
                            params_key, request_key, ts, ts,
                        ),
                    )
            if replay is None:
                db.execute(
                    'UPDATE execution_runs SET worker_heartbeat=?, updated_at=? '
                    'WHERE id=? AND worker_owner=?',
                    (ts, ts, run_id, token),
                )
            else:
                self._refresh_progress(db, run_id, token)
        if replay is not None:
            response, acquired_at = replay
            return _decode_response(response, acquired_at)
        if reservation is None:
            raise Fault('internal_error', '派发登记失败；本次请求未发送。')
        if self._closed or stop.is_set():
            self._abandon(reservation)
            raise Fault('cancelled', '执行正在停止；本次请求未派发。')
        if not self._mark_inflight(run_id, token, reservation):
            self._abandon(reservation)
            raise Fault('conflict', '派发权已被其他执行器接管；本次请求未发送。')

        try:
            data = self.gateway.fetch(product_id, clean)
        except Fault as exc:
            if exc.code == 'billing_unknown':
                self._settle_uncertain(reservation, exc.message)
                raise Fault(
                    'billing_unknown',
                    '上游请求是否计费未知；已保留派发记录，不会自动重发。',
                ) from None
            code, message = _map_error(exc.code, exc.message)
            self._settle_failed(reservation, code, message)
            raise Fault(code, message) from None
        except Exception:
            # 非 Fault 异常同样无法证明未计费，按未知处理并保留派发记录。
            self._settle_uncertain(reservation, '上游调用异常。')
            raise Fault(
                'billing_unknown',
                '上游请求是否计费未知；已保留派发记录，不会自动重发。',
            ) from None

        acquired_at = now()
        if not isinstance(data, dict):
            self._settle_failed(reservation, 'internal_error', '上游返回格式异常。')
            raise Fault('internal_error', '上游返回格式异常；本次调用已消耗计划额度。')
        try:
            encoded = pack(data)
        except (TypeError, ValueError):
            self._settle_failed(reservation, 'internal_error', '上游返回无法保存。')
            raise Fault('internal_error', '上游返回无法保存；本次调用已消耗计划额度。') from None
        self._settle_done(run_id, token, reservation, encoded, acquired_at)
        return data, acquired_at

    # -- dispatch settlement ------------------------------------------------

    def _mark_inflight(self, run_id: str, token: str, dispatch_id: str) -> bool:
        with self.store.tx(write=True) as db:
            result = db.execute(
                '''UPDATE execution_dispatches SET state='inflight', updated_at=?
                   WHERE id=? AND state='reserved'
                     AND EXISTS(SELECT 1 FROM execution_runs r
                                WHERE r.id=? AND r.worker_owner=?
                                  AND r.state IN ('queued','running')
                                  AND r.expires_at>?)''',
                (now(), dispatch_id, run_id, token, now()),
            )
            return result.rowcount == 1

    def _abandon(self, dispatch_id: str) -> None:
        # 只释放尚未派发的预留；inflight 行可能已被另一执行器发出，不能在此抹掉。
        with self.store.tx(write=True) as db:
            db.execute(
                "UPDATE execution_dispatches SET state='abandoned', updated_at=? "
                "WHERE id=? AND state='reserved'",
                (now(), dispatch_id),
            )

    def _settle_uncertain(self, dispatch_id: str, message: str) -> None:
        with self.store.tx(write=True) as db:
            db.execute(
                "UPDATE execution_dispatches SET state='uncertain', error_code='billing_unknown', "
                'error_message=?, updated_at=? WHERE id=? AND state IN (\'inflight\',\'reserved\')',
                (_clean_message(message), now(), dispatch_id),
            )

    def _settle_failed(self, dispatch_id: str, code: str, message: str) -> None:
        with self.store.tx(write=True) as db:
            db.execute(
                "UPDATE execution_dispatches SET state='failed', error_code=?, "
                'error_message=?, updated_at=? WHERE id=?',
                (code, _clean_message(message), now(), dispatch_id),
            )

    def _settle_done(self, run_id: str, token: str, dispatch_id: str,
                     encoded: str, acquired_at: str) -> None:
        with self.store.tx(write=True) as db:
            row = db.execute(
                'SELECT snapshot_id FROM execution_runs WHERE id=?', (run_id,)
            ).fetchone()
            snapshot_expiry = later(self.settings.snapshot_ttl)
            if row is not None:
                snap = db.execute(
                    'SELECT expires_at FROM snapshots WHERE id=?', (row['snapshot_id'],)
                ).fetchone()
                if snap is not None:
                    snapshot_expiry = min(snapshot_expiry, snap['expires_at'])
            db.execute(
                "UPDATE execution_dispatches SET state='done', response=?, "
                'acquired_at=?, expires_at=?, error_code=NULL, error_message=NULL, updated_at=? '
                'WHERE id=?',
                (encoded, acquired_at, snapshot_expiry, now(), dispatch_id),
            )
            self._refresh_progress(db, run_id, token)

    def _refresh_progress(self, db, run_id: str, token: str) -> None:
        done = db.execute(
            "SELECT COUNT(*) AS pages, COUNT(DISTINCT product_id) AS products "
            "FROM execution_dispatches WHERE run_id=? AND state IN ('done','reused')",
            (run_id,),
        ).fetchone()
        db.execute(
            'UPDATE execution_runs SET pages_written=?, products_completed=?, '
            'worker_heartbeat=?, updated_at=? WHERE id=? AND worker_owner=?',
            (done['pages'], done['products'], now(), now(), run_id, token),
        )

    # -- run finalization / recovery ---------------------------------------

    def _requeue(self, run_id: str, token: str) -> None:
        with self.store.tx(write=True) as db:
            row = db.execute('SELECT * FROM execution_runs WHERE id=?', (run_id,)).fetchone()
            if row is not None:
                self._requeue_tx(db, row, token)

    @staticmethod
    def _requeue_tx(db, row, token: str) -> None:
        if row['worker_owner'] != token:
            return
        db.execute(
            "UPDATE execution_dispatches SET state='abandoned', updated_at=? "
            "WHERE run_id=? AND state='reserved'",
            (now(), row['id']),
        )
        db.execute(
            "UPDATE execution_runs SET state='queued', worker_owner=NULL, worker_heartbeat=NULL, "
            "updated_at=? WHERE id=? AND worker_owner=? AND state IN ('queued','running')",
            (now(), row['id'], token),
        )

    def _finish(self, run_id: str, token: str, state: str, complete: bool,
                code: str | None = None, message: str | None = None,
                trace: str | None = None) -> None:
        with self.store.tx(write=True) as db:
            row = db.execute('SELECT * FROM execution_runs WHERE id=?', (run_id,)).fetchone()
            if row is None or row['worker_owner'] != token or row['state'] not in NONTERMINAL_STATES:
                return
            db.execute(
                "UPDATE execution_dispatches SET state='abandoned', updated_at=? "
                "WHERE run_id=? AND state='reserved'",
                (now(), run_id),
            )
            self._finish_tx(db, row, state, complete, code, message, trace)

    def _finish_tx(self, db, row, state: str, complete: bool,
                   code: str | None = None, message: str | None = None,
                   trace: str | None = None) -> None:
        requested = state if state in RUN_STATES else 'failed'
        state = requested
        if state in ('succeeded', 'partial'):
            unknown = db.execute(
                "SELECT 1 FROM execution_dispatches WHERE run_id=? "
                "AND (state IN ('inflight','uncertain') OR error_code='billing_unknown') LIMIT 1",
                (row['id'],),
            ).fetchone()
            if unknown is not None:
                state, complete = 'failed', False
                code = 'billing_unknown'
                message = '存在计费状态未知的上游请求；已保留派发记录，不会自动重发。'
            elif state == 'succeeded':
                gap = db.execute(
                    "SELECT 1 FROM execution_dispatches WHERE run_id=? "
                    "AND state NOT IN ('done','reused') LIMIT 1",
                    (row['id'],),
                ).fetchone()
                if gap is not None:
                    # 有未成功取得的派发键，不能声称完整覆盖。
                    state, complete, requested = 'partial', False, 'partial'
        if state == 'succeeded' and not complete:
            state, code, message = 'failed', 'internal_error', '运行结果不完整。'
        snapshot = db.execute(
            'SELECT * FROM snapshots WHERE id=? AND tenant=?',
            (row['snapshot_id'], row['tenant']),
        ).fetchone()
        result = None
        if snapshot is None:
            state, code = 'failed', code or 'snapshot_not_found'
            message = message or '运行结果快照不存在。'
        elif snapshot['expires_at'] <= now():
            state, code = 'failed', code or 'snapshot_expired'
            message = message or '运行结果快照已过期。'
        else:
            self.store.seal(db, row['snapshot_id'])
            views, evidence_count, coverage_count, subject_count = self._snapshot_stats(
                db, row['snapshot_id']
            )
            if views:
                result = {
                    'snapshot_id': row['snapshot_id'],
                    'views_available': views,
                    'subject_count': _clamp(subject_count, 0, 100_000),
                    'evidence_count': _clamp(evidence_count, 0, 10_000_000),
                    'coverage_count': _clamp(coverage_count, 0, 10_000_000),
                    'complete': bool(complete and state == 'succeeded'),
                }
            elif requested in ('succeeded', 'partial'):
                state = 'failed'
                code = code or ('internal_error' if requested == 'succeeded' else 'partial_failure')
                message = message or '运行没有产生可读取的结果。'
        if state == 'failed':
            code, message = _map_error(code, message)
            trace = trace or uid('trace')
        else:
            code = message = None
            trace = None
        done = db.execute(
            "SELECT COUNT(*) AS pages, COUNT(DISTINCT product_id) AS products "
            "FROM execution_dispatches WHERE run_id=? AND state IN ('done','reused')",
            (row['id'],),
        ).fetchone()
        records = db.execute(
            'SELECT COUNT(*) FROM records WHERE snapshot_id=? AND body IS NOT NULL',
            (row['snapshot_id'],),
        ).fetchone()[0]
        ts = now()
        db.execute(
            '''UPDATE execution_runs SET state=?, complete=?, updated_at=?, finished_at=?,
                 products_completed=?, subjects_completed=?, pages_written=?,
                 records_written=?, error_code=?, error_message=?, error_retryable=0,
                 error_retry_after=NULL, trace_id=?, result_json=?, worker_owner=NULL,
                 worker_heartbeat=NULL
               WHERE id=?''',
            (
                state, 1 if (state == 'succeeded' and complete) else 0, ts, ts,
                (row['products_total'] if state == 'succeeded' else done['products']),
                (row['subjects_total'] if state == 'succeeded'
                 else (result['subject_count'] if result else 0)), done['pages'], records,
                code, message, trace, (pack(result) if result is not None else None),
                row['id'],
            ),
        )

    def _snapshot_stats(self, db, snapshot_id: str) -> tuple[list[str], int, int, int]:
        views: list[str] = []
        for view in PAGE_VIEWS:
            for _key, _record in self.store.records(db, snapshot_id, view, limit=1):
                views.append(view)
                break
        evidence_count = 0
        subjects: set[str] = set()
        for _key, record in self.store.records(db, snapshot_id, 'evidence'):
            evidence_count += 1
            if isinstance(record, dict):
                entity = record.get('subject_id') or record.get('entity_id')
                if isinstance(entity, str) and entity:
                    subjects.add(entity)
        coverage_count = sum(1 for _ in self.store.records(db, snapshot_id, 'coverage'))
        if not subjects:
            for _key, record in self.store.records(db, snapshot_id, 'subjects'):
                if isinstance(record, dict) and isinstance(record.get('subject_id'), str):
                    subjects.add(record['subject_id'])
        return views, evidence_count, coverage_count, len(subjects)

    def _recover_one(self, db, row) -> tuple[str, str] | None:
        """检查一次中断的运行；返回 (run_id, token) 表示恢复成功。"""
        inflight = db.execute(
            "SELECT id FROM execution_dispatches WHERE run_id=? AND state='inflight' LIMIT 1",
            (row['id'],),
        ).fetchone()
        if inflight is not None:
            # 已标记派发但无法确认结果：保留记录，终止而不重发。
            db.execute(
                "UPDATE execution_dispatches SET state='uncertain', error_code='billing_unknown', "
                "error_message=?, updated_at=? WHERE run_id=? AND state='inflight'",
                ('上次进程在派发之后中断；计费状态未知。', now(), row['id']),
            )
            db.execute(
                "UPDATE execution_dispatches SET state='abandoned', updated_at=? "
                "WHERE run_id=? AND state='reserved'",
                (now(), row['id']),
            )
            self._finish_tx(
                db, row, 'failed', False, 'billing_unknown',
                '上次进程在派发之后中断；是否计费待核，已保留派发记录且不会自动重发。',
            )
            return None
        db.execute(
            "UPDATE execution_dispatches SET state='abandoned', updated_at=? "
            "WHERE run_id=? AND state='reserved'",
            (now(), row['id']),
        )
        snapshot = db.execute(
            'SELECT * FROM snapshots WHERE id=? AND tenant=?',
            (row['snapshot_id'], row['tenant']),
        ).fetchone()
        if snapshot is None:
            raise Fault('snapshot_not_found', '运行结果快照不存在，不能恢复。')
        if snapshot['sealed']:
            raise Fault('internal_error', '运行结果快照状态异常，不能恢复。')
        if snapshot['expires_at'] <= now():
            raise Fault('snapshot_expired', '运行结果快照已过期，不能恢复。')
        plan = _load_plan(db, row['tenant'], row['plan_id'])
        if plan['plan_hash'] != row['plan_hash']:
            raise Fault('plan_hash_mismatch', '计划已发生变化，不能恢复。')
        _plan_gate(self.store, db, row['tenant'], plan)
        if not self.settings.live:
            return None
        token = _new_owner()
        claimed = db.execute(
            'UPDATE execution_runs SET worker_owner=?, worker_heartbeat=?, updated_at=? '
            "WHERE id=? AND state IN ('queued','running') AND worker_owner IS ?",
            (token, now(), now(), row['id'], row['worker_owner']),
        )
        if claimed.rowcount != 1:
            return None
        return row['id'], token


# ---------------------------------------------------------------------------
# Frozen plan helpers shared by execute, dispatch, recovery and the collector.
# ---------------------------------------------------------------------------


def _load_plan(db, tenant: str, plan_id: str) -> dict:
    row = db.execute(
        'SELECT * FROM plans WHERE id=? AND tenant=?', (plan_id, tenant)
    ).fetchone()
    if row is None:
        raise Fault('invalid_argument', '计划不存在或不属于当前租户。')
    plan = dict(row)
    plan['args'] = _obj(plan.get('args'), 'plan_not_executable')
    plan['scope_plan'] = _obj(plan.get('scope_plan'), 'plan_not_executable')
    return plan


def _plan_gate(store, db, tenant: str, plan: dict) -> dict:
    """校验冻结计划仍可执行，并返回执行期的调用硬上限。

    计划一旦冻结就只按 ``scope_plan`` 执行：不重新报价、不读取金额、不接受
    客户端追加范围。调用上限取计划上限与逐产品上限的较严值。
    """
    scope_plan = plan.get('scope_plan')
    if not isinstance(scope_plan, dict):
        raise Fault('plan_not_executable', '计划缺少冻结范围，不能执行。')
    if plan.get('stage') not in ('base', 'resolution', 'topics'):
        raise Fault('plan_not_executable', '未知的计划阶段，不能执行。')
    if scope_plan.get('stage') != plan.get('stage'):
        raise Fault('plan_hash_mismatch', '计划阶段与冻结范围不一致，拒绝执行。')
    if scope_plan.get('plan_id') != plan.get('id'):
        raise Fault('plan_hash_mismatch', '冻结范围绑定了其他计划。')
    if scope_plan.get('plan_hash') != plan.get('plan_hash'):
        raise Fault('plan_hash_mismatch', '冻结范围绑定了其他计划哈希。')
    if scope_plan.get('roster_id') != plan.get('roster_id'):
        raise Fault('plan_hash_mismatch', '冻结范围绑定了其他名单。')
    if scope_plan.get('snapshot_id') != plan.get('snapshot_id'):
        raise Fault('plan_hash_mismatch', '冻结范围绑定了其他输入快照。')
    try:
        store.require_snapshot(db, tenant, _collection_parent(plan))
    except Fault as exc:
        if exc.code == 'out_of_scope':
            raise Fault('snapshot_not_found', '计划输入快照不可用，不能执行。') from None
        raise
    if scope_plan.get('executable') is not True:
        blockers = scope_plan.get('blockers')
        reason = ''
        if isinstance(blockers, list) and blockers:
            first = blockers[0] if isinstance(blockers[0], dict) else {}
            if isinstance(first.get('code'), str):
                reason = '：' + first['code']
        raise Fault('plan_not_executable', '计划不可执行（存在阻断项）' + reason + '。')
    if scope_plan.get('blockers'):
        raise Fault('plan_not_executable', '计划仍有阻断项，不能执行。')

    plan_expiry = plan.get('expires_at')
    if not isinstance(plan_expiry, str):
        raise Fault('plan_not_executable', '计划缺少有效期，不能执行。')
    if plan_expiry <= now():
        raise Fault('plan_expired', '计划已过期，不能执行。')
    scope_expiry = scope_plan.get('expires_at')
    if scope_expiry is not None:
        if not isinstance(scope_expiry, str) or scope_expiry != plan_expiry:
            raise Fault('plan_hash_mismatch', '冻结范围的有效期与计划不一致。')
        if scope_expiry <= now():
            raise Fault('plan_expired', '计划已过期，不能执行。')

    scope = scope_plan.get('scope') if isinstance(scope_plan.get('scope'), dict) else {}
    products = scope.get('product_ids')
    if not isinstance(products, list) or not products:
        raise Fault('plan_not_executable', '计划没有冻结任何产品，不能执行。')
    if (any(not isinstance(pid, str) or pid not in ROUTES for pid in products)
            or len(set(products)) != len(products)):
        raise Fault('plan_not_executable', '计划产品集合损坏或不在上游白名单内，不能执行。')
    limits = scope_plan.get('limits') if isinstance(scope_plan.get('limits'), dict) else {}
    estimate = (scope_plan.get('estimate')
                if isinstance(scope_plan.get('estimate'), dict) else {})
    max_calls = _positive_int(limits.get('max_calls'))
    est_calls = _positive_int(estimate.get('max_calls'))
    if max_calls is None or est_calls is None:
        raise Fault('plan_not_executable', '计划调用上限缺失或无效，不能执行。')
    max_calls = min(max_calls, est_calls)

    caps: dict[str, int] = {}
    for item in estimate.get('products') or []:
        if not isinstance(item, dict):
            continue
        pid = item.get('product_id')
        calls = _positive_int(item.get('max_calls'))
        if pid in products and calls is not None:
            caps[pid] = calls
    if set(products) - set(caps):
        raise Fault('plan_not_executable', '计划缺少逐产品调用上限，不能执行。')
    return {
        'max_calls': max_calls,
        'product_caps': caps,
        'product_ids': products,
        'subjects_total': _scope_subject_count(scope_plan),
        'expires_at': plan_expiry,
        'roster_id': plan['roster_id'],
    }


def _require_snapshot_writable(db, snapshot_id: str, tenant: str) -> None:
    row = db.execute(
        'SELECT * FROM snapshots WHERE id=? AND tenant=?', (snapshot_id, tenant)
    ).fetchone()
    if row is None:
        raise Fault('snapshot_not_found', '运行结果快照不存在，不能继续执行。')
    if row['sealed']:
        raise Fault('internal_error', '运行结果快照已发布，不能继续写入。')
    if row['expires_at'] <= now():
        raise Fault('snapshot_expired', '运行结果快照已过期，不能继续执行。')


def _active_duplicate(db, run_id: str, tenant: str, request_key: str):
    """同租户/同条件已在其他运行中派发或待派的请求；不得再次付费。"""
    return db.execute(
        """SELECT state,run_id FROM execution_dispatches
           WHERE tenant=? AND request_key=? AND run_id<>?
             AND state IN ('reserved','inflight','uncertain')
           LIMIT 1""",
        (tenant, request_key, run_id),
    ).fetchone()


def _find_reusable(db, run, product_id: str, params_key: str):
    return db.execute(
        '''SELECT d.* FROM execution_dispatches d JOIN execution_runs r ON r.id=d.run_id
           WHERE d.tenant=? AND r.roster_id=? AND d.product_id=? AND d.params_key=?
             AND d.state IN ('done','reused') AND d.response IS NOT NULL
             AND d.acquired_at IS NOT NULL
             AND d.expires_at IS NOT NULL AND d.expires_at>?
           ORDER BY d.updated_at DESC LIMIT 1''',
        (run['tenant'], run['roster_id'], product_id, params_key, now()),
    ).fetchone()


def _check_budget(db, run, terms: dict, product_id: str) -> None:
    used = db.execute(
        f'SELECT COUNT(*) AS calls FROM execution_dispatches '
        f'WHERE run_id=? AND state IN {_USED_CALL_STATES}',
        (run['id'],),
    ).fetchone()
    if used['calls'] + 1 > run['max_calls']:
        raise Fault('limit_exceeded', '本次计划调用量将达到硬上限，未派发该请求。')
    cap = terms['product_caps'].get(product_id)
    if cap is None:
        raise Fault('out_of_scope', '产品不在计划冻结范围内，拒绝派发。')
    product = db.execute(
        f'SELECT COUNT(*) AS calls FROM execution_dispatches '
        f'WHERE run_id=? AND product_id=? AND state IN {_USED_CALL_STATES}',
        (run['id'], product_id),
    ).fetchone()
    if product['calls'] + 1 > cap:
        raise Fault('limit_exceeded', '该产品调用量将达到计划上限，未派发该请求。')


def _usage(db, run_id: str) -> int:
    row = db.execute(
        f'SELECT COUNT(*) AS calls FROM execution_dispatches '
        f'WHERE run_id=? AND state IN {_USED_CALL_STATES}',
        (run_id,),
    ).fetchone()
    return int(row['calls'] or 0)


def _error_view(row) -> dict | None:
    code = row['error_code']
    if not code:
        return None
    if code not in RUN_ERROR_CODES:
        code = 'internal_error'
    error = {
        'code': code,
        'message': _clean_message(row['error_message']),
        'retryable': bool(row['error_retryable']),
    }
    trace = row['trace_id']
    if isinstance(trace, str) and trace:
        error['trace_id'] = trace[:128]
    return error


def _decode_response(response: Any, acquired_at: Any) -> tuple[dict, str]:
    if not isinstance(response, str) or not response:
        raise Fault('internal_error', '本地已购结果损坏，不能复用。')
    try:
        data = json.loads(response)
    except (TypeError, ValueError):
        raise Fault('internal_error', '本地已购结果损坏，不能复用。') from None
    if not isinstance(data, dict):
        raise Fault('internal_error', '本地已购结果损坏，不能复用。')
    if not isinstance(acquired_at, str) or not acquired_at:
        # 取得时间不可伪造，否则会破坏“何时可知”的判断。
        raise Fault('internal_error', '本地已购结果缺少取得时间，不能复用。')
    return data, acquired_at


def _clamp(value: Any, low: int, high: int) -> int:
    if type(value) is not int:
        value = low
    return max(low, min(high, value))


