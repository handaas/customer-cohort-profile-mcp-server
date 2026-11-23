"""Three-stage evidence collector for customer cohort snapshots.

``collect(store, tenant, plan, snapshot_id, fetch)`` is the only entry point
used by ``execution.Engine``.  It reads a frozen plan, performs real calls
through the ledger-owned ``fetch`` callback, and writes contract-shaped records
into the unpublished snapshot.  It never seals a snapshot, never writes
execution tables and never contacts the upstream gateway directly.

Views written:

``identities``  one record per original roster row (row_id key)
``candidates``  ambiguous keyword hits that need a human decision
``subjects``    de-duplicated confirmed enterprises
``evidence``    subject/product/field facts with time, unit and status
``coverage``    subject x product coverage, including unqueried products
``source``      run-internal raw pages (never exposed through read_page)

The internal ``source`` view is keyed by ``stable_id('src', subject, product)``
plus a period or parameter digest suffix.  Parent snapshots are followed by
``store.records`` so already purchased pages are reused without a new call.
"""

from __future__ import annotations

import hashlib
import json
import re
from calendar import monthrange
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from server.common import Fault, ROUTES, now, pack, stable_id
from server import evidence as evid

IDENTITY_VIEW = "identities"
CANDIDATE_VIEW = "candidates"
SUBJECT_VIEW = "subjects"
EVIDENCE_VIEW = "evidence"
COVERAGE_VIEW = "coverage"
SOURCE_VIEW = "source"

BASE_TOPICS = {"identity", "base", "supplement"}
EXACT_MATCH_REASONS = {"exact_name", "former_name", "social_credit_code"}
COVERAGE_STATES = {"covered", "empty", "not_queried", "not_applicable", "error"}
MAX_CANDIDATES = 100
MAX_SUBJECT_ROWS = 1000


def _pid_by_name(name: str) -> Optional[str]:
    for product_id, route in ROUTES.items():
        if route.get("name") == name:
            return product_id
    return None


FACE_PID = _pid_by_name("工商照面")
KEYWORD_PID = _pid_by_name("关键词查询企业")
TAG_PID = _pid_by_name("企业标签")
SUPPLEMENT_IDS: Tuple[str, ...] = tuple(
    product_id
    for product_id, route in ROUTES.items()
    if route.get("topic") == "supplement"
)
SUPPLEMENT_RULES: Dict[str, Tuple[Optional[str], str]] = {}
if FACE_PID:
    SUPPLEMENT_RULES["企业行业（国标）"] = (FACE_PID, "industry.firstIndustry")
    SUPPLEMENT_RULES["企业简介"] = (FACE_PID, "business")
if TAG_PID:
    SUPPLEMENT_RULES["企业产品标签"] = (TAG_PID, "businessTags")
BASE_PIDS = tuple(
    product_id for product_id, route in ROUTES.items() if route.get("topic") in BASE_TOPICS
)
TOPIC_PIDS = tuple(
    product_id
    for product_id, route in ROUTES.items()
    if route.get("topic") not in BASE_TOPICS
)
CHAINS: Dict[str, Dict[str, Any]] = {
    product_id: route["chain"]
    for product_id, route in ROUTES.items()
    if isinstance(route.get("chain"), dict)
}
PARENT_PRODUCT = {
    product_id: chain.get("parent_product_id") for product_id, chain in CHAINS.items()
}


# ---------------------------------------------------------------------------
# Small helpers.
# ---------------------------------------------------------------------------


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _hash_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256(
        "\x1f".join(str(part) for part in parts).encode("utf-8")
    ).hexdigest()
    return prefix + digest[:40]


def _clean_text(value: Any, limit: int = 300) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    return text[:limit]


def _norm_name(value: Optional[str]) -> str:
    return re.sub(r"\s+", "", (value or "")).casefold()


def _clean_code(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    code = re.sub(r"\s+", "", value).upper()
    if re.fullmatch(r"[0-9A-HJ-NP-RT-UW-Y]{18}", code):
        return code
    return None


def _clean_name_id(value: Any) -> Optional[str]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
    else:
        return None
    if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", text):
        return text
    return None


def _former_names(value: Any) -> List[str]:
    names: List[str] = []
    if isinstance(value, str):
        names.append(value)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, str):
                names.append(item)
            elif isinstance(item, dict):
                for key in ("formerName", "name", "formerNames"):
                    if isinstance(item.get(key), str):
                        names.append(item[key])
                        break
    elif isinstance(value, dict):
        for key in ("formerName", "name"):
            candidate = value.get(key)
            if isinstance(candidate, str):
                names.append(candidate)
    return names


def _row_value(row: Any, key: str, index: int = 0) -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        pass
    try:
        return row[index]
    except (IndexError, TypeError):
        return None


def _json_load_body(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return value if isinstance(value, dict) else {}
    return {}


def _field_names(route: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    fields = (route.get("inputs") or {}).get("fields") or []
    return {
        field.get("name"): field
        for field in fields
        if isinstance(field, dict) and isinstance(field.get("name"), str)
    }


def _canonical_params(params: Dict[str, Any]) -> str:
    try:
        return pack(params)
    except Exception:
        return json.dumps(params, sort_keys=True, ensure_ascii=False, default=str)


class _Period:
    __slots__ = ("start_month", "end_month", "cutoff", "months", "years", "months_param")

    def __init__(
        self,
        start_month: str,
        end_month: str,
        cutoff: date,
        months: List[str],
    ) -> None:
        self.start_month = start_month
        self.end_month = end_month
        self.cutoff = cutoff
        self.months = months
        self.years = sorted({month[:4] for month in months})
        self.months_param = [month.replace("-", "") for month in months]

    @property
    def window_start(self) -> date:
        year, month = (int(part) for part in self.start_month.split("-"))
        return date(year, month, 1)

    @property
    def business_end(self) -> date:
        year, month = (int(part) for part in self.end_month.split("-"))
        end_of_month = date(year, month, monthrange(year, month)[1])
        return min(end_of_month, self.cutoff)


class _Budget:
    def __init__(self, limits: Dict[str, Any]) -> None:
        def _int(key: str, default: int, minimum: int, maximum: int) -> int:
            try:
                value = int(limits.get(key, default))
            except (TypeError, ValueError):
                value = default
            return max(minimum, min(maximum, value))

        self.max_calls = _int("max_calls", 1, 1, 100_000)
        self.max_page_size = _int("max_page_size", 20, 1, 200)
        self.max_pages_per_view = _int("max_pages_per_view", 1, 1, 10_000)
        self.max_child_keys = _int("max_child_keys", 1, 1, 10_000)
        self.max_targets = _int("max_targets", 1, 1, 100_000)
        self.calls = 0
        self.pages: Dict[Tuple[str, str], int] = {}
        self.stopped = False
        self.partial = False

    def take_call(self) -> bool:
        if self.stopped or self.calls >= self.max_calls:
            self.stopped = True
            self.partial = True
            return False
        self.calls += 1
        return True

    def take_page(self, product_id: str, target_key: str) -> bool:
        key = (product_id, target_key)
        used = self.pages.get(key, 0)
        if used >= self.max_pages_per_view:
            self.partial = True
            return False
        self.pages[key] = used + 1
        return True


# ---------------------------------------------------------------------------
# Collector.
# ---------------------------------------------------------------------------


class _Collector:
    def __init__(
        self,
        store: Any,
        tenant: str,
        plan: Dict[str, Any],
        snapshot_id: str,
        fetch: Callable[[str, Dict[str, Any]], Tuple[Dict[str, Any], str]],
    ) -> None:
        self.store = store
        self.tenant = tenant
        self.plan = plan if isinstance(plan, dict) else {}
        self.snapshot_id = snapshot_id
        self.fetch = fetch
        self.args = _as_dict(self.plan.get("args"))
        self.scope_plan = self.plan["scope_plan"]
        self.stage = self.scope_plan["stage"]
        self.limits = self.scope_plan["limits"]
        self.budget = _Budget(self.limits)
        estimate = self.scope_plan["estimate"]
        try:
            estimate_max_calls = int(estimate.get("max_calls"))
        except (TypeError, ValueError):
            estimate_max_calls = 0
        if estimate_max_calls > 0:
            self.budget.max_calls = min(self.budget.max_calls, estimate_max_calls)
        self.parent_sid: Optional[str] = None
        self.snapshot_parent: Optional[str] = None
        self.roster_id: Optional[str] = None
        self.snapshot_roster: Optional[str] = None

        self.pending: List[Tuple[str, str, Dict[str, Any]]] = []
        self.partial = False
        self.failures: List[str] = []
        self.last_reason: Optional[str] = None

        self.identities: Dict[str, Dict[str, Any]] = {}
        self.candidates: Dict[str, Dict[str, Any]] = {}
        self.subjects: Dict[str, Dict[str, Any]] = {}
        self.coverage: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.coverage_keys: Dict[Tuple[str, str], str] = {}
        self.inherited_coverage: Set[Tuple[str, str]] = set()
        self.source_cache: Dict[str, Dict[str, Any]] = {}
        self.face_bodies: Dict[str, Dict[str, Any]] = {}
        self.face_cache: Dict[
            Tuple[str, str], Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]]
        ] = {}
        self.keyword_cache: Dict[
            Tuple[str, int], Dict[str, Any]
        ] = {}
        self.product_calls: Dict[str, int] = {}
        self.product_caps: Dict[str, int] = {}
        for item in estimate.get("products") or []:
            if not isinstance(item, dict):
                continue
            product_id = item.get("product_id")
            max_calls = item.get("max_calls")
            if isinstance(product_id, str) and type(max_calls) is int and max_calls > 0:
                self.product_caps[product_id] = max_calls

    # -- low level persistence ------------------------------------------------

    def _queue(self, view: str, key: str, record: Dict[str, Any]) -> None:
        self.pending.append((view, key, record))

    def _flush(self) -> None:
        if not self.pending:
            return
        items = self.pending
        self.pending = []
        with self.store.tx(write=True) as db:
            for view, key, record in items:
                self.store.put(db, self.snapshot_id, view, key, record)

    def _read_records(self, view: str) -> List[Tuple[str, Dict[str, Any]]]:
        with self.store.tx() as db:
            return list(self.store.records(db, self.snapshot_id, view))

    def _open_snapshot(self) -> None:
        with self.store.tx() as db:
            row = db.execute(
                "SELECT id, tenant, roster_id, parent_id, sealed FROM snapshots WHERE id=?",
                (self.snapshot_id,),
            ).fetchone()
        if row is None:
            raise Fault("snapshot_not_found", "snapshot is not registered")
        if _row_value(row, "tenant") != self.tenant:
            raise Fault("snapshot_not_found", "snapshot is not registered for this tenant")
        if _row_value(row, "sealed"):
            raise Fault("conflict", "snapshot is already sealed")
        self.snapshot_roster = _row_value(row, "roster_id")

        self.roster_id = (
            self.plan.get("roster_id")
            or self.scope_plan.get("roster_id")
            or self.args.get("roster_id")
            or self.snapshot_roster
        )
        if not self.roster_id:
            raise Fault("roster_incomplete", "plan has no roster reference")

        if self.stage in ("resolution", "topics"):
            actual_parent = _row_value(row, "parent_id")
            candidates = [
                self.args.get("snapshot_id"),
                self.plan.get("snapshot_id"),
                self.scope_plan.get("snapshot_id"),
            ]
            if isinstance(actual_parent, str) and actual_parent in candidates:
                self.parent_sid = actual_parent
            else:
                self.parent_sid = next(
                    (candidate for candidate in candidates if isinstance(candidate, str)),
                    None,
                )
            self.snapshot_parent = (
                actual_parent if isinstance(actual_parent, str) else self.parent_sid
            )
            if not self.parent_sid:
                raise Fault("invalid_argument", "plan does not reference a source snapshot")
            with self.store.tx() as db:
                self.store.require_snapshot(db, self.tenant, self.parent_sid)
        else:
            self.parent_sid = None
            self.snapshot_parent = (
                _row_value(row, "parent_id")
                if isinstance(_row_value(row, "parent_id"), str)
                else None
            )

        if self.snapshot_roster and self.roster_id and self.snapshot_roster != self.roster_id:
            raise Fault("conflict", "snapshot belongs to a different roster")

    def _load_state(self, views: Sequence[str]) -> None:
        for view in views:
            for key, record in self._read_records(view):
                self._index_record(view, key, record)

    def _index_record(self, view: str, key: str, record: Dict[str, Any]) -> None:
        if view == IDENTITY_VIEW:
            row_id = record.get("row_id")
            if isinstance(row_id, str):
                self.identities[row_id] = record
        elif view == CANDIDATE_VIEW:
            candidate_id = record.get("candidate_id")
            if isinstance(candidate_id, str):
                self.candidates[candidate_id] = record
        elif view == SUBJECT_VIEW:
            subject_id = record.get("subject_id")
            if isinstance(subject_id, str):
                self.subjects[subject_id] = record
        elif view == COVERAGE_VIEW:
            subject_id = record.get("subject_id")
            product_id = record.get("product_id")
            if isinstance(subject_id, str) and isinstance(product_id, str):
                self.coverage[(subject_id, product_id)] = record
                self.coverage_keys[(subject_id, product_id)] = key
                self.inherited_coverage.add((subject_id, product_id))
        elif view == SOURCE_VIEW:
            self.source_cache[key] = record

    # -- roster ---------------------------------------------------------------

    def _load_roster_rows(self) -> List[Dict[str, Any]]:
        with self.store.tx() as db:
            roster = db.execute(
                "SELECT id, tenant FROM rosters WHERE id=?", (self.roster_id,)
            ).fetchone()
            if roster is None or _row_value(roster, "tenant") != self.tenant:
                raise Fault("roster_incomplete", "roster is not available for this tenant")
            raw_rows = db.execute(
                "SELECT row_id, body FROM roster_rows WHERE roster_id=?",
                (self.roster_id,),
            ).fetchall()
        rows: List[Dict[str, Any]] = []
        for raw in raw_rows:
            body = _json_load_body(_row_value(raw, "body"))
            row_id = _row_value(raw, "row_id") or body.get("row_id")
            if not isinstance(row_id, str) or not row_id:
                continue
            merged = dict(body)
            merged["row_id"] = row_id
            rows.append(merged)
        if not rows:
            raise Fault("roster_empty", "roster contains no rows")
        rows.sort(
            key=lambda row: (
                int(row.get("source_row_number") or 10_000_001),
                row["row_id"],
            )
        )
        return rows

    # -- fetch plumbing -------------------------------------------------------

    def _call(
        self, product_id: str, params: Dict[str, Any], page_key: str
    ) -> Optional[Tuple[Dict[str, Any], str]]:
        self.last_reason = None
        cap = self.product_caps.get(product_id)
        if cap is not None and self.product_calls.get(product_id, 0) >= cap:
            self.last_reason = "product_call_limit"
            self.partial = True
            return None
        if not self.budget.take_page(product_id, page_key):
            self.last_reason = "page_limited"
            self.partial = True
            return None
        if not self.budget.take_call():
            self.last_reason = "budget_exhausted"
            return None
        self.product_calls[product_id] = self.product_calls.get(product_id, 0) + 1
        try:
            result = self.fetch(product_id, dict(params))
        except Fault as exc:
            code = getattr(exc, "code", None) or "upstream_unavailable"
            if code in ("billing_unknown", "cancelled", "configuration_error"):
                raise
            self.last_reason = str(code)
            self.failures.append(str(code))
            self.partial = True
            if code == "limit_exceeded":
                self.budget.stopped = True
            return None
        except Exception:
            self.last_reason = "internal_error"
            self.failures.append("internal_error")
            self.partial = True
            return None
        if not isinstance(result, tuple) or len(result) != 2 or not isinstance(result[0], dict):
            self.last_reason = "internal_error"
            self.failures.append("internal_error")
            self.partial = True
            return None
        data, acquired_at = result
        acquired = acquired_at if isinstance(acquired_at, str) and acquired_at else now()
        return data, acquired

    def _page_size(self, route: Dict[str, Any]) -> int:
        fields = _field_names(route)
        field = fields.get("pageSize") or {}
        default = field.get("default") or 20
        maximum = field.get("max") or 50
        try:
            default_int = int(default)
        except (TypeError, ValueError):
            default_int = 20
        try:
            maximum_int = int(maximum)
        except (TypeError, ValueError):
            maximum_int = 50
        return max(1, min(self.budget.max_page_size, default_int, maximum_int))

    def _validate_params(self, product_id: str, params: Dict[str, Any]) -> bool:
        route = ROUTES.get(product_id)
        if not route:
            return False
        fields = _field_names(route)
        for key in params:
            if key not in fields:
                return False
        for name, field in fields.items():
            if field.get("required") and name not in params:
                return False
        for name, value in params.items():
            field = fields.get(name) or {}
            declared = field.get("type")
            if declared == "integer":
                if isinstance(value, bool):
                    return False
                if isinstance(value, int):
                    number = value
                elif isinstance(value, str) and value.strip().lstrip("-").isdigit():
                    number = int(value)
                else:
                    return False
                maximum = field.get("max")
                if maximum is not None and number > int(maximum):
                    return False
            elif declared == "string":
                if not isinstance(value, str):
                    return False
            enum = field.get("enum")
            if enum and value not in enum:
                return False
        return True

    def _unit_tag(self, product_id: str, params: Dict[str, Any]) -> str:
        if isinstance(params.get("statsMonth"), str):
            return re.sub(r"[^0-9]", "", params["statsMonth"]) or "month"
        if isinstance(params.get("arYear"), str):
            return re.sub(r"[^0-9]", "", params["arYear"]) or "year"
        if product_id in CHAINS:
            return "k" + hashlib.sha256(
                _canonical_params(params).encode("utf-8")
            ).hexdigest()[:24]
        return ""

    def _src_key(self, subject_id: str, product_id: str, params: Dict[str, Any]) -> str:
        base = stable_id("src", subject_id, product_id)
        tag = self._unit_tag(product_id, params)
        return base if not tag else f"{base}:{tag}"

    def _store_source(
        self,
        *,
        subject_id: str,
        product_id: str,
        params: Dict[str, Any],
        pages: List[Dict[str, Any]],
        next_page: int,
        complete: bool,
        acquired_at: str,
        page_size: int,
        page_acquired_at: Optional[List[str]] = None,
    ) -> str:
        key = self._src_key(subject_id, product_id, params)
        body = {
            "subject_id": subject_id,
            "product_id": product_id,
            "params": dict(params),
            "pages": pages,
            "page_acquired_at": list(page_acquired_at or []),
            "next_page": next_page,
            "complete": bool(complete),
            "partial": not complete,
            "acquired_at": acquired_at,
            "page_size": page_size,
        }
        self.source_cache[key] = body
        self._queue(SOURCE_VIEW, key, body)
        return key

    def _ensure_unit(
        self,
        subject_id: str,
        product_id: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        route = ROUTES.get(product_id)
        if route is None or not self._validate_params(product_id, params):
            return {
                "ok": False,
                "complete": False,
                "reused": False,
                "pages": [],
                "params": dict(params),
                "acquired_at": now(),
                "reason": "invalid_request_contract",
            }
        key = self._src_key(subject_id, product_id, params)
        existing = self.source_cache.get(key)
        if existing and existing.get("complete"):
            return {
                "ok": True,
                "complete": True,
                "reused": True,
                "pages": list(existing.get("pages") or []),
                "params": dict(existing.get("params") or params),
                "acquired_at": existing.get("acquired_at") or now(),
                "reason": None,
            }

        pages: List[Dict[str, Any]] = list(existing.get("pages") or []) if existing else []
        page_times: List[str] = (
            list(existing.get("page_acquired_at") or []) if existing else []
        )
        next_page = int(existing.get("next_page") or 1) if existing else 1
        acquired_at = existing.get("acquired_at") if existing else None
        complete = False
        new_pages = 0
        reason: Optional[str] = None
        fields = _field_names(route)
        paged = "pageIndex" in fields and "pageSize" in fields
        page_size = self._page_size(route) if paged else 0
        start_page = next_page
        if paged and existing:
            try:
                stored_page_size = int(existing.get("page_size") or 0)
            except (TypeError, ValueError):
                stored_page_size = 0
            if stored_page_size and stored_page_size <= page_size:
                page_size = stored_page_size
            elif stored_page_size:
                # A larger page size from an earlier plan would misalign offsets
                # or exceed the current limit, so restart cleanly.
                pages = []
                page_times = []
                next_page = 1
                start_page = 1

        if not paged:
            got = self._call(
                product_id, params, f"{subject_id}:{self._unit_tag(product_id, params)}"
            )
            if got is not None:
                pages.append(got[0])
                page_times.append(got[1])
                acquired_at = got[1]
                complete = True
                new_pages = 1
                self._store_source(
                    subject_id=subject_id,
                    product_id=product_id,
                    params=params,
                    pages=pages,
                    next_page=start_page + new_pages,
                    complete=True,
                    acquired_at=acquired_at or now(),
                    page_size=0,
                    page_acquired_at=page_times,
                )
                self._flush()
            else:
                reason = self.last_reason
        else:
            page = start_page
            while True:
                page_params = dict(params)
                page_params["pageIndex"] = page
                page_params["pageSize"] = page_size
                got = self._call(
                    product_id,
                    page_params,
                    f"{subject_id}:{self._unit_tag(product_id, params)}",
                )
                if got is None:
                    reason = self.last_reason
                    break
                pages.append(got[0])
                page_times.append(got[1])
                acquired_at = got[1]
                new_pages += 1
                page_records = len(evid.extract_records(route, got[0]))
                total = evid.page_total(route, got[0])
                running = sum(len(evid.extract_records(route, data)) for data in pages)
                terminal = False
                if total is not None:
                    if running >= total:
                        terminal = True
                        complete = True
                    elif page_records == 0:
                        terminal = True
                        complete = False
                        reason = "total_not_reached"
                    else:
                        terminal = False
                elif page_records < page_size:
                    terminal = True
                    complete = True
                # Persist each acquired page before the next paid call.
                self._store_source(
                    subject_id=subject_id,
                    product_id=product_id,
                    params=params,
                    pages=pages,
                    next_page=page + 1,
                    complete=complete,
                    acquired_at=acquired_at or now(),
                    page_size=page_size,
                    page_acquired_at=page_times,
                )
                self._flush()
                if terminal:
                    break
                page += 1

        if pages:
            self._store_source(
                subject_id=subject_id,
                product_id=product_id,
                params=params,
                pages=pages,
                next_page=start_page + new_pages,
                complete=complete,
                acquired_at=acquired_at or now(),
                page_size=page_size,
                page_acquired_at=page_times,
            )
            self._flush()
        return {
            "ok": bool(pages),
            "complete": complete,
            "reused": bool(existing) and new_pages == 0,
            "pages": pages,
            "params": dict(params),
            "acquired_at": acquired_at or now(),
            "reason": reason,
        }

    # -- projection -----------------------------------------------------------

    def _project_body(
        self,
        subject_id: str,
        product_id: str,
        body: Dict[str, Any],
        *,
        period: Optional[_Period] = None,
        flags: Optional[Sequence[str]] = None,
        record_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        route = ROUTES[product_id]
        params = _as_dict(body.get("params"))
        body_acquired_at = body.get("acquired_at") or now()
        page_times = body.get("page_acquired_at")
        if not isinstance(page_times, list):
            page_times = []
        stats: Dict[str, Any] = {
            "included": 0,
            "raw": 0,
            "excluded": 0,
            "undated": 0,
            "duplicates": 0,
            "late": 0,
            "late_publish": 0,
        }
        item_flags = [
            flag
            for flag in (flags or ())
            if flag
            in {
                "partial_page",
                "page_limited",
                "unit_inferred",
                "time_inferred",
                "source_duplicate",
                "parent_key_unverified",
            }
        ]
        if not body.get("complete") and "page_limited" not in item_flags:
            item_flags.append("page_limited")
        seen: Dict[str, int] = {}
        for page_index, data in enumerate(body.get("pages") or []):
            acquired_at = (
                page_times[page_index]
                if page_index < len(page_times) and isinstance(page_times[page_index], str)
                else body_acquired_at
            )
            late = bool(
                period
                and isinstance(acquired_at, str)
                and acquired_at[:10] > period.cutoff.isoformat()
            )
            page = evid.record_stats(
                route,
                data,
                request_params=params,
                allowed_months=period.months if period else None,
                allowed_years=period.years if period else None,
                window_start=period.window_start if period else None,
                window_end=period.business_end if period else None,
                cutoff=period.cutoff if period else None,
            )
            stats["raw"] += page["total"]
            stats["excluded"] += len(page["excluded"])
            stats["undated"] += page["undated"]
            stats["late_publish"] += page.get("late_publish", 0)
            for record_path, index, record in page["records"]:
                record_id = evid.source_record_id(route, record_path, record, index)
                if record_prefix:
                    record_id = f"{record_prefix}:{record_id}"[:200]
                occurrence = seen.get(record_id, 0) + 1
                seen[record_id] = occurrence
                flags_for_record = list(item_flags)
                if occurrence > 1:
                    stats["duplicates"] += 1
                    record_id = f"{record_id}#{occurrence}"[:200]
                    if "source_duplicate" not in flags_for_record:
                        flags_for_record.append("source_duplicate")
                records = evid.build_record_evidence(
                    route,
                    record_path,
                    record,
                    entity_id=subject_id,
                    acquired_at=acquired_at,
                    record_id=record_id,
                    request_params=params,
                    flags=flags_for_record,
                    index=index,
                )
                if late:
                    stats["late"] += 1
                for item in records:
                    if late:
                        existing_note = item.get("note")
                        marker = "late_snapshot: acquired after observation_cutoff"
                        item["note"] = (
                            marker + ("; " + existing_note if existing_note else "")
                        )[:500]
                        flags = list(item.get("quality_flags") or [])
                        if "time_inferred" not in flags:
                            flags.append("time_inferred")
                        item["quality_flags"] = flags[:10]
                    item_key = item.get("evidence_id")
                    if isinstance(item_key, str):
                        self._queue(EVIDENCE_VIEW, item_key, item)
                stats["included"] += 1
            root_fields = evid.root_level_fields(route)
            if root_fields:
                root_id = f"root:{page_index}"
                if record_prefix:
                    root_id = f"{record_prefix}:{root_id}"[:200]
                for item in evid.build_root_evidence(
                    route,
                    data,
                    entity_id=subject_id,
                    acquired_at=acquired_at,
                    record_id=root_id,
                    request_params=params,
                    flags=item_flags,
                ):
                    if late:
                        existing_note = item.get("note")
                        marker = "late_snapshot: acquired after observation_cutoff"
                        item["note"] = (
                            marker + ("; " + existing_note if existing_note else "")
                        )[:500]
                        flags = list(item.get("quality_flags") or [])
                        if "time_inferred" not in flags:
                            flags.append("time_inferred")
                        item["quality_flags"] = flags[:10]
                    item_key = item.get("evidence_id")
                    if isinstance(item_key, str):
                        self._queue(EVIDENCE_VIEW, item_key, item)
        return stats

    # -- coverage -------------------------------------------------------------

    def _set_coverage(
        self,
        subject_id: str,
        product_id: str,
        state: str,
        record_count: int,
        eligible: bool,
        reason: Optional[str] = None,
    ) -> None:
        route = ROUTES.get(product_id) or {}
        key = (subject_id, product_id)
        existing = self.coverage.get(key)
        coverage_id = (
            existing.get("coverage_id")
            if isinstance(existing, dict) and isinstance(existing.get("coverage_id"), str)
            else _hash_id("cov_", subject_id, product_id)
        )
        if state not in COVERAGE_STATES:
            state = "error"
        record_count = int(record_count)
        if state == "covered":
            record_count = max(1, record_count)
        elif state in ("empty", "not_queried", "not_applicable"):
            record_count = 0
        if state in ("not_queried", "not_applicable", "error"):
            eligible = False
        record: Dict[str, Any] = {
            "coverage_id": coverage_id,
            "subject_id": subject_id,
            "product_id": product_id,
            "topic": route.get("topic"),
            "state": state,
            "record_count": max(0, min(10_000_000, record_count)),
            "denominator_eligible": bool(eligible),
        }
        if reason:
            record["reason"] = str(reason)[:300]
        self.coverage[key] = record
        self.coverage_keys[key] = coverage_id
        self._queue(COVERAGE_VIEW, coverage_id, record)

    def _finalize_coverage(
        self, subject_id: str, product_id: str, units: List[Dict[str, Any]]
    ) -> bool:
        if not units:
            self._set_coverage(
                subject_id,
                product_id,
                "not_queried",
                0,
                False,
                "no_period_units",
            )
            return False
        ok_units = [unit for unit in units if unit.get("ok")]
        failed_units = [unit for unit in units if not unit.get("ok")]
        included = sum(int(unit.get("included") or 0) for unit in ok_units)
        excluded = sum(int(unit.get("excluded") or 0) for unit in ok_units)
        undated = sum(int(unit.get("undated") or 0) for unit in ok_units)
        duplicates = sum(int(unit.get("duplicates") or 0) for unit in ok_units)
        late = sum(int(unit.get("late") or 0) for unit in ok_units)
        late_publish = sum(int(unit.get("late_publish") or 0) for unit in ok_units)
        page_partial = any(not unit.get("complete") for unit in ok_units)
        reasons: List[str] = []
        if page_partial:
            reasons.append("page_limited")
        if excluded:
            reasons.append(f"filtered_out_of_period:{excluded}")
        if undated:
            reasons.append("undated_records_included")
        if duplicates:
            reasons.append("source_duplicate")
        if late:
            reasons.append(f"late_snapshot:{late}")
        if late_publish:
            reasons.append(f"excluded_publish_after_cutoff:{late_publish}")
        if failed_units:
            failure_codes = sorted(
                {
                    str(unit.get("reason") or "upstream_unavailable")
                    for unit in failed_units
                }
            )
            reasons.append("partial_fetch:" + ",".join(failure_codes)[:160])
            failed_labels = [
                str(unit.get("label"))
                for unit in failed_units
                if isinstance(unit.get("label"), str) and unit.get("label")
            ]
            if failed_labels:
                reasons.append("uncovered:" + ",".join(failed_labels)[:120])

        if not ok_units:
            codes = {
                str(unit.get("reason") or "upstream_unavailable")
                for unit in failed_units
            }
            if codes and codes <= {
                "budget_exhausted",
                "product_call_limit",
                "page_limited",
            }:
                self._set_coverage(
                    subject_id,
                    product_id,
                    "not_queried",
                    0,
                    False,
                    ",".join(sorted(codes)),
                )
            else:
                code = sorted(codes)[0] if codes else "upstream_unavailable"
                self._set_coverage(
                    subject_id,
                    product_id,
                    "error",
                    0,
                    False,
                    f"fetch_failed:{code}"[:300],
                )
            return False

        if included > 0:
            self._set_coverage(
                subject_id,
                product_id,
                "covered",
                included,
                True,
                "; ".join(reasons) or None,
            )
        elif late_publish:
            reasons.append("not_known_at_observation_cutoff")
            self._set_coverage(
                subject_id,
                product_id,
                "not_queried",
                0,
                False,
                "; ".join(reasons)[:300],
            )
        else:
            conclusive = not page_partial and not failed_units
            self._set_coverage(
                subject_id,
                product_id,
                "empty",
                0,
                conclusive,
                "; ".join(reasons) or ("filtered_out_of_period" if excluded else None),
            )
        return not page_partial and not failed_units

    # -- subject helpers ------------------------------------------------------

    def _subject_id_for(
        self, code: Optional[str], name_id: Optional[str], name: Optional[str]
    ) -> str:
        if code:
            return _hash_id("subj_", "code", code)
        if name_id:
            return _hash_id("subj_", "nameid", name_id)
        return _hash_id("subj_", "name", _norm_name(name))

    def _find_subject(
        self,
        code: Optional[str],
        name_id: Optional[str],
        name: Optional[str],
        exclude: Optional[str] = None,
    ) -> Optional[str]:
        direct = self._subject_id_for(code, name_id, name)
        if direct in self.subjects and direct != exclude:
            return direct
        normalized = _norm_name(name)
        for subject_id, record in self.subjects.items():
            if subject_id == exclude:
                continue
            if code and record.get("social_credit_code") == code:
                return subject_id
            if name_id and record.get("name_id") == name_id:
                return subject_id
            if normalized and _norm_name(record.get("enterprise_name")) == normalized:
                # Merge by name only when the existing subject has no stronger
                # identifier that contradicts the incoming one.
                if code and record.get("social_credit_code"):
                    continue
                if name_id and record.get("name_id"):
                    continue
                return subject_id
        return None

    def _make_subject(
        self,
        *,
        code: Optional[str],
        name_id: Optional[str],
        name: str,
        row_id: str,
    ) -> Tuple[str, Dict[str, Any], bool]:
        existing_id = self._find_subject(code, name_id, name)
        duplicate = False
        if existing_id is None:
            subject_id = self._subject_id_for(code, name_id, name)
            record: Dict[str, Any] = {
                "subject_id": subject_id,
                "enterprise_name": name[:300],
                "resolution_status": "confirmed",
                "source_row_ids": [row_id],
                "duplicate_row_count": 0,
                "cohort_keys": [],
            }
            if code:
                record["social_credit_code"] = code
            if name_id:
                record["name_id"] = name_id
        else:
            subject_id = existing_id
            record = self.subjects[subject_id]
            rows = list(record.get("source_row_ids") or [])
            if row_id not in rows:
                duplicate = bool(rows)
                rows.append(row_id)
            record["source_row_ids"] = rows[:MAX_SUBJECT_ROWS]
            record["duplicate_row_count"] = max(0, min(1000, len(rows) - 1))
            record["resolution_status"] = "deduplicated" if rows else "confirmed"
            if code and not record.get("social_credit_code"):
                record["social_credit_code"] = code
            if name_id and not record.get("name_id"):
                record["name_id"] = name_id
            if not record.get("enterprise_name"):
                record["enterprise_name"] = name[:300]
        self.subjects[subject_id] = record
        self._queue(SUBJECT_VIEW, subject_id, record)
        return subject_id, record, duplicate

    def _identity_base(
        self,
        row: Dict[str, Any],
        *,
        mapping_status: str,
        subject_id: Optional[str],
        candidate_count: int,
        canonical_name: Optional[str] = None,
        canonical_code: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        row_id = row.get("row_id")
        if not isinstance(row_id, str):
            return None
        name = _clean_text(row.get("enterprise_name")) or canonical_name
        code = _clean_code(row.get("social_credit_code")) or canonical_code
        if not name and not code:
            return None
        record: Dict[str, Any] = {
            "row_id": row_id,
            "mapping_status": mapping_status,
            "candidate_count": max(0, min(MAX_CANDIDATES, candidate_count)),
        }
        if name:
            record["enterprise_name"] = name
        if code:
            record["social_credit_code"] = code
        row_number = row.get("source_row_number")
        if isinstance(row_number, int) and 1 <= row_number <= 10_000_000:
            record["source_row_number"] = row_number
        if subject_id:
            record["subject_id"] = subject_id
        self.identities[row_id] = record
        self._queue(IDENTITY_VIEW, row_id, record)
        return record

    # -- base stage -----------------------------------------------------------

    def _base_scope(self) -> List[str]:
        scope: List[str] = []
        for product_id in self.scope_plan["scope"]["product_ids"]:
            route = ROUTES.get(product_id)
            if route and route.get("topic") in BASE_TOPICS:
                scope.append(product_id)
        if not any((ROUTES[p].get("topic") == "base") for p in scope):
            for product_id in BASE_PIDS:
                if ROUTES[product_id].get("topic") == "base":
                    scope.append(product_id)
        ordered: List[str] = []
        if FACE_PID:
            ordered.append(FACE_PID)
        for product_id in scope:
            if product_id in (FACE_PID, KEYWORD_PID):
                continue
            if product_id not in ordered:
                ordered.append(product_id)
        return ordered

    def _source_has_field(
        self, subject_id: str, product_id: Optional[str], path: str
    ) -> bool:
        if not product_id or product_id not in ROUTES:
            return False
        route = ROUTES[product_id]
        for body in self._source_units(subject_id, product_id):
            for data in body.get("pages") or []:
                for _record_path, _index, record in evid.extract_records(route, data):
                    value = evid.get_path(record, path)
                    if value is evid.MISSING or value is None:
                        continue
                    if isinstance(value, str):
                        if value.strip() not in ("", "-", "--", "未披露", "暂无", "无"):
                            return True
                    elif isinstance(value, (list, dict)):
                        if value:
                            return True
                    else:
                        return True
        return False

    def _supplement_needed(self, subject_id: str, product_id: str) -> bool:
        name = str(ROUTES.get(product_id, {}).get("name") or "")
        rule = SUPPLEMENT_RULES.get(name)
        if rule is None:
            return True
        prerequisite, path = rule
        if not self._source_units(subject_id, prerequisite):
            return True
        return not self._source_has_field(subject_id, prerequisite, path)

    def _subject_params(
        self, subject: Dict[str, Any], route: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        fields = _field_names(route)
        if "matchKeyword" not in fields:
            return {}
        keyword_type = fields.get("matchKeyword")
        type_field = fields.get("keywordType") or {}
        allowed_types = type_field.get("enum") or []
        name_id = subject.get("name_id")
        code = subject.get("social_credit_code")
        name = subject.get("enterprise_name")
        selected: Optional[str] = None
        selected_type: Optional[str] = None
        if name_id and "nameId" in allowed_types:
            selected, selected_type = name_id, "nameId"
        elif code and "socialCreditCode" in allowed_types:
            selected, selected_type = code, "socialCreditCode"
        elif name and "name" in allowed_types:
            selected, selected_type = name, "name"
        if not selected:
            if keyword_type and keyword_type.get("required"):
                return None
            return {}
        params: Dict[str, Any] = {"matchKeyword": selected}
        if "keywordType" in fields:
            params["keywordType"] = selected_type
        return params

    def _units_for_subject(
        self, subject: Dict[str, Any], product_id: str, period: Optional[_Period]
    ) -> List[Dict[str, Any]]:
        route = ROUTES[product_id]
        fields = _field_names(route)
        base = self._subject_params(subject, route)
        if base is None:
            return []
        units: List[Dict[str, Any]] = []
        if "statsMonth" in fields:
            if not period:
                return []
            for month in period.months_param:
                params = dict(base)
                params["statsMonth"] = month
                units.append(params)
        elif "arYear" in fields:
            if not period:
                return []
            for year in period.years:
                params = dict(base)
                params["arYear"] = year
                units.append(params)
        else:
            units.append(dict(base))
        return units

    def _canonical_from_data(self, data: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(data, dict):
            return None
        name = _clean_text(data.get("name") or data.get("entName"))
        code = _clean_code(data.get("socialCreditCode"))
        name_id = _clean_name_id(data.get("nameId"))
        if not name and not code and not name_id:
            return None
        return {
            "name": name,
            "code": code,
            "name_id": name_id,
            "former_names": _former_names(data.get("formerNames")),
            "data": data,
        }

    def _match_reason(
        self, canonical: Dict[str, Any], name: Optional[str], code: Optional[str]
    ) -> str:
        if code and canonical.get("code") == code:
            return "social_credit_code"
        normalized = _norm_name(name)
        if normalized and _norm_name(canonical.get("name")) == normalized:
            return "exact_name"
        if normalized:
            for former in canonical.get("former_names") or []:
                if _norm_name(former) == normalized:
                    return "former_name"
        return "keyword_match"

    def _face_lookup(
        self, params: Dict[str, Any], page_key: str
    ) -> Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]]:
        if not FACE_PID or not self._validate_params(FACE_PID, params):
            return None
        cache_key = (
            str(params.get("matchKeyword") or ""),
            str(params.get("keywordType") or "name"),
        )
        if cache_key in self.face_cache:
            return self.face_cache[cache_key]
        got = self._call(FACE_PID, params, page_key)
        if got is None:
            self.face_cache[cache_key] = None
            return None
        canonical = self._canonical_from_data(got[0])
        if canonical is None:
            self.face_cache[cache_key] = None
            return None
        result = (got[0], got[1], canonical)
        self.face_cache[cache_key] = result
        return result

    def _candidate_from_result(
        self,
        row_id: str,
        name: Optional[str],
        code: Optional[str],
        result: Dict[str, Any],
        *,
        seed_reason: Optional[str] = None,
        face: Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]] = None,
    ) -> Optional[Dict[str, Any]]:
        catch = result.get("catchReason") if isinstance(result.get("catchReason"), dict) else {}
        raw_name = _clean_text(result.get("name") or result.get("entName"))
        raw_name_id = _clean_name_id(result.get("nameId"))
        raw_code = _clean_code(
            result.get("socialCreditCode") or catch.get("socialCreditCode")
        )
        canonical: Optional[Dict[str, Any]] = None
        acquired_at: Optional[str] = None
        if face is not None:
            canonical = face[2]
            acquired_at = face[1]
            raw_name = canonical.get("name") or raw_name
            raw_code = canonical.get("code") or raw_code
            raw_name_id = canonical.get("name_id") or raw_name_id
        if not raw_name or not raw_name_id:
            return None
        if canonical is None:
            canonical = {
                "name": raw_name,
                "code": raw_code,
                "name_id": raw_name_id,
                "former_names": _former_names(catch.get("formerNames")),
                "data": None,
            }
        reason = (
            self._match_reason(canonical, name, code)
            if face is not None
            else (seed_reason or "keyword_match")
        )
        candidate_id = _hash_id("cand_", row_id, raw_name_id)
        record: Dict[str, Any] = {
            "candidate_id": candidate_id,
            "row_id": row_id,
            "name_id": raw_name_id,
            "enterprise_name": (raw_name or canonical.get("name") or "")[:300],
            "match_reason": reason,
        }
        if raw_code:
            record["social_credit_code"] = raw_code
        return {
            "record": record,
            "canonical": canonical,
            "face": face,
            "acquired_at": acquired_at,
            "reason": reason,
        }

    def _collect_keyword_candidates(
        self,
        row: Dict[str, Any],
        name: str,
        code: Optional[str],
        seeds: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        if not KEYWORD_PID:
            return seeds, None
        route = ROUTES[KEYWORD_PID]
        fields = _field_names(route)
        page_size = self._page_size(route) if "pageSize" in fields else 10
        row_id = row["row_id"]
        candidates: List[Dict[str, Any]] = list(seeds)
        seen_name_ids: Set[str] = {
            candidate["record"]["name_id"] for candidate in candidates
        }
        seen_candidate_ids = {candidate["record"]["candidate_id"] for candidate in candidates}
        account = {"seen": 0, "total": None}
        page = 1
        reason: Optional[str] = None
        while True:
            params: Dict[str, Any] = {"matchKeyword": name}
            if "pageIndex" in fields:
                params["pageIndex"] = page
            if "pageSize" in fields:
                params["pageSize"] = page_size
            cache_key = (name, page)
            cached_page = self.keyword_cache.get(cache_key)
            if cached_page is not None:
                got = cached_page.get("got")
                if got is None:
                    reason = cached_page.get("reason") or "upstream_unavailable"
                    self.partial = True
                    break
            else:
                got = self._call(KEYWORD_PID, params, f"{row_id}:keyword")
                self.keyword_cache[cache_key] = {
                    "got": got,
                    "reason": self.last_reason if got is None else None,
                }
                if got is None:
                    reason = self.last_reason
                    break
            data = got[0]
            total = evid.page_total(route, data)
            if total is not None:
                account["total"] = total
            records = evid.extract_records(route, data)
            for _path, _index, result in records:
                if len(candidates) >= MAX_CANDIDATES:
                    self.partial = True
                    break
                name_id = _clean_name_id(result.get("nameId"))
                if not name_id or name_id in seen_name_ids:
                    continue
                seen_name_ids.add(name_id)
                candidate_id = _hash_id("cand_", row_id, name_id)
                if candidate_id in seen_candidate_ids:
                    continue
                face: Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]] = None
                cached = self.source_cache.get(stable_id("cand", candidate_id))
                if cached and isinstance(cached.get("data"), dict):
                    canonical = self._canonical_from_data(cached["data"])
                    if canonical:
                        cached_name_id = _clean_name_id(canonical.get("name_id"))
                        if not cached_name_id or cached_name_id == name_id:
                            face = (
                                cached["data"],
                                cached.get("acquired_at") or now(),
                                canonical,
                            )
                if face is None:
                    confirm_params = {"matchKeyword": name_id, "keywordType": "nameId"}
                    if self._validate_params(FACE_PID, confirm_params):
                        face = self._face_lookup(confirm_params, f"{candidate_id}:confirm")
                        if face is not None:
                            face_name_id = _clean_name_id(face[2].get("name_id"))
                            if face_name_id and face_name_id != name_id:
                                face = None
                        if face is not None:
                            body = {
                                "candidate_id": candidate_id,
                                "row_id": row_id,
                                "data": face[0],
                                "acquired_at": face[1],
                            }
                            self.source_cache[stable_id("cand", candidate_id)] = body
                            self._queue(SOURCE_VIEW, stable_id("cand", candidate_id), body)
                candidate = self._candidate_from_result(
                    row_id, name, code, result, face=face
                )
                if candidate is None:
                    continue
                if candidate["record"]["candidate_id"] not in seen_candidate_ids:
                    seen_candidate_ids.add(candidate["record"]["candidate_id"])
                    candidates.append(candidate)
            account["seen"] += len(records)
            if len(records) < page_size:
                break
            if total is not None and account["seen"] >= total:
                break
            if page >= self.budget.max_pages_per_view:
                self.partial = True
                reason = "page_limited"
                break
            page += 1
        return candidates, reason

    def _match_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row_id = row["row_id"]
        name = _clean_text(row.get("enterprise_name"))
        code = _clean_code(row.get("social_credit_code"))
        seeds: List[Dict[str, Any]] = []
        canonical: Optional[Dict[str, Any]] = None
        face: Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]] = None

        if code and FACE_PID:
            lookup = self._face_lookup(
                {"matchKeyword": code, "keywordType": "socialCreditCode"},
                f"{row_id}:face:code",
            )
            if lookup is not None and lookup[2].get("code") == code:
                canonical, face = lookup[2], lookup

        if canonical is None and name and FACE_PID:
            lookup = self._face_lookup(
                {"matchKeyword": name, "keywordType": "name"},
                f"{row_id}:face:name",
            )
            if lookup is not None:
                canonical0 = lookup[2]
                match_reason = self._match_reason(canonical0, name, code)
                if match_reason in EXACT_MATCH_REASONS:
                    canonical, face = canonical0, lookup
                else:
                    seed = self._candidate_from_result(
                        row_id, name, code, {}, seed_reason="keyword_match", face=lookup
                    )
                    if seed is not None:
                        seeds.append(seed)

        candidates: List[Dict[str, Any]] = []
        if canonical is None and name:
            candidates, keyword_reason = self._collect_keyword_candidates(
                row, name, code, seeds
            )
            exact = [
                candidate
                for candidate in candidates
                if candidate["reason"] in EXACT_MATCH_REASONS
                and candidate.get("face") is not None
            ]
            exact_ids = {candidate["record"]["name_id"] for candidate in exact}
            if len(exact_ids) == 1:
                chosen = next(
                    candidate for candidate in exact if candidate["record"]["name_id"]
                )
                canonical = chosen["canonical"]
                face = chosen["face"]
            elif keyword_reason:
                self.partial = True

        if canonical is not None and not canonical.get("name") and name:
            canonical["name"] = name
        if canonical is not None and not canonical.get("name"):
            canonical["name"] = canonical.get("code") or canonical.get("name_id") or name

        if canonical is not None and canonical.get("name"):
            subject_id, subject_record, duplicate = self._make_subject(
                code=canonical.get("code"),
                name_id=canonical.get("name_id"),
                name=canonical["name"],
                row_id=row_id,
            )
            if face is not None:
                params = (
                    {"matchKeyword": canonical["name_id"], "keywordType": "nameId"}
                    if canonical.get("name_id")
                    else (
                        {"matchKeyword": canonical["code"], "keywordType": "socialCreditCode"}
                        if canonical.get("code")
                        else {"matchKeyword": canonical["name"], "keywordType": "name"}
                    )
                )
                key = self._src_key(subject_id, FACE_PID, params)
                if key not in self.source_cache:
                    body = {
                        "subject_id": subject_id,
                        "product_id": FACE_PID,
                        "params": params,
                        "pages": [face[0]],
                        "page_acquired_at": [face[1]],
                        "next_page": 1,
                        "complete": True,
                        "partial": False,
                        "acquired_at": face[1],
                        "page_size": 0,
                    }
                    self.source_cache[key] = body
                    self._queue(SOURCE_VIEW, key, body)
                self.face_bodies[subject_id] = self.source_cache[key]
            identity = self._identity_base(
                row,
                mapping_status="duplicate" if duplicate else "matched",
                subject_id=subject_id,
                candidate_count=0,
                canonical_name=canonical.get("name"),
                canonical_code=canonical.get("code"),
            )
            return {
                "identity": identity,
                "candidates": candidates,
                "subject": subject_record,
                "face": face,
            }

        records: List[Dict[str, Any]] = []
        seen_ids: Set[str] = set()
        for candidate in candidates:
            record = candidate["record"]
            if record["candidate_id"] in seen_ids:
                continue
            seen_ids.add(record["candidate_id"])
            records.append(record)
            self.candidates[record["candidate_id"]] = record
            self._queue(CANDIDATE_VIEW, record["candidate_id"], record)
        status = "ambiguous" if records else "unmatched"
        identity = self._identity_base(
            row,
            mapping_status=status,
            subject_id=None,
            candidate_count=len(records),
        )
        return {"identity": identity, "candidates": records, "subject": None, "face": None}

    def _collect_base(self) -> bool:
        rows = self._load_roster_rows()
        scope = self._base_scope()
        complete = True
        attempted = 0
        for row in rows:
            if self.budget.stopped:
                complete = False
                break
            self._match_row(row)
            attempted += 1
            self._flush()
        if attempted < len(rows):
            complete = False

        subject_ids = list(self.subjects.keys())
        target_ids = subject_ids[: self.budget.max_targets]
        if len(subject_ids) > len(target_ids):
            complete = False
        for subject_id in subject_ids:
            if subject_id not in target_ids:
                for product_id in scope:
                    self._set_coverage(
                        subject_id,
                        product_id,
                        "not_queried",
                        0,
                        False,
                        "max_targets",
                    )
                continue
            subject = self.subjects.get(subject_id)
            if subject is None:
                continue
            for product_id in scope:
                if self.budget.stopped:
                    self._set_coverage(
                        subject_id,
                        product_id,
                        "not_queried",
                        0,
                        False,
                        "budget_exhausted",
                    )
                    complete = False
                    continue
                if product_id == FACE_PID:
                    body = self.face_bodies.get(subject_id)
                    face_reason: Optional[str] = None
                    if body is None:
                        params = self._subject_params(subject, ROUTES[FACE_PID])
                        if params is None:
                            self._set_coverage(
                                subject_id,
                                product_id,
                                "not_queried",
                                0,
                                False,
                                "no_identity_key",
                            )
                            complete = False
                            continue
                        result = self._ensure_unit(subject_id, product_id, params)
                        face_reason = result.get("reason")
                        body = self.source_cache.get(
                            self._src_key(subject_id, product_id, params)
                        )
                    if body is not None:
                        stats = self._project_body(subject_id, product_id, body)
                        done = self._finalize_coverage(
                            subject_id,
                            product_id,
                            [
                                {
                                    "ok": True,
                                    "complete": body.get("complete", True),
                                    "included": stats["included"],
                                    "excluded": stats["excluded"],
                                    "undated": stats["undated"],
                                    "duplicates": stats["duplicates"],
                                    "reason": None,
                                }
                            ],
                        )
                        complete = complete and done
                    else:
                        self._set_coverage(
                            subject_id,
                            product_id,
                            "not_queried"
                            if face_reason
                            in (
                                "budget_exhausted",
                                "product_call_limit",
                                "page_limited",
                            )
                            else "error",
                            0,
                            False,
                            face_reason or "fetch_failed",
                        )
                        complete = False
                    continue
                if product_id in SUPPLEMENT_IDS and not self._supplement_needed(
                    subject_id, product_id
                ):
                    self._set_coverage(
                        subject_id,
                        product_id,
                        "not_queried",
                        0,
                        False,
                        "base_fields_present",
                    )
                    continue
                params = self._subject_params(subject, ROUTES[product_id])
                if params is None:
                    self._set_coverage(
                        subject_id,
                        product_id,
                        "not_queried",
                        0,
                        False,
                        "no_identity_key",
                    )
                    complete = False
                    continue
                result = self._ensure_unit(subject_id, product_id, params)
                if result["ok"]:
                    body = self.source_cache.get(
                        self._src_key(subject_id, product_id, params)
                    ) or {
                        "params": params,
                        "pages": result["pages"],
                        "complete": result["complete"],
                        "acquired_at": result["acquired_at"],
                    }
                    stats = self._project_body(subject_id, product_id, body)
                    done = self._finalize_coverage(
                        subject_id,
                        product_id,
                        [
                            {
                                "ok": True,
                                "complete": result["complete"],
                                "included": stats["included"],
                                "excluded": stats["excluded"],
                                "undated": stats["undated"],
                                "duplicates": stats["duplicates"],
                                "reason": result["reason"],
                            }
                        ],
                    )
                    complete = complete and done
                else:
                    self._set_coverage(
                        subject_id,
                        product_id,
                        "not_queried"
                        if result["reason"] in (
                            "budget_exhausted",
                            "product_call_limit",
                            "page_limited",
                        )
                        else "error",
                        0,
                        False,
                        result["reason"] or "fetch_failed",
                    )
                    complete = False
                self._flush()
        self._flush()
        if self.budget.stopped or self.budget.partial or self.partial:
            complete = False
        return complete and not self.failures

    # -- resolution stage -----------------------------------------------------

    def _drop_subject_entity(self, subject_id: str) -> None:
        self._flush()
        with self.store.tx() as db:
            evidence_keys = [
                key
                for key, record in self.store.records(
                    db, self.snapshot_id, EVIDENCE_VIEW
                )
                if record.get("entity_id") == subject_id
            ]
        with self.store.tx(write=True) as db:
            for key in evidence_keys:
                self.store.drop(db, self.snapshot_id, EVIDENCE_VIEW, key)
            for (coverage_subject, product_id), coverage_id in list(
                self.coverage_keys.items()
            ):
                if coverage_subject == subject_id:
                    self.store.drop(db, self.snapshot_id, COVERAGE_VIEW, coverage_id)
                    self.coverage.pop((coverage_subject, product_id), None)
                    self.coverage_keys.pop((coverage_subject, product_id), None)
            self.store.drop(db, self.snapshot_id, SUBJECT_VIEW, subject_id)
        self.subjects.pop(subject_id, None)

    def _remove_row_from_subject(self, row_id: str) -> Optional[str]:
        for subject_id, record in list(self.subjects.items()):
            rows = list(record.get("source_row_ids") or [])
            if row_id not in rows:
                continue
            rows.remove(row_id)
            if not rows:
                self._drop_subject_entity(subject_id)
                return subject_id
            record["source_row_ids"] = rows
            record["duplicate_row_count"] = max(0, len(rows) - 1)
            record["resolution_status"] = (
                "deduplicated" if len(rows) > 1 else "confirmed"
            )
            self.subjects[subject_id] = record
            self._queue(SUBJECT_VIEW, subject_id, record)
            return subject_id
        return None

    def _candidate_confirmation_matches(
        self, candidate: Dict[str, Any], canonical: Dict[str, Any]
    ) -> bool:
        """Reject a 工商回查 that does not identify the selected candidate."""
        candidate_name_id = _clean_name_id(candidate.get("name_id"))
        canonical_name_id = _clean_name_id(canonical.get("name_id"))
        if candidate_name_id and canonical_name_id:
            return candidate_name_id == canonical_name_id
        candidate_code = _clean_code(candidate.get("social_credit_code"))
        canonical_code = _clean_code(canonical.get("code"))
        if candidate_code and canonical_code:
            return candidate_code == canonical_code
        candidate_name = _norm_name(_clean_text(candidate.get("enterprise_name")))
        canonical_name = _norm_name(_clean_text(canonical.get("name")))
        if candidate_name and canonical_name:
            return candidate_name == canonical_name
        return False

    def _candidate_face(
        self, candidate: Dict[str, Any]
    ) -> Optional[Tuple[Dict[str, Any], str, Dict[str, Any]]]:
        candidate_id = candidate.get("candidate_id")
        name_id = candidate.get("name_id")
        if not isinstance(name_id, str):
            return None
        cached = self.source_cache.get(stable_id("cand", candidate_id))
        if cached and isinstance(cached.get("data"), dict):
            canonical = self._canonical_from_data(cached["data"])
            if canonical:
                return cached["data"], cached.get("acquired_at") or now(), canonical
        if not FACE_PID:
            return None
        params = {"matchKeyword": name_id, "keywordType": "nameId"}
        face = self._face_lookup(params, f"{candidate_id}:confirm")
        if face is not None:
            body = {
                "candidate_id": candidate_id,
                "row_id": candidate.get("row_id"),
                "data": face[0],
                "acquired_at": face[1],
            }
            self.source_cache[stable_id("cand", candidate_id)] = body
            self._queue(SOURCE_VIEW, stable_id("cand", candidate_id), body)
        return face

    def _decision_identity(self, row_id: str) -> Optional[Dict[str, Any]]:
        identity = self.identities.get(row_id)
        if identity is not None:
            return identity
        with self.store.tx() as db:
            raw = db.execute(
                "SELECT body FROM roster_rows WHERE roster_id=? AND tenant=? AND row_id=?",
                (self.roster_id, self.tenant, row_id),
            ).fetchone()
        if raw is None:
            return None
        body = _json_load_body(_row_value(raw, "body"))
        record: Dict[str, Any] = {
            "row_id": row_id,
            "mapping_status": "unmatched",
            "candidate_count": 0,
        }
        name = _clean_text(body.get("enterprise_name"))
        code = _clean_code(body.get("social_credit_code"))
        if name:
            record["enterprise_name"] = name
        if code:
            record["social_credit_code"] = code
        row_number = body.get("source_row_number")
        if isinstance(row_number, int) and 1 <= row_number <= 10_000_000:
            record["source_row_number"] = row_number
        return record

    def _resolution_inherit_mode(self) -> bool:
        """True when the execution parent already contains applied decisions.

        ScopePlanner freezes ``args.snapshot_id`` as the pre-decision snapshot
        and ``plans.snapshot_id`` as a projection with the decisions applied.
        Execution normally uses the former as the parent; if it used the
        latter, replaying decisions would double-apply them.
        """
        pre = self.args.get("snapshot_id")
        post = self.plan.get("snapshot_id")
        return bool(
            isinstance(pre, str)
            and isinstance(post, str)
            and pre != post
            and self.snapshot_parent == post
        )

    def _collect_resolution(self) -> bool:
        decisions = self.args.get("decisions") or []
        if not isinstance(decisions, list) or not decisions:
            raise Fault("invalid_argument", "resolution plan has no decisions")
        self._load_state(
            (
                IDENTITY_VIEW,
                CANDIDATE_VIEW,
                SUBJECT_VIEW,
                COVERAGE_VIEW,
                SOURCE_VIEW,
            )
        )
        inherit = self._resolution_inherit_mode()
        seen_rows: Set[str] = set()
        impacted: Set[str] = set()
        complete = True
        for decision in decisions:
            if not isinstance(decision, dict):
                raise Fault("invalid_argument", "resolution decision must be an object")
            row_id = decision.get("row_id")
            action = decision.get("action")
            if not isinstance(row_id, str) or row_id in seen_rows:
                raise Fault("invalid_argument", "duplicate or missing row_id in decisions")
            seen_rows.add(row_id)
            identity = self._decision_identity(row_id)
            if identity is None:
                raise Fault("out_of_scope", "decision row does not belong to the snapshot")
            self.identities.setdefault(row_id, identity)
            if action not in ("exclude", "select_candidate"):
                raise Fault("invalid_argument", "unsupported resolution action")
            if inherit:
                if action == "select_candidate":
                    subject_id = identity.get("subject_id")
                    if (
                        identity.get("mapping_status") not in ("matched", "duplicate")
                        or not isinstance(subject_id, str)
                    ):
                        raise Fault(
                            "conflict",
                            "source snapshot does not contain the applied decisions",
                        )
                    impacted.add(subject_id)
                continue
            if action == "exclude":
                old_subject = self._remove_row_from_subject(row_id)
                if old_subject:
                    impacted.add(old_subject)
                identity.pop("subject_id", None)
                identity["mapping_status"] = "excluded"
                self.identities[row_id] = identity
                self._queue(IDENTITY_VIEW, row_id, identity)
                reason_code = decision.get("reason_code") or "user_excluded"
                note = decision.get("note")
                body = {
                    "row_id": row_id,
                    "reason_code": str(reason_code),
                    "note": str(note)[:300] if note else None,
                }
                key = stable_id("excl", row_id)
                self.source_cache[key] = body
                self._queue(SOURCE_VIEW, key, body)
                self._flush()
                continue
            if action != "select_candidate":
                raise Fault("invalid_argument", "unsupported resolution action")
            candidate_id = decision.get("candidate_id")
            candidate = self.candidates.get(candidate_id) if isinstance(candidate_id, str) else None
            if candidate is None or candidate.get("row_id") != row_id:
                raise Fault(
                    "out_of_scope", "candidate does not belong to the decision row"
                )
            face = self._candidate_face(candidate)
            if face is None or not self._candidate_confirmation_matches(
                candidate, face[2]
            ):
                # Without a matching 工商回查 the row stays unconfirmed.
                self.partial = True
                complete = False
                continue
            canonical = dict(face[2])
            if not canonical.get("name"):
                canonical["name"] = candidate.get("enterprise_name")
            if not canonical.get("code"):
                canonical["code"] = candidate.get("social_credit_code")
            if not canonical.get("name_id"):
                canonical["name_id"] = candidate.get("name_id")
            old_subject = self._remove_row_from_subject(row_id)
            if old_subject:
                impacted.add(old_subject)
            subject_id, _subject_record, duplicate = self._make_subject(
                code=canonical.get("code"),
                name_id=canonical.get("name_id"),
                name=canonical.get("name") or candidate.get("enterprise_name") or "",
                row_id=row_id,
            )
            impacted.add(subject_id)
            identity["mapping_status"] = "duplicate" if duplicate else "matched"
            identity["subject_id"] = subject_id
            self.identities[row_id] = identity
            self._queue(IDENTITY_VIEW, row_id, identity)
            params = (
                {"matchKeyword": canonical["name_id"], "keywordType": "nameId"}
                if canonical.get("name_id")
                else (
                    {"matchKeyword": canonical["code"], "keywordType": "socialCreditCode"}
                    if canonical.get("code")
                    else {"matchKeyword": canonical["name"], "keywordType": "name"}
                )
            )
            face_key = self._src_key(subject_id, FACE_PID, params)
            if face_key not in self.source_cache:
                body = {
                    "subject_id": subject_id,
                    "product_id": FACE_PID,
                    "params": params,
                    "pages": [face[0]],
                    "page_acquired_at": [face[1]],
                    "next_page": 1,
                    "complete": True,
                    "partial": False,
                    "acquired_at": face[1],
                    "page_size": 0,
                }
                self.source_cache[face_key] = body
                self.face_bodies[subject_id] = body
                self._queue(SOURCE_VIEW, face_key, body)
            self._flush()

        # Conditional base completion for subjects touched by the decisions.
        scope = self._base_scope()
        for subject_id in impacted:
            subject = self.subjects.get(subject_id)
            if subject is None:
                continue
            for product_id in scope:
                if self.budget.stopped:
                    complete = False
                    continue
                existing_coverage = self.coverage.get((subject_id, product_id))
                if (
                    existing_coverage
                    and existing_coverage.get("state") in ("covered", "empty")
                    and (subject_id, product_id) in self.inherited_coverage
                ):
                    continue
                if product_id == FACE_PID:
                    body = self.face_bodies.get(subject_id)
                    if body is None:
                        params = self._subject_params(subject, ROUTES[FACE_PID])
                        if params is None:
                            continue
                        key = self._src_key(subject_id, FACE_PID, params)
                        body = self.source_cache.get(key)
                        if body is None:
                            self._ensure_unit(subject_id, FACE_PID, params)
                            body = self.source_cache.get(key)
                    if body is not None:
                        stats = self._project_body(subject_id, FACE_PID, body)
                        done = self._finalize_coverage(
                            subject_id,
                            FACE_PID,
                            [
                                {
                                    "ok": True,
                                    "complete": body.get("complete", True),
                                    "included": stats["included"],
                                    "excluded": stats["excluded"],
                                    "undated": stats["undated"],
                                    "duplicates": stats["duplicates"],
                                    "reason": None,
                                }
                            ],
                        )
                        complete = complete and done
                    else:
                        self._set_coverage(
                            subject_id, FACE_PID, "error", 0, False, "fetch_failed"
                        )
                        complete = False
                    continue
                if product_id in SUPPLEMENT_IDS and not self._supplement_needed(
                    subject_id, product_id
                ):
                    self._set_coverage(
                        subject_id,
                        product_id,
                        "not_queried",
                        0,
                        False,
                        "base_fields_present",
                    )
                    continue
                params = self._subject_params(subject, ROUTES[product_id])
                if params is None:
                    continue
                key = self._src_key(subject_id, product_id, params)
                result = self._ensure_unit(subject_id, product_id, params)
                if result["ok"]:
                    body = self.source_cache.get(key) or {
                        "params": params,
                        "pages": result["pages"],
                        "complete": result["complete"],
                        "acquired_at": result["acquired_at"],
                    }
                    stats = self._project_body(subject_id, product_id, body)
                    done = self._finalize_coverage(
                        subject_id,
                        product_id,
                        [
                            {
                                "ok": True,
                                "complete": result["complete"],
                                "included": stats["included"],
                                "excluded": stats["excluded"],
                                "undated": stats["undated"],
                                "duplicates": stats["duplicates"],
                                "reason": result["reason"],
                            }
                        ],
                    )
                    complete = complete and done
                else:
                    self._set_coverage(
                        subject_id,
                        product_id,
                        "not_queried"
                        if result["reason"] in (
                            "budget_exhausted",
                            "product_call_limit",
                            "page_limited",
                        )
                        else "error",
                        0,
                        False,
                        result["reason"] or "fetch_failed",
                    )
                    complete = False
                self._flush()
        self._flush()
        if self.budget.stopped or self.budget.partial or self.partial:
            complete = False
        return complete and not self.failures

    # -- topics stage ---------------------------------------------------------

    def _parse_period(self) -> _Period:
        period = self.scope_plan["period"]
        start_month = period.get("start_month")
        end_month = period.get("end_month")
        cutoff_raw = period.get("observation_cutoff")
        if not isinstance(start_month, str) or not isinstance(end_month, str):
            raise Fault("invalid_argument", "period start_month/end_month are required")
        if not isinstance(cutoff_raw, str):
            raise Fault("invalid_argument", "period observation_cutoff is required")
        try:
            cutoff = date.fromisoformat(cutoff_raw)
        except ValueError as exc:
            raise Fault("invalid_argument", "observation_cutoff is not a date") from exc
        start_match = re.fullmatch(r"((?:19|20)\d{2})-(0[1-9]|1[0-2])", start_month)
        end_match = re.fullmatch(r"((?:19|20)\d{2})-(0[1-9]|1[0-2])", end_month)
        if not start_match or not end_match:
            raise Fault("invalid_argument", "period months must use YYYY-MM")
        if start_month > end_month:
            raise Fault("period_out_of_range", "start_month is after end_month")
        try:
            max_months = int(period.get("max_months_per_entity") or 1)
        except (TypeError, ValueError) as exc:
            raise Fault("invalid_argument", "max_months_per_entity must be an integer") from exc
        months: List[str] = []
        year, month = int(start_match.group(1)), int(start_match.group(2))
        end_year, end_month_number = int(end_match.group(1)), int(end_match.group(2))
        cutoff_month = cutoff.strftime("%Y-%m")
        while (year, month) <= (end_year, end_month_number) and len(months) < 120:
            candidate = f"{year:04d}-{month:02d}"
            if candidate <= cutoff_month:
                months.append(candidate)
            month += 1
            if month > 12:
                month = 1
                year += 1
        months = months[: max(1, max_months)]
        return _Period(start_month, end_month, cutoff, months)

    def _selection_state(
        self,
    ) -> Tuple[List[str], Dict[str, Set[str]], Dict[str, Set[str]]]:
        selections = self.args.get("selections") or []
        if not isinstance(selections, list) or not selections:
            raise Fault("invalid_argument", "topics plan has no selections")
        order: List[str] = []
        requested: Dict[str, Set[str]] = {}
        cohorts: Dict[str, Set[str]] = {}
        for selection in selections:
            if not isinstance(selection, dict):
                raise Fault("invalid_argument", "selection must be an object")
            cohort_key = selection.get("cohort_key")
            topic = selection.get("topic")
            if not isinstance(cohort_key, str) or not cohort_key:
                raise Fault("invalid_argument", "selection cohort_key is required")
            product_ids = selection.get("product_ids") or []
            subject_ids = selection.get("subject_ids") or []
            if not isinstance(product_ids, list) or not isinstance(subject_ids, list):
                raise Fault("invalid_argument", "selection product/subject lists are invalid")
            if not product_ids:
                raise Fault("no_products_selected", "selection has no product ids")
            for product_id in product_ids:
                route = ROUTES.get(product_id)
                if route is None or route.get("topic") != topic:
                    raise Fault(
                        "out_of_scope", "selection product does not belong to its topic"
                    )
            for subject_id in subject_ids:
                subject = self.subjects.get(subject_id)
                if (
                    subject is None
                    or subject.get("resolution_status")
                    not in ("confirmed", "deduplicated")
                    or not subject.get("source_row_ids")
                ):
                    raise Fault(
                        "out_of_scope",
                        "selection subject is not confirmed in the source snapshot",
                    )
                if subject_id not in order:
                    order.append(subject_id)
                requested.setdefault(subject_id, set()).update(product_ids)
                cohorts.setdefault(subject_id, set()).add(cohort_key)
        if not order:
            raise Fault("no_applicable_subjects", "selections contain no subjects")
        return order, requested, cohorts

    def _parent_authorized(self) -> Set[str]:
        authorized: Set[str] = set()
        for product_id in self.scope_plan["scope"]["product_ids"]:
            if product_id in ROUTES:
                authorized.add(product_id)
        return authorized

    def _parent_inclusion_map(self) -> Dict[Tuple[str, str], Set[str]]:
        """Read ``args.dependencies`` for chain-parent inclusion subjects.

        A parent product that only appears in the frozen scope (not in any selection)
        may be fetched *only* for the subject ids listed on its dependency
        entry.  This prevents a child requested by one cohort from scanning the
        whole plan for unrelated subjects.
        """
        inclusions: Dict[Tuple[str, str], Set[str]] = {}
        dependencies = self.args.get("dependencies")
        if not isinstance(dependencies, list):
            return inclusions
        for dependency in dependencies:
            if not isinstance(dependency, dict):
                continue
            reason = dependency.get("reason")
            if reason not in (None, "chain_parent_inclusion"):
                continue
            parent = dependency.get("product_id")
            child = dependency.get("for_product_id")
            subject_ids = dependency.get("subject_ids")
            if not isinstance(parent, str) or not isinstance(child, str):
                continue
            if child not in CHAINS or PARENT_PRODUCT.get(child) != parent:
                continue
            if not isinstance(subject_ids, list):
                continue
            allowed = {
                subject_id
                for subject_id in subject_ids
                if isinstance(subject_id, str)
            }
            inclusions.setdefault((parent, child), set()).update(allowed)
        return inclusions

    def _unit_key(self, params: Dict[str, Any]) -> Optional[str]:
        stats_month = params.get("statsMonth")
        if isinstance(stats_month, str) and stats_month:
            return stats_month
        year = params.get("arYear")
        if isinstance(year, str) and year:
            return year
        return None

    def _child_key_label(self, params: Dict[str, Any]) -> str:
        match_keyword = params.get("matchKeyword")
        if isinstance(match_keyword, str) and match_keyword:
            return match_keyword
        parts: List[str] = []
        for key in ("platform", "shopId", "statsMonth"):
            value = params.get(key)
            if isinstance(value, (str, int)) and str(value):
                parts.append(str(value))
        return ":".join(parts)

    def _source_units(self, subject_id: str, product_id: str) -> List[Dict[str, Any]]:
        units: List[Dict[str, Any]] = []
        for body in self.source_cache.values():
            if not isinstance(body, dict):
                continue
            if body.get("subject_id") != subject_id:
                continue
            if body.get("product_id") != product_id:
                continue
            units.append(body)
        units.sort(key=lambda body: _canonical_params(_as_dict(body.get("params"))))
        return units

    def _chain_param_sets(
        self, chain: Dict[str, Any], data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        parent_paths = list(chain.get("parent_records") or [])
        mappings = list(chain.get("mapping") or [])
        records: List[Dict[str, Any]] = []
        for path in parent_paths:
            value = evid.get_path(data, path)
            if isinstance(value, list):
                records.extend(item for item in value if isinstance(item, dict))
            elif isinstance(value, dict):
                records.append(value)
        params_list: List[Dict[str, Any]] = []
        for record in records:
            params: Dict[str, Any] = {}
            valid = True
            for mapping in mappings:
                if not isinstance(mapping, dict):
                    valid = False
                    break
                target = mapping.get("target")
                source = mapping.get("source")
                if not isinstance(target, str) or not isinstance(source, str):
                    valid = False
                    break
                if "[]." in source:
                    field = source.split("[].")[1]
                else:
                    field = source
                field = field.strip()
                value = evid.get_path(record, field)
                if value is evid.MISSING or value is None or value == "":
                    valid = False
                    break
                if target == "statsMonth":
                    text = re.sub(r"[^0-9]", "", str(value))
                    value = text
                elif isinstance(value, (int, float, bool)):
                    value = str(value)
                params[target] = value
            if valid and params:
                params_list.append(params)
        return params_list

    def _listing_status(self, subject_id: str) -> Optional[bool]:
        if not TAG_PID:
            return None
        route = ROUTES[TAG_PID]
        for body in self._source_units(subject_id, TAG_PID):
            for data in body.get("pages") or []:
                for _path, _index, record in evid.extract_records(route, data):
                    if "hasStock" not in record:
                        continue
                    value = record.get("hasStock")
                    if isinstance(value, bool):
                        return value
                    if isinstance(value, (int, float)) and value in (0, 1):
                        return bool(value)
                    if isinstance(value, str):
                        text = value.strip().lower()
                        if text in ("true", "1", "是", "yes"):
                            return True
                        if text in ("false", "0", "否", "no"):
                            return False
        return None

    def _collect_direct_topic(
        self,
        subject: Dict[str, Any],
        product_id: str,
        period: _Period,
    ) -> bool:
        route = ROUTES[product_id]
        subject_id = subject["subject_id"]
        listing_status: Optional[bool] = None
        if route.get("topic") == "listed":
            listing_status = self._listing_status(subject_id)
            if listing_status is False:
                self._set_coverage(
                    subject_id,
                    product_id,
                    "not_applicable",
                    0,
                    False,
                    "base_tag_hasStock=false",
                )
                return True

        units_params = self._units_for_subject(subject, product_id, period)
        unit_results: List[Dict[str, Any]] = []
        for params in units_params:
            result = self._ensure_unit(subject_id, product_id, params)
            if result["ok"]:
                body = self.source_cache.get(
                    self._src_key(subject_id, product_id, params)
                ) or {
                    "params": params,
                    "pages": result["pages"],
                    "complete": result["complete"],
                    "acquired_at": result["acquired_at"],
                }
                stats = self._project_body(
                    subject_id,
                    product_id,
                    body,
                    period=period,
                    record_prefix=self._unit_key(params),
                )
                unit_results.append(
                    {
                        "ok": True,
                        "complete": result["complete"],
                        "included": stats["included"],
                        "excluded": stats["excluded"],
                        "undated": stats["undated"],
                        "duplicates": stats["duplicates"],
                        "late": stats["late"],
                        "late_publish": stats["late_publish"],
                        "reason": result["reason"],
                        "label": params.get("statsMonth") or params.get("arYear") or "",
                    }
                )
            else:
                unit_results.append(
                    {
                        "ok": False,
                        "complete": False,
                        "included": 0,
                        "excluded": 0,
                        "undated": 0,
                        "duplicates": 0,
                        "late": 0,
                        "late_publish": 0,
                        "reason": result["reason"],
                        "label": params.get("statsMonth") or params.get("arYear") or "",
                    }
                )
            self._flush()
        done = self._finalize_coverage(subject_id, product_id, unit_results)
        if route.get("topic") == "listed" and listing_status is None:
            coverage = self.coverage.get((subject_id, product_id))
            if coverage is not None and coverage.get("state") == "empty":
                coverage["denominator_eligible"] = False
                coverage["reason"] = "listing_status_unknown"
                self._queue(
                    COVERAGE_VIEW, coverage["coverage_id"], coverage
                )
        return done

    def _emit_truncation_evidence(
        self, subject_id: str, product_id: str, source_field: str, note: str
    ) -> None:
        item = {
            "evidence_id": evid.evidence_id(
                subject_id, product_id, "truncated", source_field
            ),
            "entity_id": subject_id,
            "product_id": product_id,
            "topic": ROUTES[product_id].get("topic"),
            "source_field": evid.sanitize_source_field(source_field),
            "source_record_id": "truncated",
            "display_value": "",
            "unit": "none",
            "business_time": {"granularity": "unknown"},
            "acquired_at": now(),
            "status": "truncated",
            "note": note[:500],
        }
        self._queue(EVIDENCE_VIEW, item["evidence_id"], item)

    def _collect_child_topic(
        self,
        subject: Dict[str, Any],
        product_id: str,
        period: _Period,
    ) -> bool:
        subject_id = subject["subject_id"]
        chain = CHAINS[product_id]
        parent_id = chain.get("parent_product_id")
        route = ROUTES[product_id]
        if not parent_id or parent_id not in ROUTES:
            self._set_coverage(
                subject_id,
                product_id,
                "not_queried",
                0,
                False,
                "parent_not_authorized",
            )
            return False
        bodies = self._source_units(subject_id, parent_id)
        child_fields = _field_names(route)
        monthly_child = "statsMonth" in child_fields
        if monthly_child and not period.months_param:
            self._set_coverage(
                subject_id,
                product_id,
                "not_queried",
                0,
                False,
                "no_period_units",
            )
            return False
        if monthly_child:
            allowed = set(period.months_param)
            filtered: List[Dict[str, Any]] = []
            for body in bodies:
                params = _as_dict(body.get("params"))
                stats_month = params.get("statsMonth")
                if isinstance(stats_month, str) and allowed and stats_month not in allowed:
                    continue
                filtered.append(body)
            bodies = filtered
        if not bodies:
            self._set_coverage(
                subject_id,
                product_id,
                "not_queried",
                0,
                False,
                "parent_source_unavailable",
            )
            return False

        parent_partial = any(not body.get("complete") for body in bodies)
        derived: List[Dict[str, Any]] = []
        seen: Set[str] = set()
        cap_hit = False
        for body in bodies:
            for data in body.get("pages") or []:
                if not isinstance(data, dict):
                    continue
                for params in self._chain_param_sets(chain, data):
                    if monthly_child:
                        stats_month = params.get("statsMonth")
                        if (
                            not isinstance(stats_month, str)
                            or stats_month not in period.months_param
                            or not re.fullmatch(
                                r"(?:19|20)\d{2}(?:0[1-9]|1[0-2])", stats_month
                            )
                        ):
                            continue
                    signature = _canonical_params(params)
                    if signature in seen:
                        continue
                    seen.add(signature)
                    if len(derived) >= self.budget.max_child_keys:
                        cap_hit = True
                        break
                    derived.append(params)
                if cap_hit:
                    break
            if cap_hit:
                break
        if cap_hit:
            self.partial = True

        if not derived:
            parent_raw = 0
            for body in bodies:
                for data in body.get("pages") or []:
                    parent_raw += len(evid.extract_records(ROUTES[parent_id], data))
            conclusive = not parent_partial
            reason = "parent_empty" if parent_raw == 0 else "no_child_keys"
            self._set_coverage(
                subject_id,
                product_id,
                "empty",
                0,
                conclusive,
                reason if conclusive else reason + ";parent_partial",
            )
            return conclusive

        unit_results: List[Dict[str, Any]] = []
        for params in derived:
            result = self._ensure_unit(subject_id, product_id, params)
            if result["ok"]:
                body = self.source_cache.get(
                    self._src_key(subject_id, product_id, params)
                ) or {
                    "params": params,
                    "pages": result["pages"],
                    "complete": result["complete"],
                    "acquired_at": result["acquired_at"],
                }
                stats = self._project_body(
                    subject_id,
                    product_id,
                    body,
                    period=period,
                    record_prefix=self._child_key_label(params),
                )
                unit_results.append(
                    {
                        "ok": True,
                        "complete": result["complete"],
                        "included": stats["included"],
                        "excluded": stats["excluded"],
                        "undated": stats["undated"],
                        "duplicates": stats["duplicates"],
                        "late": stats["late"],
                        "late_publish": stats["late_publish"],
                        "reason": result["reason"],
                        "label": params.get("statsMonth")
                        or self._child_key_label(params),
                    }
                )
            else:
                unit_results.append(
                    {
                        "ok": False,
                        "complete": False,
                        "included": 0,
                        "excluded": 0,
                        "undated": 0,
                        "duplicates": 0,
                        "late": 0,
                        "late_publish": 0,
                        "reason": result["reason"],
                        "label": params.get("statsMonth")
                        or self._child_key_label(params),
                    }
                )
            self._flush()
        if parent_partial:
            unit_results.append(
                {
                    "ok": False,
                    "complete": False,
                    "included": 0,
                    "excluded": 0,
                    "undated": 0,
                    "duplicates": 0,
                    "reason": "parent_source_partial",
                }
            )
        if cap_hit:
            unit_results.append(
                {
                    "ok": False,
                    "complete": False,
                    "included": 0,
                    "excluded": 0,
                    "undated": 0,
                    "duplicates": 0,
                    "reason": "max_child_keys",
                }
            )
        if cap_hit:
            self._emit_truncation_evidence(
                subject_id,
                product_id,
                "max_child_keys",
                "child key cap reached; additional parent keys were not queried",
            )
        done = self._finalize_coverage(subject_id, product_id, unit_results)
        return done and not parent_partial and not cap_hit

    def _update_cohort_keys(
        self, subject: Dict[str, Any], cohort_keys: Set[str]
    ) -> None:
        subject_id = subject["subject_id"]
        merged = list(
            dict.fromkeys(list(subject.get("cohort_keys") or []) + sorted(cohort_keys))
        )
        subject["cohort_keys"] = merged[:50]
        self.subjects[subject_id] = subject
        self._queue(SUBJECT_VIEW, subject_id, subject)

    def _collect_topics(self) -> bool:
        period = self._parse_period()
        self._load_state((SUBJECT_VIEW, COVERAGE_VIEW, SOURCE_VIEW))
        order, requested, cohorts = self._selection_state()
        authorized = self._parent_authorized()
        inclusions = self._parent_inclusion_map()
        complete = True

        target_order = order[: self.budget.max_targets]
        target_set = set(target_order)
        if len(order) > len(target_order):
            complete = False

        # Pre-write coverage so a crash/stop still exposes honest gaps.
        for subject_id in order:
            for product_id in TOPIC_PIDS:
                key = (subject_id, product_id)
                if product_id in requested.get(subject_id, set()):
                    if subject_id in target_set:
                        self._set_coverage(
                            subject_id,
                            product_id,
                            "not_queried",
                            0,
                            False,
                            "not_reached",
                        )
                    else:
                        self._set_coverage(
                            subject_id,
                            product_id,
                            "not_queried",
                            0,
                            False,
                            "max_targets",
                        )
                elif key not in self.coverage:
                    self._set_coverage(
                        subject_id,
                        product_id,
                        "not_queried",
                        0,
                        False,
                        "not_selected",
                    )
            subject = self.subjects.get(subject_id)
            if subject is not None:
                self._update_cohort_keys(subject, cohorts.get(subject_id, set()))
        self._flush()

        for subject_id in target_order:
            subject = self.subjects.get(subject_id)
            if subject is None:
                complete = False
                continue
            requested_products = set(requested.get(subject_id) or ())
            parent_pairs = {
                (PARENT_PRODUCT[product_id], product_id)
                for product_id in requested_products
                if product_id in CHAINS and PARENT_PRODUCT.get(product_id)
            }
            fetchable_parents: Set[str] = set()
            for parent, child in parent_pairs:
                if parent not in authorized:
                    continue
                if parent in requested_products:
                    fetchable_parents.add(parent)
                elif subject_id in inclusions.get((parent, child), set()):
                    fetchable_parents.add(parent)
            products = requested_products | fetchable_parents
            ordered_products = [pid for pid in TOPIC_PIDS if pid in products]

            for product_id in ordered_products:
                if product_id in CHAINS:
                    continue
                if self.budget.stopped:
                    break
                done = self._collect_direct_topic(subject, product_id, period)
                complete = complete and done
                self._flush()
            for product_id in ordered_products:
                if product_id not in CHAINS:
                    continue
                if self.budget.stopped:
                    break
                done = self._collect_child_topic(subject, product_id, period)
                complete = complete and done
                self._flush()

        if self.budget.stopped or self.budget.partial or self.partial:
            complete = False
        return complete and not self.failures


# ---------------------------------------------------------------------------
# Public entry point.
# ---------------------------------------------------------------------------


def collect(
    store: Any,
    tenant: str,
    plan: Dict[str, Any],
    snapshot_id: str,
    fetch: Callable[[str, Dict[str, Any]], Tuple[Dict[str, Any], str]],
) -> bool:
    """Collect one frozen plan into ``snapshot_id``.

    Returns ``True`` only when every intended call completed.  Ambiguous or
    unmatched roster rows are legitimate base outcomes and do not make the run
    partial by themselves.  ``False`` means budget/page/period limits or
    upstream errors left a readable but partial result.
    """
    if not isinstance(tenant, str) or not tenant:
        raise Fault("invalid_argument", "tenant is required")
    if not isinstance(snapshot_id, str) or not snapshot_id.startswith("snap_"):
        raise Fault("invalid_argument", "snapshot_id is invalid")
    if not isinstance(plan, dict):
        raise Fault("invalid_argument", "plan must be a mapping")
    collector = _Collector(store, tenant, plan, snapshot_id, fetch)
    collector._open_snapshot()
    try:
        if collector.stage == "base":
            return collector._collect_base()
        if collector.stage == "resolution":
            return collector._collect_resolution()
        if collector.stage == "topics":
            return collector._collect_topics()
    except Fault:
        collector._flush()
        raise
    raise Fault("invalid_argument", "unknown plan stage")
