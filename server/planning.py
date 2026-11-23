"""Roster intake and frozen, offline data-scope plans.

This module owns the roster tables and the ``plans`` table used by the
execution ledger.  It never talks to an upstream data service: roster chunks
are validated locally and every plan freezes its scope, products, call caps
and expiry.  Plan generation is local: it never triggers upstream calls or
attaches commercial or authorization data.

The public surface is intentionally small:

``DDL``
    Idempotent SQLite DDL loaded by ``store.Store`` at startup.
``Planner(store).stage(tenant, args)``
    Chunked, idempotent roster intake.
``Planner(store).base(tenant, args)``
    Base plan for a complete roster.
``Planner(store).resolve(tenant, args)``
    Resolution decisions plus a plan for the remaining back-checks.
``Planner(store).topics(tenant, args)``
    Topic plan for explicitly selected, already-confirmed subjects.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

from server.common import (
    Fault,
    ROUTES,
    digest,
    later,
    now,
    pack,
    stable_id,
    uid,
)

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS rosters (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL,
    batch_key TEXT,
    input_snapshot_id TEXT NOT NULL DEFAULT '',
    row_count INTEGER NOT NULL DEFAULT 0,
    complete INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT '',
    completed_at TEXT,
    UNIQUE (tenant, batch_key)
);

CREATE TABLE IF NOT EXISTS roster_batches (
    tenant TEXT NOT NULL,
    batch_key TEXT NOT NULL,
    chunk_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tenant, batch_key)
);

CREATE TABLE IF NOT EXISTS roster_chunks (
    tenant TEXT NOT NULL,
    batch_key TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    accepted_rows INTEGER NOT NULL DEFAULT 0,
    duplicate_rows INTEGER NOT NULL DEFAULT 0,
    rejected_rows INTEGER NOT NULL DEFAULT 0,
    receipt JSON NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tenant, batch_key, chunk_index),
    FOREIGN KEY (tenant, batch_key) REFERENCES roster_batches (tenant, batch_key)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS roster_rows (
    roster_id TEXT NOT NULL,
    tenant TEXT NOT NULL DEFAULT '',
    row_id TEXT NOT NULL,
    body JSON NOT NULL,
    created_at TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (roster_id, row_id)
);

CREATE INDEX IF NOT EXISTS roster_rows_tenant
    ON roster_rows (tenant, roster_id);

CREATE TABLE IF NOT EXISTS plans (
    id TEXT PRIMARY KEY,
    tenant TEXT NOT NULL,
    plan_hash TEXT NOT NULL,
    stage TEXT NOT NULL,
    roster_id TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    args JSON NOT NULL,
    scope_plan JSON NOT NULL,
    expires_at TEXT NOT NULL,
    cache_key TEXT,
    created_at TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS plans_cache
    ON plans (tenant, plan_hash, expires_at);
CREATE INDEX IF NOT EXISTS plans_request_cache
    ON plans (tenant, cache_key, expires_at);
CREATE INDEX IF NOT EXISTS plans_roster
    ON plans (tenant, roster_id, created_at);
"""

# ---------------------------------------------------------------------------
# Trusted route catalogue
# ---------------------------------------------------------------------------

BASE_ROUTING_KEYS: Tuple[str, ...] = ("identity", "base", "supplement")
CORE_BASE_NAMES: Tuple[str, ...] = ("工商照面", "企业标签", "企业指标")
IDENTITY_NAME = "关键词查询企业"
SUPPLEMENT_NAMES: Tuple[str, ...] = (
    "企业行业（国标）",
    "企业产品标签",
    "企业简介",
)

BASE_PRODUCT_IDS: Tuple[str, ...] = tuple(
    product_id
    for product_id, route in ROUTES.items()
    if str(route.get("topic", "")) in BASE_ROUTING_KEYS
)
TOPIC_KEYS: Tuple[str, ...] = tuple(
    dict.fromkeys(
        str(route.get("topic", ""))
        for route in ROUTES.values()
        if str(route.get("topic", "")) not in BASE_ROUTING_KEYS
    )
)
TOPIC_PRODUCT_IDS: Tuple[str, ...] = tuple(
    product_id
    for product_id, route in ROUTES.items()
    if str(route.get("topic", "")) not in BASE_ROUTING_KEYS
)
PRODUCT_ORDER: Tuple[str, ...] = tuple(ROUTES)
PRODUCT_IDS_BY_TOPIC: Dict[str, Tuple[str, ...]] = {
    topic: tuple(pid for pid in TOPIC_PRODUCT_IDS if ROUTES[pid].get("topic") == topic)
    for topic in TOPIC_KEYS
}


def _product_id_by_name(name: str) -> str:
    for product_id, route in ROUTES.items():
        if route.get("name") == name:
            return product_id
    raise RuntimeError(f"product-routes.json is missing required product: {name}")


CORE_BASE_IDS: Tuple[str, ...] = tuple(_product_id_by_name(n) for n in CORE_BASE_NAMES)
IDENTITY_PRODUCT_ID = _product_id_by_name(IDENTITY_NAME)
SUPPLEMENT_PRODUCT_IDS: Tuple[str, ...] = tuple(
    _product_id_by_name(n) for n in SUPPLEMENT_NAMES
)

if not BASE_PRODUCT_IDS or not TOPIC_PRODUCT_IDS or not TOPIC_KEYS:
    raise RuntimeError("product-routes.json does not describe both base and topic products")

# ---------------------------------------------------------------------------
# Limits and identifiers
# ---------------------------------------------------------------------------

MAX_ROSTER_ROWS = 100_000
MAX_ROWS_PER_CHUNK = 500
MAX_CHUNKS = 1_000
MAX_CALLS = 100_000

ROW_ID_RE = re.compile(r"^row_[A-Za-z0-9_-]{1,64}$")
BATCH_KEY_RE = re.compile(r"^batch_[A-Za-z0-9_-]{8,64}$")
SNAPSHOT_ID_RE = re.compile(r"^snap_[A-Za-z0-9_-]{8,64}$")
ROSTER_ID_RE = re.compile(r"^roster_[A-Za-z0-9_-]{8,64}$")
CANDIDATE_ID_RE = re.compile(r"^cand_[A-Za-z0-9_-]{8,64}$")
SUBJECT_ID_RE = re.compile(r"^subj_[A-Za-z0-9_-]{8,64}$")
CREDIT_CODE_RE = re.compile(r"^[0-9A-HJ-NP-RT-UW-Y]{18}$")
SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
YEAR_MONTH_RE = re.compile(r"^(?:19|20)[0-9]{2}-(?:0[1-9]|1[0-2])$")
DATE_RE = re.compile(
    r"^(?:19|20)[0-9]{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])$"
)

LIMIT_KEYS: Tuple[str, ...] = (
    "max_targets",
    "max_calls",
    "max_page_size",
    "max_pages_per_view",
    "max_child_keys",
)
LIMIT_BOUNDS: Dict[str, Tuple[int, int]] = {
    "max_targets": (1, 100_000),
    "max_calls": (1, MAX_CALLS),
    "max_page_size": (1, 200),
    "max_pages_per_view": (1, 10_000),
    "max_child_keys": (1, 10_000),
}
PERIOD_KEYS: Tuple[str, ...] = (
    "start_month",
    "end_month",
    "observation_cutoff",
    "max_months_per_entity",
)
ROW_KEYS: Tuple[str, ...] = (
    "row_id",
    "enterprise_name",
    "social_credit_code",
    "source_row_number",
)
RESOLVE_REASONS = {
    "not_a_customer",
    "duplicate_subject",
    "no_reliable_match",
    "out_of_scope",
    "user_excluded",
}


# ---------------------------------------------------------------------------
# Small validation helpers
# ---------------------------------------------------------------------------


def _bad(message: str, code: str = "invalid_argument") -> None:
    raise Fault(code, message)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _object(value: Any, field: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        _bad(f"{field} 必须是对象。")
    return value


def _exact_keys(value: Dict[str, Any], required: Sequence[str], field: str) -> None:
    allowed = set(required)
    unknown = sorted(set(value) - allowed)
    missing = [key for key in required if key not in value]
    if unknown:
        _bad(f"{field} 包含未知字段：{', '.join(unknown)}。")
    if missing:
        _bad(f"{field} 缺少必填字段：{', '.join(missing)}。")


def _text(
    value: Any,
    field: str,
    *,
    minimum: int = 1,
    maximum: Optional[int] = None,
    pattern: Optional[re.Pattern[str]] = None,
    strip: bool = True,
) -> str:
    if not isinstance(value, str):
        _bad(f"{field} 必须是字符串。")
    text = value.strip() if strip else value
    if len(text) < minimum:
        _bad(f"{field} 不能为空。")
    if maximum is not None and len(text) > maximum:
        _bad(f"{field} 超过 {maximum} 字符上限。")
    if pattern is not None and pattern.fullmatch(text) is None:
        _bad(f"{field} 格式不合法。")
    return text


def _integer(value: Any, field: str, minimum: int, maximum: int) -> int:
    if not _is_int(value):
        _bad(f"{field} 必须是整数。")
    if value < minimum or value > maximum:
        _bad(f"{field} 必须在 {minimum}..{maximum} 之间。")
    return value


def _sha256(value: Any, field: str) -> str:
    return _text(value, field, minimum=71, maximum=71, pattern=SHA256_RE)


def _limits(value: Any) -> Dict[str, int]:
    obj = _object(value, "limits")
    _exact_keys(obj, LIMIT_KEYS, "limits")
    result: Dict[str, int] = {}
    for key in LIMIT_KEYS:
        low, high = LIMIT_BOUNDS[key]
        result[key] = _integer(obj[key], f"limits.{key}", low, high)
    return result


def _period(value: Any) -> Dict[str, Any]:
    obj = _object(value, "period")
    _exact_keys(obj, PERIOD_KEYS, "period")
    start = _text(obj["start_month"], "period.start_month", minimum=7, maximum=7, pattern=YEAR_MONTH_RE)
    end = _text(obj["end_month"], "period.end_month", minimum=7, maximum=7, pattern=YEAR_MONTH_RE)
    cutoff = _text(
        obj["observation_cutoff"],
        "period.observation_cutoff",
        minimum=10,
        maximum=10,
        pattern=DATE_RE,
    )
    try:
        cutoff_date = date.fromisoformat(cutoff)
    except ValueError:
        _bad("period.observation_cutoff 必须是有效日期（YYYY-MM-DD）。")
    months = _integer(
        obj["max_months_per_entity"],
        "period.max_months_per_entity",
        1,
        120,
    )
    return {
        "start_month": start,
        "end_month": end,
        "observation_cutoff": cutoff_date.isoformat(),
        "max_months_per_entity": months,
    }


def _month_index(year_month: str) -> int:
    year, month = year_month.split("-", 1)
    return int(year) * 12 + int(month)


def _month_span(start: str, end: str) -> int:
    return _month_index(end) - _month_index(start) + 1


def _month_label(index: int) -> str:
    year, month = divmod(index - 1, 12)
    return f"{year:04d}-{month + 1:02d}"


def _normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKC", value).strip().casefold()
    return re.sub(r"\s+", "", text)


def _identity_key(row: Dict[str, Any]) -> str:
    code = row.get("social_credit_code")
    if code:
        return "credit:" + str(code).upper()
    return "name:" + _normalize_name(str(row.get("enterprise_name", "")))


def _decode(value: Any) -> Any:
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


def _stable(prefix: str, pattern: re.Pattern[str], *parts: Any) -> str:
    value = stable_id(prefix, *parts)
    if pattern.fullmatch(value):
        return value
    # Defensive fallback keeps identifiers inside the public schema even if a
    # deployment overrides the shared helper with a different format.
    suffix = digest([prefix, *parts]).split(":", 1)[-1][:40]
    return f"{prefix}_{suffix}"


# ---------------------------------------------------------------------------
# Planner
# ---------------------------------------------------------------------------


class Planner:
    """Offline roster staging and data-scope planning."""

    def __init__(self, store: Any) -> None:
        self.store = store
        self.settings = store.settings

    # -- roster intake ------------------------------------------------------

    def stage(self, tenant: str, args: Dict[str, Any]) -> Dict[str, Any]:
        tenant = _text(tenant, "tenant", maximum=128)
        data = _object(args, "args")
        _exact_keys(data, ("batch_key", "chunk_index", "chunk_count", "rows"), "args")
        batch_key = _text(
            data["batch_key"], "batch_key", minimum=14, maximum=70, pattern=BATCH_KEY_RE
        )
        chunk_count = _integer(data["chunk_count"], "chunk_count", 1, MAX_CHUNKS)
        chunk_index = _integer(data["chunk_index"], "chunk_index", 1, MAX_CHUNKS)
        if chunk_index > chunk_count:
            _bad("chunk_index 不能大于 chunk_count。")
        rows = data["rows"]
        if not isinstance(rows, list):
            _bad("rows 必须是数组。")
        if len(rows) < 1 or len(rows) > MAX_ROWS_PER_CHUNK:
            _bad(f"rows 每块必须在 1..{MAX_ROWS_PER_CHUNK} 行之间。")

        canonical_rows = [self._canonical_input_row(row) for row in rows]
        ordered_rows = sorted(canonical_rows, key=lambda row: pack(row))
        content_hash = digest(
            {
                "batch_key": batch_key,
                "chunk_index": chunk_index,
                "chunk_count": chunk_count,
                "rows": ordered_rows,
            }
        )
        roster_id = _stable("roster", ROSTER_ID_RE, tenant, batch_key)

        with self.store.tx(write=True) as db:
            batch = db.execute(
                "SELECT chunk_count FROM roster_batches WHERE tenant=? AND batch_key=?",
                (tenant, batch_key),
            ).fetchone()
            created = now()
            if batch is None:
                db.execute(
                    "INSERT INTO roster_batches (tenant, batch_key, chunk_count, created_at) "
                    "VALUES (?, ?, ?, ?)",
                    (tenant, batch_key, chunk_count, created),
                )
            elif int(batch["chunk_count"]) != chunk_count:
                _bad(
                    "同一批次键的 chunk_count 与此前上传不一致；请使用新的批次键重新上传。",
                    "conflict",
                )

            existing = db.execute(
                "SELECT content_hash, receipt FROM roster_chunks "
                "WHERE tenant=? AND batch_key=? AND chunk_index=?",
                (tenant, batch_key, chunk_index),
            ).fetchone()
            if existing is not None:
                if str(existing["content_hash"]) != content_hash:
                    _bad(
                        "同一分块已登记不同内容；请使用新的批次键重新上传完整名单。",
                        "conflict",
                    )
                receipt = _decode(existing["receipt"])
                return self._freshen_receipt(db, tenant, roster_id, receipt)

            roster = db.execute(
                "SELECT id, complete FROM rosters WHERE id=? AND tenant=?",
                (roster_id, tenant),
            ).fetchone()
            if roster is not None and int(roster["complete"]):
                _bad("该批次已生成完整名单，不能追加新分块。", "conflict")

            total = int(
                db.execute(
                    "SELECT COUNT(*) AS n FROM roster_rows WHERE roster_id=?",
                    (roster_id,),
                ).fetchone()["n"]
            )
            accepted = 0
            duplicates = 0
            rejected: List[Dict[str, str]] = []
            seen: Dict[str, Dict[str, Any]] = {}

            for row in canonical_rows:
                outcome, body = self._classify_row(row)
                if outcome is not None:
                    rejected.append(outcome)
                    continue
                assert body is not None
                row_id = str(body["row_id"])
                previous = seen.get(row_id)
                if previous is not None:
                    if previous == body:
                        duplicates += 1
                    else:
                        rejected.append(
                            self._rejection(row_id, "duplicate_row_id", "同一分块内 row_id 重复且内容不一致。")
                        )
                    continue
                seen[row_id] = body
                stored = db.execute(
                    "SELECT body FROM roster_rows WHERE roster_id=? AND row_id=?",
                    (roster_id, row_id),
                ).fetchone()
                if stored is not None:
                    if _decode(stored["body"]) == body:
                        duplicates += 1
                    else:
                        rejected.append(
                            self._rejection(
                                row_id,
                                "duplicate_row_id",
                                "该 row_id 已在其他分块使用且内容不一致。",
                            )
                        )
                    continue
                if total >= MAX_ROSTER_ROWS:
                    rejected.append(
                        self._rejection(
                            row_id,
                            "row_limit_exceeded",
                            f"名单最多接收 {MAX_ROSTER_ROWS} 行，本行未登记。",
                        )
                    )
                    continue
                db.execute(
                    "INSERT INTO roster_rows (roster_id, tenant, row_id, body, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (roster_id, tenant, row_id, pack(body), created),
                )
                total += 1
                accepted += 1

            present = {
                int(row["chunk_index"]): int(row["rejected_rows"])
                for row in db.execute(
                    "SELECT chunk_index, rejected_rows FROM roster_chunks "
                    "WHERE tenant=? AND batch_key=?",
                    (tenant, batch_key),
                ).fetchall()
            }
            present[chunk_index] = len(rejected)
            complete = (
                len(present) == chunk_count
                and all(count == 0 for count in present.values())
                and total <= MAX_ROSTER_ROWS
            )

            snapshot_id: Optional[str] = None
            if complete:
                current = db.execute(
                    "SELECT input_snapshot_id, complete FROM rosters WHERE id=? AND tenant=?",
                    (roster_id, tenant),
                ).fetchone()
                if current is not None and int(current["complete"]):
                    snapshot_id = str(current["input_snapshot_id"])
                    if not snapshot_id:
                        snapshot_id = self._new_input_snapshot(db, tenant, roster_id)
                        db.execute(
                            "UPDATE rosters SET input_snapshot_id=?, row_count=? "
                            "WHERE id=? AND tenant=?",
                            (snapshot_id, total, roster_id, tenant),
                        )
                else:
                    if current is None:
                        db.execute(
                            "INSERT INTO rosters "
                            "(id, tenant, batch_key, input_snapshot_id, row_count, complete, created_at) "
                            "VALUES (?, ?, ?, '', ?, 0, ?)",
                            (roster_id, tenant, batch_key, total, created),
                        )
                    snapshot_id = self._new_input_snapshot(db, tenant, roster_id)
                    db.execute(
                        "UPDATE rosters SET input_snapshot_id=?, row_count=?, complete=1, "
                        "completed_at=? WHERE id=? AND tenant=?",
                        (snapshot_id, total, created, roster_id, tenant),
                    )

            receipt: Dict[str, Any] = {
                "batch_key": batch_key,
                "chunk_index": chunk_index,
                "chunk_count": chunk_count,
                "status": "complete" if complete else "partial",
                "content_hash": content_hash,
                "accepted_rows": accepted,
                "duplicate_rows": duplicates,
                "rejected_rows": len(rejected),
                "rejected": rejected[:MAX_ROWS_PER_CHUNK],
            }
            if complete and snapshot_id:
                receipt["roster_id"] = roster_id
            else:
                receipt["next_chunk_index"] = self._next_chunk_index(
                    present, chunk_count, chunk_index
                )

            db.execute(
                "INSERT INTO roster_chunks "
                "(tenant, batch_key, chunk_index, chunk_count, content_hash, accepted_rows, "
                "duplicate_rows, rejected_rows, receipt, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    tenant,
                    batch_key,
                    chunk_index,
                    chunk_count,
                    content_hash,
                    accepted,
                    duplicates,
                    len(rejected),
                    pack(receipt),
                    created,
                ),
            )
            return self._freshen_receipt(db, tenant, roster_id, receipt)

    def _new_input_snapshot(self, db: Any, tenant: str, roster_id: str) -> str:
        return self.store.new_snapshot(db, tenant, roster_id, sealed=True)

    def _freshen_receipt(
        self,
        db: Any,
        tenant: str,
        roster_id: str,
        receipt: Dict[str, Any],
    ) -> Dict[str, Any]:
        roster = db.execute(
            "SELECT input_snapshot_id, complete FROM rosters WHERE id=? AND tenant=?",
            (roster_id, tenant),
        ).fetchone()
        if roster is not None and int(roster["complete"]):
            snapshot_id = str(roster["input_snapshot_id"])
            if snapshot_id:
                receipt = dict(receipt)
                receipt["status"] = "complete"
                receipt["roster_id"] = roster_id
                receipt.pop("next_chunk_index", None)
        return receipt

    def _next_chunk_index(
        self,
        present: Dict[int, int],
        chunk_count: int,
        chunk_index: int,
    ) -> int:
        for index in range(1, chunk_count + 1):
            if index not in present:
                return index
        rejected = [
            index
            for index in range(1, chunk_count + 1)
            if present.get(index, 0)
        ]
        if rejected:
            return min(rejected)
        return min(max(chunk_index, 1), chunk_count)

    def _canonical_input_row(self, raw: Any) -> Dict[str, Any]:
        row = _object(raw, "rows[]")
        unknown = sorted(set(row) - set(ROW_KEYS))
        if unknown:
            _bad(f"rows[] 包含未知字段：{', '.join(unknown)}。")
        canonical: Dict[str, Any] = {}
        if "row_id" in row:
            if not isinstance(row["row_id"], str):
                _bad("rows[].row_id 必须是字符串。")
            canonical["row_id"] = row["row_id"].strip()
        if "enterprise_name" in row:
            if not isinstance(row["enterprise_name"], str):
                _bad("rows[].enterprise_name 必须是字符串。")
            canonical["enterprise_name"] = row["enterprise_name"].strip()
        if "social_credit_code" in row:
            if not isinstance(row["social_credit_code"], str):
                _bad("rows[].social_credit_code 必须是字符串。")
            canonical["social_credit_code"] = row["social_credit_code"].strip()
        if "source_row_number" in row:
            canonical["source_row_number"] = _integer(
                row["source_row_number"], "rows[].source_row_number", 1, 10_000_000
            )
        return canonical

    def _classify_row(
        self, row: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, Any]]]:
        raw_row_id = str(row.get("row_id", ""))
        if ROW_ID_RE.fullmatch(raw_row_id) is None:
            placeholder = "row_invalid_" + digest({"invalid_row_id": raw_row_id}).split(":", 1)[-1][:16]
            return (
                self._rejection(placeholder, "invalid_row_id", "row_id 格式不合法，应以 row_ 开头。"),
                None,
            )
        name = str(row.get("enterprise_name", "")).strip()
        code = str(row.get("social_credit_code", "")).strip()
        if len(name) > 300:
            return (
                self._rejection(raw_row_id, "missing_identity", "enterprise_name 超过 300 字符。"),
                None,
            )
        if code and CREDIT_CODE_RE.fullmatch(code) is None:
            return (
                self._rejection(
                    raw_row_id,
                    "invalid_credit_code",
                    "统一社会信用代码格式不合法，应提供 18 位有效代码或仅提供企业名称。",
                ),
                None,
            )
        if not name and not code:
            return (
                self._rejection(raw_row_id, "missing_identity", "每行至少需要企业名称或统一社会信用代码。"),
                None,
            )
        body: Dict[str, Any] = {"row_id": raw_row_id}
        if name:
            body["enterprise_name"] = name
        if code:
            body["social_credit_code"] = code
        if "source_row_number" in row:
            body["source_row_number"] = int(row["source_row_number"])
        return None, body

    @staticmethod
    def _rejection(row_id: str, code: str, message: str) -> Dict[str, str]:
        return {"row_id": row_id, "code": code, "message": message[:300]}

    # -- base plan ----------------------------------------------------------

    def base(self, tenant: str, args: Dict[str, Any]) -> Dict[str, Any]:
        tenant = _text(tenant, "tenant", maximum=128)
        data = _object(args, "args")
        _exact_keys(data, ("roster_id", "business_scope_hash", "limits"), "args")
        roster_id = _text(
            data["roster_id"], "roster_id", minimum=15, maximum=71, pattern=ROSTER_ID_RE
        )
        business_scope_hash = _sha256(data["business_scope_hash"], "business_scope_hash")
        limits = _limits(data["limits"])

        with self.store.tx(write=True) as db:
            roster = db.execute(
                "SELECT * FROM rosters WHERE id=? AND tenant=?",
                (roster_id, tenant),
            ).fetchone()
            if roster is None or not int(roster["complete"]):
                _bad("名单尚未完整接收，无法生成计划；请补齐全部分块后再试。", "roster_incomplete")
            snapshot_id = str(roster["input_snapshot_id"])
            if not snapshot_id:
                _bad("名单输入快照缺失，无法生成计划。", "roster_incomplete")
            self.store.require_snapshot(db, tenant, snapshot_id)

            rows = [
                _decode(row["body"])
                for row in db.execute(
                    "SELECT body FROM roster_rows WHERE roster_id=? ORDER BY row_id",
                    (roster_id,),
                ).fetchall()
            ]
            if not rows:
                _bad("名单为空，无法生成基础计划；请上传至少一行。", "roster_empty")

            unique_count, name_only_unique = self._roster_stats(rows)
            pages = limits["max_pages_per_view"]
            core_estimated = max(unique_count, 1)
            products: List[Dict[str, Any]] = []
            raw_estimates: Dict[str, int] = {}
            for product_id in BASE_PRODUCT_IDS:
                route = ROUTES[product_id]
                if product_id in CORE_BASE_IDS:
                    estimated = core_estimated
                    maximum = max(core_estimated, 1)
                    if product_id == CORE_BASE_IDS[0]:
                        backcheck_cap = max(1, pages * min(50, limits["max_page_size"]))
                        maximum = max(
                            maximum, unique_count + name_only_unique * backcheck_cap
                        )
                        note = (
                            "按去重后的输入身份+简称候选回查上限估算；回查候选触顶即截断，"
                            "不扩张名单也不冒充完整。"
                        )
                    else:
                        note = "按去重后的输入身份估算；不扩张名单。"
                elif product_id == IDENTITY_PRODUCT_ID:
                    estimated = name_only_unique * pages
                    maximum = max(estimated, 1)
                    note = "仅无信用代码行走关键词候选；每行最多按页面帽估算，候选须逐行确认。"
                elif product_id in SUPPLEMENT_PRODUCT_IDS:
                    estimated = 0
                    maximum = max(unique_count, 1)
                    note = f"条件补全：基础三项出现缺口时才调用；预计 0，封顶 {maximum} 次，触顶截断。"
                else:
                    estimated = 0
                    maximum = 1
                    note = "本阶段不主动调用。"
                raw_estimates[product_id] = estimated
                products.append(
                    self._product_spec(
                        product_id,
                        estimated,
                        maximum,
                        limits,
                        note=note,
                        route=route,
                    )
                )

            blockers: List[Dict[str, str]] = []
            self._limit_blockers(blockers, limits, unique_count, products)
            self._estimate_overflow_blockers(blockers, raw_estimates)
            plan = self._build_plan(
                stage="base",
                roster_id=roster_id,
                snapshot_id=snapshot_id,
                frozen_args=data,
                limits=limits,
                product_specs=products,
                blockers=blockers,
                scope={
                    "row_count": len(rows),
                    "topic_keys": [],
                    "product_ids": list(BASE_PRODUCT_IDS),
                },
                period=None,
                business_scope_hash=business_scope_hash,
                analysis_scope_hash=None,
            )
            return self._save_plan(db, tenant, "base", roster_id, snapshot_id, data, plan=plan)

    @staticmethod
    def _roster_stats(rows: Sequence[Dict[str, Any]]) -> Tuple[int, int]:
        keys = set()
        name_only = set()
        for row in rows:
            key = _identity_key(row)
            keys.add(key)
            if not row.get("social_credit_code"):
                name_only.add(key)
        return len(keys), len(name_only)

    # -- resolution plan ----------------------------------------------------

    def resolve(self, tenant: str, args: Dict[str, Any]) -> Dict[str, Any]:
        tenant = _text(tenant, "tenant", maximum=128)
        data = _object(args, "args")
        _exact_keys(data, ("snapshot_id", "decisions", "limits"), "args")
        parent_id = _text(data["snapshot_id"], "snapshot_id", minimum=13, maximum=69, pattern=SNAPSHOT_ID_RE)
        limits = _limits(data["limits"])
        if not isinstance(data["decisions"], list) or not 1 <= len(data["decisions"]) <= 2_000:
            _bad("decisions 必须在 1..2000 条之间。")
        decisions = self._validate_decisions(data["decisions"])
        cache_key = digest(["resolution", parent_id, decisions, limits])

        with self.store.tx(write=True) as db:
            parent = self.store.require_snapshot(db, tenant, parent_id)
            roster_id = parent["roster_id"]
            cached = db.execute(
                "SELECT scope_plan FROM plans WHERE tenant=? AND cache_key=? AND expires_at>? "
                "ORDER BY created_at DESC LIMIT 1", (tenant, cache_key, now()),
            ).fetchone()
            if cached is not None:
                return _decode(cached["scope_plan"])

            sid = self.store.new_snapshot(db, tenant, roster_id, parent_id=parent_id)
            pending = set()
            detached = set()
            for decision in decisions:
                row_id = decision["row_id"]
                row = self.store.record(db, parent_id, "identities", row_id)
                if row is None:
                    _bad("名单行不属于该已查证快照。", "out_of_scope")
                old_subject = row.get("subject_id")
                if decision["action"] == "select_candidate":
                    candidate = self.store.record(db, parent_id, "candidates", decision["candidate_id"])
                    if candidate is None or candidate["row_id"] != row_id:
                        _bad("候选不属于该快照的指定名单行。", "out_of_scope")
                    credit, name_id = candidate.get("social_credit_code"), candidate.get("name_id")
                    old = self.store.record(db, parent_id, "subjects", old_subject) if old_subject else None
                    if old and (credit == old.get("social_credit_code") if credit and old.get("social_credit_code")
                                else bool(name_id and name_id == old.get("name_id"))):
                        continue
                    pending.add(("credit", credit) if credit else ("nameId", name_id))
                    # A human selection authorizes a proposed identity, not an upstream fact.
                    # Only successful registration verification may publish its new subject.
                    row["mapping_status"] = "ambiguous"
                    row["candidate_count"] = max(1, row["candidate_count"])
                else:
                    row["mapping_status"] = "excluded"
                if old_subject:
                    detached.add(row.pop("subject_id"))
                self.store.put(db, sid, "identities", row_id, row)
            if detached:
                self._rebuild_detached_subjects(db, sid, detached)
            self.store.seal(db, sid)

            count = len(pending)
            product_ids = (*CORE_BASE_IDS, *SUPPLEMENT_PRODUCT_IDS) if count else (CORE_BASE_IDS[0],)
            raw_estimates = {pid: count if pid in CORE_BASE_IDS else 0 for pid in product_ids}
            products = [
                self._product_spec(
                    pid, raw_estimates[pid], max(count, 1), limits, route=ROUTES[pid],
                    note=("仅核对用户选定的候选；工商核对通过再补基础画像，不重搜候选。"
                          if pid in CORE_BASE_IDS else "仅在本次新确权主体的基础字段缺失时补查。"),
                ) for pid in product_ids
            ]
            blockers = []
            if not count:
                blockers.append({"code": "no_applicable_subjects", "message": "排除或已有确权已记录在本快照，无需执行上游查询。"})
            self._limit_blockers(blockers, limits, count, products)
            self._estimate_overflow_blockers(blockers, raw_estimates)
            plan = self._build_plan(
                stage="resolution", roster_id=roster_id, snapshot_id=sid, frozen_args=data,
                limits=limits, product_specs=products, blockers=blockers,
                scope={"row_count": self._roster_row_count(db, tenant, roster_id),
                       "subject_count": count, "unique_subject_count": count,
                       "topic_keys": [], "product_ids": list(product_ids)},
                period=None, business_scope_hash=None, analysis_scope_hash=None,
            )
            return self._save_plan(db, tenant, "resolution", roster_id, sid, data, plan=plan, cache_key=cache_key)

    def _rebuild_detached_subjects(self, db, sid, detached):
        # A subject's public source_row_ids is capped; identities is the full row mapping.
        counts = dict.fromkeys(detached, 0)
        samples = {subject: [] for subject in detached}
        for row_id, row in self.store.records(db, sid, "identities"):
            subject = row.get("subject_id")
            if subject in counts:
                counts[subject] += 1
                if len(samples[subject]) < 1000:
                    samples[subject].append(row_id)
        for subject, count in counts.items():
            if not count:
                self.store.drop(db, sid, "subjects", subject)
                continue
            record = self.store.record(db, sid, "subjects", subject)
            record.update(source_row_ids=samples[subject], duplicate_row_count=min(1000, count - 1),
                          resolution_status="confirmed" if count == 1 else "deduplicated")
            self.store.put(db, sid, "subjects", subject, record)


    def _validate_decisions(self, decisions: Sequence[Any]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        seen_rows: set[str] = set()
        for decision in decisions:
            obj = _object(decision, "decisions[]")
            action = obj.get("action")
            if action == "select_candidate":
                _exact_keys(obj, ("row_id", "action", "candidate_id"), "decisions[]")
                row_id = _text(obj["row_id"], "decisions[].row_id", minimum=5, maximum=68, pattern=ROW_ID_RE)
                candidate_id = _text(
                    obj["candidate_id"],
                    "decisions[].candidate_id",
                    minimum=13,
                    maximum=69,
                    pattern=CANDIDATE_ID_RE,
                )
                if row_id in seen_rows:
                    _bad("同一 row_id 重复提交确权，拒绝后写覆盖。")
                seen_rows.add(row_id)
                normalized.append(
                    {"row_id": row_id, "action": "select_candidate", "candidate_id": candidate_id}
                )
            elif action == "exclude":
                allowed = {"row_id", "action", "reason_code", "note"}
                unknown = sorted(set(obj) - allowed)
                if unknown:
                    _bad(f"decisions[] 包含未知字段：{', '.join(unknown)}。")
                for required_key in ("row_id", "reason_code"):
                    if required_key not in obj:
                        _bad(f"decisions[] 缺少必填字段：{required_key}。")
                row_id = _text(obj["row_id"], "decisions[].row_id", minimum=5, maximum=68, pattern=ROW_ID_RE)
                reason = _text(obj["reason_code"], "decisions[].reason_code")
                if reason not in RESOLVE_REASONS:
                    _bad("decisions[].reason_code 不在允许的排除原因中。")
                if row_id in seen_rows:
                    _bad("同一 row_id 重复提交确权，拒绝后写覆盖。")
                seen_rows.add(row_id)
                item: Dict[str, Any] = {
                    "row_id": row_id,
                    "action": "exclude",
                    "reason_code": reason,
                }
                if "note" in obj:
                    item["note"] = _text(
                        obj["note"], "decisions[].note", minimum=0, maximum=300
                    )
                normalized.append(item)
            else:
                _bad("decisions[].action 必须是 select_candidate 或 exclude。")
        return normalized

    # -- topic plan ---------------------------------------------------------

    def topics(self, tenant: str, args: Dict[str, Any]) -> Dict[str, Any]:
        tenant = _text(tenant, "tenant", maximum=128)
        data = _object(args, "args")
        _exact_keys(
            data,
            ("snapshot_id", "analysis_scope_hash", "selections", "period", "limits"),
            "args",
        )
        snapshot_id = _text(
            data["snapshot_id"], "snapshot_id", minimum=13, maximum=69, pattern=SNAPSHOT_ID_RE
        )
        analysis_scope_hash = _sha256(data["analysis_scope_hash"], "analysis_scope_hash")
        limits = _limits(data["limits"])
        period = _period(data["period"])
        selections = data["selections"]
        if not isinstance(selections, list) or not (1 <= len(selections) <= 100):
            _bad("selections 必须在 1..100 条之间。")
        normalized_selections = self._validate_selections(selections)

        with self.store.tx(write=True) as db:
            snapshot = self.store.require_snapshot(db, tenant, snapshot_id)
            roster_id = str(snapshot["roster_id"] or "")
            if not roster_id or ROSTER_ID_RE.fullmatch(roster_id) is None:
                _bad("专题计划必须引用绑定完整名单的已发布快照。", "out_of_scope")

            product_subjects: Dict[str, set[str]] = {}
            chosen_products: List[str] = []
            cohort_keys: set[str] = set()
            all_subjects: set[str] = set()
            subject_count = 0
            topic_keys: List[str] = []
            subject_records: Dict[str, Dict[str, Any]] = {}

            for selection in normalized_selections:
                topic = selection["topic"]
                if topic not in topic_keys:
                    topic_keys.append(topic)
                cohort_keys.add(selection["cohort_key"])
                subject_count += len(selection["subject_ids"])
                all_subjects.update(selection["subject_ids"])
                for subject_id in selection["subject_ids"]:
                    record = subject_records.get(subject_id)
                    if record is None:
                        record = self.store.record(db, snapshot_id, "subjects", subject_id)
                        if record is None:
                            _bad("专题主体不属于该快照，已拒绝交叉引用。", "out_of_scope")
                        if record.get("resolution_status") not in ("confirmed", "deduplicated"):
                            _bad("专题只能包含已确权主体；请先完成候选确认。", "out_of_scope")
                        subject_records[subject_id] = dict(record)
                for product_id in selection["product_ids"]:
                    if product_id not in chosen_products:
                        chosen_products.append(product_id)
                    product_subjects.setdefault(product_id, set()).update(selection["subject_ids"])

            month_span = _month_span(period["start_month"], period["end_month"])
            capped_months = max(0, min(month_span, period["max_months_per_entity"]))
            years_count = max(
                0,
                int(period["end_month"][:4]) - int(period["start_month"][:4]) + 1,
            )
            capped_years = max(0, min(years_count, period["max_months_per_entity"]))
            pages = limits["max_pages_per_view"]
            child_keys = limits["max_child_keys"]
            allowed_months = {
                _month_label(index)
                for index in range(
                    _month_index(period["start_month"]),
                    _month_index(period["end_month"]) + 1,
                )
            }

            blockers: List[Dict[str, str]] = []
            if month_span < 1:
                blockers.append(
                    {
                        "code": "period_out_of_range",
                        "message": "start_month 必须不晚于 end_month；当前期间无法形成有效企业月份。",
                    }
                )
            elif date.fromisoformat(period["observation_cutoff"]) < date(
                int(period["end_month"][:4]), int(period["end_month"][5:7]), 1
            ):
                blockers.append(
                    {
                        "code": "period_out_of_range",
                        "message": "observation_cutoff 早于 end_month 起始日，不能声称覆盖完整观察期。",
                    }
                )

            dependencies: List[Dict[str, Any]] = []
            added_products: List[str] = []
            for product_id in list(chosen_products):
                route = ROUTES[product_id]
                chain = route.get("chain")
                if not isinstance(chain, dict):
                    continue
                parent_id = str(chain.get("parent_product_id", ""))
                if not parent_id or parent_id not in ROUTES:
                    blockers.append(
                        {
                            "code": "product_topic_mismatch",
                            "message": "链式子产品的父产品不在可信产品目录中，无法安全纳入计划。",
                            "product_id": product_id,
                        }
                    )
                    continue
                parent_topic = str(ROUTES[parent_id].get("topic", ""))
                if parent_topic != route.get("topic"):
                    blockers.append(
                        {
                            "code": "product_topic_mismatch",
                            "message": "链式父产品与子产品不属于同一专题，拒绝跨专题拼接。",
                            "product_id": product_id,
                        }
                    )
                    continue
                subjects = product_subjects.get(product_id, set())
                if not subjects:
                    continue
                reusable = self._parent_reusable(
                    db, snapshot_id, parent_id, subjects, chain, allowed_months
                )
                if not reusable:
                    existing_parent_subjects = product_subjects.setdefault(parent_id, set())
                    already_selected = parent_id in chosen_products
                    existing_parent_subjects.update(subjects)
                    if not already_selected:
                        chosen_products.append(parent_id)
                        added_products.append(parent_id)
                    dependency = {
                        "product_id": parent_id,
                        "for_product_id": product_id,
                        "topic": str(route.get("topic", "")),
                        "reason": "chain_parent_inclusion",
                        "subject_ids": sorted(subjects),
                    }
                    if dependency not in dependencies:
                        dependencies.append(dependency)

            ordered_products = [
                pid for pid in PRODUCT_ORDER if pid in set(chosen_products) | set(added_products)
            ]
            if not ordered_products:
                _bad("所选专题没有任何可取数产品。")

            products: List[Dict[str, Any]] = []
            raw_estimates: Dict[str, int] = {}
            for product_id in ordered_products:
                route = ROUTES[product_id]
                subjects = product_subjects.get(product_id, set())
                subject_total = len(subjects)
                chain = route.get("chain")
                chain_child = isinstance(chain, dict) and product_id in chosen_products
                if chain_child:
                    if self._is_paged(product_id):
                        estimated = subject_total * child_keys * pages
                        note = (
                            f"按每主体最多 {child_keys} 个可追溯父键、每键最多 {pages} 页估算；"
                            "父键缺失即截断。"
                        )
                    else:
                        estimated = subject_total * child_keys
                        note = f"按每主体最多 {child_keys} 个可追溯父键估算；不接受调用方注入子键。"
                elif self._is_monthly(product_id, route):
                    estimated = subject_total * capped_months
                    note = f"按每主体最多 {capped_months} 个观察月估算；无数据不补零。"
                elif self._is_yearly(product_id, route):
                    estimated = subject_total * capped_years
                    note = f"按每主体最多 {capped_years} 个年度键估算；未返回年份不补零。"
                elif self._is_paged(product_id):
                    estimated = subject_total * pages
                    note = f"按每主体最多 {pages} 页估算；触顶即标记部分覆盖。"
                else:
                    estimated = subject_total
                    note = "按去重主体一次请求估算；不推导集团或扩大名单。"
                if product_id in added_products:
                    note = self._note(
                        "链式父产品：因缺少可复用父键而显式纳入本计划；"
                        + note
                    )
                if product_id in SUPPLEMENT_PRODUCT_IDS:
                    note = self._note("条件补全产品：" + note)
                raw_estimates[product_id] = estimated
                maximum = max(estimated, 1)
                products.append(
                    self._product_spec(
                        product_id,
                        estimated,
                        maximum,
                        limits,
                        note=note,
                        route=route,
                    )
                )

            unique_subject_count = len(all_subjects)
            if unique_subject_count > 100_000 or subject_count > 100_000:
                _bad(
                    "专题主体数超过单计划 100000 上限，请拆分计划或缩小主体集合。",
                    "invalid_argument",
                )
            entity_month_count = unique_subject_count * capped_months
            if entity_month_count > 10_000_000:
                _bad(
                    "企业月份数超过单计划上限，请缩短观察期或拆分计划。",
                    "invalid_argument",
                )
            if unique_subject_count > limits["max_targets"]:
                blockers.append(
                    {
                        "code": "subject_limit_exceeded",
                        "message": "已确权主体数超过 max_targets，请缩小专题主体集合或提高上限。",
                    }
                )
            self._limit_blockers(blockers, limits, unique_subject_count, products)
            self._estimate_overflow_blockers(blockers, raw_estimates)

            scope = {
                "row_count": self._roster_row_count(db, tenant, roster_id),
                "subject_count": subject_count,
                "unique_subject_count": unique_subject_count,
                "cohort_count": len(cohort_keys),
                "topic_keys": [topic for topic in TOPIC_KEYS if topic in topic_keys],
                "product_ids": ordered_products,
                "entity_month_count": entity_month_count,
            }
            frozen_args = {
                "snapshot_id": snapshot_id,
                "analysis_scope_hash": analysis_scope_hash,
                "selections": normalized_selections,
                "period": period,
                "limits": limits,
                "dependencies": dependencies,
            }
            plan = self._build_plan(
                stage="topics",
                roster_id=roster_id,
                snapshot_id=snapshot_id,
                frozen_args=frozen_args,
                limits=limits,
                product_specs=products,
                blockers=blockers,
                scope=scope,
                period=period,
                business_scope_hash=None,
                analysis_scope_hash=analysis_scope_hash,
            )
            return self._save_plan(
                db,
                tenant,
                "topics",
                roster_id,
                snapshot_id,
                frozen_args,
                plan=plan,
            )

    def _validate_selections(self, selections: Sequence[Any]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        seen_selections: set[str] = set()
        for selection in selections:
            obj = _object(selection, "selections[]")
            _exact_keys(obj, ("cohort_key", "topic", "subject_ids", "product_ids"), "selections[]")
            cohort_key = _text(obj["cohort_key"], "selections[].cohort_key", maximum=80)
            topic = _text(obj["topic"], "selections[].topic")
            if topic not in PRODUCT_IDS_BY_TOPIC:
                _bad("selections[].topic 不是受支持的专题。")
            subject_ids = obj["subject_ids"]
            if not isinstance(subject_ids, list) or not (1 <= len(subject_ids) <= 5_000):
                _bad("selections[].subject_ids 必须在 1..5000 个之间。")
            normalized_subjects: List[str] = []
            seen_subjects: set[str] = set()
            for subject_id in subject_ids:
                value = _text(
                    subject_id,
                    "selections[].subject_ids[]",
                    minimum=13,
                    maximum=69,
                    pattern=SUBJECT_ID_RE,
                )
                if value in seen_subjects:
                    _bad("同一 selection 中 subject_ids 不能重复。")
                seen_subjects.add(value)
                normalized_subjects.append(value)
            product_ids = obj["product_ids"]
            allowed_products = PRODUCT_IDS_BY_TOPIC[topic]
            if not isinstance(product_ids, list) or not product_ids:
                _bad("selections[].product_ids 至少需要一个产品。")
            if len(product_ids) > len(allowed_products):
                _bad("selections[].product_ids 超过该专题产品上限。")
            normalized_products: List[str] = []
            seen_products: set[str] = set()
            for product_id in product_ids:
                value = _text(product_id, "selections[].product_ids[]")
                if value not in allowed_products:
                    _bad("产品不属于所选专题，拒绝跨专题拼接。")
                if value in seen_products:
                    _bad("同一 selection 中 product_ids 不能重复。")
                seen_products.add(value)
                normalized_products.append(value)
            item = {
                "cohort_key": cohort_key,
                "topic": topic,
                "subject_ids": normalized_subjects,
                "product_ids": normalized_products,
            }
            marker = pack(item)
            if marker in seen_selections:
                _bad("selections 不能包含完全重复的专题选择。")
            seen_selections.add(marker)
            normalized.append(item)
        return normalized

    def _parent_reusable(
        self,
        db: Any,
        snapshot_id: str,
        parent_id: str,
        subject_ids: set[str],
        chain: Dict[str, Any],
        allowed_months: set[str],
    ) -> bool:
        sources = []
        for mapping in chain.get("mapping") or []:
            source = str(mapping.get("source", "")).replace("[]", "")
            if source:
                sources.append(source)
        if not sources or not subject_ids:
            return False
        required = set(sources)
        monthly_parent = self._is_monthly(parent_id, ROUTES[parent_id])
        seen: Dict[str, set[str]] = {subject_id: set() for subject_id in subject_ids}
        try:
            for _, record in self.store.records(db, snapshot_id, "evidence"):
                if str(record.get("product_id")) != parent_id:
                    continue
                if str(record.get("status", "")) not in ("observed", "zero"):
                    continue
                if monthly_parent:
                    business_time = record.get("business_time") or {}
                    if (
                        business_time.get("granularity") != "month"
                        or str(business_time.get("value")) not in allowed_months
                    ):
                        continue
                entity = str(record.get("entity_id", ""))
                if entity not in seen:
                    continue
                field = str(record.get("source_field", "")).replace("[]", "")
                if field:
                    seen[entity].add(field)
                if all(required <= fields for fields in seen.values()):
                    return True
        except Fault:
            return False
        return all(required <= fields for fields in seen.values())

    @staticmethod
    def _is_paged(product_id: str) -> bool:
        fields = ROUTES[product_id].get("inputs", {}).get("fields") or []
        return any(str(field.get("name")) in ("pageIndex", "pageSize") for field in fields)

    @staticmethod
    def _is_monthly(product_id: str, route: Dict[str, Any]) -> bool:
        if (route.get("time") or {}).get("filter") == "statsMonth":
            return True
        return (route.get("inputs") or {}).get("subject") == "enterprise_month"

    @staticmethod
    def _is_yearly(product_id: str, route: Dict[str, Any]) -> bool:
        return (route.get("time") or {}).get("filter") == "arYear"

    # -- shared plan construction ------------------------------------------

    def _product_spec(
        self,
        product_id: str,
        estimated: int,
        maximum: int,
        limits: Dict[str, int],
        *,
        note: str,
        route: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "product_id": product_id,
            "routing_key": str(route.get("topic")),
            "estimated_calls": min(max(0, int(estimated)), MAX_CALLS),
            "max_calls": min(
                max(1, int(maximum)), MAX_CALLS, max(limits["max_calls"], 1)
            ),
            "note": self._note(note),
        }

    def _limit_blockers(
        self,
        blockers: List[Dict[str, str]],
        limits: Dict[str, int],
        unique_subject_count: int,
        products: Sequence[Dict[str, Any]],
    ) -> None:
        estimated_calls = 0
        for product in products:
            estimated_calls += int(product["estimated_calls"])
            if int(product["estimated_calls"]) > limits["max_calls"]:
                blockers.append(
                    {
                        "code": "call_limit_insufficient",
                        "message": "该产品预计调用数超过 max_calls，请提高上限或缩小范围。",
                        "product_id": str(product["product_id"]),
                    }
                )
        if estimated_calls > limits["max_calls"]:
            blockers.append(
                {
                    "code": "call_limit_insufficient",
                    "message": "预计调用总数超过 max_calls，请提高上限或缩小名单/专题范围。",
                }
            )
        if unique_subject_count > limits["max_targets"]:
            # Callers add their own specific subject_limit blocker; avoid duplicates.
            if not any(item["code"] == "subject_limit_exceeded" for item in blockers):
                blockers.append(
                    {
                        "code": "subject_limit_exceeded",
                        "message": "主体数超过 max_targets，请提高上限或缩小范围。",
                    }
                )

    @staticmethod
    def _estimate_overflow_blockers(
        blockers: List[Dict[str, str]], raw_estimates: Dict[str, int]
    ) -> None:
        for product_id, estimated in raw_estimates.items():
            if estimated > MAX_CALLS:
                blockers.append(
                    {
                        "code": "call_limit_insufficient",
                        "message": "预计调用数超过单产品调用上限，执行只能按上限截断；请缩小范围。",
                        "product_id": product_id,
                    }
                )

    def _build_plan(
        self,
        *,
        stage: str,
        roster_id: str,
        snapshot_id: str,
        frozen_args: Dict[str, Any],
        limits: Dict[str, int],
        product_specs: Sequence[Dict[str, Any]],
        blockers: List[Dict[str, str]],
        scope: Dict[str, Any],
        period: Optional[Dict[str, Any]],
        business_scope_hash: Optional[str],
        analysis_scope_hash: Optional[str],
    ) -> Dict[str, Any]:
        products = list(product_specs)
        estimated_calls = min(
            sum(int(product["estimated_calls"]) for product in products),
            MAX_CALLS,
        )
        max_calls = min(
            limits["max_calls"],
            sum(int(product["max_calls"]) for product in products),
        )
        ttl_value = self.settings.plan_ttl
        ttl = int(900 if ttl_value is None else ttl_value)
        if ttl <= 0:
            raise Fault("configuration_error", "plan_ttl 必须是正整数秒数。")
        expires_at = later(ttl)
        plan_hash = digest(
            {
                "stage": stage,
                "roster_id": roster_id,
                "snapshot_id": snapshot_id,
                "args": frozen_args,
                "limits": limits,
                "scope": scope,
                "period": period,
                "business_scope_hash": business_scope_hash,
                "analysis_scope_hash": analysis_scope_hash,
                "products": products,
            }
        )
        plan: Dict[str, Any] = {
            "plan_id": uid("plan"),
            "plan_hash": plan_hash,
            "stage": stage,
            "executable": not blockers,
            "expires_at": expires_at,
            "roster_id": roster_id,
            "snapshot_id": snapshot_id,
            "scope": scope,
            "estimate": {
                "estimated_calls": estimated_calls,
                "max_calls": max_calls,
                "products": products,
            },
            "limits": limits,
            "human_summary": {
                "headline": self._headline(stage, scope, len(products))[:200],
                "bullets": self._bullets(
                    scope, limits, estimated_calls, max_calls, expires_at, blockers
                ),
            },
            "blockers": blockers[:100],
        }
        if business_scope_hash is not None:
            plan["business_scope_hash"] = business_scope_hash
        if analysis_scope_hash is not None:
            plan["analysis_scope_hash"] = analysis_scope_hash
        if period is not None:
            plan["period"] = period
        return plan

    def _save_plan(
        self,
        db: Any,
        tenant: str,
        stage: str,
        roster_id: str,
        snapshot_id: str,
        frozen_args: Dict[str, Any],
        *,
        plan: Dict[str, Any],
        cache_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        if cache_key:
            existing = db.execute(
                "SELECT scope_plan FROM plans WHERE tenant=? AND cache_key=? AND expires_at>? "
                "ORDER BY created_at DESC LIMIT 1",
                (tenant, cache_key, now()),
            ).fetchone()
        else:
            existing = db.execute(
                "SELECT scope_plan FROM plans WHERE tenant=? AND plan_hash=? AND expires_at>? "
                "ORDER BY created_at DESC LIMIT 1",
                (tenant, plan["plan_hash"], now()),
            ).fetchone()
        if existing is not None:
            return _decode(existing["scope_plan"])
        db.execute(
            "INSERT INTO plans "
            "(id, tenant, plan_hash, stage, roster_id, snapshot_id, args, scope_plan, "
            "expires_at, cache_key, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                plan["plan_id"],
                tenant,
                plan["plan_hash"],
                stage,
                roster_id,
                snapshot_id,
                pack(frozen_args),
                pack(plan),
                plan["expires_at"],
                cache_key,
                now(),
            ),
        )
        return plan

    def _roster_row_count(self, db: Any, tenant: str, roster_id: str) -> int:
        roster = db.execute(
            "SELECT row_count FROM rosters WHERE id=? AND tenant=?",
            (roster_id, tenant),
        ).fetchone()
        if roster is not None:
            return int(roster["row_count"])
        count = db.execute(
            "SELECT COUNT(*) AS n FROM roster_rows WHERE roster_id=?",
            (roster_id,),
        ).fetchone()
        return int(count["n"]) if count is not None else 0

    @staticmethod
    def _note(text: str) -> str:
        return text[:300]

    @staticmethod
    def _headline(stage: str, scope: Dict[str, Any], product_count: int) -> str:
        labels = {"base": "基础名单核对", "resolution": "候选确权", "topics": "专题取数"}
        return (
            f"{labels.get(stage, stage)}计划：{scope.get('row_count', 0)} 行名单，"
            f"{product_count} 项产品。"
        )

    @staticmethod
    def _bullets(
        scope: Dict[str, Any],
        limits: Dict[str, int],
        estimated_calls: int,
        max_calls: int,
        expires_at: str,
        blockers: Sequence[Dict[str, str]],
    ) -> List[str]:
        subject_text = (
            f"去重主体 {scope['unique_subject_count']}"
            if "unique_subject_count" in scope
            else f"名单行 {scope.get('row_count', 0)}"
        )
        bullets = [
            f"范围：{scope.get('row_count', 0)} 行、{subject_text}；"
            f"产品 {len(scope.get('product_ids', []))} 项。",
            f"调用：预计 {estimated_calls} 次，执行硬上限 {max_calls} 次；"
            f"主体上限 {limits['max_targets']}。",
            f"计划有效期至 {expires_at}。",
            "本工具只冻结范围与调用上限，不触发上游请求。",
            "达到分页、父子键或调用上限时停止并标记部分覆盖，不以截断结果冒充全量。",
        ]
        if blockers:
            bullets.append(f"当前有 {len(blockers)} 个阻断项，计划不可执行；请修正范围或名单。")
        return [item[:300] for item in bullets[:20]]
