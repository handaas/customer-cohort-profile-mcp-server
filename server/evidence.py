"""Projection helpers that turn raw upstream product data into evidence records.

This module is deliberately free of I/O, billing and SQLite concerns.  The
collector feeds it a trusted route entry from ``product-routes.json`` (via
``server.common.ROUTES``), a raw record and the request parameters; it returns
records shaped exactly like the ``evidence`` view of ``portrait_read_page``.

Design rules implemented here:

* raw values, source display strings and units are preserved; no local
  reformatting of percentages or currencies;
* a real zero and an unknown value are different statuses;
* business time is taken from source record fields where the product exposes
  them, otherwise the product's explicit period parameter, otherwise unknown;
* snapshot-only products are never presented as historical observations.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

MISSING = object()

DISPLAY_LIMIT = 2000
CONTAINER_LIMIT = 50
MAX_FIELDS_PER_RECORD = 200

_ZERO_RE = re.compile(
    r"^\s*[+-]?(?:0+(?:\.0*)?|\.0+)(?:[eE][+-]?\d+)?\s*[%％]?\s*$"
)
_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_THOUSANDS_RE = re.compile(r"^[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?$")
_SUFFIX_RE = re.compile(
    r"^\s*([+-]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*"
    r"(万元|亿元|万美元|万件|美元|元|万|%|％|人|个|家|件|千克|吨|年|月|天)?\s*$"
)
_NULL_TOKENS = {
    "",
    "-",
    "--",
    "—",
    "–",
    "/",
    "null",
    "none",
    "n/a",
    "na",
    "未披露",
    "未公开",
    "暂无",
    "无",
    "无数据",
}
_NA_TOKENS = {"不适用", "不涉及", "无此数据", "不纳入"}

_QUALITY_FLAGS = {
    "partial_page",
    "page_limited",
    "unit_inferred",
    "time_inferred",
    "source_duplicate",
    "parent_key_unverified",
}

# Units that the bundled product contract states explicitly.  These are not
# guesses: the notes in product-routes.json specify them for the exact fields
# below.  Everything else keeps ``none`` (source strings may still carry a
# unit suffix, which is preserved verbatim).
_UNIT_HINTS_BY_PRODUCT_NAME: Dict[str, Dict[str, str]] = {
    "店铺月销售情况": {
        "shopSalesAmountSumMth": "万元",
        "shopSalesVolumeSumMth": "万件",
        "shopSalesAmountSumMthYoy": "%",
        "shopSalesVolumeSumMthYoy": "%",
    },
    "企业外贸订单地区": {"amount": "美元", "weight": "千克"},
    "企业外贸出口趋势": {"amount": "美元"},
    "建筑工程项目明细": {"totalInvestment": "元"},
    "企业经营关系图谱": {
        "totalAmount": "万",
        "projectAmount": "万",
        "cooperationTimes": "次",
    },
}


# ---------------------------------------------------------------------------
# Generic traversal.
# ---------------------------------------------------------------------------


def get_path(value: Any, path: str) -> Any:
    """Resolve a dotted path; lists expand to the list of remaining values."""
    if not path or path == "$":
        return value
    parts = [part for part in path.split(".") if part]
    return _get_parts(value, parts)


def _get_parts(value: Any, parts: Sequence[str]) -> Any:
    if not parts:
        return value
    head, rest = parts[0], parts[1:]
    if isinstance(value, dict):
        if head not in value:
            return MISSING
        return _get_parts(value[head], rest)
    if isinstance(value, list):
        collected: List[Any] = []
        for item in value:
            found = _get_parts(item, parts)
            if found is MISSING:
                continue
            if isinstance(found, list):
                collected.extend(found)
            else:
                collected.append(found)
        return collected if collected else MISSING
    return MISSING


def _iter_leaves(value: Any, prefix: str = "", depth: int = 0) -> Iterable[Tuple[str, Any]]:
    """Yield scalar leaves and scalar lists; nested dicts are expanded."""
    if depth > 8:
        return
    if isinstance(value, dict):
        for key in sorted(value):
            child = value[key]
            path = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(child, dict):
                yield from _iter_leaves(child, path, depth + 1)
            else:
                yield path, child
        return
    yield prefix, value


# ---------------------------------------------------------------------------
# Route-aware record enumeration.
# ---------------------------------------------------------------------------


def extract_records(route: Dict[str, Any], data: Any) -> List[Tuple[str, int, Dict[str, Any]]]:
    """Return ``(record_path, index, record)`` for non-empty records."""
    if not isinstance(data, dict):
        return []
    paths = route.get("response", {}).get("record_paths") or ["$"]
    records: List[Tuple[str, int, Dict[str, Any]]] = []
    for path in paths:
        if path == "$":
            value: Any = data
        else:
            value = get_path(data, path)
        if isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, dict):
                    record = dict(item)
                else:
                    record = {"value": item}
                if record:
                    records.append((path, index, record))
        elif isinstance(value, dict):
            if value:
                records.append((path, 0, dict(value)))
        elif value is not MISSING and value not in (None, ""):
            records.append((path, 0, {"value": value}))
    return records


def page_total(route: Dict[str, Any], data: Any) -> Optional[int]:
    if not isinstance(data, dict):
        return None
    value = data.get("total")
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and value.is_integer():
        return int(value) if value >= 0 else None
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
    return None


def source_record_id(
    route: Dict[str, Any], record_path: str, record: Dict[str, Any], index: int
) -> str:
    keys = route.get("response", {}).get("split_keys") or []
    parts: List[str] = []
    for key in keys:
        if record_path != "$" and key.startswith(record_path + "."):
            lookup = key[len(record_path) + 1 :]
        elif record_path == "$" or key == record_path:
            lookup = key
        else:
            continue
        value = get_path(record, lookup)
        if value is MISSING or value in (None, ""):
            continue
        parts.append(_scalar_id(value))
    if parts:
        return ":".join(parts)[:200]
    canonical = json.dumps(record, sort_keys=True, ensure_ascii=False, default=str)
    return "rec_" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


def _scalar_id(value: Any) -> str:
    if isinstance(value, list):
        return "|".join(_scalar_id(item) for item in value)[:200]
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)[:200]
    return str(value)[:200]


# ---------------------------------------------------------------------------
# Time handling.
# ---------------------------------------------------------------------------


def relative_field(record_path: str, field: str) -> str:
    if record_path != "$" and field.startswith(record_path + "."):
        return field[len(record_path) + 1 :]
    return field


def parse_time(value: Any) -> Optional[Dict[str, str]]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not value.is_integer():
            return None
        year = int(value)
        if 1900 <= year <= 2100:
            return {"granularity": "year", "value": f"{year:04d}"}
        return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    patterns: List[Tuple[str, str]] = [
        ("day", r"^((?:19|20)\d{2})-(\d{2})-(\d{2})(?:[T ].*)?$"),
        ("day", r"^((?:19|20)\d{2})/(\d{2})/(\d{2})$"),
        ("day", r"^((?:19|20)\d{2})\.(\d{2})\.(\d{2})$"),
        ("day", r"^((?:19|20)\d{2})(\d{2})(\d{2})$"),
        ("day", r"^((?:19|20)\d{2})年(\d{1,2})月(\d{1,2})日"),
        ("month", r"^((?:19|20)\d{2})-(\d{2})$"),
        ("month", r"^((?:19|20)\d{2})(\d{2})$"),
        ("month", r"^((?:19|20)\d{2})年(\d{1,2})月"),
        ("year", r"^((?:19|20)\d{2})(?:年|年度)?$"),
    ]
    for granularity, pattern in patterns:
        match = re.match(pattern, text)
        if not match:
            continue
        try:
            if granularity == "day":
                year, month, day = (int(part) for part in match.groups())
                value_date = date(year, month, day)
                return {"granularity": "day", "value": value_date.isoformat()}
            if granularity == "month":
                year, month = int(match.group(1)), int(match.group(2))
                date(year, month, 1)
                return {"granularity": "month", "value": f"{year:04d}-{month:02d}"}
            if granularity == "year":
                year = int(match.group(1))
                return {"granularity": "year", "value": f"{year:04d}"}
        except ValueError:
            continue
    return None


def _is_temporal_field(field: str) -> bool:
    tail = field.rsplit(".", 1)[-1].lower()
    return any(
        token in tail
        for token in ("time", "date", "year", "month", "day", "日期", "时间", "年度", "年份")
    )


def _first_time_from_fields(
    record: Dict[str, Any], record_path: str, fields: Sequence[str]
) -> Optional[Dict[str, str]]:
    for field in fields:
        value = get_path(record, relative_field(record_path, field))
        if value is MISSING:
            continue
        if isinstance(value, list):
            for item in value:
                parsed = parse_time(item)
                if parsed:
                    return parsed
        else:
            parsed = parse_time(value)
            if parsed:
                return parsed
    return None


def business_time(
    route: Dict[str, Any],
    record_path: str,
    record: Dict[str, Any],
    request_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Return the source business period of a record (never acquired time)."""
    params = request_params or {}
    time_meta = route.get("time") or {}
    basis = time_meta.get("basis") or "snapshot"
    if basis == "snapshot":
        return {"granularity": "unknown"}

    fields = list(time_meta.get("fields") or [])
    if basis in ("month", "year", "year_series", "report_period"):
        if basis == "month":
            fields = ["statsMonth"] + fields
        elif basis == "year":
            fields = ["arYear", "year"] + fields
        elif basis == "year_series":
            fields = fields + ["year", "arYear"]
        parsed = _first_time_from_fields(record, record_path, fields)
        if parsed:
            return parsed
    elif basis in ("event_date", "event_interval"):
        parsed = _first_time_from_fields(record, record_path, fields)
        if parsed:
            return parsed
        if basis == "event_interval":
            parsed = _first_time_from_fields(
                record, record_path, list(time_meta.get("end_fields") or [])
            )
            if parsed:
                return parsed
        # Some products do not declare every date field; scan temporal leaves.
        for leaf, value in _iter_leaves(record):
            if _is_temporal_field(leaf):
                parsed = parse_time(value)
                if parsed:
                    return parsed

    for key in ("statsMonth", "arYear"):
        if key in params:
            parsed = parse_time(params[key])
            if parsed:
                return parsed

    if basis in ("event_date", "event_interval"):
        for leaf, value in _iter_leaves(record):
            if _is_temporal_field(leaf):
                parsed = parse_time(value)
                if parsed:
                    return parsed
    return {"granularity": "unknown"}


def _window_decision(
    bt: Dict[str, str],
    *,
    allowed_months: Optional[Sequence[str]],
    allowed_years: Optional[Sequence[str]],
    window_start: Optional[date],
    window_end: Optional[date],
    cutoff: Optional[date],
) -> Optional[str]:
    """Return include/exclude for one parsed time, or None when unknown."""
    granularity = bt.get("granularity")
    value = bt.get("value")
    if granularity in ("unknown", None) or not value:
        return None
    months = set(allowed_months or ())
    years = set(allowed_years or ())
    if granularity == "month":
        if months and value not in months:
            return "exclude"
        return "include"
    if granularity == "year":
        if years and value not in years:
            return "exclude"
        return "include"
    if granularity == "day":
        try:
            observed = date.fromisoformat(value)
        except ValueError:
            return None
        if window_start and observed < window_start:
            return "exclude"
        if window_end and observed > window_end:
            return "exclude"
        if cutoff and observed > cutoff:
            return "exclude"
        return "include"
    return None


def _parsed_day(bt: Optional[Dict[str, str]]) -> Optional[date]:
    """Best-effort calendar day for interval overlap; month/year use day one."""
    if not bt:
        return None
    value = bt.get("value")
    granularity = bt.get("granularity")
    if not value:
        return None
    try:
        if granularity == "day":
            return date.fromisoformat(value)
        if granularity == "month":
            year, month = (int(part) for part in value.split("-"))
            return date(year, month, 1)
        if granularity == "year":
            return date(int(value), 1, 1)
    except (TypeError, ValueError):
        return None
    return None


def _interval_decision(
    route: Dict[str, Any],
    record_path: str,
    record: Dict[str, Any],
    *,
    window_start: Optional[date],
    window_end: Optional[date],
    cutoff: Optional[date],
) -> str:
    """Include an interval when [start, end-or-open] overlaps the window.

    Used by products such as 经营异常 where 列入 and 移出 are two event dates.
    A missing end date is an open interval, never a confirmed resolution state.
    """
    time_meta = route.get("time") or {}
    start = _first_time_from_fields(record, record_path, time_meta.get("fields") or [])
    end = _first_time_from_fields(record, record_path, time_meta.get("end_fields") or [])
    start_day = _parsed_day(start)
    end_day = _parsed_day(end)
    if start_day is None and end_day is None:
        return "include"
    if window_end:
        if start_day is not None and start_day > window_end:
            return "exclude"
        if start_day is None and end_day is not None and end_day > window_end:
            return "include"
    if window_start and end_day is not None and end_day < window_start:
        return "exclude"
    if cutoff and start_day is not None and start_day > cutoff:
        return "exclude"
    return "include"


def period_decision(
    route: Dict[str, Any],
    bt: Dict[str, str],
    *,
    allowed_months: Optional[Sequence[str]],
    allowed_years: Optional[Sequence[str]],
    window_start: Optional[date],
    window_end: Optional[date],
    cutoff: Optional[date],
) -> str:
    """Return include/exclude for a record's business time.

    A record with unknown time is included: dropping it would silently lose
    real evidence, and its ``business_time`` remains ``unknown``.
    """
    basis = (route.get("time") or {}).get("basis") or "snapshot"
    if basis == "snapshot":
        return "include"
    decision = _window_decision(
        bt,
        allowed_months=allowed_months,
        allowed_years=allowed_years,
        window_start=window_start,
        window_end=window_end,
        cutoff=cutoff,
    )
    return decision or "include"


# ---------------------------------------------------------------------------
# Value normalization.
# ---------------------------------------------------------------------------


def _display(value: Any) -> str:
    if isinstance(value, str):
        text = value
    elif isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (int, float)):
        text = str(value)
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            text = str(value)
    if len(text) > DISPLAY_LIMIT:
        return text[: DISPLAY_LIMIT - 1] + "…"
    return text


def _truncate_unit(unit: str) -> str:
    text = (unit or "none").strip() or "none"
    return text[:32]


def _hint_unit(route: Dict[str, Any], field: str, record: Dict[str, Any]) -> Optional[str]:
    name = route.get("name")
    hints = _UNIT_HINTS_BY_PRODUCT_NAME.get(name or "")
    field_tail = field.rsplit(".", 1)[-1]
    if hints and field_tail in hints:
        return hints[field_tail]
    if field_tail == "value" and field != "value":
        prefix = field.rsplit(".", 1)[0]
        coin = get_path(record, prefix + ".coinType")
        if isinstance(coin, str) and coin.strip():
            return coin.strip()
    coin = get_path(record, "coinType")
    if field_tail in ("value", "amount") and isinstance(coin, str) and coin.strip():
        return coin.strip()
    return None


def _parse_unit_suffix(text: str) -> Optional[Tuple[float, Optional[str]]]:
    match = _SUFFIX_RE.match(text)
    if not match:
        return None
    raw_number = match.group(1)
    unit = match.group(2)
    try:
        number = float(raw_number.replace(",", ""))
    except ValueError:
        return None
    if unit:
        unit = unit.strip() or None
    return number, unit


def _looks_like_date_string(text: str) -> bool:
    stripped = text.strip()
    patterns = (
        r"^(?:19|20)\d{2}-\d{2}-\d{2}(?:[T ].*)?$",
        r"^(?:19|20)\d{2}/\d{2}/\d{2}$",
        r"^(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])$",
        r"^(?:19|20)\d{2}-(?:0[1-9]|1[0-2])$",
        r"^(?:19|20)\d{2}(?:0[1-9]|1[0-2])$",
        r"^(?:19|20)\d{2}年",
    )
    return any(re.match(pattern, stripped) for pattern in patterns)


def normalize_value(
    route: Dict[str, Any], field: str, value: Any, record: Dict[str, Any]
) -> Dict[str, Any]:
    """Normalize one source value while preserving raw/display/unit exactly."""
    hint = _hint_unit(route, field, record)
    flags: List[str] = []
    note: Optional[str] = None

    if value is None:
        return {
            "status": "not_disclosed",
            "display_value": "",
            "unit": _truncate_unit(hint or "none"),
        }

    if isinstance(value, bool):
        return {
            "status": "observed",
            "raw_value": value,
            "display_value": "true" if value else "false",
            "unit": _truncate_unit(hint or "none"),
        }

    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return {
                "status": "error",
                "display_value": "",
                "unit": _truncate_unit(hint or "none"),
                "note": "source returned a non-finite number",
            }
        unit = _truncate_unit(hint or "none")
        if value == 0:
            return {
                "status": "zero",
                "raw_value": value,
                "display_value": _display(value),
                "numeric_value": 0,
                "unit": unit,
            }
        return {
            "status": "observed",
            "raw_value": value,
            "display_value": _display(value),
            "numeric_value": value,
            "unit": unit,
        }

    if isinstance(value, (list, dict)):
        if isinstance(value, list) and len(value) > CONTAINER_LIMIT:
            return {
                "status": "truncated",
                "display_value": _display(value[:DISPLAY_LIMIT]),
                "unit": _truncate_unit(hint or "none"),
                "note": "source container exceeds output size limit",
            }
        if isinstance(value, dict) and len(value) > CONTAINER_LIMIT:
            return {
                "status": "truncated",
                "display_value": "",
                "unit": _truncate_unit(hint or "none"),
                "note": "source object exceeds output size limit",
            }
        display = _display(value)
        if len(display) >= DISPLAY_LIMIT:
            note = "display string truncated; raw container retained"
        return {
            "status": "observed",
            "raw_value": value,
            "display_value": display,
            "unit": _truncate_unit(hint or "none"),
            "quality_flags": flags,
            "note": note,
        }

    text = value if isinstance(value, str) else str(value)
    stripped = text.strip()
    if stripped.lower() in _NULL_TOKENS:
        return {
            "status": "not_disclosed",
            "display_value": text,
            "unit": _truncate_unit(hint or "none"),
        }
    if stripped in _NA_TOKENS:
        return {
            "status": "not_applicable",
            "display_value": text,
            "unit": _truncate_unit(hint or "none"),
        }
    if len(text) > DISPLAY_LIMIT:
        return {
            "status": "truncated",
            "display_value": text[: DISPLAY_LIMIT - 1] + "…",
            "unit": _truncate_unit(hint or "none"),
            "note": "source value exceeds output size limit",
        }

    if _is_temporal_field(field) or _looks_like_date_string(text):
        return {
            "status": "observed",
            "raw_value": text,
            "display_value": text,
            "unit": _truncate_unit(hint or "none"),
        }

    numeric: Optional[float] = None
    unit: Optional[str] = None
    parsed = _parse_unit_suffix(stripped)
    if parsed is not None:
        numeric, unit = parsed
    if numeric is None:
        if _DECIMAL_RE.match(stripped):
            try:
                numeric = float(stripped)
            except ValueError:
                numeric = None
        elif _THOUSANDS_RE.match(stripped):
            try:
                numeric = float(stripped.replace(",", ""))
            except ValueError:
                numeric = None
    if numeric is not None and not math.isfinite(numeric):
        numeric = None
        if not note:
            note = "source numeric value is not finite"
    if unit is None:
        unit = hint
    resolved_unit = _truncate_unit(unit or "none")

    if numeric is not None and numeric == 0 and _ZERO_RE.match(stripped):
        return {
            "status": "zero",
            "raw_value": text,
            "display_value": text,
            "numeric_value": 0,
            "unit": resolved_unit,
        }

    result: Dict[str, Any] = {
        "status": "observed",
        "raw_value": text,
        "display_value": text,
        "unit": resolved_unit,
    }
    if numeric is not None:
        result["numeric_value"] = numeric
        if numeric == 0:
            note = "zero value carries a source unit; kept as observed"
    if note:
        result["note"] = note
    return result


# ---------------------------------------------------------------------------
# Evidence records.
# ---------------------------------------------------------------------------


def _evidence_fields(
    route: Dict[str, Any], record_path: str, record: Dict[str, Any]
) -> List[Tuple[str, str]]:
    """Return ``(source_field, lookup_path)`` pairs for one record."""
    declared = list(route.get("response", {}).get("evidence") or [])
    if record_path != "$":
        prefix = record_path + "."
        prefixed = [field for field in declared if field.startswith(prefix)]
        if prefixed:
            return [(field, field[len(prefix) :]) for field in prefixed]
        leaves: List[Tuple[str, str]] = []
        for leaf, _value in _iter_leaves(record):
            if not leaf:
                continue
            leaves.append((f"{record_path}.{leaf}", leaf))
            if len(leaves) >= MAX_FIELDS_PER_RECORD:
                break
        return leaves
    fields: List[Tuple[str, str]] = []
    for field in declared:
        fields.append((field, field))
        if len(fields) >= MAX_FIELDS_PER_RECORD:
            break
    if not fields:
        for leaf, _value in _iter_leaves(record):
            if leaf:
                fields.append((leaf, leaf))
                if len(fields) >= MAX_FIELDS_PER_RECORD:
                    break
    return fields


def sanitize_source_field(field: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.\[\]-]", "_", str(field or "value"))
    if not re.match(r"^[A-Za-z_]", text):
        text = "f_" + text
    return text[:128] or "value"


def evidence_id(entity_id: str, product_id: str, record_id: str, field: str) -> str:
    digest = hashlib.sha256(
        "\x1f".join((entity_id, product_id, record_id, field)).encode("utf-8")
    ).hexdigest()
    return "ev_" + digest[:40]


def root_level_fields(route: Dict[str, Any]) -> List[str]:
    """Declared evidence fields that live at the response root, not in a record list."""
    record_paths = [
        path
        for path in (route.get("response", {}).get("record_paths") or [])
        if path != "$"
    ]
    if not record_paths:
        return []
    fields: List[str] = []
    for field in route.get("response", {}).get("evidence") or []:
        if any(field == path or field.startswith(path + ".") for path in record_paths):
            continue
        fields.append(field)
    return fields


def build_root_evidence(
    route: Dict[str, Any],
    data: Any,
    *,
    entity_id: str,
    acquired_at: str,
    record_id: str,
    request_params: Optional[Dict[str, Any]] = None,
    flags: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Project the response-root evidence fields of a list-shaped product."""
    if not isinstance(data, dict):
        return []
    fields = root_level_fields(route)
    if not fields:
        return []
    product_id = route.get("product_id")
    topic = route.get("topic")
    bt = business_time(route, "$", {}, request_params)
    base_flags = [flag for flag in (flags or ()) if flag in _QUALITY_FLAGS]
    evidence: List[Dict[str, Any]] = []
    for source_field in fields:
        display_field = sanitize_source_field(source_field)
        value = get_path(data, source_field)
        if value is MISSING:
            normalized = {"status": "not_returned", "display_value": "", "unit": "none"}
        else:
            normalized = normalize_value(route, source_field, value, data)
        status = normalized["status"]
        item: Dict[str, Any] = {
            "evidence_id": evidence_id(entity_id, product_id, record_id, display_field),
            "entity_id": entity_id,
            "product_id": product_id,
            "topic": topic,
            "source_field": display_field,
            "source_record_id": record_id[:200],
            "display_value": normalized.get("display_value", ""),
            "unit": normalized.get("unit", "none"),
            "business_time": dict(bt),
            "acquired_at": acquired_at,
            "status": status,
        }
        if status in ("observed", "zero") and "raw_value" in normalized:
            item["raw_value"] = normalized["raw_value"]
        if status in ("observed", "zero") and "numeric_value" in normalized:
            item["numeric_value"] = normalized["numeric_value"]
        if status == "zero":
            item["numeric_value"] = 0
        item_flags = list(base_flags)
        for flag in normalized.get("quality_flags") or ():
            if flag in _QUALITY_FLAGS and flag not in item_flags:
                item_flags.append(flag)
        if item_flags:
            item["quality_flags"] = item_flags[:10]
        note = normalized.get("note")
        if note:
            item["note"] = str(note)[:500]
        evidence.append(item)
    return evidence


def build_record_evidence(
    route: Dict[str, Any],
    record_path: str,
    record: Dict[str, Any],
    *,
    entity_id: str,
    acquired_at: str,
    record_id: str,
    request_params: Optional[Dict[str, Any]] = None,
    flags: Optional[Sequence[str]] = None,
    index: int = 0,
) -> List[Dict[str, Any]]:
    product_id = route.get("product_id")
    topic = route.get("topic")
    bt = business_time(route, record_path, record, request_params)
    base_flags = [flag for flag in (flags or ()) if flag in _QUALITY_FLAGS]
    evidence: List[Dict[str, Any]] = []
    fields = _evidence_fields(route, record_path, record)
    for source_field, lookup in fields:
        source_field = sanitize_source_field(source_field)
        value = get_path(record, lookup) if lookup else MISSING
        if value is MISSING:
            normalized: Dict[str, Any] = {
                "status": "not_returned",
                "display_value": "",
                "unit": "none",
            }
        else:
            normalized = normalize_value(route, lookup or source_field, value, record)
        status = normalized["status"]
        item: Dict[str, Any] = {
            "evidence_id": evidence_id(entity_id, product_id, record_id, source_field),
            "entity_id": entity_id,
            "product_id": product_id,
            "topic": topic,
            "source_field": source_field,
            "source_record_id": record_id[:200],
            "display_value": normalized.get("display_value", ""),
            "unit": normalized.get("unit", "none"),
            "business_time": dict(bt),
            "acquired_at": acquired_at,
            "status": status,
        }
        if status in ("observed", "zero"):
            if "raw_value" in normalized:
                item["raw_value"] = normalized["raw_value"]
            if "numeric_value" in normalized and status in ("observed", "zero"):
                item["numeric_value"] = normalized["numeric_value"]
        if status == "zero":
            item["numeric_value"] = 0
        item_flags = list(base_flags)
        for flag in normalized.get("quality_flags") or ():
            if flag in _QUALITY_FLAGS and flag not in item_flags:
                item_flags.append(flag)
        if item_flags:
            item["quality_flags"] = item_flags[:10]
        note = normalized.get("note")
        if note:
            item["note"] = str(note)[:500]
        evidence.append(item)
    return evidence


def _published_after_cutoff(
    route: Dict[str, Any],
    record_path: str,
    record: Dict[str, Any],
    cutoff: Optional[date],
) -> bool:
    """True when a record carries a disclosure/publish date beyond the cutoff.

    Report year (``arYear``) is not a publication date; a report for that year
    may only be used when its publication date is known to be at or before the
    observation cutoff.  When no publish date is present the record is kept and
    its ``acquired_at`` remains the only timing warning.
    """
    if cutoff is None:
        return False
    candidates: List[str] = []
    for field in (route.get("time") or {}).get("fields") or []:
        tail = field.rsplit(".", 1)[-1].lower()
        if "publish" in tail or "pubdate" in tail or "disclos" in tail:
            candidates.append(relative_field(record_path, field))
    for key in record:
        lower = str(key).lower()
        if "publish" in lower or "pubdate" in lower or "disclos" in lower:
            candidates.append(str(key))
    for path in candidates:
        value = get_path(record, path)
        if value is MISSING:
            continue
        values = value if isinstance(value, list) else [value]
        for item in values:
            parsed = parse_time(item)
            if not parsed:
                continue
            if parsed.get("granularity") == "day":
                try:
                    if date.fromisoformat(parsed["value"]) > cutoff:
                        return True
                except ValueError:
                    continue
            elif parsed.get("granularity") == "month":
                try:
                    year, month = (int(part) for part in parsed["value"].split("-"))
                    if date(year, month, 1) > cutoff:
                        return True
                except ValueError:
                    continue
            elif parsed.get("granularity") == "year":
                try:
                    if int(parsed["value"]) > cutoff.year:
                        return True
                except ValueError:
                    continue
    return False


def record_stats(
    route: Dict[str, Any],
    data: Any,
    *,
    request_params: Optional[Dict[str, Any]] = None,
    allowed_months: Optional[Sequence[str]] = None,
    allowed_years: Optional[Sequence[str]] = None,
    window_start: Optional[date] = None,
    window_end: Optional[date] = None,
    cutoff: Optional[date] = None,
) -> Dict[str, Any]:
    """Split one page into included/excluded records without projecting values."""
    records = extract_records(route, data)
    included: List[Tuple[str, int, Dict[str, Any]]] = []
    excluded: List[Tuple[str, int, Dict[str, Any]]] = []
    undated = 0
    late_publish = 0
    basis = (route.get("time") or {}).get("basis") or "snapshot"
    for path, index, record in records:
        if _published_after_cutoff(route, path, record, cutoff):
            excluded.append((path, index, record))
            late_publish += 1
            continue
        bt = business_time(route, path, record, request_params)
        if basis == "event_interval":
            decision = _interval_decision(
                route,
                path,
                record,
                window_start=window_start,
                window_end=window_end,
                cutoff=cutoff,
            )
        else:
            decision = period_decision(
                route,
                bt,
                allowed_months=allowed_months,
                allowed_years=allowed_years,
                window_start=window_start,
                window_end=window_end,
                cutoff=cutoff,
            )
        if decision == "exclude":
            excluded.append((path, index, record))
            continue
        if bt.get("granularity") == "unknown" and basis not in ("snapshot",):
            undated += 1
        included.append((path, index, record))
    return {
        "records": included,
        "excluded": excluded,
        "total": len(records),
        "undated": undated,
        "late_publish": late_publish,
    }
