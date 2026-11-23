#!/usr/bin/env python3
"""Export the design-time MCP contract for the customer-cohort-profile service.

This module is *not* an MCP server and performs no I/O beyond reading the
bundled ``product-routes.json`` and printing JSON.  It uses only the Python
standard library and has no dependency on a sibling skill, the old MCP server,
or a live catalogue service.  The emitted JSON Schemas are fully expanded and
self-contained.

Run from the repository root:

    python3 docs/customer-cohort-profile/mcp_contract.py

The stdout document has the MCP ``tools/list`` shape::

    {"tools": [{"name": ..., "description": ..., "inputSchema": ...,
                "outputSchema": ..., "annotations": ...}, ...]}

All schemas are JSON Schema 2020-12, self-contained, and strict
(``additionalProperties: false``).  Successful results are returned through
MCP ``structuredContent``; business failures use ``isError: true`` and the
error codes documented in each tool description.  This contract is money-free:
estimates and usage contain call counts only, and execution accepts exactly
``plan_id``/``plan_hash``.  Business scope confirmation belongs to the calling
Skill; this design file adds no separate confirmation step.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

JsonSchema = Dict[str, Any]
SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"

# ---------------------------------------------------------------------------
# Product catalogue: the single source of product IDs/names/topics is the
# bundled product-routes.json.  The schemas below are expanded from it, so the
# emitted contract has no external $ref and no second product whitelist.
# ---------------------------------------------------------------------------

PRODUCT_ROUTES_PATH = Path(__file__).with_name("product-routes.json")

BASE_ROUTING_KEYS: tuple[str, ...] = ("identity", "base", "supplement")
TOPIC_KEYS: tuple[str, ...] = (
    "factory",
    "channel",
    "projects",
    "construction",
    "trade",
    "food_chain",
    "estore",
    "technology",
    "office",
    "organization",
    "workforce",
    "performance",
    "listed",
)
ROUTING_KEYS: tuple[str, ...] = BASE_ROUTING_KEYS + TOPIC_KEYS


def _load_products(path: Path) -> Dict[str, Dict[str, str]]:
    """Read product_id/name/topic from the bundled route file.

    Only these three fields are copied.  The contract deliberately does not
    depend on the rest of the route file or on any live catalogue.
    """
    try:
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except OSError as exc:
        raise RuntimeError(f"cannot read bundled product routes: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON in bundled product routes: {path}") from exc

    routes = document.get("products") if isinstance(document, dict) else None
    if not isinstance(routes, list):
        raise RuntimeError("product-routes.json must contain a products array")

    products: Dict[str, Dict[str, str]] = {}
    for index, route in enumerate(routes):
        if not isinstance(route, dict):
            raise RuntimeError(f"products[{index}] must be an object")
        product_id = route.get("product_id")
        name = route.get("name")
        topic = route.get("topic")
        if not isinstance(product_id, str) or not product_id:
            raise RuntimeError(f"products[{index}].product_id must be a non-empty string")
        if not isinstance(name, str) or not name:
            raise RuntimeError(f"products[{index}].name must be a non-empty string")
        if topic not in ROUTING_KEYS:
            raise RuntimeError(
                f"products[{index}].topic={topic!r} is not one of {ROUTING_KEYS!r}"
            )
        if product_id in products:
            raise RuntimeError(f"duplicate product_id in product-routes.json: {product_id}")
        products[product_id] = {"name": name, "routing_key": topic}

    if len(products) != 37:
        raise RuntimeError(
            f"product-routes.json must contain 37 products, found {len(products)}"
        )
    return products


PRODUCTS: Dict[str, Dict[str, str]] = _load_products(PRODUCT_ROUTES_PATH)
BASE_PRODUCTS: Dict[str, Dict[str, str]] = {
    product_id: record
    for product_id, record in PRODUCTS.items()
    if record["routing_key"] in BASE_ROUTING_KEYS
}
TOPIC_PRODUCTS: Dict[str, Dict[str, str]] = {
    product_id: record
    for product_id, record in PRODUCTS.items()
    if record["routing_key"] in TOPIC_KEYS
}

if len(BASE_PRODUCTS) != 7 or len(TOPIC_PRODUCTS) != 30:
    raise RuntimeError(
        "product-routes.json must describe 7 base/supplement/identity products "
        f"and 30 topic products, found {len(BASE_PRODUCTS)} and {len(TOPIC_PRODUCTS)}"
    )

BASE_PRODUCT_IDS: tuple[str, ...] = tuple(BASE_PRODUCTS)
TOPIC_PRODUCT_IDS: tuple[str, ...] = tuple(TOPIC_PRODUCTS)
ALL_PRODUCT_IDS: tuple[str, ...] = tuple(PRODUCTS)
PRODUCT_IDS_BY_TOPIC: Dict[str, tuple[str, ...]] = {
    topic: tuple(
        product_id
        for product_id, record in TOPIC_PRODUCTS.items()
        if record["routing_key"] == topic
    )
    for topic in TOPIC_KEYS
}

if any(not PRODUCT_IDS_BY_TOPIC[topic] for topic in TOPIC_KEYS):
    missing = [topic for topic in TOPIC_KEYS if not PRODUCT_IDS_BY_TOPIC[topic]]
    raise RuntimeError(f"product-routes.json has no products for topics: {missing!r}")

TOOL_NAMES: tuple[str, ...] = (
    "portrait_stage_roster",
    "portrait_plan_base",
    "portrait_plan_resolution",
    "portrait_plan_topics",
    "portrait_execute_plan",
    "portrait_get_run",
    "portrait_read_page",
)

RUN_STATES: tuple[str, ...] = (
    "queued",
    "running",
    "partial",
    "succeeded",
    "failed",
    "cancelled",
)
PAGE_VIEWS: tuple[str, ...] = (
    "identities",
    "candidates",
    "subjects",
    "evidence",
    "coverage",
)
PLAN_STAGES: tuple[str, ...] = ("base", "resolution", "topics")

# ---------------------------------------------------------------------------
# Common patterns and small schema helpers.
# ---------------------------------------------------------------------------

SHA256_PATTERN = r"^sha256:[0-9a-f]{64}$"
CREDIT_CODE_PATTERN = r"^[0-9A-HJ-NP-RT-UW-Y]{18}$"
YEAR_MONTH_PATTERN = r"^(?:19|20)[0-9]{2}-(?:0[1-9]|1[0-2])$"
DATE_PATTERN = r"^(?:19|20)[0-9]{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])$"
YEAR_PATTERN = r"^(?:19|20)[0-9]{2}$"

BLOCKER_CODES: tuple[str, ...] = (
    "roster_incomplete",
    "roster_empty",
    "ambiguous_rows_unresolved",
    "no_applicable_subjects",
    "no_products_selected",
    "product_not_in_plan",
    "product_topic_mismatch",
    "call_limit_insufficient",
    "period_out_of_range",
    "analysis_scope_mismatch",
    "subject_limit_exceeded",
    "paging_limit_invalid",
    "snapshot_incomplete",
)

RUN_ERROR_CODES: tuple[str, ...] = (
    "invalid_argument",
    "plan_hash_mismatch",
    "plan_expired",
    "plan_not_executable",
    "limit_exceeded",
    "out_of_scope",
    "roster_incomplete",
    "no_applicable_subjects",
    "snapshot_not_found",
    "snapshot_expired",
    "cursor_invalid",
    "run_not_found",
    "conflict",
    "already_running",
    "upstream_unavailable",
    "partial_failure",
    "billing_unknown",
    "cancelled",
    "internal_error",
)

ERROR_NOTE = (
    "错误语义：成功时 structuredContent 必须严格符合本 outputSchema；"
    "协议参数错误按 MCP 规范返回；业务拒绝返回 isError=true 与脱敏原因，"
    "不得伪造成功对象，也不得把拒绝写成“查无企业”。"
    "重试：invalid_argument、plan_hash_mismatch、plan_expired、"
    "plan_not_executable、cursor_invalid、snapshot_not_found、run_not_found 不可重试；"
    "仅 upstream_unavailable、internal_error 等瞬态错误可在退避后重试；"
    "billing_unknown 表示上游是否已计费待核，不得自动重发。"
)


def _string(
    *,
    description: Optional[str] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    pattern: Optional[str] = None,
    enum: Optional[Sequence[str]] = None,
    const: Optional[str] = None,
    format: Optional[str] = None,
    default: Optional[str] = None,
) -> JsonSchema:
    schema: JsonSchema = {"type": "string"}
    if description is not None:
        schema["description"] = description
    if min_length is not None:
        schema["minLength"] = min_length
    if max_length is not None:
        schema["maxLength"] = max_length
    if pattern is not None:
        schema["pattern"] = pattern
    if enum is not None:
        schema["enum"] = list(enum)
    if const is not None:
        schema["const"] = const
    if format is not None:
        schema["format"] = format
    if default is not None:
        schema["default"] = default
    return schema


def _integer(
    *,
    description: Optional[str] = None,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
    const: Optional[int] = None,
    default: Optional[int] = None,
) -> JsonSchema:
    schema: JsonSchema = {"type": "integer"}
    if description is not None:
        schema["description"] = description
    if minimum is not None:
        schema["minimum"] = minimum
    if maximum is not None:
        schema["maximum"] = maximum
    if const is not None:
        schema["const"] = const
    if default is not None:
        schema["default"] = default
    return schema


def _number(
    *,
    description: Optional[str] = None,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> JsonSchema:
    schema: JsonSchema = {"type": "number"}
    if description is not None:
        schema["description"] = description
    if minimum is not None:
        schema["minimum"] = minimum
    if maximum is not None:
        schema["maximum"] = maximum
    return schema


def _boolean(*, description: Optional[str] = None, const: Optional[bool] = None) -> JsonSchema:
    schema: JsonSchema = {"type": "boolean"}
    if description is not None:
        schema["description"] = description
    if const is not None:
        schema["const"] = const
    return schema


def _literal(value: Any, *, description: Optional[str] = None) -> JsonSchema:
    schema: JsonSchema = {"const": value}
    if description is not None:
        schema["description"] = description
    return schema


def _enum(values: Sequence[str], *, description: Optional[str] = None) -> JsonSchema:
    return _string(enum=values, description=description)


def _array(
    items: JsonSchema,
    *,
    description: Optional[str] = None,
    min_items: Optional[int] = None,
    max_items: Optional[int] = None,
    unique_items: bool = False,
    default: Optional[list] = None,
) -> JsonSchema:
    schema: JsonSchema = {"type": "array", "items": items}
    if description is not None:
        schema["description"] = description
    if min_items is not None:
        schema["minItems"] = min_items
    if max_items is not None:
        schema["maxItems"] = max_items
    if unique_items:
        schema["uniqueItems"] = True
    if default is not None:
        schema["default"] = default
    return schema


def _strict_object(
    properties: Dict[str, JsonSchema],
    required: Sequence[str],
    *,
    description: Optional[str] = None,
    min_properties: Optional[int] = None,
    max_properties: Optional[int] = None,
) -> JsonSchema:
    schema: JsonSchema = {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }
    if description is not None:
        schema["description"] = description
    if min_properties is not None:
        schema["minProperties"] = min_properties
    if max_properties is not None:
        schema["maxProperties"] = max_properties
    return schema


def _conditional(if_schema: JsonSchema, then_schema: JsonSchema) -> JsonSchema:
    return {"if": if_schema, "then": then_schema}


def _not_required(property_name: str) -> JsonSchema:
    return {"not": {"required": [property_name]}}


def _date() -> JsonSchema:
    return _string(pattern=DATE_PATTERN, format="date", min_length=10, max_length=10)


def _year_month() -> JsonSchema:
    return _string(pattern=YEAR_MONTH_PATTERN, min_length=7, max_length=7)


def _date_time(*, description: Optional[str] = None) -> JsonSchema:
    return _string(
        format="date-time",
        pattern=(
            r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"(?:\.[0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})$"
        ),
        min_length=20,
        max_length=40,
        description=(
            description
            if description is not None
            else "RFC 3339 时间戳（UTC 或带偏移），例如 2026-11-23T08:30:00Z。"
        ),
    )


def _sha256(*, description: str) -> JsonSchema:
    return _string(
        pattern=SHA256_PATTERN,
        min_length=71,
        max_length=71,
        description=description,
    )


def _row_id() -> JsonSchema:
    return _string(
        pattern=r"^row_[A-Za-z0-9_-]{1,64}$",
        min_length=5,
        max_length=68,
        description="Skill 生成的稳定名单行标识；不得包含姓名、手机号或文件路径。",
    )


def _candidate_id() -> JsonSchema:
    return _string(
        pattern=r"^cand_[A-Za-z0-9_-]{8,64}$",
        min_length=13,
        max_length=69,
        description="快照中的候选引用；只能引用本快照该行的候选。",
    )


def _subject_id() -> JsonSchema:
    return _string(
        pattern=r"^subj_[A-Za-z0-9_-]{8,64}$",
        min_length=13,
        max_length=69,
        description="已确权独立主体引用（不是工商注册号或 nameId 本身）。",
    )


def _snapshot_id() -> JsonSchema:
    return _string(
        pattern=r"^snap_[A-Za-z0-9_-]{8,64}$",
        min_length=13,
        max_length=69,
        description="不可变结果快照引用；分页、候选和确权均绑定该快照。",
    )


def _roster_id() -> JsonSchema:
    return _string(
        pattern=r"^roster_[A-Za-z0-9_-]{8,64}$",
        min_length=15,
        max_length=71,
        description="名单完整接收后生成的不可变名单引用。",
    )


def _plan_id() -> JsonSchema:
    return _string(
        pattern=r"^plan_[A-Za-z0-9_-]{8,64}$",
        min_length=13,
        max_length=69,
        description="计划引用，只能与同一 plan_hash 使用。",
    )


def _run_id() -> JsonSchema:
    return _string(
        pattern=r"^run_[A-Za-z0-9_-]{8,64}$",
        min_length=12,
        max_length=68,
        description="运行引用；断线恢复使用同一引用。",
    )


def _cursor() -> JsonSchema:
    return _string(
        pattern=r"^cur_[A-Za-z0-9._~-]{8,512}$",
        min_length=12,
        max_length=516,
        description="不透明游标，绑定 snapshot_id 与 view；不得由调用方拼造。",
    )


# ---------------------------------------------------------------------------
# Shared input objects.
# ---------------------------------------------------------------------------


def _limits_schema() -> JsonSchema:
    return _strict_object(
        {
            "max_targets": _integer(
                minimum=1,
                maximum=100_000,
                description="本计划最多覆盖的独立主体数。",
            ),
            "max_calls": _integer(
                minimum=1,
                maximum=100_000,
                description="本次计划允许的上游调用总上限（含补页与子查询）。",
            ),
            "max_page_size": _integer(
                minimum=1,
                maximum=200,
                description="单次分页读取的记录上限；服务端可进一步收紧。",
            ),
            "max_pages_per_view": _integer(
                minimum=1,
                maximum=10_000,
                description="每个视图允许写入/读取的快照页数上限。",
            ),
            "max_child_keys": _integer(
                minimum=1,
                maximum=10_000,
                description=(
                    "链式子键上限（建筑项目 _id、餐饮 brandId、店铺 platform+shopId 等）；"
                    "子键必须可追溯到本名单与已确认父产品。"
                ),
            ),
        },
        required=[
            "max_targets",
            "max_calls",
            "max_page_size",
            "max_pages_per_view",
            "max_child_keys",
        ],
        description="调用、分页、目标与链式子键的硬上限；全部必填，不接受默认放开。",
    )


def _period_schema() -> JsonSchema:
    return _strict_object(
        {
            "start_month": _year_month(),
            "end_month": _year_month(),
            "observation_cutoff": _date(),
            "max_months_per_entity": _integer(
                minimum=1,
                maximum=120,
                description="单个主体最多纳入的企业月份数；用于限制一企一期明细量。",
            ),
        },
        required=["start_month", "end_month", "observation_cutoff", "max_months_per_entity"],
        description=(
            "历史观察窗口与观察上限。start_month/end_month 使用 YYYY-MM；"
            "observation_cutoff 是本次允许使用资料的日期上限。"
            "服务端必须校验 start_month <= end_month，且不得用无期间汇总冒充同期数据。"
        ),
    )


# ---------------------------------------------------------------------------
# Plan output (shared by plan_base / plan_resolution / plan_topics).
# ---------------------------------------------------------------------------


def _plan_product_estimate_schema(stage: str) -> JsonSchema:
    product_ids = TOPIC_PRODUCT_IDS if stage == "topics" else BASE_PRODUCT_IDS
    routing_keys = TOPIC_KEYS if stage == "topics" else BASE_ROUTING_KEYS
    return _strict_object(
        {
            "product_id": _enum(
                product_ids,
                description=(
                    "本阶段实际可选产品；topics 只能是专题产品，"
                    "base/resolution 只能是名单/基础/补全产品。"
                ),
            ),
            "routing_key": _enum(
                routing_keys,
                description="identity/base/supplement 或 13 个专题路由键。",
            ),
            "estimated_calls": _integer(
                minimum=0,
                maximum=100_000,
                description="预计调用数；条件分页与子查询按实际上限估算，不是每企业固定一次。",
            ),
            "max_calls": _integer(
                minimum=1,
                maximum=100_000,
                description="该产品的调用封顶数；不得被总上限放大。",
            ),
            "note": _string(
                max_length=300,
                description="脱敏的范围与估算说明；不得写凭证。",
            ),
        },
        required=["product_id", "routing_key", "estimated_calls", "max_calls"],
        description="逐产品预计与封顶调用数；调用方无需从总量反推产品范围。",
    )


def _estimate_schema(stage: str) -> JsonSchema:
    return _strict_object(
        {
            "estimated_calls": _integer(minimum=0, maximum=100_000),
            "max_calls": _integer(minimum=1, maximum=100_000),
            "products": _array(
                _plan_product_estimate_schema(stage),
                min_items=1,
                max_items=(
                    len(TOPIC_PRODUCT_IDS) if stage == "topics" else len(BASE_PRODUCT_IDS)
                ),
                unique_items=True,
                description="逐产品预计/封顶调用数；不得只给总量。",
            ),
        },
        required=["estimated_calls", "max_calls", "products"],
        description="只描述预计与封顶调用数。",
    )


def _blocker_schema() -> JsonSchema:
    return _strict_object(
        {
            "code": _enum(BLOCKER_CODES),
            "message": _string(min_length=1, max_length=500),
            "product_id": _enum(ALL_PRODUCT_IDS),
        },
        required=["code", "message"],
        description="不可执行的脱敏原因；executable=true 时不得出现。",
    )


def _plan_scope_schema(stage: str) -> JsonSchema:
    if stage == "topics":
        product_ids = _enum(TOPIC_PRODUCT_IDS)
        topic_min, topic_max = 1, len(TOPIC_KEYS)
    else:
        product_ids = _enum(BASE_PRODUCT_IDS)
        topic_min, topic_max = 0, 0

    properties: Dict[str, JsonSchema] = {
        "row_count": _integer(minimum=0, maximum=10_000_000),
        "subject_count": _integer(minimum=0, maximum=100_000),
        "unique_subject_count": _integer(minimum=0, maximum=100_000),
        "cohort_count": _integer(minimum=0, maximum=10_000),
        "topic_keys": _array(
            _enum(TOPIC_KEYS),
            min_items=topic_min,
            max_items=topic_max,
            unique_items=True,
        ),
        "product_ids": _array(
            product_ids,
            min_items=1,
            max_items=(
                len(TOPIC_PRODUCT_IDS) if stage == "topics" else len(BASE_PRODUCT_IDS)
            ),
            unique_items=True,
            description="本计划实际冻结的产品集合；不是全目录。",
        ),
        "entity_month_count": _integer(minimum=0, maximum=10_000_000),
    }
    required = ["row_count", "topic_keys", "product_ids"]
    if stage in ("resolution", "topics"):
        required.extend(["subject_count", "unique_subject_count"])
    if stage == "topics":
        required.extend(["cohort_count", "entity_month_count"])
    return _strict_object(
        properties,
        required=required,
        description=(
            "冻结范围计数。subject_count 为选中主体数，unique_subject_count 为跨客群"
            "去重后的主体数；两者不等表示同一主体出现在多个客群。"
        ),
    )


def _human_summary_schema() -> JsonSchema:
    return _strict_object(
        {
            "headline": _string(min_length=1, max_length=200),
            "bullets": _array(_string(min_length=1, max_length=300), min_items=1, max_items=20),
        },
        required=["headline", "bullets"],
        description="供用户查看的计划摘要；只描述范围与调用量。",
    )


def _plan_output_schema(stage: str) -> JsonSchema:
    properties: Dict[str, JsonSchema] = {
        "plan_id": _plan_id(),
        "plan_hash": _sha256(description="计划规范化内容的 SHA-256；执行时必须原样回传。"),
        "stage": _literal(stage),
        "executable": _boolean(
            description="仅在 blockers 为空且未过期时为 true；false 时执行工具必须拒绝。"
        ),
        "expires_at": _date_time(
            description="计划可执行截止时间，由服务端 plan_ttl 计算；过期后必须重新生成计划。"
        ),
        "roster_id": _roster_id(),
        "snapshot_id": _snapshot_id(),
        "business_scope_hash": _sha256(
            description="基础计划输入的业务线/产品或服务摘要；服务只比对哈希，不收原始 CRM 文本。"
        ),
        "analysis_scope_hash": _sha256(
            description="用户已确认的分析设置摘要（业务问题、统计单位、客群成员、标签范围等）。"
        ),
        "scope": _plan_scope_schema(stage),
        "period": _period_schema(),
        "estimate": _estimate_schema(stage),
        "limits": _limits_schema(),
        "human_summary": _human_summary_schema(),
        "blockers": _array(
            _blocker_schema(),
            max_items=100,
            description="不可执行原因；executable=true 时必须为空。",
        ),
    }
    required = [
        "plan_id",
        "plan_hash",
        "stage",
        "executable",
        "expires_at",
        "roster_id",
        "snapshot_id",
        "scope",
        "estimate",
        "limits",
        "human_summary",
        "blockers",
    ]
    if stage == "base":
        required.append("business_scope_hash")
    if stage == "topics":
        required.extend(["analysis_scope_hash", "period"])

    schema = _strict_object(
        properties,
        required=required,
        description=(
            "三类计划工具返回的同一 Plan 形状。计划只冻结范围与调用量，不触发上游查询；"
            "executable、expires_at、逐产品调用量和硬上限都必须可见。"
        ),
    )
    schema["allOf"] = [
        _conditional(
            {"properties": {"executable": {"const": True}}, "required": ["executable"]},
            {"properties": {"blockers": {"maxItems": 0}}},
        ),
        _conditional(
            {"properties": {"executable": {"const": False}}, "required": ["executable"]},
            {"properties": {"blockers": {"minItems": 1}}},
        ),
    ]
    if stage == "base":
        schema["allOf"].append(
            _conditional(
                {"properties": {"stage": {"const": "base"}}, "required": ["stage"]},
                {"required": ["business_scope_hash"]},
            )
        )
    if stage == "topics":
        schema["allOf"].append(
            _conditional(
                {"properties": {"stage": {"const": "topics"}}, "required": ["stage"]},
                {"required": ["analysis_scope_hash", "period"]},
            )
        )
    return schema


# ---------------------------------------------------------------------------
# Input schemas.
# ---------------------------------------------------------------------------


def _stage_roster_input_schema() -> JsonSchema:
    row_schema = _strict_object(
        {
            "row_id": _row_id(),
            "enterprise_name": _string(
                min_length=1,
                max_length=300,
                pattern=r"\S",
                description="原名单中的企业名称原文；不得替换为本地路径、备注或联系人。",
            ),
            "social_credit_code": _string(
                pattern=CREDIT_CODE_PATTERN,
                min_length=18,
                max_length=18,
                description="统一社会信用代码；与 enterprise_name 至少提供一个。",
            ),
            "source_row_number": _integer(
                minimum=1,
                maximum=10_000_000,
                description="原表行号（仅用于本地对账，不要求整表上传）。",
            ),
        },
        required=["row_id"],
        description="一行最小身份信息；不接受联系人、备注、文件路径或令牌。",
    )
    row_schema["anyOf"] = [
        {"required": ["enterprise_name"]},
        {"required": ["social_credit_code"]},
    ]

    return _strict_object(
        {
            "batch_key": _string(
                pattern=r"^batch_[A-Za-z0-9_-]{8,64}$",
                min_length=14,
                max_length=70,
                description="本次名单上传的幂等批次键；同一批次的所有分块共用。",
            ),
            "chunk_index": _integer(
                minimum=1,
                maximum=1_000,
                description="从 1 开始的分块序号；必须满足 chunk_index <= chunk_count。",
            ),
            "chunk_count": _integer(
                minimum=1,
                maximum=1_000,
                description="分块总数；服务端按 batch_key 校验缺块、重复块和内容一致性。",
            ),
            "rows": _array(
                row_schema,
                min_items=1,
                max_items=500,
                unique_items=True,
                description="本块身份行；每行至少一个企业名称或统一社会信用代码。",
            ),
        },
        required=["batch_key", "chunk_index", "chunk_count", "rows"],
        description=(
            "分块暂存最少身份列，不查询企业、不收费。同一 batch_key + chunk_index + "
            "规范化内容必须幂等；缺块、重复行号或冲突不得生成 roster_id。"
        ),
    )


def _plan_base_input_schema() -> JsonSchema:
    return _strict_object(
        {
            "roster_id": _roster_id(),
            "business_scope_hash": _sha256(
                description=(
                    "本次业务线/具体产品或服务/应用场景摘要的 SHA-256；"
                    "服务只接收哈希，不接受原始 CRM 文本或客户资料。"
                )
            ),
            "limits": _limits_schema(),
        },
        required=["roster_id", "business_scope_hash", "limits"],
        description=(
            "用完整名单和业务口径摘要生成基础计划；只冻结范围与调用量，"
            "不触发任何上游网络请求。名单不完整时返回可识别的 blockers，"
            "不得静默按部分名单生成计划。"
        ),
    )


def _resolution_decision_schema() -> JsonSchema:
    select_schema = _strict_object(
        {
            "row_id": _row_id(),
            "action": _literal("select_candidate"),
            "candidate_id": _candidate_id(),
        },
        required=["row_id", "action", "candidate_id"],
        description="确权：只接受本快照中该行的候选引用，不接受自由文本企业名。",
    )
    exclude_schema = _strict_object(
        {
            "row_id": _row_id(),
            "action": _literal("exclude"),
            "reason_code": _enum(
                (
                    "not_a_customer",
                    "duplicate_subject",
                    "no_reliable_match",
                    "out_of_scope",
                    "user_excluded",
                )
            ),
            "note": _string(max_length=300),
        },
        required=["row_id", "action", "reason_code"],
        description="排除：只记录行级原因；不得借此添加名单外企业。",
    )
    return {"oneOf": [select_schema, exclude_schema]}


def _plan_resolution_input_schema() -> JsonSchema:
    return _strict_object(
        {
            "snapshot_id": _snapshot_id(),
            "decisions": _array(
                _resolution_decision_schema(),
                min_items=1,
                max_items=2_000,
                unique_items=True,
                description=(
                    "逐行确权或排除。row_id 必须属于该快照；candidate_id 必须属于该行。"
                    "同一 row_id 重复出现时服务端必须拒绝，而不是后写覆盖。"
                ),
            ),
            "limits": _limits_schema(),
        },
        required=["snapshot_id", "decisions", "limits"],
        description=(
            "基于已有候选快照提交确权/排除并重新生成计划；不执行上游查询。"
            "未提交的行保持未知，不自动取第一条候选，也不扩大名单。"
        ),
    )


def _topic_selection_schema(topic: str) -> JsonSchema:
    product_ids = PRODUCT_IDS_BY_TOPIC[topic]
    return _strict_object(
        {
            "cohort_key": _string(
                min_length=1,
                max_length=80,
                description="用户确认的客群键；多个客群可包含相同主体。",
            ),
            "topic": _literal(topic),
            "subject_ids": _array(
                _subject_id(),
                min_items=1,
                max_items=5_000,
                unique_items=True,
                description="该客群中已确权、且本次获批纳入该专题的主体引用。",
            ),
            "product_ids": _array(
                _enum(product_ids),
                min_items=1,
                max_items=len(product_ids),
                unique_items=True,
                description=(
                    "该专题实际需要的产品子集；只能来自本专题目录，服务端还要校验"
                    "其属于原基础计划且未被用户跳过。"
                ),
            ),
        },
        required=["cohort_key", "topic", "subject_ids", "product_ids"],
        description=f"{topic} 专题的冻结选择；产品与专题的从属关系由 schema 先限定。",
    )


def _plan_topics_input_schema() -> JsonSchema:
    return _strict_object(
        {
            "snapshot_id": _snapshot_id(),
            "analysis_scope_hash": _sha256(
                description=(
                    "用户已确认分析设置的 SHA-256 摘要，覆盖业务线/问题、统计单位、"
                    "客群成员、期间、标签范围等。服务端据此校验主体与真实取数范围，"
                    "不接受上传 CRM 值或原始业务描述。"
                )
            ),
            "selections": _array(
                {"oneOf": [_topic_selection_schema(topic) for topic in TOPIC_KEYS]},
                min_items=1,
                max_items=100,
                unique_items=True,
                description=(
                    "专题 × 客群 × 适用主体 × 产品子集。允许同一主体出现在多个客群；"
                    "服务端必须按 subject_id 去重调用与取数，同时保留客群成员关系。"
                ),
            ),
            "period": _period_schema(),
            "limits": _limits_schema(),
        },
        required=["snapshot_id", "analysis_scope_hash", "selections", "period", "limits"],
        description=(
            "冻结专题取数范围与调用量；不执行上游查询。product_ids 只能缩到本专题"
            "实际需要的产品，服务端再次校验原计划、专题归属和主体适用性。"
        ),
    )


# ---------------------------------------------------------------------------
# Run schema (execute_plan output and get_run output).
# ---------------------------------------------------------------------------


def _run_error_schema() -> JsonSchema:
    return _strict_object(
        {
            "code": _enum(RUN_ERROR_CODES),
            "message": _string(min_length=1, max_length=500),
            "retryable": _boolean(
                description=(
                    "true 仅表示同参数在 retry_after_seconds 后可安全重试；"
                    "execute_plan 必须使用同一 plan_id/plan_hash，避免重复派发。"
                )
            ),
            "retry_after_seconds": _integer(minimum=0, maximum=86_400),
            "trace_id": _string(min_length=1, max_length=128),
        },
        required=["code", "message", "retryable"],
        description="脱敏错误；不包含名单、令牌、签名或上游原始报文。",
    )
    # Keep a stable shape while allowing retry_after_seconds only when retryable.
    schema["allOf"] = [
        _conditional(
            {"properties": {"retryable": {"const": False}}, "required": ["retryable"]},
            _not_required("retry_after_seconds"),
        )
    ]
    return schema


def _run_result_schema() -> JsonSchema:
    return _strict_object(
        {
            "snapshot_id": _snapshot_id(),
            "views_available": _array(
                _enum(PAGE_VIEWS),
                min_items=1,
                max_items=len(PAGE_VIEWS),
                unique_items=True,
                description="本次运行已可读取的视图；未列出的视图不得请求。",
            ),
            "subject_count": _integer(minimum=0, maximum=100_000),
            "evidence_count": _integer(minimum=0, maximum=10_000_000),
            "coverage_count": _integer(minimum=0, maximum=10_000_000),
            "complete": _boolean(
                description="true 表示完整；false 表示因调用上限/分页/上游缺口产生的部分结果。"
            ),
        },
        required=[
            "snapshot_id",
            "views_available",
            "subject_count",
            "evidence_count",
            "coverage_count",
            "complete",
        ],
        description="结果快照引用与计数；不包含大附件或整批明细。",
    )


def _run_output_schema() -> JsonSchema:
    properties: Dict[str, JsonSchema] = {
        "run_id": _run_id(),
        "plan_id": _plan_id(),
        "plan_hash": _sha256(description="本次运行绑定的计划哈希。"),
        "state": _enum(RUN_STATES, description="运行标准状态。"),
        "is_terminal": _boolean(),
        "is_resumable": _boolean(description="服务端能否在原计划与调用上限内从账本恢复；客户端只按 run_id 读取，不传续跑令牌。"),
        "created_at": _date_time(),
        "updated_at": _date_time(),
        "progress": _strict_object(
            {
                "products_total": _integer(minimum=0, maximum=37),
                "products_completed": _integer(minimum=0, maximum=37),
                "subjects_total": _integer(minimum=0, maximum=100_000),
                "subjects_completed": _integer(minimum=0, maximum=100_000),
                "pages_written": _integer(minimum=0, maximum=10_000_000),
                "records_written": _integer(minimum=0, maximum=100_000_000),
            },
            required=[
                "products_total",
                "products_completed",
                "subjects_total",
                "subjects_completed",
                "pages_written",
                "records_written",
            ],
        ),
        "usage": _strict_object(
            {
                "calls_used": _integer(minimum=0, maximum=100_000),
                "max_calls": _integer(minimum=1, maximum=100_000),
            },
            required=["calls_used", "max_calls"],
            description="已使用调用量与硬上限；服务端计数为准，不依赖调用方自报。",
        ),
        "result": _run_result_schema(),
        "error": _run_error_schema(),
    }
    schema = _strict_object(
        properties,
        required=[
            "run_id",
            "plan_id",
            "plan_hash",
            "state",
            "is_terminal",
            "is_resumable",
            "created_at",
            "updated_at",
            "progress",
            "usage",
        ],
        description=(
            "运行状态、恢复能力、调用量使用、错误与结果快照。partial 表示已有可读部分结果；"
            "succeeded/failed/cancelled 为终止状态。"
        ),
    )
    schema["allOf"] = [
        _conditional(
            {"properties": {"state": {"enum": ["queued", "running"]}}, "required": ["state"]},
            {"properties": {"is_terminal": {"const": False}}},
        ),
        _conditional(
            {"properties": {"state": {"const": "succeeded"}}, "required": ["state"]},
            {
                "properties": {
                    "is_terminal": {"const": True},
                    "is_resumable": {"const": False},
                    "result": {
                        "properties": {"complete": {"const": True}},
                        "required": ["complete"],
                    },
                },
                "required": ["result"],
            },
        ),
        _conditional(
            {"properties": {"state": {"const": "failed"}}, "required": ["state"]},
            {
                "properties": {
                    "is_terminal": {"const": True},
                    "is_resumable": {"const": False},
                },
                "required": ["error"],
            },
        ),
        _conditional(
            {"properties": {"state": {"const": "cancelled"}}, "required": ["state"]},
            {
                "properties": {
                    "is_terminal": {"const": True},
                    "is_resumable": {"const": False},
                }
            },
        ),
        _conditional(
            {"properties": {"state": {"const": "partial"}}, "required": ["state"]},
            {
                "properties": {
                    "result": {
                        "properties": {"complete": {"const": False}},
                        "required": ["complete"],
                    }
                },
                "required": ["result"],
            },
        ),
        _conditional(
            {"properties": {"is_terminal": {"const": True}}, "required": ["is_terminal"]},
            {"properties": {"is_resumable": {"const": False}}},
        ),
    ]
    return schema


# ---------------------------------------------------------------------------
# Page view record schemas.
# ---------------------------------------------------------------------------


def _identity_record_schema() -> JsonSchema:
    schema = _strict_object(
        {
            "row_id": _row_id(),
            "enterprise_name": _string(min_length=1, max_length=300),
            "social_credit_code": _string(pattern=CREDIT_CODE_PATTERN, min_length=18, max_length=18),
            "source_row_number": _integer(minimum=1, maximum=10_000_000),
            "mapping_status": _enum(
                ("matched", "ambiguous", "unmatched", "excluded", "duplicate")
            ),
            "subject_id": _subject_id(),
            "candidate_count": _integer(minimum=0, maximum=100),
        },
        required=["row_id", "mapping_status", "candidate_count"],
        description="原名单行去向；不包含联系人、备注或附件正文。",
    )
    schema["anyOf"] = [
        {"required": ["enterprise_name"]},
        {"required": ["social_credit_code"]},
    ]
    schema["allOf"] = [
        _conditional(
            {"properties": {"mapping_status": {"const": "matched"}}, "required": ["mapping_status"]},
            {"required": ["subject_id"]},
        ),
        _conditional(
            {"properties": {"mapping_status": {"const": "ambiguous"}}, "required": ["mapping_status"]},
            {"properties": {"candidate_count": {"minimum": 1}}},
        ),
        _conditional(
            {"properties": {"mapping_status": {"enum": ["unmatched", "excluded"]}}, "required": ["mapping_status"]},
            _not_required("subject_id"),
        ),
    ]
    return schema


def _candidate_record_schema() -> JsonSchema:
    return _strict_object(
        {
            "candidate_id": _candidate_id(),
            "row_id": _row_id(),
            "name_id": _string(
                min_length=1,
                max_length=128,
                pattern=r"^[A-Za-z0-9._:-]+$",
                description="旷湖 nameId，用于回查工商照面；不是本服务的 subject_id。",
            ),
            "enterprise_name": _string(min_length=1, max_length=300),
            "social_credit_code": _string(pattern=CREDIT_CODE_PATTERN, min_length=18, max_length=18),
            "match_reason": _enum(
                ("exact_name", "former_name", "social_credit_code", "keyword_match")
            ),
        },
        required=["candidate_id", "row_id", "name_id", "enterprise_name", "match_reason"],
        description=(
            "候选主体引用；用户必须明确选择或排除。品牌/产品/地址命中不得自动确权。"
        ),
    )


def _subject_record_schema() -> JsonSchema:
    return _strict_object(
        {
            "subject_id": _subject_id(),
            "enterprise_name": _string(min_length=1, max_length=300),
            "social_credit_code": _string(pattern=CREDIT_CODE_PATTERN, min_length=18, max_length=18),
            "name_id": _string(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$"),
            "resolution_status": _enum(("confirmed", "deduplicated", "unresolved")),
            "source_row_ids": _array(
                _row_id(),
                min_items=1,
                max_items=1_000,
                unique_items=True,
                description="映射到该主体的原名单行；重复行共享主体。",
            ),
            "duplicate_row_count": _integer(minimum=0, maximum=1_000),
            "cohort_keys": _array(
                _string(min_length=1, max_length=80),
                min_items=0,
                max_items=50,
                unique_items=True,
                description="该主体所属的已确认客群键；同一主体可属于多个客群。",
            ),
        },
        required=[
            "subject_id",
            "enterprise_name",
            "resolution_status",
            "source_row_ids",
            "duplicate_row_count",
        ],
        description="去重后的独立主体及其原名单行；不自动合并总分公司。",
    )


def _business_time_schema() -> JsonSchema:
    return {
        "oneOf": [
            _strict_object(
                {"granularity": _literal("day"), "value": _date()},
                required=["granularity", "value"],
            ),
            _strict_object(
                {"granularity": _literal("month"), "value": _year_month()},
                required=["granularity", "value"],
            ),
            _strict_object(
                {
                    "granularity": _literal("year"),
                    "value": _string(pattern=YEAR_PATTERN, min_length=4, max_length=4),
                },
                required=["granularity", "value"],
            ),
            _strict_object({"granularity": _literal("unknown")}, required=["granularity"]),
        ],
        "description": "业务时间粒度与原文；unknown 表示接口没有可判读的业务时间。",
    }


def _raw_value_schema() -> JsonSchema:
    return {
        "oneOf": [
            _string(max_length=2_000),
            _number(),
            _boolean(),
            _array({}, max_items=50),
            {
                "type": "object",
                "maxProperties": 50,
                "additionalProperties": True,
                "description": "原始对象；大附件必须改用 attachments 引用，不得内联。",
            },
        ],
        "description": "接口原始值（未经本地改写）；字符串上限 2000 字符，容器上限 50。",
    }


def _attachment_schema() -> JsonSchema:
    return _strict_object(
        {
            "name": _string(min_length=1, max_length=200),
            "media_type": _string(min_length=1, max_length=100),
            "size_bytes": _integer(minimum=0, maximum=1_000_000_000),
            "ref": _string(
                pattern=r"^att_[A-Za-z0-9._~-]{8,256}$",
                min_length=12,
                max_length=260,
                description="附件引用；内容不塞入 MCP 响应，由 Skill 另行受控读取。",
            ),
        },
        required=["name", "size_bytes", "ref"],
        description="附件元数据引用，不含正文。",
    )


def _evidence_record_schema() -> JsonSchema:
    schema = _strict_object(
        {
            "evidence_id": _string(
                pattern=r"^ev_[A-Za-z0-9_-]{8,64}$",
                min_length=11,
                max_length=67,
            ),
            "entity_id": _subject_id(),
            "product_id": _enum(ALL_PRODUCT_IDS),
            "topic": _enum(ROUTING_KEYS),
            "source_field": _string(
                pattern=r"^[A-Za-z_][A-Za-z0-9_.\[\]-]{0,127}$",
                min_length=1,
                max_length=128,
                description="来源字段路径，例如 addressValue.province 或 resultList._id。",
            ),
            "source_record_id": _string(min_length=1, max_length=200),
            "raw_value": _raw_value_schema(),
            "display_value": _string(
                min_length=0,
                max_length=2_000,
                description="来源显示串；与数值分开保存，不把本地格式化结果当原文。",
            ),
            "numeric_value": _number(description="可比较的数值；文本字段不出现。"),
            "unit": _string(
                min_length=1,
                max_length=32,
                description="计量单位；无单位文本使用 none。不得缺省。",
            ),
            "business_time": _business_time_schema(),
            "acquired_at": _date_time(),
            "status": _enum(
                (
                    "observed",
                    "zero",
                    "not_disclosed",
                    "not_returned",
                    "not_applicable",
                    "not_queried",
                    "truncated",
                    "error",
                ),
                description=(
                    "observed/zero 为可判读事实；not_disclosed 为未披露；"
                    "not_returned 为请求但未回；not_applicable 为不适用；"
                    "not_queried 为未查；truncated 为调用上限/分页截断；error 为取数或解析错误。"
                ),
            ),
            "quality_flags": _array(
                _enum(
                    (
                        "partial_page",
                        "page_limited",
                        "unit_inferred",
                        "time_inferred",
                        "source_duplicate",
                        "parent_key_unverified",
                    )
                ),
                max_items=10,
                unique_items=True,
            ),
            "attachments": _array(_attachment_schema(), max_items=5),
            "note": _string(max_length=500),
        },
        required=[
            "evidence_id",
            "entity_id",
            "product_id",
            "topic",
            "source_field",
            "display_value",
            "unit",
            "business_time",
            "acquired_at",
            "status",
        ],
        description=(
            "单条可复核证据：主体、产品、字段、原始值/显示值、单位、业务时间、"
            "取得时间与状态均可区分；大附件只返回引用。"
        ),
    )
    schema["allOf"] = [
        _conditional(
            {
                "properties": {"status": {"enum": ["observed", "zero"]}},
                "required": ["status"],
            },
            {"required": ["raw_value"]},
        ),
        _conditional(
            {
                "properties": {"status": {"enum": ["not_disclosed", "not_returned", "not_applicable", "not_queried", "truncated", "error"]}},
                "required": ["status"],
            },
            {"not": {"anyOf": [{"required": ["raw_value"]}, {"required": ["numeric_value"]}]}},
        ),
        _conditional(
            {"properties": {"status": {"const": "zero"}}, "required": ["status"]},
            {"properties": {
                "raw_value": {"anyOf": [{"const": 0}, {"type": "string", "pattern": r"^\s*[+-]?(?:0+(?:\.0*)?|\.0+)(?:[eE][+-]?[0-9]+)?\s*[%％]?\s*$"}]},
                "numeric_value": {"const": 0},
            }},
        ),
    ]
    return schema


def _coverage_record_schema() -> JsonSchema:
    schema = _strict_object(
        {
            "coverage_id": _string(
                pattern=r"^cov_[A-Za-z0-9_-]{8,64}$",
                min_length=12,
                max_length=68,
            ),
            "subject_id": _subject_id(),
            "product_id": _enum(ALL_PRODUCT_IDS),
            "topic": _enum(ROUTING_KEYS),
            "state": _enum(("covered", "empty", "not_queried", "not_applicable", "error")),
            "record_count": _integer(minimum=0, maximum=10_000_000),
            "denominator_eligible": _boolean(
                description="是否可进入 K/A 等有效分母；未查、不适用和错误必须为 false。"
            ),
            "reason": _string(max_length=300),
        },
        required=[
            "coverage_id",
            "subject_id",
            "product_id",
            "topic",
            "state",
            "record_count",
            "denominator_eligible",
        ],
        description=(
            "主体 × 产品的覆盖记录。covered/empty 与 not_queried/not_applicable/error 分开，"
            "供 Skill 复算 K、A、H 分母。"
        ),
    )
    schema["allOf"] = [
        _conditional(
            {"properties": {"state": {"const": "covered"}}, "required": ["state"]},
            {"properties": {"record_count": {"minimum": 1}}},
        ),
        _conditional(
            {"properties": {"state": {"const": "empty"}}, "required": ["state"]},
            {"properties": {"record_count": {"const": 0}}},
        ),
        _conditional(
            {
                "properties": {"state": {"enum": ["not_queried", "not_applicable", "error"]}},
                "required": ["state"],
            },
            {"properties": {"denominator_eligible": {"const": False}}},
        ),
    ]
    return schema


# ---------------------------------------------------------------------------
# Page output.
# ---------------------------------------------------------------------------


def _page_output_schema() -> JsonSchema:
    record_schemas = {
        "identities": _identity_record_schema(),
        "candidates": _candidate_record_schema(),
        "subjects": _subject_record_schema(),
        "evidence": _evidence_record_schema(),
        "coverage": _coverage_record_schema(),
    }
    properties: Dict[str, JsonSchema] = {
        "snapshot_id": _snapshot_id(),
        "view": _enum(PAGE_VIEWS),
        "limit": _integer(minimum=1, maximum=200),
        "returned": _integer(minimum=0, maximum=200),
        "has_more": _boolean(),
        "next_cursor": _cursor(),
        "records": _array(
            {},
            max_items=200,
            description="按 view 判别化的记录；不同视图不得混装。",
        ),
        "coverage_note": _string(
            max_length=500,
            description="分页/筛选覆盖范围说明；部分页不得声称全量。",
        ),
    }
    all_of: List[JsonSchema] = [
        _conditional(
            {"properties": {"has_more": {"const": True}}, "required": ["has_more"]},
            {"required": ["next_cursor"]},
        ),
        _conditional(
            {"properties": {"has_more": {"const": False}}, "required": ["has_more"]},
            _not_required("next_cursor"),
        ),
    ]
    for view, record_schema in record_schemas.items():
        all_of.append(
            _conditional(
                {"properties": {"view": {"const": view}}, "required": ["view"]},
                {"properties": {"records": {"items": copy.deepcopy(record_schema)}}},
            )
        )
    schema = _strict_object(
        properties,
        required=["snapshot_id", "view", "limit", "returned", "has_more", "records"],
        description=(
            "快照分页。读取已有数据不触发上游请求；next_cursor 只在 has_more=true 时出现，"
            "不得用 null 伪装结束。"
        ),
    )
    schema["allOf"] = all_of
    return schema


# ---------------------------------------------------------------------------
# Tool descriptors.
# ---------------------------------------------------------------------------


def _tool(
    name: str,
    title: str,
    description: str,
    input_schema: JsonSchema,
    output_schema: JsonSchema,
    *,
    read_only: bool,
    destructive: bool,
    idempotent: bool,
    open_world: bool,
) -> Dict[str, Any]:
    return {
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": {"$schema": SCHEMA_DIALECT, **input_schema},
        "outputSchema": {"$schema": SCHEMA_DIALECT, **output_schema},
        "annotations": {
            "title": title,
            "readOnlyHint": read_only,
            "destructiveHint": destructive,
            "idempotentHint": idempotent,
            "openWorldHint": open_world,
        },
    }


def build_tools() -> List[Dict[str, Any]]:
    """Return the seven MCP tool descriptors in execution order."""
    return [
        _tool(
            "portrait_stage_roster",
            "分块暂存客户名单",
            (
                "分块上传最少身份列（row_id + 企业名称或统一社会信用代码至少一项），"
                "不查询企业、不触发上游请求。同一 batch_key/chunk_index 按内容幂等；"
                "只有全部分块完整且无重复行号/冲突时才返回 roster_id。回执只含计数和"
                "被拒行，不返回全名单。禁止本地文件路径、令牌、联系人和备注。"
                + ERROR_NOTE
            ),
            _stage_roster_input_schema(),
            _stage_roster_output_schema(),
            read_only=False,
            destructive=False,
            idempotent=True,
            open_world=False,
        ),
        _tool(
            "portrait_plan_base",
            "基础取数计划",
            (
                "用完整 roster_id、business_scope_hash 和硬上限生成基础计划；只冻结范围"
                "与调用量，不触发任何上游网络请求。计划必须逐产品给出预计/封顶调用数，"
                "并给出 expires_at、executable 与 blockers；executable=false 时执行"
                "工具必须拒绝。"
                + ERROR_NOTE
            ),
            _plan_base_input_schema(),
            _plan_output_schema("base"),
            read_only=False,
            destructive=False,
            idempotent=True,
            open_world=False,
        ),
        _tool(
            "portrait_plan_resolution",
            "候选确权计划",
            (
                "基于快照候选提交逐行确权或排除，并估算必要的工商核对调用量。只接受 "
                "row_id + candidate_id 或 row_id + 排除原因；不接受自由文本企业名、"
                "追加公司或未解析查询。未提交行保持未知，主体集合变化后后续计划须重制。"
                + ERROR_NOTE
            ),
            _plan_resolution_input_schema(),
            _plan_output_schema("resolution"),
            read_only=False,
            destructive=False,
            idempotent=True,
            open_world=False,
        ),
        _tool(
            "portrait_plan_topics",
            "专题取数计划",
            (
                "提交已确认的 analysis_scope_hash、专题×客群×适用主体×产品子集、历史"
                "期间和限额，用已有快照生成计划。product_ids 只能属于所选专题且是原方案的"
                "子集，服务端再次校验；同一主体出现在多个客群时按 subject_id 去重，"
                "同时保留客群成员。期间以企业月份为界，观察上限必须可见。"
                + ERROR_NOTE
            ),
            _plan_topics_input_schema(),
            _plan_output_schema("topics"),
            read_only=False,
            destructive=False,
            idempotent=True,
            open_world=False,
        ),
        _tool(
            "portrait_execute_plan",
            "执行已冻结计划",
            (
                "唯一可能触发上游真实调用的工具。只接受 plan_id、plan_hash；不接受 "
                "confirmed=true、动态产品、追加企业、原始查询或本地文件。服务端必须校验"
                "计划绑定租户、哈希、范围与硬上限、未过期且 executable=true。同一 "
                "plan_id/plan_hash 重试返回同一运行，不重复派发；超时且 billing_unknown "
                "时不得自动重发。工具 annotations 只是提示，不是执行边界。"
                + ERROR_NOTE
            ),
            _execute_plan_input_schema(),
            _run_output_schema(),
            read_only=False,
            destructive=False,
            idempotent=True,
            open_world=True,
        ),
        _tool(
            "portrait_get_run",
            "读取运行状态",
            (
                "读取同一运行的进度、调用量使用、错误和结果快照。partial 表示"
                "已有部分结果；succeeded/failed/cancelled 为终止状态。恢复不自动增加 "
                "max_calls；需要更多调用量时应另做计划。只读，不触发上游请求。"
                + ERROR_NOTE
            ),
            _get_run_input_schema(),
            _run_output_schema(),
            read_only=True,
            destructive=False,
            idempotent=True,
            open_world=False,
        ),
        _tool(
            "portrait_read_page",
            "分页读取快照",
            (
                "按 view（identities/candidates/subjects/evidence/coverage）读取已有"
                "快照，游标绑定 snapshot_id 与 view。证据记录按主体、产品、字段、原始值、"
                "显示值、单位、业务时间、取得时间和状态判别化；大附件只返回引用。读取"
                "已取得数据不触发上游请求，也不代表同意追加翻页。"
                + ERROR_NOTE
            ),
            _read_page_input_schema(),
            _page_output_schema(),
            read_only=True,
            destructive=False,
            idempotent=True,
            open_world=False,
        ),
    ]


# ---------------------------------------------------------------------------
# Remaining input/output schemas referenced above.
# ---------------------------------------------------------------------------


def _stage_roster_output_schema() -> JsonSchema:
    rejected_schema = _strict_object(
        {
            "row_id": _row_id(),
            "code": _enum(
                (
                    "missing_identity",
                    "invalid_credit_code",
                    "invalid_row_id",
                    "duplicate_row_id",
                    "row_limit_exceeded",
                )
            ),
            "message": _string(max_length=300),
        },
        required=["row_id", "code"],
        description="被拒行的最小回执；不回传企业名称或其他身份值。",
    )
    schema = _strict_object(
        {
            "batch_key": _string(pattern=r"^batch_[A-Za-z0-9_-]{8,64}$"),
            "chunk_index": _integer(minimum=1, maximum=1_000),
            "chunk_count": _integer(minimum=1, maximum=1_000),
            "status": _enum(("partial", "complete")),
            "content_hash": _sha256(description="本块规范化内容的哈希，用于重传幂等。"),
            "accepted_rows": _integer(minimum=0, maximum=500),
            "duplicate_rows": _integer(minimum=0, maximum=500),
            "rejected_rows": _integer(minimum=0, maximum=500),
            "rejected": _array(rejected_schema, max_items=500),
            "roster_id": _roster_id(),
            "next_chunk_index": _integer(minimum=1, maximum=1_000),
        },
        required=[
            "batch_key",
            "chunk_index",
            "chunk_count",
            "status",
            "content_hash",
            "accepted_rows",
            "duplicate_rows",
            "rejected_rows",
            "rejected",
        ],
        description=(
            "小块回执。complete 才返回 roster_id；partial 返回 next_chunk_index。"
            "不得包含完整名单、本地路径、令牌或联系人。"
        ),
    )
    schema["allOf"] = [
        _conditional(
            {"properties": {"status": {"const": "complete"}}, "required": ["status"]},
            {"required": ["roster_id"], **_not_required("next_chunk_index")},
        ),
        _conditional(
            {"properties": {"status": {"const": "partial"}}, "required": ["status"]},
            {"required": ["next_chunk_index"], **_not_required("roster_id")},
        ),
    ]
    return schema


def _execute_plan_input_schema() -> JsonSchema:
    return _strict_object(
        {
            "plan_id": _plan_id(),
            "plan_hash": _sha256(description="必须与计划返回的 plan_hash 完全一致。"),
        },
        required=["plan_id", "plan_hash"],
        description=(
            "唯一可能触发上游真实调用的入参面。只有 plan_id/plan_hash；"
            "confirmed、动态产品、追加企业、期间或本地文件路径都属于未知字段，"
            "additionalProperties=false 会直接拒绝。"
        ),
    )


def _get_run_input_schema() -> JsonSchema:
    return _strict_object(
        {"run_id": _run_id()},
        required=["run_id"],
        description="只读同一运行的进度、调用量、错误和结果快照引用。",
    )


def _read_page_input_schema() -> JsonSchema:
    return _strict_object(
        {
            "snapshot_id": _snapshot_id(),
            "view": _enum(PAGE_VIEWS, description="未知 view 直接拒绝。"),
            "cursor": _cursor(),
            "limit": _integer(
                minimum=1,
                maximum=200,
                default=50,
                description="单页记录上限；未提供时按 50，服务端可进一步收紧。",
            ),
        },
        required=["snapshot_id", "view"],
        description=(
            "读取已有快照页。首次不传 cursor；后续只回传服务端游标。"
            "cursor 与 snapshot_id/view 绑定，不得跨视图或跨快照复用。"
        ),
    )


def build_contract() -> Dict[str, Any]:
    """Build the complete, self-contained tools/list document."""
    return {"tools": build_tools()}


def main() -> None:
    """Print the contract as UTF-8 JSON (not a server start command)."""
    try:
        json.dump(build_contract(), fp=sys.stdout, ensure_ascii=False, indent=2)
        print()
    except BrokenPipeError:
        # Allow `... | head` without a traceback; no work was lost.
        try:
            sys.stdout.close()
        finally:
            raise SystemExit(0)


if __name__ == "__main__":
    main()
