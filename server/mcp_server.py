"""企业客户画像洞察：每次工具调用只查询一个旷湖产品，不保存任务状态。"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import os
import re
import sys
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import anyio
import httpx
import uvicorn
from dotenv import load_dotenv
from mcp import types
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.responses import JSONResponse

GATEWAY_BASE_URL = "https://console.handaas.com/api/v1/integrator/call_api"
REQUEST_TIMEOUT_SECONDS = 30.0
CREDENTIAL_NAMES = ("INTEGRATOR_ID", "SECRET_ID", "SECRET_KEY")

# 官方接口文档模板 code=10000；现有产品网关实测 code=1。只认这两个成功码，
# 不把 0、200 等未记录值当作成功。
_SUCCESS_CODES = frozenset({"1", "10000"})
_EMPTY_CODE = "10001"
_EMPTY_MESSAGE = "查询数据为空"

_CN_TZ = timezone(timedelta(hours=8))
_MIN_YEAR = 1900
_MONTH_PATTERN = re.compile(r"[0-9]{6}")
_YEAR_PATTERN = re.compile(r"[0-9]{4}")

# 非超时的连接失败可确定“尚未把请求送达网关”；任何超时及发送后异常都按计费未知处理。
_NOT_SENT_ERRORS = (
    httpx.ConnectError,
    httpx.ProxyError,
    httpx.UnsupportedProtocol,
)


class Fault(Exception):
    """固定文案，不包含用户参数或上游数据。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def pack(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _validate_call(product_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
    if product_id not in _INPUT_FIELDS:
        raise Fault('invalid_argument', '产品不在获准的上游白名单内。')
    fields = _INPUT_FIELDS[product_id]
    if not isinstance(params, Mapping) or params.keys() - fields.keys():
        raise Fault('invalid_argument', '参数不符合此工具定义的字段。')
    for name, field in fields.items():
        if name not in params:
            if field.get('required'):
                raise Fault('invalid_argument', f'缺少必填参数 {name}。')
            continue
        value = params[name]
        if field['type'] == 'integer':
            maximum = field.get('max')
            if type(value) is not int or value < 1 or (maximum is not None and value > maximum):
                raise Fault('invalid_argument', f'参数 {name} 必须是工具范围内的整数。')
            continue

        if not isinstance(value, str) or not value.strip():
            raise Fault('invalid_argument', f'参数 {name} 必须是非空字符串。')
        try:
            value.encode('utf-8')
        except UnicodeEncodeError:
            raise Fault('invalid_argument', f'参数 {name} 含无效字符。') from None
        if 'enum' in field and value not in field['enum']:
            raise Fault('invalid_argument', f'参数 {name} 不在允许的枚举内。')
        if name == 'statsMonth':
            _validate_month(value)
        elif name == 'arYear':
            _validate_year(value)
    # 不填充默认参数，字段缺省由上游处理。
    return dict(params)


def _now_cn() -> datetime:
    return datetime.now(_CN_TZ)


def _validate_month(value: str) -> None:
    if not _MONTH_PATTERN.fullmatch(value):
        raise Fault("invalid_argument", "参数 statsMonth 必须是有效的 YYYYMM 月份。")
    year, month = int(value[:4]), int(value[4:])
    now = _now_cn()
    if year < _MIN_YEAR or not 1 <= month <= 12 or (year, month) > (now.year, now.month):
        raise Fault("invalid_argument", "参数 statsMonth 必须是有效且不晚于当前月的 YYYYMM 月份。")


def _validate_year(value: str) -> None:
    if not _YEAR_PATTERN.fullmatch(value):
        raise Fault("invalid_argument", "参数 arYear 必须是四位年份。")
    year = int(value)
    if year < _MIN_YEAR or year > _now_cn().year:
        raise Fault("invalid_argument", "参数 arYear 必须是有效且不晚于当前年份的四位年份。")


def _credentials() -> tuple[str, str, str]:
    values: dict[str, str] = {}
    missing: list[str] = []
    invalid: list[str] = []
    for name in CREDENTIAL_NAMES:
        value = os.getenv(name, "").strip()
        # 与既有服务一致：模板占位值不视为可用凭据，绝不能因此访问真实网关。
        if not value or value.startswith("your_"):
            missing.append(name)
        elif not value.isprintable() or any(char.isspace() for char in value):
            invalid.append(name)
        else:
            values[name] = value
    if missing:
        raise Fault(
            "configuration_error",
            "缺少上游网关环境变量：" + "、".join(missing) + "；未发起请求。",
        )
    if invalid:
        raise Fault(
            "configuration_error",
            "上游网关环境变量含空白或控制字符：" + "、".join(invalid) + "；未发起请求。",
        )
    return values["INTEGRATOR_ID"], values["SECRET_ID"], values["SECRET_KEY"]


def _signature(form: Mapping[str, str], secret_key: str) -> str:
    plain = "".join(str(form[key]) for key in sorted(form)) + secret_key
    return hashlib.md5(plain.encode("utf-8")).hexdigest()


def _data_dict(data: Any) -> dict[str, Any]:
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise Fault(
            "billing_unknown",
            "上游返回的数据结构不可用；是否计费待核，不得自动重发。",
        )
    return data


def _is_denied(message: Any) -> bool:
    if not isinstance(message, str):
        return False
    return any(
        token in message
        for token in (
            "无查询权限",
            "没有查询权限",
            "无权限",
            "没有权限",
            "权限不足",
            "未授权",
            "无权",
            "未开通",
            "未订阅",
            "订阅已过期",
            "未购买",
            "禁止访问",
        )
    )


def _raise_for_status(status: int) -> None:
    if status in (401, 403):
        raise Fault("upstream_denied", "上游网关拒绝访问；请检查产品订阅与凭据权限。")
    if status == 408 or 500 <= status <= 599:
        raise Fault(
            "billing_unknown",
            "上游网关未给出确定结果；是否计费待核，不得自动重发。",
        )
    if status == 429:
        raise Fault("upstream_unavailable", "上游网关限流；本次查询未取得结果。")
    if 300 <= status <= 399:
        raise Fault("upstream_unavailable", "上游网关返回重定向；本服务拒绝跟随。")
    if 400 <= status <= 499:
        raise Fault("upstream_rejected", "上游网关拒绝了本次请求。")
    raise Fault(
        "billing_unknown",
        "上游网关返回了非预期响应；是否计费待核，不得自动重发。",
    )


def call_api(
    product_id: str, params: dict[str, Any], client: httpx.Client, enabled: bool,
) -> dict[str, Any]:
    """校验参数后用现有网关协议发一次签名 POST，不重试。"""
    clean = _validate_call(product_id, params)
    if not enabled:
        raise Fault('live_disabled', '旧配置 PORTRAIT_LIVE=0 禁用了真实查询；未发起请求。')
    if client.is_closed:
        raise Fault("configuration_error", "上游网关客户端已关闭；未发起请求。")
    integrator_id, secret_id, secret_key = _credentials()
    if integrator_id in (".", "..") or "/" in integrator_id or "\\" in integrator_id:
        raise Fault(
            "configuration_error",
            "INTEGRATOR_ID 不是有效的对接器标识；未发起请求。",
        )
    try:
        params_json = json.dumps(clean, ensure_ascii=False)
    except (TypeError, ValueError):
        raise Fault("internal_error", "上游请求参数无法规范化；未发起请求。") from None

    form = {
        "product_id": product_id,
        "secret_id": secret_id,
        "params": params_json,
    }
    form["signature"] = _signature(form, secret_key)
    url = f"{GATEWAY_BASE_URL}/{quote(integrator_id, safe='')}"

    try:
        response = client.post(url, data=form)
    except httpx.TimeoutException:
        raise Fault(
            "billing_unknown",
            "上游网关请求超时；是否计费待核，不得自动重发。",
        ) from None
    except _NOT_SENT_ERRORS:
        raise Fault("upstream_unavailable", "上游网关未接通；本次请求未送达。") from None
    except httpx.HTTPError:
        raise Fault(
            "billing_unknown",
            "上游网关请求中断；是否计费待核，不得自动重发。",
        ) from None
    except Exception:
        raise Fault(
            "billing_unknown",
            "上游网关请求异常；是否计费待核，不得自动重发。",
        ) from None

    if response.status_code != 200:
        _raise_for_status(response.status_code)
    try:
        payload = response.json()
    except Exception:
        raise Fault(
            "billing_unknown",
            "上游网关响应无法解析；是否计费待核，不得自动重发。",
        ) from None
    if not isinstance(payload, dict):
        raise Fault(
            "billing_unknown",
            "上游网关响应格式异常；是否计费待核，不得自动重发。",
        )

    code = payload.get("code")
    code_text = None if code is None else str(code).strip()
    message = payload.get("msgCN") or payload.get("message")

    # 权限/订阅拒绝优先于状态码与“查询数据为空”，绝不把拒绝写成无记录。
    if _is_denied(message):
        raise Fault("upstream_denied", "上游网关拒绝访问；请检查产品订阅与凭据权限。")
    if code_text in _SUCCESS_CODES:
        if "data" not in payload and message != _EMPTY_MESSAGE:
            raise Fault("billing_unknown", "上游成功信封缺少数据字段；计费待核，不自动重发。")
        return _data_dict(payload.get("data"))
    if code_text == _EMPTY_CODE or (code_text is None and message == _EMPTY_MESSAGE):
        return {}
    if code_text is None:
        raise Fault(
            "billing_unknown",
            "上游网关未返回可识别的状态；是否计费待核，不得自动重发。",
        )
    raise Fault("upstream_rejected", "上游网关拒绝了本次查询。")


# 补全产品证据表中只声明了父容器的已知子字段。未登记的子对象不整块透传。
_REGION = 'province city district name value'
_AMOUNT = 'value unit coinType origin'
_RANGE = 'year growth_rate ' + ' '.join(
    f'{edge}.{key}' for edge in ('min', 'max') for key in _AMOUNT.split()
)
_PATENT_COUNTS = 'appearanceDesignPatentCount inventionLicPatentCount utilityModelPatentCount inventionAppPatentCount'
_STRUCTURED_FIELDS = {
    'mainDeviceList': 'equipName brand equipVersion equipNum',
    'productCategoriesStatInfo': 'name value',
    'resultList.projectRegion': _REGION,
    'resultList.purchasingBidderList': 'nameId name',
    'resultList.winningBidderList': 'nameId name',
    'biddingCustomers.biddingRegion': _REGION,
    'resultList.region': _REGION,
    'resultList.contractAmount': _AMOUNT,
    'resultList.hsCodeName': 'firstProduct firstProductCode secondProduct secondProductCode thirdProduct thirdProductCode',
    'storeList.brandClassification': 'firstClassify secondClassify',
    'storeList.brandStoreCityStats': 'city count',
    'storeList.brandStoreProvinceStats': 'province count',
    'patentTypeStat': _PATENT_COUNTS,
    'patentDivAppLegalStat': 'patentDivAppLegal patentCount',
    'patentTypeAppTimeStat': 'year ' + _PATENT_COUNTS,
    'patentTypePubTimeStat': 'year ' + _PATENT_COUNTS,
    'resultList.stdDraftUnit': 'name nameId',
    'resultList.officeAddress': _REGION,
    'entDetail.parentEnterpriseRegCapital': _AMOUNT,
    'resultList.addressValue': _REGION,
    'resultList.regCapital': _AMOUNT,
    'resultList.subscriptionAmount': _AMOUNT,
    'resultList.operStatus': 'name value',
    'annualReportList.arHolderList': (
        'holder payCapitalMode subscriptionCapitalTime subscriptionCapitalMode payAmount subscriptionAmount payDate'
    ),
    'resultList.recruitingDistrict': 'name value province city',
    'resultList.workingAddress': 'value province city',
    'resultList.workingSalary': 'min max coinType',
    'resultList.workingSeniority': 'min max',
    'tmAppTimeStat': 'year count',
    'tmRegTimeStat': 'year count',
    'tmStatusStat': 'tmStatus count',
    'tmTypeStats': 'tmName count',
    **{path: _RANGE for path in ('incomeList', 'profitList', 'taxList', 'contributeCapitalList', 'registerCapitalList')},
}
_IDENTITY_FIELDS = {
    'portrait_query_enterprise_base': ('nameId', 'entName'),
    'portrait_query_enterprise_search': (
        'resultList.entName', 'resultList.socialCreditCode',
        'resultList.catchReason.holderList', 'resultList.catchReason.recruitingName',
        'resultList.catchReason.address', 'resultList.catchReason.operBrandList',
        'resultList.catchReason.goodsNameList', 'resultList.catchReason.patentNameList',
        'resultList.catchReason.certNameList', 'resultList.catchReason.prmtKeys',
    ),
}


def _projection_error() -> Fault:
    return Fault('billing_unknown', '上游业务数据结构无法验证；是否计费待核，不得自动重发。')


def _projection_paths() -> dict[str, set[str]]:
    fields_by_product = {}
    for pid, route in ROUTES.items():
        response, time = route['response'], route['time']
        fields = set(response['evidence']) | set(response['split_keys']) | set(time['fields'])
        fields.update(time.get('end_fields', ()))
        fields.update(_IDENTITY_FIELDS.get(route['tool_name'], ()))
        fields.update(
            f'{field}.{child}'
            for field in list(fields)
            for child in _STRUCTURED_FIELDS.get(field, '').split()
        )
        fields_by_product[pid] = fields
    # 来源业务键必须从父工具可见；使用路由映射本身，避免两边另写一份。
    for route in ROUTES.values():
        if chain := route.get('chain'):
            fields_by_product[chain['parent_product_id']].update(
                field['source'].replace('[]', '') for field in chain['mapping']
            )
    return fields_by_product


def _field_tree(paths: set[str]) -> dict[str, Any]:
    tree: dict[str, Any] = {}
    for path in paths:
        node = tree
        for part in path.split('.'):
            node = node.setdefault(part, {})
    return tree


def _scalar(value: Any) -> bool:
    return value is None or isinstance(value, (bool, str, int, float))


def _include_shape(value: Any, tree: dict[str, Any], path: str, leaves: set[str]) -> Any:
    if isinstance(value, dict):
        if not tree and path != '$':
            if value:
                raise _projection_error()
            return {}
        projected = {
            key: _include_shape(value[key], branch, key if path == '$' else f'{path}.{key}', leaves)
            for key, branch in tree.items() if key in value
        }
        if value and not projected:
            raise _projection_error()
        return projected

    if isinstance(value, list):
        items = []
        for item in value:
            if isinstance(item, dict) and tree and item:
                items.append(_include_shape(item, tree, path, leaves))
            elif _scalar(item) and path in leaves:
                items.append(item)
            else:
                raise _projection_error()
        return items

    if _scalar(value) and (value is None or path in leaves):
        return value
    raise _projection_error()


def _is_count(value: Any) -> bool:
    return (
        type(value) is int and value >= 0
        or type(value) is float and value.is_integer() and value >= 0
        or isinstance(value, str) and value.strip().isascii() and value.strip().isdigit()
    )


def project_data(product_id: str, data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise _projection_error()
    for path in ROUTES[product_id]['response']['record_paths']:
        if path == '$' or path not in data:
            continue
        rows = data[path]
        if not isinstance(rows, list) or any(not isinstance(row, dict) or not row for row in rows):
            raise _projection_error()
    projected = _include_shape(data, _TREES[product_id], '$', _FIELDS_BY_PRODUCT[product_id])
    # 分页汇总可能返回整数、整数浮点数或数字字符串；保留原值，但不可把
    # 页结构损坏或缺失伪装为已查完且无记录。
    if 'total' in projected and not _is_count(projected['total']):
        raise _projection_error()
    if product_id in _PAGED_PRODUCTS and projected:
        if not any(path in projected for path in ROUTES[product_id]['response']['record_paths']):
            if 'total' not in projected or int(projected['total']) != 0:
                raise _projection_error()
    return projected


QUERY_CONTRACT = 'v1'
OUTPUT_SCHEMA = {
    'type': 'object',
    'properties': {'data': {'type': 'object', 'description': '当前单次查询的上游业务数据（只保留声明字段）。'}},
    'required': ['data'],
    'additionalProperties': False,
}
KEYWORD_TYPES = {
    'name': '企业全称',
    'nameId': '旷湖企业 ID（从已核验记录获取）',
    'regNumber': '注册号',
    'socialCreditCode': '统一社会信用代码',
    'reportStockCode': '已知上市主体的证券代码',
}
FIELD_DESCRIPTIONS = {
    'address': '单一地区；省级与市级用英文逗号分隔，例如 广东省,深圳市；不是日期或全国列表。',
    'arYear': '报告年度，四位字符串 YYYY（例如 "2024"），不可晚于当前年份，不传 null。',
    'pageIndex': '从 1 开始，每次只取指定页；工具不会自动追页。',
    'pageSize': '每页记录上限，须为整数；不是总记录数或总页数。',
    'platform': '同一店铺记录的原始平台名，不替换成平台别名。',
    'shopId': '同一店铺记录的 shopId，不传企业 ID、店名或产品 ID。',
    'statsMonth': '月份字符串 YYYYMM（例如 "202505"），不可晚于当前月；不是 YYYY-MM 或数字。',
}
MATCH_KEYWORDS = {
    'candidate': '单个待核验企业的名称、曾用名或关键词。返回候选而非确权结果，不自动选择第一条。',
    'project': '建筑项目 _id，不是企业 ID、biddingId、项目名称或产品 ID。',
    'brand': '餐饮品牌 brandId，不是企业 ID、品牌名称或产品 ID。',
    'enterprise_or_stock': '单个已明确上市主体的企业标识或证券代码；必须和 keywordType 对应。',
}


def _param_schema(field: dict[str, Any], route: dict[str, Any], source: str | None) -> dict[str, Any]:
    name = field['name']
    field_schema: dict[str, Any] = {'type': field['type']}
    if field['type'] == 'integer':
        field_schema['minimum'] = 1
    else:
        field_schema.update(minLength=1, pattern=r'\S')
    if name == 'statsMonth':
        field_schema['pattern'] = r'^[0-9]{4}(0[1-9]|1[0-2])$'
    elif name == 'arYear':
        field_schema['pattern'] = r'^[0-9]{4}$'
    if name == 'matchKeyword':
        description = MATCH_KEYWORDS.get(
            route['inputs']['subject'],
            '单个企业标识，与 keywordType 对应；省略 keywordType 时用企业全称，不传产品 ID 或逗号分隔的名单。',
        )
    elif name == 'keywordType':
        description = '匹配类型：' + '；'.join(f'{key}={KEYWORD_TYPES[key]}' for key in field['enum']) + '。'
    else:
        description = FIELD_DESCRIPTIONS[name]
    if source:
        description += f' 必须使用同一条父工具记录的 {source}。'
    if name == 'arYear':
        description += ' ' + route['inputs']['note']
    if field.get('enum') is not None:
        field_schema['enum'] = field['enum']
    if field.get('max') is not None:
        field_schema['maximum'] = field['max']
        description += f' 最大 {field["max"]}。'
    if field.get('default') is not None:
        field_schema['default'] = field['default']
        description += f' 省略时上游默认 {field["default"]}。'
    elif not field.get('required'):
        description += ' 不筛选时省略，不传 null。'
    field_schema['description'] = description
    return field_schema


def _tool(product_id: str, route: dict[str, Any]) -> types.Tool:
    fields = route['inputs']['fields']
    chain = route.get('chain')
    parent = ROUTES[chain['parent_product_id']]['tool_name'] if chain else None
    sources = {
        mapping['target']: f'{parent} 的 data.{mapping["source"]}'
        for mapping in (chain['mapping'] if chain else ())
    }
    description = [
        f'查询{route["name"]}。',
        route['inputs'].get('note', ''),
        route.get('time', {}).get('note', ''),
    ]
    if parent:
        description.append(f'先用 {parent} 获取来源业务标识，再按入参说明取值；缺少父键时不要猜测。')
    records = [f'data.{path}' for path in route['response']['record_paths'] if path != '$']
    description.append('记录位于 ' + '、'.join(records) + '。' if records else '业务字段位于 data 对象。')
    description.append('一次调用只查这个产品一次（可能计费）；不自动分页、重试或保存结果。空成功不代表字段值为零。')
    if any(field['name'] == 'pageIndex' for field in fields):
        description.append('有页码时只返回一页；没有明确授权不要继续翻页。')
    return types.Tool(
        name=route['tool_name'],
        title=route['name'],
        description=' '.join(filter(None, description)),
        inputSchema={
            'type': 'object',
            'properties': {f['name']: _param_schema(f, route, sources.get(f['name'])) for f in fields},
            'required': [f['name'] for f in fields if f['required']],
            'additionalProperties': False,
        },
        outputSchema=OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(readOnlyHint=False, idempotentHint=False, openWorldHint=True),
        _meta={'portrait_query_contract': QUERY_CONTRACT, 'portrait_product_id': product_id},
    )


LOG = logging.getLogger('portrait')


def error(code: str, message: str) -> types.CallToolResult:
    return types.CallToolResult(
        isError=True,
        content=[types.TextContent(type='text', text=pack({'code': code, 'message': message}))],
    )


class BearerGuard:
    def __init__(self, app, token: bytes):
        self.app = app
        self.header = b'Bearer ' + token

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http':
            auth = [value for key, value in scope['headers'] if key == b'authorization']
            if len(auth) != 1 or not hmac.compare_digest(auth[0], self.header):
                response = JSONResponse({'error': 'unauthorized'}, status_code=401,
                                        headers={'WWW-Authenticate': 'Bearer'})
                return await response(scope, receive, send)
        return await self.app(scope, receive, send)


def _allowlist(name: str) -> list[str]:
    values = [value.strip() for value in os.getenv(name, '').split(',') if value.strip()]
    if any('*' in value for value in values):
        raise Fault('configuration_error', f'{name} 只能配置明确的可信地址。')
    return values


class PortraitMCP(FastMCP):
    """保留原始 JSON 参数与 isError 契约，避免 SDK 隐式转换入参。"""

    def __init__(self, *, enabled: bool = True, transport: httpx.BaseTransport | None = None):
        super().__init__(
            'customer-cohort-profile', stateless_http=True, json_response=True,
            instructions='每个工具只查询一个固定产品或一页，可能计费。名单、计划、进度和结果由调用方管理。',
        )
        self.enabled = enabled
        self.client = httpx.Client(
            transport=transport, timeout=REQUEST_TIMEOUT_SECONDS, follow_redirects=False, trust_env=False,
        )

    async def list_tools(self) -> list[types.Tool]:
        return TOOLS.copy()

    async def call_tool(  # type: ignore[override]
        self, name: str, arguments: dict[str, Any],
    ) -> types.CallToolResult:
        if (product_id := PRODUCT_IDS.get(name)) is None:
            return error('unknown_tool', '工具不存在，请重新获取工具清单。')
        try:
            raw = await anyio.to_thread.run_sync(call_api, product_id, arguments, self.client, self.enabled)
            result = {'data': project_data(product_id, raw)}
            text = pack(result)
        except Fault as exc:
            return error(exc.code, exc.message)
        except Exception as exc:
            # 可能已经执行上游查询，不得回显参数或重发。
            LOG.error('查询结果未知 tool=%s type=%s', name, type(exc).__name__)
            return error('billing_unknown', '未取得可确认的结果；是否计费待核，不得自动重发。')
        return types.CallToolResult(
            structuredContent=result, content=[types.TextContent(type='text', text=text)],
        )

    def streamable_http_app(self) -> Starlette:
        token = os.getenv('CUSTOMER_COHORT_PROFILE_MCP_TOKEN', '')
        if not token.isascii() or not token.isprintable() or len(token.strip()) < 32:
            raise Fault('configuration_error', 'HTTP 模式需要配置至少 32 字符的专用访问令牌。')
        self.settings.transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=['127.0.0.1', '127.0.0.1:*', 'localhost', 'localhost:*', '[::1]', '[::1]:*']
            + _allowlist('PORTRAIT_ALLOWED_HOSTS'),
            allowed_origins=_allowlist('PORTRAIT_ALLOWED_ORIGINS'),
        )
        app = super().streamable_http_app()
        app.add_middleware(BearerGuard, token=token.encode('ascii'))
        return app

    def close(self) -> None:
        self.client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='企业客户画像洞察 MCP 服务', allow_abbrev=False)
    parser.add_argument('transport', nargs='?', default='stdio', choices=['stdio', 'streamable-http'])
    # 兼容现有 Skill：连接旧服务时，在任务恢复或查询前拒绝版本不匹配。
    parser.add_argument('--query-contract', choices=(QUERY_CONTRACT,), help='要求指定的查询契约版本')
    args = parser.parse_args()
    load_dotenv(Path(__file__).resolve().parents[1] / '.env')
    for logger in ('httpx', 'httpcore', 'mcp'):
        logging.getLogger(logger).setLevel(logging.WARNING)
    # 不再要求新用户设置 PORTRAIT_LIVE；不能让旧部署显式禁用的查询自动开启。
    live = os.getenv('PORTRAIT_LIVE', '1')
    if live not in ('0', '1'):
        raise Fault('configuration_error', 'PORTRAIT_LIVE 必须为 0 或 1。')
    mcp = PortraitMCP(enabled=live == '1')
    try:
        if args.transport == 'stdio':
            mcp.run(transport='stdio')
        else:
            port = os.getenv('PORTRAIT_HTTP_PORT', '8000')
            if not port.isascii() or not port.isdigit() or len(port) > 5 or not 1 <= int(port) <= 65535:
                raise Fault('configuration_error', 'HTTP 端口无效。')
            uvicorn.run(mcp.streamable_http_app(), host=os.getenv('PORTRAIT_HTTP_HOST', '127.0.0.1'),
                        port=int(port), access_log=False)
    finally:
        mcp.close()


# 固定数据产品定义：随源码发布，无需额外配置。
_MATCH_KEYWORD = {'name': 'matchKeyword', 'type': 'string', 'required': True}
_KEYWORD_TYPE = {
    'name': 'keywordType',
    'type': 'string',
    'required': False,
    'default': 'name',
    'enum': ['name', 'nameId', 'regNumber', 'socialCreditCode'],
}
_PAGE_INDEX = {'name': 'pageIndex', 'type': 'integer', 'required': False, 'default': 1}
_PAGE_SIZE_50 = {'name': 'pageSize', 'type': 'integer', 'required': False, 'default': 50, 'max': 50}

ROUTES: dict[str, dict[str, Any]] = {
    '66dbccbec7a7e3460f5e613f': {
        'tool_name': 'portrait_query_enterprise_base',
        'name': '工商照面',
        'topic': 'base',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'name', 'socialCreditCode', 'regNumber', 'formerNames', 'addressValue.province',
                'addressValue.city', 'addressValue.district', 'industry.firstIndustry', 'industry.secondIndustry',
                'industry.thirdIndustry', 'industry.fourthIndustry', 'industry.originCode', 'foundTime',
                'enterpriseType', 'operStatus', 'regCapital.value', 'regCapital.coinType', 'calEnterpriseScale',
                'business', 'licensingDate', 'deactivateDate', 'revokeDate',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'foundTime',
                'licensingDate',
                'deactivateDate',
                'revokeDate',
            ],
            'filter': None,
            'snapshot': True,
            'note': '工商登记当前快照；成立/核准/吊销/注销日期只是字段日期，接口无历史时点参数，不能还原任意截点状态。',
        },
    },
    '669e531ce1fd7bff82321d8d': {
        'tool_name': 'portrait_query_enterprise_tags',
        'name': '企业标签',
        'topic': 'base',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'businessTags', 'enterpriseClassification', 'enterpriseScaleAlgValue', 'hasStock',
                'isHighTechEnterprise', 'isSpecializedAndNew', 'isSpecializedAndNewGiant', 'isUnicornEnterprise',
                'isGazelleEnterprise', 'isTopEnterprise', 'isAnomaly', 'operStatus', 'financingSeries',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '标签为当前快照，不能补写成交当年特征；标签不能单独证明交易、产量或收入。',
        },
    },
    '67f395f086f49583db6a7939': {
        'tool_name': 'portrait_query_enterprise_metrics',
        'name': '企业指标',
        'topic': 'base',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'enterpriseSize', 'isMicroEnterprise', 'isAboveScaleIndustry', 'isAboveScaleWholesaleRetail',
                'isAboveScaleAccommodationCatering', 'isAboveScaleService',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '企业规模与规上/限上标记为当前快照，无统计年份；规模主图用本口径，不用注册资本或员工数猜四档。',
        },
    },
    '675cea1f0e009a9ea37edaa1': {
        'tool_name': 'portrait_query_enterprise_search',
        'name': '关键词查询企业',
        'topic': 'identity',
        'inputs': {
            'subject': 'candidate',
            'fields': [
                _MATCH_KEYWORD,
                _PAGE_INDEX,
                {'name': 'pageSize', 'type': 'integer', 'required': False, 'default': 10, 'max': 50},
            ],
            'note': '仅传 matchKeyword 与可选 pageIndex/pageSize，不传 keywordType；只用于核实名单主体，不扩大名单。',
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.nameId'],
            'evidence': [
                'resultList.nameId', 'resultList.name', 'resultList.address', 'resultList.foundTime',
                'resultList.operStatus', 'resultList.catchReason.name', 'resultList.catchReason.formerNames',
                'resultList.catchReason.socialCreditCode', 'total',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'resultList.foundTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '候选检索为当前快照；命中原因用于区分名称/曾用名/信用代码，不证明业务关系。',
        },
    },
    '669fabd89dc832f54db93e56': {
        'tool_name': 'portrait_query_enterprise_industry',
        'name': '企业行业（国标）',
        'topic': 'supplement',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'firstIndustry', 'firstIndustryCode', 'secondIndustry', 'secondIndustryCode', 'thirdIndustry',
                'thirdIndustryCode', 'fourthIndustry', 'fourthIndustryCode',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '国标行业为当前登记快照；工商照面已返回的相同字段不重复查。',
        },
    },
    '66c33eff3c0917a9a02feb6f': {
        'tool_name': 'portrait_query_enterprise_product_tags',
        'name': '企业产品标签',
        'topic': 'supplement',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {'record_paths': ['$'], 'split_keys': [], 'evidence': ['tagNames']},
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '产品标签为当前快照，只能补产品主题，不能证明交易、产量或收入。',
        },
    },
    '6682b0b370f56cb7d77701e0': {
        'tool_name': 'portrait_query_enterprise_summary',
        'name': '企业简介',
        'topic': 'supplement',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {'record_paths': ['$'], 'split_keys': [], 'evidence': ['desc']},
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '企业简介为当前文本快照，用于核对“提供什么、服务谁、应用在哪”，保留原文依据。',
        },
    },
    '66aa4eac4bb1f40b86c46ebd': {
        'tool_name': 'portrait_query_factory_info',
        'name': '工厂信息',
        'topic': 'factory',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'factoryAddress.province', 'factoryAddress.city', 'factoryAddress.district',
                'factoryAddress.value', 'factoryTypeList', 'companyTechnicList', 'factoryControlOemMode',
                'agentBrandInfo', 'isSupportProofing', 'factoryInspectionType', 'inspectionGroup',
                'managementSystemCertification', 'enterpriseCertification', 'assemblyLine', 'mainDeviceList',
                'factoryAreaValue', 'factoryScale', 'enterpriseScaleAlgValueV2', 'annualTurnoverAlgValueV2',
                'clients', 'foundTime', 'description',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'foundTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '工厂资料为当前快照；缺少工厂记录不等于不生产，工厂成立日期不能替代企业成立日期。',
        },
    },
    '6725e5b9ba65854594baebd2': {
        'tool_name': 'portrait_query_factory_products',
        'name': '工厂产品统计',
        'topic': 'factory',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': ['mainProductCount', 'productCategoriesStatInfo', 'serviceBrandCount', 'tagNames'],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '产品类目统计为当前快照聚合，不代表交易、产量或收入。',
        },
    },
    '66b71b498dd25dbfbe8b0a90': {
        'tool_name': 'portrait_query_enterprise_channels',
        'name': '企业渠道信息',
        'topic': 'channel',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'productList', 'channelTypeList', 'tradeModelList', 'domesticMainMarketList',
                'foreignMainMarketList', 'foreignMainMarketValueList', 'domesticTradeSourceList',
                'productBrandList', 'shopNum', 'businessScopeList', 'description', 'foundTime',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'foundTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '渠道资料为当前快照；销售区域不是注册地，展示产品不是采购标的。',
        },
    },
    '66bf124bf134a4c21b4fc2fa': {
        'tool_name': 'portrait_query_enterprise_bids',
        'name': '企业招投标信息',
        'topic': 'projects',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.biddingId'],
            'evidence': [
                'resultList.biddingId', 'resultList.title', 'resultList.infoType', 'resultList.role',
                'resultList.publishDate', 'resultList.projectAmount', 'resultList.projectRegion',
                'resultList.purchasingBidderList', 'resultList.winningBidderList', 'resultList.subjectMatterList',
                'total',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'resultList.publishDate',
            ],
            'filter': None,
            'snapshot': True,
            'note': '公告发布时间为记录日期；接口无期间过滤，分页每页最多50条，返回集合是当前库快照，不能声称覆盖任一年度全部公告。',
        },
    },
    '66bf124bf134a4c21b4fc30b': {
        'tool_name': 'portrait_query_enterprise_business_relations',
        'name': '企业经营关系图谱',
        'topic': 'projects',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['biddingCustomers', 'biddingSuppliers'],
            'split_keys': ['biddingCustomers.customId', 'biddingSuppliers.nameId'],
            'evidence': [
                'biddingCustomers.customId', 'biddingCustomers.customName', 'biddingCustomers.cooperationTimes',
                'biddingCustomers.totalAmount', 'biddingCustomers.anncPubTime', 'biddingCustomers.biddingId',
                'biddingCustomers.biddingTitle', 'biddingCustomers.biddingProjectType',
                'biddingCustomers.biddingRegion', 'biddingSuppliers.nameId', 'biddingSuppliers.name',
                'biddingSuppliers.cooperationTimes', 'biddingSuppliers.totalAmount',
                'biddingSuppliers.anncPubTime', 'biddingSuppliers.biddingId', 'biddingSuppliers.biddingTitle',
                'biddingSuppliers.projectAmount', 'biddingSuppliers.subjectMatter',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'biddingCustomers.anncPubTime',
                'biddingSuppliers.anncPubTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '累计合作次数/金额为当前汇总，anncPubTime 只是最近合作日期，不是完整合作序列；接口无期间过滤。',
        },
    },
    '66aba795520b164ce252e5e7': {
        'tool_name': 'portrait_query_construction_projects',
        'name': '建筑工程项目明细',
        'topic': 'construction',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList._id'],
            'evidence': [
                'resultList._id', 'resultList.projectName', 'resultList.projectType', 'resultList.region',
                'resultList.totalInvestment', 'total',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '接口与响应均无项目日期字段；不能按年筛选或判断项目时点。',
        },
        'feeds': [{'product_id': '66aba795520b164ce252e60d', 'mapping': ['resultList[]._id→matchKeyword']}],
    },
    '66aba795520b164ce252e60d': {
        'tool_name': 'portrait_query_construction_contracts',
        'name': '建筑业项目合同信息',
        'topic': 'construction',
        'inputs': {
            'subject': 'project',
            'fields': [_MATCH_KEYWORD, _PAGE_INDEX, _PAGE_SIZE_50],
            'note': 'matchKeyword 只接收建筑项目 _id，不能传企业 ID 或标讯 biddingId；一个项目的多条合同记录不能算多个项目。',
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.contractId'],
            'evidence': [
                'resultList.contractId', 'resultList.contractNum', 'resultList.contractType',
                'resultList.signingDate', 'resultList.recordTime', 'resultList.contractAmount',
                'resultList.proprietor', 'resultList.proprietorSocialCreditCode', 'resultList.contractingUnit',
                'resultList.contractingUnitSocialCreditCode', 'resultList.consortiumContractor',
                'resultList.constructionScale', 'resultList.engineeringName', 'resultList.projectName',
                'resultList.dataSource', 'resultList.dataGrade', 'total',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'resultList.signingDate',
                'resultList.recordTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '签约/登记日期为记录日期；接口无期间过滤，分页每页最多50条，返回集合是当前库快照。',
        },
        'chain': {
            'parent_product_id': '66aba795520b164ce252e5e7',
            'parent_records': ['resultList'],
            'mapping': [{'target': 'matchKeyword', 'source': 'resultList[]._id'}],
        },
    },
    '66c9a953268e9d7292c1a928': {
        'tool_name': 'portrait_query_trade_details',
        'name': '企业外贸数据明细',
        'topic': 'trade',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList._id'],
            'evidence': [
                'resultList._id', 'resultList.tradeTime', 'resultList.buyerName', 'resultList.productName',
                'resultList.hsCode', 'resultList.hsCodeName', 'resultList.origCountry', 'resultList.origPort',
                'resultList.destCountry', 'resultList.destPort', 'resultList.incoterm', 'total',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'resultList.tradeTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '贸易日期为记录日期；接口无期间过滤，分页每页最多50条，返回集合是当前库快照，不能当完整贸易流水。',
        },
    },
    '66aa4eac4bb1f40b86c46ea0': {
        'tool_name': 'portrait_query_trade_destinations',
        'name': '企业外贸订单地区',
        'topic': 'trade',
        'inputs': {
            'subject': 'enterprise',
            'fields': [
                _MATCH_KEYWORD,
                _KEYWORD_TYPE,
                {'name': 'pageIndex', 'type': 'integer', 'required': False},
                {'name': 'pageSize', 'type': 'integer', 'required': False, 'max': 50},
            ],
            'note': 'pageIndex/pageSize 无默认值，pageSize 最多50；amount 单位为美元、weight 单位为千克。',
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.destCountry'],
            'evidence': [
                'resultList.destCountry', 'resultList.count', 'resultList.amount', 'resultList.weight', 'total',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '聚合结果无期间字段且无时间过滤；不得用其冒充用户指定期间数据，也不能当同期全量。',
        },
    },
    '66aa4eac4bb1f40b86c46eab': {
        'tool_name': 'portrait_query_trade_export_trends',
        'name': '企业外贸出口趋势',
        'topic': 'trade',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['exportTrend'],
            'split_keys': ['exportTrend.year'],
            'evidence': ['exportTrend.year', 'exportTrend.count', 'exportTrend.amount', 'total'],
        },
        'time': {
            'basis': 'year_series',
            'fields': [
                'exportTrend.year',
            ],
            'filter': None,
            'snapshot': True,
            'note': '按年聚合，接口无年份过滤；年份由记录给出，可跨年比较，但集合为当前快照、覆盖年份不保证；amount 单位为美元。',
        },
    },
    '66f3d8bf64bd2be52d68a0e9': {
        'tool_name': 'portrait_query_restaurant_brands',
        'name': '企业旗下餐饮品牌门店',
        'topic': 'food_chain',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['storeList'],
            'split_keys': ['storeList.brandId'],
            'evidence': [
                'storeList.brandId', 'storeList.brandName', 'storeList.brandClassification',
                'storeList.brandStoreNum', 'storeList.mallStoreNum', 'storeList.brandStoreCityStats',
                'storeList.brandStoreProvinceStats', 'storeList.brandCradle', 'storeTotal',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '门店为当前快照；城市/省份分布只返回前十条，企业摘要前十城不是全国；品牌店不等于法人自营店。',
        },
        'feeds': [{'product_id': '66f3d8c064bd2be52d68a159', 'mapping': ['storeList[].brandId→matchKeyword']}],
    },
    '66f3d8c064bd2be52d68a159': {
        'tool_name': 'portrait_query_restaurant_brand_locations',
        'name': '餐饮品牌门店统计',
        'topic': 'food_chain',
        'inputs': {
            'subject': 'brand',
            'fields': [
                _MATCH_KEYWORD,
                _PAGE_INDEX,
                {'name': 'pageSize', 'type': 'integer', 'required': False, 'default': 10, 'max': 50},
            ],
            'note': 'matchKeyword 只接收 brandId；同品牌只取一次，不能传企业名称。',
        },
        'response': {
            'record_paths': ['cityStatsList', 'provinceStatsList'],
            'split_keys': ['cityStatsList.city', 'provinceStatsList.province'],
            'evidence': [
                'cityStatsList.city', 'cityStatsList.count', 'cityStatsTotal', 'provinceStatsList.province',
                'provinceStatsList.count', 'provinceStatsTotal',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '品牌门店统计为当前快照；接口无时间过滤，不能当历史同期门店数；这条链只用于餐饮品牌。',
        },
        'chain': {
            'parent_product_id': '66f3d8bf64bd2be52d68a0e9',
            'parent_records': ['storeList'],
            'mapping': [{'target': 'matchKeyword', 'source': 'storeList[].brandId'}],
        },
    },
    '66d5b7df537c3f61d646c327': {
        'tool_name': 'portrait_query_enterprise_estore_overview',
        'name': '企业国内外网店概况',
        'topic': 'estore',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'domesticEshopCount', 'domesticEshopPlatformList', 'domesticEshopBrandList',
                'domesticEshopProductList', 'domesticEshopSourceList', 'domesticEshopProductCount',
                'foreignEshopCount', 'foreignEshopPlatformList', 'foreignEshopBrandList',
                'foreignEshopProductList', 'foreignEshopSourceList', 'foreignEshopProductCount',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '网店概况为当前快照；区分多平台开店与多平台发生销售，概况不代表销售额或全渠道营收。',
        },
    },
    '684ad781d4819127431fa004': {
        'tool_name': 'portrait_query_enterprise_estore_shops',
        'name': '企业店铺明细',
        'topic': 'estore',
        'inputs': {
            'subject': 'enterprise_month',
            'fields': [
                _MATCH_KEYWORD,
                {
                    'name': 'keywordType',
                    'type': 'string',
                    'required': False,
                    'default': 'name',
                    'enum': ['name', 'regNumber', 'socialCreditCode', 'nameId'],
                },
                {'name': 'statsMonth', 'type': 'string', 'required': True, 'note': 'YYYYMM，如 202505'},
            ],
            'note': 'statsMonth 必填；无分页参数，返回的是该月已收录店铺集合。',
        },
        'response': {
            'record_paths': ['shopList'],
            'split_keys': ['shopList.platform', 'shopList.shopId', 'shopList.statsMonth'],
            'evidence': [
                'nameId', 'name', 'companyShopNum', 'level', 'score', 'shopList.platform', 'shopList.shopId',
                'shopList.shopName', 'shopList.statsMonth', 'foundTime',
            ],
        },
        'time': {
            'basis': 'month',
            'fields': [
                'shopList.statsMonth',
            ],
            'filter': 'statsMonth',
            'snapshot': False,
            'note': 'statsMonth 必填（YYYYMM）；只代表该月已收录店铺集合，缺报/未收录不可见，不能当全渠道店铺全集。',
        },
        'feeds': [
            {
                'product_id': '684ad781d4819127431f9ff8',
                'mapping': [
                    'shopList[].platform→platform', 'shopList[].shopId→shopId',
                    'shopList[].statsMonth→statsMonth',
                ],
            },
        ],
    },
    '684ad781d4819127431f9ff8': {
        'tool_name': 'portrait_query_estore_monthly_sales',
        'name': '店铺月销售情况',
        'topic': 'estore',
        'inputs': {
            'subject': 'shop_month',
            'fields': [
                {'name': 'platform', 'type': 'string', 'required': True},
                {'name': 'shopId', 'type': 'string', 'required': True},
                {'name': 'statsMonth', 'type': 'string', 'required': True, 'note': 'YYYYMM，如 202505'},
            ],
            'note': 'platform、shopId、statsMonth 三键必填且必须同源同月。',
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': ['platform', 'shopId'],
            'evidence': [
                'platform', 'shopId', 'shopName', 'shopSalesAmountSumMth', 'shopSalesAmountSumMthYoy',
                'shopSalesVolumeSumMth', 'shopSalesVolumeSumMthYoy',
            ],
        },
        'time': {
            'basis': 'month',
            'fields': [],
            'filter': 'statsMonth',
            'snapshot': False,
            'note': 'statsMonth 必填（YYYYMM）；同一 platform+shopId 的当月值，金额单位万元、销量单位万件、同比单位%；只描述接口覆盖的可比店铺，不是企业总营收。',
        },
        'chain': {
            'parent_product_id': '684ad781d4819127431fa004',
            'parent_records': ['shopList'],
            'mapping': [
                {'target': 'platform', 'source': 'shopList[].platform'},
                {'target': 'shopId', 'source': 'shopList[].shopId'},
                {'target': 'statsMonth', 'source': 'shopList[].statsMonth'},
            ],
        },
    },
    '66b338e274bf098447db7ed0': {
        'tool_name': 'portrait_query_software_copyrights',
        'name': '软著明细',
        'topic': 'technology',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.srRegisterNum'],
            'evidence': [
                'resultList.srSoftwareName', 'resultList.srSoftwareShortName', 'resultList.srSoftwareVersion',
                'resultList.srRegisterNum', 'resultList.srRegisterDate', 'resultList.srPubDate', 'total',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'resultList.srRegisterDate',
                'resultList.srPubDate',
            ],
            'filter': None,
            'snapshot': True,
            'note': '登记/发布日期为记录日期；接口无期间过滤，分页每页最多50条，返回集合是当前库快照；软著不等于对外销售。',
        },
    },
    '66d5b7df537c3f61d646c230': {
        'tool_name': 'portrait_query_patent_statistics',
        'name': '专利统计',
        'topic': 'technology',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['patentDivAppLegalStat', 'patentTypeAppTimeStat', 'patentTypePubTimeStat'],
            'split_keys': [
                'patentTypeAppTimeStat.year', 'patentTypePubTimeStat.year',
                'patentDivAppLegalStat.patentDivAppLegal',
            ],
            'evidence': [
                'patentTypeStat', 'patentDivAppLegalStat', 'patentTypeAppTimeStat', 'patentTypePubTimeStat',
            ],
        },
        'time': {
            'basis': 'year_series',
            'fields': [
                'patentTypeAppTimeStat.year',
                'patentTypePubTimeStat.year',
            ],
            'filter': None,
            'snapshot': True,
            'note': '申请/授权趋势按年，接口无年份过滤；仅为当前库中已收录专利的年度聚合，专利数量不等于竞争力。',
        },
    },
    '66bf124bf134a4c21b4fc38f': {
        'tool_name': 'portrait_query_industry_standards',
        'name': '行业标准信息',
        'topic': 'technology',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.stdStandardNumber'],
            'evidence': [
                'resultList.stdTitle', 'resultList.stdStandardNumber', 'resultList.stdStandardType',
                'resultList.stdStandardKind', 'resultList.stdStandardStatus', 'resultList.stdIssueDate',
                'resultList.stdImplementationDate', 'resultList.stdDraftUnit', 'total',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'resultList.stdIssueDate',
                'resultList.stdImplementationDate',
            ],
            'filter': None,
            'snapshot': True,
            'note': '发布/实施日期为记录日期；接口无期间过滤，分页每页最多50条，返回集合是当前库快照。',
        },
    },
    '66a2515d95054069ad854d06': {
        'tool_name': 'portrait_query_qualification_certificates',
        'name': '资质证书明细',
        'topic': 'technology',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.certId'],
            'evidence': [
                'resultList.certType', 'resultList.certId', 'resultList.certAuthority',
                'resultList.certPublishTime', 'resultList.certExpireTime', 'resultList.certStatus', 'total',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'resultList.certPublishTime',
                'resultList.certExpireTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '颁证/截止日期为记录日期，证书状态为当前快照；接口无期间过滤，分页每页最多50条，返回集合是当前库快照。',
        },
    },
    '6786528b1677add9f934359d': {
        'tool_name': 'portrait_query_office_statistics',
        'name': '办公地址统计',
        'topic': 'office',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': ['cityCount', 'officeCityStats.city', 'officeCityStats.count'],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '办公地址统计为当前快照；注册地、办公地、招聘地、项目地必须分开，不能互相替代。',
        },
    },
    '6786528b1677add9f934358f': {
        'tool_name': 'portrait_query_office_details',
        'name': '办公地址明细',
        'topic': 'office',
        'inputs': {
            'subject': 'enterprise',
            'fields': [
                _MATCH_KEYWORD,
                _KEYWORD_TYPE,
                {'name': 'address', 'type': 'string', 'required': False, 'note': '单一省或市，省/市用英文逗号分隔'},
                _PAGE_INDEX,
                {'name': 'pageSize', 'type': 'integer', 'required': False, 'default': 10, 'max': 10},
            ],
            'note': 'address 仅支持单一省市，省/市用英文逗号分隔；分页每页最多10条。',
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.estateId'],
            'evidence': [
                'resultList.estateId', 'resultList.estateName', 'resultList.officeAddress',
                'resultList.officeSettleType', 'resultList.officeSourceType', 'total',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '办公地址明细为当前快照；address 是查询过滤条件，不是时间过滤；地址来源需保留。',
        },
    },
    '66bf124bf134a4c21b4fc2eb': {
        'tool_name': 'portrait_query_parent_enterprise',
        'name': '企业所属总公司查询',
        'topic': 'organization',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'nameId', 'entDetail.parentEnterpriseName', 'entDetail.parentEnterpriseFoundTime',
                'entDetail.parentEnterpriseRegCapital', 'legalRepresentative.parentEnterpriseLegal',
                'parentEnterpriseBranchNum',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'entDetail.parentEnterpriseFoundTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '总公司关系为当前快照；总分公司关系不能推为控股子公司或统一采购。',
        },
    },
    '669fa757c629692bdb8d80b7': {
        'tool_name': 'portrait_query_enterprise_branches',
        'name': '分支机构',
        'topic': 'organization',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList._id'],
            'evidence': [
                'resultList._id', 'resultList.name', 'resultList.socialCreditCode',
                'resultList.legalRepresentative', 'resultList.foundTime', 'resultList.operStatus',
                'resultList.addressValue', 'resultList.registrationAuthority', 'resultList.orgCode', 'total',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'resultList.foundTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '分支机构列表为当前快照；成立日期只是字段日期，接口无历史时点参数；分支不自动并入母公司。',
        },
    },
    '67189489ae286373219cdd32': {
        'tool_name': 'portrait_query_enterprise_scale',
        'name': '企业经营规模',
        'topic': 'workforce',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {'record_paths': ['$'], 'split_keys': [], 'evidence': ['annualTurnover', 'enterpriseScale']},
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '年营业额/人员规模为当前快照字段，无统计年份；不能与工商年报同年口径混用。',
        },
    },
    '66a0dfa111b5da4a2495bc91': {
        'tool_name': 'portrait_query_annual_reports',
        'name': '工商年报',
        'topic': 'workforce',
        'inputs': {
            'subject': 'enterprise_year',
            'fields': [
                _MATCH_KEYWORD,
                _KEYWORD_TYPE,
                {'name': 'arYear', 'type': 'string', 'required': False, 'note': '年报年份'},
            ],
            'note': 'arYear 可选；不传时返回库中已有年度，缺失年份不补零。',
        },
        'response': {
            'record_paths': ['annualReportList'],
            'split_keys': ['annualReportList.arYear'],
            'evidence': [
                'annualReportList.arYear', 'annualReportList.arPublishDate', 'annualReportList.arEmployeesNum',
                'annualReportList.arBusiness', 'annualReportList.arAddress',
                'annualReportList.totalOperatingIncome', 'annualReportList.mainOperatingIncome',
                'annualReportList.netProfit', 'annualReportList.profit', 'annualReportList.totalTax',
                'annualReportList.totalAssets', 'annualReportList.totalLiabilities',
                'annualReportList.totalEquity', 'annualReportList.oldAgeInsuranceNum',
                'annualReportList.medicalInsuranceNum', 'annualReportList.unemploymentInsuranceNum',
                'annualReportList.employmentInjuryInsuranceNum', 'annualReportList.maternityInsuranceNum',
                'annualReportList.arHolderList',
            ],
        },
        'time': {
            'basis': 'year',
            'fields': [
                'annualReportList.arYear',
                'annualReportList.arPublishDate',
            ],
            'filter': 'arYear',
            'snapshot': False,
            'note': '按 arYear 查询年度报告；不传 arYear 时返回库中已有年度，覆盖不保证；从业人数为年报披露值，缺失与 0 分开。',
        },
    },
    '66a25a5fc9953b6f556f310c': {
        'tool_name': 'portrait_query_social_insurance',
        'name': '社保信息',
        'topic': 'workforce',
        'inputs': {
            'subject': 'enterprise_year',
            'fields': [
                _MATCH_KEYWORD,
                _KEYWORD_TYPE,
                {'name': 'arYear', 'type': 'string', 'required': False, 'note': '年报年份'},
            ],
            'note': 'arYear 可选，默认返回最新一年；单一险种参保人数不能当员工总数。',
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'year', 'oldAgeInsuranceNum', 'oldAgeInsuranceBase', 'oldAgeInsurancePractical',
                'oldAgeInsuranceArrears', 'medicalInsuranceNum', 'medicalInsuranceBase',
                'medicalInsurancePractical', 'medicalInsuranceArrears', 'unemploymentInsuranceNum',
                'unemploymentInsuranceBase', 'unemploymentInsurancePractical', 'unemploymentInsuranceArrears',
                'employmentInjuryInsuranceNum', 'employmentInjuryInsuranceBase',
                'employmentInjuryInsurancePractical', 'employmentInjuryInsuranceArrears', 'maternityInsuranceNum',
                'maternityInsuranceBase', 'maternityInsurancePractical', 'maternityInsuranceArrears',
            ],
        },
        'time': {
            'basis': 'year',
            'fields': [
                'year',
            ],
            'filter': 'arYear',
            'snapshot': False,
            'note': '按 arYear 查询，默认最新一年；同年单一险种参保人数与年报从业人数分别统计，不混成一个“员工数”。',
        },
    },
    '66b338e274bf098447db7f09': {
        'tool_name': 'portrait_query_recruitment_details',
        'name': '招聘明细',
        'topic': 'performance',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': [
                'resultList.recruitingName', 'resultList.recruitingCity', 'resultList.recruitingPublishedTime',
            ],
            'evidence': [
                'resultList.recruitingName', 'resultList.recruitingCity', 'resultList.recruitingDistrict',
                'resultList.recruitingEducation', 'resultList.recruitingPublishedTime',
                'resultList.recruitingSource', 'resultList.workingAddress', 'resultList.workingSalary',
                'resultList.workingSeniority', 'resultList.recruitingDesc', 'total',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'resultList.recruitingPublishedTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '岗位发布日期为记录日期；接口无期间过滤，分页每页最多50条；招聘记录不等于净增员。',
        },
    },
    '66a0f4dc5107227d28e4d15f': {
        'tool_name': 'portrait_query_enterprise_operating_strength',
        'name': '经营实力',
        'topic': 'performance',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': [
                'contributeCapitalList', 'incomeList', 'profitList', 'registerCapitalList', 'taxList',
            ],
            'split_keys': [
                'incomeList.year', 'profitList.year', 'taxList.year', 'contributeCapitalList.year',
                'registerCapitalList.year',
            ],
            'evidence': ['incomeList', 'profitList', 'taxList', 'contributeCapitalList', 'registerCapitalList'],
        },
        'time': {
            'basis': 'year_series',
            'fields': [
                'incomeList.year',
                'profitList.year',
                'taxList.year',
                'contributeCapitalList.year',
                'registerCapitalList.year',
            ],
            'filter': None,
            'snapshot': True,
            'note': '按统计年份的区间序列；接口无年份过滤；营收/利润为区间，不能折算成精确值。',
        },
    },
    '66a0f56efc5601eba12cc2e3': {
        'tool_name': 'portrait_query_enterprise_financing',
        'name': '融资信息',
        'topic': 'performance',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['fpFinancingList'],
            'split_keys': ['fpFinancingList.financingTime', 'fpFinancingList.financingSeries'],
            'evidence': [
                'fpFinancingCount', 'fpFinancingList.financingTime', 'fpFinancingList.financingSeries',
                'fpFinancingList.financingAmount', 'fpFinancingList.investorList',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'fpFinancingList.financingTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '融资日期为记录日期；接口无期间过滤；融资不等于客户采购预算。',
        },
    },
    '66f509f628fcc6f71008a41a': {
        'tool_name': 'portrait_query_listed_business_segments',
        'name': '上市公司主营构成分析表详情',
        'topic': 'listed',
        'inputs': {
            'subject': 'enterprise_or_stock',
            'fields': [
                _MATCH_KEYWORD,
                {
                    'name': 'keywordType',
                    'type': 'string',
                    'required': False,
                    'default': 'name',
                    'enum': ['name', 'nameId', 'socialCreditCode', 'reportStockCode'],
                },
                _PAGE_INDEX,
                {'name': 'pageSize', 'type': 'integer', 'required': False, 'default': 10, 'max': 10},
            ],
            'note': 'keywordType 额外支持 reportStockCode；分页每页最多10条。',
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': [
                'resultList.reportDate', 'resultList.reportName',
                'resultList.reportMainBusinessAnalysis.itemName',
                'resultList.reportMainBusinessAnalysis.mainOpType',
            ],
            'evidence': [
                'resultList.reportDate', 'resultList.reportName', 'resultList.reportStockCode',
                'resultList.shortName', 'resultList.reportMainBusinessAnalysis.itemName',
                'resultList.reportMainBusinessAnalysis.mainOpType',
                'resultList.reportMainBusinessAnalysis.mainBusinessIncome',
                'resultList.reportMainBusinessAnalysis.mainBusinessCost',
                'resultList.reportMainBusinessAnalysis.mainBusinessRProfit',
                'resultList.reportMainBusinessAnalysis.mbiRatio',
                'resultList.reportMainBusinessAnalysis.mbcRatio',
                'resultList.reportMainBusinessAnalysis.mbrRatio',
                'resultList.reportMainBusinessAnalysis.grossRProfitRatio',
                'resultList.reportMainBusinessAnalysis.rank', 'total',
            ],
        },
        'time': {
            'basis': 'report_period',
            'fields': [
                'resultList.reportDate',
            ],
            'filter': None,
            'snapshot': True,
            'note': '按报告期记录，接口无报告期过滤；仅上市披露主体；同一收入的产品/地区拆分不能相加。',
        },
    },
    '669e5ee54efb02e6f96c7c9c': {
        'tool_name': 'portrait_query_enterprise_investments',
        'name': '对外投资',
        'topic': 'investment',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
            'note': 'matchKeyword 只接受名单已确权主体；结果只表示名单企业对外投资，不把被投企业反向并回名单。',
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.scCode', 'resultList.name'],
            'evidence': [
                'resultList.name', 'resultList.scCode', 'resultList.ratio', 'resultList.subscriptionAmount',
                'resultList.regCapital', 'resultList.foundTime', 'resultList.operStatus', 'resultList.isListed',
                'resultList.addressValue', 'resultList.business', 'total',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'resultList.foundTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': (
                '对外投资为当前快照；被投企业成立日期只是字段日期，不是投资发生日。ratio 按接口原样保留，仅凭数值大小不得推断为百分比；subscriptionAmount/regCapital '
                '是原文声明的 dict，不虚构子字段。'
            ),
        },
    },
    '66c33eff3c0917a9a02febdd': {
        'tool_name': 'portrait_query_enterprise_promotion',
        'name': '推广情况',
        'topic': 'promotion',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': ['lastPrmtTime', 'prmtCompetitorsNumber', 'prmtLinksNumber', 'prmtSources'],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'lastPrmtTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': (
                '推广情况为当前快照；lastPrmtTime 是最后推广日期字段值，仅作为字段日期保留，不用来过滤竞品数、链接数或渠道，也不能推出广告费、获客成本、在投状态或全期间投放次数。竞品数/链'
                '接数为接口原值，0 是明确零，缺失不补零。'
            ),
        },
    },
    '66a0f258ce3408eb23b56706': {
        'tool_name': 'portrait_query_enterprise_domains',
        'name': '域名信息',
        'topic': 'website',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.domainName', 'resultList.domainUrl'],
            'evidence': [
                'resultList.domainName', 'resultList.domainUrl', 'resultList.filingAuditTime',
                'resultList.isHomePage', 'resultList.websiteRecord', 'total',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [
                'resultList.filingAuditTime',
            ],
            'filter': None,
            'snapshot': True,
            'note': '域名备案审核日期只是字段日期，不代表网站上线、可访问或已付费；有关联域名不等于有官网，不探测站点，也不推流量、订单或收款额。',
        },
    },
    '66bf124bf134a4c21b4fc2cb': {
        'tool_name': 'portrait_query_enterprise_website',
        'name': '企业官网信息',
        'topic': 'website',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'domainAppDownload', 'domainApplyTrial', 'domainCustomerCase', 'domainEnglishVersion',
                'domainHttps', 'domainJoinUs', 'domainLogin', 'domainMerchant', 'domainSolution',
                'domainStatsModule',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '官网功能模块为当前快照布尔标记；false 是接口明确标记无该模块，字段缺失不等于 false。不探测站点；官网模块不能推出流量、订单或收款额。',
        },
    },
    '66bb1aa834d3cd3e43928163': {
        'tool_name': 'portrait_query_enterprise_litigation',
        'name': '法律诉讼风险概况',
        'topic': 'risk',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': [
                'caKaitingList', 'caLianList', 'caList', 'edList', 'enforcementList', 'jdList', 'limitedList',
            ],
            'split_keys': [
                'caKaitingList._id', 'caLianList._id', 'caList._id', 'edList._id', 'enforcementList._id',
                'jdList._id', 'limitedList._id',
            ],
            'evidence': [
                'caKaitingList._id', 'caKaitingList.caPublishDate', 'caKaitingList.caRelatedCaseNumber',
                'caKaitingList.caType', 'caLianList._id', 'caLianList.caPublishDate',
                'caLianList.caRelatedCaseNumber', 'caLianList.caType', 'caList._id', 'caList.caPublishDate',
                'caList.caRelatedCaseNumber', 'caList.caType', 'edList._id', 'edList.efDishonestCaseCreateTime',
                'edList.efDishonestCaseNumber', 'enforcementList._id', 'enforcementList.efCaseCreateTime',
                'enforcementList.efCaseNumberDetail', 'jdList._id', 'jdList.jdCaseNumber', 'jdList.jdPublishDate',
                'limitedList._id', 'limitedList.efCaseCreateTime', 'limitedList.efCaseNumber', 'caKaitingTotal',
                'caLianTotal', 'caTotal', 'edTotal', 'enforcementTotal', 'jdTotal', 'limitedTotal',
            ],
        },
        'time': {
            'basis': 'event_date',
            'fields': [
                'caKaitingList.caPublishDate',
                'caLianList.caPublishDate',
                'caList.caPublishDate',
                'edList.efDishonestCaseCreateTime',
                'enforcementList.efCaseCreateTime',
                'jdList.jdPublishDate',
                'limitedList.efCaseCreateTime',
            ],
            'filter': 'event_window',
            'snapshot': False,
            'note': (
                '七类公告/被执行/失信/限高分别计数，累计总数不可相加。各类最多返回50条明细，按各自公告/立案/发布日期进入观察窗口；同一案号可跨类出现，各类独立保留，不合并成综合风险分。缺失不当零'
                '，发生过不等于当前有效。'
            ),
        },
    },
    '66a248f381d41651f2689d95': {
        'tool_name': 'portrait_query_enterprise_operating_anomalies',
        'name': '经营异常',
        'topic': 'risk',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['anomalyList'],
            'split_keys': [],
            'evidence': [
                'anomalyList.createAuthority', 'anomalyList.createDate', 'anomalyList.createReason',
                'anomalyList.removeAuthority', 'anomalyList.removeDate', 'anomalyList.removeReason',
                'anomalyCount',
            ],
        },
        'time': {
            'basis': 'event_interval',
            'fields': [
                'anomalyList.createDate',
            ],
            'end_fields': ['anomalyList.removeDate'],
            'filter': 'event_interval',
            'snapshot': False,
            'note': (
                '列入与移出都是事件日期；按列入日至移出日（未移出则为开口区间）与观察窗口相交纳入，因此先列入、后于窗口内移出的记录仍保留。未见移出日期只表示接口未返回，不得据此写成已确认未移出、仍异常'
                '或已结案；anomalyCount 是接口累计数，不等于窗口内事件数。'
            ),
        },
    },
    '66d5b7df537c3f61d646c2dc': {
        'tool_name': 'portrait_query_trademark_statistics',
        'name': '商标统计',
        'topic': 'trademark',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': ['tmAppTimeStat', 'tmRegTimeStat', 'tmStatusStat', 'tmTypeStats'],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '商标申请、注册、状态和类别统计；资料快照，不当作当前有效商标、品牌价值或市场地位。',
        },
    },
    '671357d127ab3417e1f3f21b': {
        'tool_name': 'portrait_query_trademark_overview',
        'name': '商标概况',
        'topic': 'trademark',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE],
        },
        'response': {
            'record_paths': ['$'],
            'split_keys': [],
            'evidence': [
                'tmCount', 'tmInvalidNumber', 'tmNumberThisYear', 'tmStatusList', 'tmStatusStat', 'tmTypeList',
                'tmValidNumber',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '商标总数、有效/无效数量与类别清单；资料快照，不当作使用状态、品牌价值或商品销售。',
        },
    },
    '6713537822fc5904b878fbcc': {
        'tool_name': 'portrait_query_trademark_details',
        'name': '商标明细',
        'topic': 'trademark',
        'inputs': {
            'subject': 'enterprise',
            'fields': [_MATCH_KEYWORD, _KEYWORD_TYPE, _PAGE_INDEX, _PAGE_SIZE_50],
        },
        'response': {
            'record_paths': ['resultList'],
            'split_keys': ['resultList.tmId'],
            'evidence': [
                'resultList.tmId', 'resultList.tmName', 'resultList.tmInternationalClass',
                'resultList.tmServiceContents.detail', 'resultList.tmStatus', 'resultList.tmApplicationTime',
                'resultList.tmRegNum', 'resultList.tmType', 'total',
            ],
        },
        'time': {
            'basis': 'snapshot',
            'fields': [],
            'filter': None,
            'snapshot': True,
            'note': '商标申请与专用权期限明细；只按申请日期核对，不代表当前使用、品牌影响或商品销售。',
        },
    },
}


# 从产品定义生成工具和响应字段索引，不访问网关。
_INPUT_FIELDS = {
    product_id: {field['name']: field for field in route['inputs']['fields']}
    for product_id, route in ROUTES.items()
}

_FIELDS_BY_PRODUCT = _projection_paths()
_TREES = {pid: _field_tree(paths) for pid, paths in _FIELDS_BY_PRODUCT.items()}
_PAGED_PRODUCTS = {
    pid for pid, route in ROUTES.items()
    if any(field['name'] == 'pageIndex' for field in route['inputs']['fields'])
}

PRODUCT_IDS = {route['tool_name']: product_id for product_id, route in ROUTES.items()}
if len(PRODUCT_IDS) != len(ROUTES):
    raise RuntimeError('企业客户画像洞察工具名称重复')
TOOLS = [_tool(product_id, route) for product_id, route in ROUTES.items()]


if __name__ == '__main__':
    try:
        main()
    except Fault as err:
        sys.exit(err.message)
