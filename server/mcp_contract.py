"""从固定产品目录发布 MCP 工具契约，不接受调用方选择上游产品。"""
from __future__ import annotations

from typing import Any

from mcp import types

from server.common import ROUTES

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


TOOLS_BY_PRODUCT = {product_id: _tool(product_id, route) for product_id, route in ROUTES.items()}
