"""按产品声明裁剪业务响应，保留记录结构与来源格式，不泄露未声明字段。"""
from __future__ import annotations

from typing import Any

from server.common import Fault, ROUTES

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


_FIELDS_BY_PRODUCT = _projection_paths()
_TREES = {pid: _field_tree(paths) for pid, paths in _FIELDS_BY_PRODUCT.items()}
_PAGED_PRODUCTS = {
    pid for pid, route in ROUTES.items()
    if any(field['name'] == 'pageIndex' for field in route['inputs']['fields'])
}


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
