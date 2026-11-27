"""固定产品目录与脱敏错误。"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


def _load_routes(path: Path) -> dict[str, dict[str, Any]]:
    document = json.loads(path.read_text(encoding='utf-8'))
    entries = document.get('products') if isinstance(document, dict) else None
    if not isinstance(entries, list) or not entries:
        raise RuntimeError('product-routes.json must contain a non-empty products array')

    routes: dict[str, dict[str, Any]] = {}
    names: set[str] = set()
    for index, route in enumerate(entries):
        if not isinstance(route, dict):
            raise RuntimeError(f'products[{index}] must be an object')
        product_id = route.get('product_id')
        if not isinstance(product_id, str) or not product_id:
            raise RuntimeError(f'products[{index}].product_id must be a non-empty string')
        if product_id in routes:
            raise RuntimeError(f'duplicate product_id in product-routes.json: {product_id}')
        name = route.get('tool_name')
        if not isinstance(name, str) or not re.fullmatch(r'portrait_query_[a-z][a-z0-9_]{0,48}', name):
            raise RuntimeError(f'products[{index}].tool_name is not a valid query name')
        if name in names:
            raise RuntimeError(f'duplicate tool_name in product-routes.json: {name}')
        names.add(name)
        routes[product_id] = route
    return routes


ROUTES = _load_routes(Path(__file__).resolve().with_name('product-routes.json'))


class Fault(Exception):
    """固定文案，不包含用户参数或上游数据。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def pack(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
