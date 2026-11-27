"""服务配置、稳定标识和可安全返回的业务错误。"""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent


def _load_routes(path: Path) -> dict[str, dict[str, Any]]:
    document = json.loads(path.read_text(encoding='utf-8'))
    entries = document.get('products') if isinstance(document, dict) else None
    if not isinstance(entries, list):
        raise RuntimeError('product-routes.json must contain a products array')

    routes = {}
    for index, route in enumerate(entries):
        if not isinstance(route, dict):
            raise RuntimeError(f'products[{index}] must be an object')
        product_id = route.get('product_id')
        if not isinstance(product_id, str) or not product_id:
            raise RuntimeError(f'products[{index}].product_id must be a non-empty string')
        if product_id in routes:
            raise RuntimeError(f'duplicate product_id in product-routes.json: {product_id}')
        routes[product_id] = route
    return routes


ROUTES = _load_routes(HERE / 'product-routes.json')

# 协议常量只在这里定义；各模块从 common 引用，避免计数/枚举各写一份。
BASE_ROUTING_KEYS = ('identity', 'base', 'supplement')
PAGE_VIEWS = ('identities', 'candidates', 'subjects', 'evidence', 'coverage')
VIEWS = frozenset(PAGE_VIEWS)
RUN_STATES = ('queued', 'running', 'partial', 'succeeded', 'failed', 'cancelled')
RUN_ERROR_CODES = (
    'invalid_argument', 'plan_hash_mismatch', 'plan_expired', 'plan_not_executable',
    'limit_exceeded', 'out_of_scope', 'roster_incomplete', 'no_applicable_subjects',
    'snapshot_not_found', 'snapshot_expired', 'cursor_invalid', 'run_not_found',
    'conflict', 'already_running', 'upstream_unavailable', 'partial_failure',
    'billing_unknown', 'cancelled', 'internal_error',
)


class Fault(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def pack(value) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def digest(value) -> str:
    return 'sha256:' + hashlib.sha256(pack(value).encode()).hexdigest()


def uid(prefix: str) -> str:
    return prefix + '_' + secrets.token_hex(16)


def stable_id(prefix: str, *parts) -> str:
    return prefix + '_' + hashlib.sha256(pack(parts).encode()).hexdigest()[:32]


def later(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec='seconds').replace('+00:00', 'Z')


def now() -> str:
    return later(0)


def _seconds(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    if not raw.isascii() or not raw.isdigit() or len(raw) > 9 or not 1 <= int(raw) <= 31_536_000:
        raise Fault('configuration_error', f'{name} 必须是 1 到 31536000 的整数秒数。')
    return int(raw)


@dataclass(frozen=True)
class Settings:
    db_path: Path
    tenant: str = 'local'
    live: bool = False
    plan_ttl: int = 900
    snapshot_ttl: int = 604800

    @classmethod
    def from_env(cls) -> Settings:
        tenant = os.getenv('PORTRAIT_TENANT', 'local')
        live = os.getenv('PORTRAIT_LIVE', '0')
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', tenant) or live not in ('0', '1'):
            raise Fault('configuration_error', '请检查 PORTRAIT_TENANT 和 PORTRAIT_LIVE（0/1）。')
        default_db = Path.home() / '.local/share/handaas/customer-cohort-profile/portrait.sqlite3'
        return cls(Path(os.getenv('PORTRAIT_DB_PATH') or default_db).expanduser(), tenant, live == '1',
                   _seconds('PORTRAIT_PLAN_TTL_SECONDS', 900), _seconds('PORTRAIT_SNAPSHOT_TTL_SECONDS', 604800))

