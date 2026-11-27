"""旷湖 Integrator Gateway 适配器：离线可验、固定端点、单次签名请求。

环境变量（只在 ``fetch`` 时读取，均不写入日志或异常）：

- ``INTEGRATOR_ID``：网关路径中的对接器 ID；
- ``SECRET_ID``：请求表单中的密钥 ID；
- ``SECRET_KEY``：仅参与本地 MD5 签名，不发送给网关。

本模块不读取任何 ``.env`` 文件，只使用服务进程已经加载的环境变量；本服务的入口
只加载自己的 ``.env``，不得借用其他 MCP 服务的凭据。凭据在每次 ``fetch`` 时读取；
检查通过前不会创建 HTTP 请求。缺任一变量即抛 ``Fault('configuration_error')``，
不会访问网络。默认 ``Gateway()`` 只构造 httpx 客户端，不产生任何网络动作。

固定通道：``POST https://console.handaas.com/api/v1/integrator/call_api/{INTEGRATOR_ID}``，
30 秒超时，不跟随重定向，不重试，不接受 MCP 客户端传入 URL、请求头或凭据。
表单只含 ``product_id``、``secret_id``、``params``、``signature``；签名沿用现有服务
的规范：对这三个键按字典序拼接字符串，再拼 ``SECRET_KEY``，取 UTF-8 MD5 小写十六进制。

成功解析（仅接受官方信封）：

- ``code`` 为 ``"1"`` 或 ``"10000"``：返回原始 ``data`` 字典；``data`` 为空或缺省返回 ``{}``；
- ``code`` 为 ``"10001"`` 或 ``msgCN``/``message`` 恰为 ``查询数据为空``：合法空结果 ``{}``；
- 其他业务码、无权限：分别抛 ``upstream_rejected`` / ``upstream_denied``；
- 无法识别的状态（且非明确空结果）、响应不可解析、结构异常、超时或发送后中断：
  抛 ``billing_unknown``，表示是否计费待核，调用方不得自动重发。

异常约定（均为脱敏固定文案，绝不携带网关响应、状态码、请求 ID、签名或凭据）：

- ``invalid_argument``：产品不在 46 项白名单、字段/类型/枚举/分页/月年越界，未发请求；
- ``configuration_error``：凭据缺失/占位/含空白或控制字符、客户端已关闭或
  ``INTEGRATOR_ID`` 路径非法，未发请求；
- ``upstream_unavailable``：确定未送达、限流或网关重定向，未取得数据，可退避重试；
- ``upstream_denied``：网关明确拒绝访问（权限/订阅），最终失败，不自动重试；
- ``upstream_rejected``：网关明确的业务拒绝，最终失败，不自动重试；
- ``billing_unknown``：请求已发出但无法确定结果或计费状态，禁止隐式重发；
- ``internal_error``：本地请求规范化失败，未发请求。

离线验证：
``Gateway(transport=httpx.MockTransport(handler))``；端点与签名通道没有公开参数可改写。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import httpx

from server.common import Fault, ROUTES

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

_STRING_FIELDS = frozenset(
    {"matchKeyword", "keywordType", "address", "statsMonth", "arYear", "platform", "shopId"}
)
_INTEGER_FIELDS = frozenset({"pageIndex", "pageSize"})


# 非超时的连接失败可确定“尚未把请求送达网关”；任何超时及发送后异常都按计费未知处理。
_NOT_SENT_ERRORS = (
    httpx.ConnectError,
    httpx.ProxyError,
    httpx.UnsupportedProtocol,
)


def _build_specs() -> dict[str, dict[str, dict[str, Any]]]:
    """从共享 ROUTES 生成全量白名单，补齐分页/月份/年份约束。"""
    specs: dict[str, dict[str, dict[str, Any]]] = {}
    for product_id, route in ROUTES.items():
        raw_fields = route.get("inputs", {}).get("fields", [])
        if not isinstance(raw_fields, list) or not raw_fields:
            raise RuntimeError(f"产品 {product_id} 缺少输入字段契约")
        fields: dict[str, dict[str, Any]] = {}
        for raw in raw_fields:
            name = raw.get("name")
            field_type = raw.get("type")
            if not isinstance(name, str) or name in fields:
                raise RuntimeError(f"产品 {product_id} 的输入字段名无效或重复")
            if field_type == "string":
                if name not in _STRING_FIELDS:
                    raise RuntimeError(f"产品 {product_id} 出现未核验的字符串字段 {name}")
                rule: dict[str, Any] = {"type": "string", "required": bool(raw.get("required"))}
                enum = raw.get("enum")
                if enum is not None:
                    if (
                        not isinstance(enum, list)
                        or not enum
                        or any(not isinstance(item, str) or not item for item in enum)
                    ):
                        raise RuntimeError(f"产品 {product_id} 的 {name} 枚举无效")
                    rule["enum"] = frozenset(enum)
                if name == "keywordType" and "enum" not in rule:
                    raise RuntimeError(f"产品 {product_id} 的 keywordType 缺少枚举")
            elif field_type == "integer":
                if name not in _INTEGER_FIELDS:
                    raise RuntimeError(f"产品 {product_id} 出现未核验的整数字段 {name}")
                rule = {"type": "integer", "required": bool(raw.get("required")), "min": 1}
                if name == "pageSize":
                    maximum = raw.get("max")
                    if type(maximum) is not int or maximum < 1:
                        raise RuntimeError(f"产品 {product_id} 缺少有效的 pageSize 上限")
                    rule["max"] = maximum
                elif raw.get("max") is not None:
                    raise RuntimeError(f"产品 {product_id} 的 {name} 出现未核验的上限")
            else:
                raise RuntimeError(f"产品 {product_id} 的字段 {name} 类型未核验：{field_type!r}")
            fields[name] = rule
        specs[product_id] = fields

    if len(specs) != 46 or set(specs) != set(ROUTES):
        raise RuntimeError("上游产品白名单必须完整覆盖 46 个共享路由")
    return specs


_SPECS = _build_specs()


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


def _validate_call(product_id: str, params: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(product_id, str) or product_id not in _SPECS:
        raise Fault("invalid_argument", "产品不在获准的上游白名单内。")
    if params is None:
        params = {}
    if not isinstance(params, Mapping):
        raise Fault("invalid_argument", "上游参数必须是 JSON 对象。")
    spec = _SPECS[product_id]
    if any(not isinstance(key, str) or key not in spec for key in params):
        allowed = "、".join(sorted(spec))
        raise Fault("invalid_argument", f"上游参数包含未获准字段；允许字段：{allowed}。")

    clean: dict[str, Any] = {}
    for name, value in params.items():
        if value is None:
            continue
        rule = spec[name]
        if rule["type"] == "string":
            if not isinstance(value, str):
                raise Fault("invalid_argument", f"参数 {name} 必须是字符串。")
            if not value or not value.strip():
                raise Fault("invalid_argument", f"参数 {name} 不能为空。")
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                raise Fault("invalid_argument", f"参数 {name} 含无效字符。") from None
            if name == "statsMonth":
                _validate_month(value)
            elif name == "arYear":
                _validate_year(value)
            enum = rule.get("enum")
            if enum is not None and value not in enum:
                raise Fault("invalid_argument", f"参数 {name} 不在允许的枚举内。")
        else:
            if type(value) is not int:
                raise Fault("invalid_argument", f"参数 {name} 必须是整数。")
            if value < rule["min"]:
                raise Fault("invalid_argument", f"参数 {name} 必须是不小于 1 的整数。")
            maximum = rule.get("max")
            if maximum is not None and value > maximum:
                raise Fault("invalid_argument", f"参数 {name} 必须在 1 到 {maximum} 之间。")
        clean[name] = value
    for name, rule in spec.items():
        if rule["required"] and name not in clean:
            raise Fault("invalid_argument", f"缺少必填参数 {name}。")
    return clean


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


class Gateway:
    """固定官方网关的单次签名调用；可注入传输层做离线同构验证。"""

    def __init__(self, *, transport: httpx.BaseTransport | None = None) -> None:
        self._client = httpx.Client(
            transport=transport,
            timeout=REQUEST_TIMEOUT_SECONDS,
            follow_redirects=False,
            trust_env=False,
        )

    def validate_call(
        self, product_id: str, params: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """校验并返回只含白名单字段的参数副本；不读取凭据、不访问网络。"""
        return _validate_call(product_id, params)

    def fetch(
        self, product_id: str, params: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """执行一次真实签名请求并返回原始 ``data`` 字典；不重试、不改写数值。"""
        clean = self.validate_call(product_id, params)
        if self._client.is_closed:
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
            response = self._client.post(url, data=form)
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

    def close(self) -> None:
        self._client.close()
