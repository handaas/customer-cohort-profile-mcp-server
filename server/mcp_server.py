"""企业客户画像洞察：固定产品、单次上游查询，不保存任务状态。"""
from __future__ import annotations

import argparse
import asyncio
import hmac
import logging
import os
import sys
from pathlib import Path
from typing import Any

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import anyio
import uvicorn
from dotenv import load_dotenv
from jsonschema import Draft202012Validator
from mcp import types
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

from server.common import Fault, pack
from server.gateway import Gateway
from server.mcp_contract import QUERY_CONTRACT, TOOLS_BY_PRODUCT
from server.projection import project_data

LOG = logging.getLogger('portrait')
TOOLS = {tool.name: tool for tool in TOOLS_BY_PRODUCT.values()}
PRODUCT_IDS = {tool.name: product_id for product_id, tool in TOOLS_BY_PRODUCT.items()}
INPUTS = {name: Draft202012Validator(tool.inputSchema) for name, tool in TOOLS.items()}


def error(code: str, message: str) -> types.CallToolResult:
    return types.CallToolResult(
        isError=True,
        content=[types.TextContent(type='text', text=pack({'code': code, 'message': message}))],
    )


class QueryMCP(FastMCP):
    def __init__(self, gateway: Gateway, live: bool, security: TransportSecuritySettings):
        super().__init__(
            'customer-cohort-profile', stateless_http=True, json_response=True,
            transport_security=security,
            instructions='每个工具只查询一个固定产品或一页，可能计费。名单、计划、进度和结果由调用方管理。',
        )
        self.gateway = gateway
        self.live = live

    async def list_tools(self) -> list[types.Tool]:
        return list(TOOLS.values())

    # FastMCP 的底层 call_tool handler 直接支持 CallToolResult，包括 isError。
    async def call_tool(  # type: ignore[override]
        self, name: str, arguments: dict[str, Any],
    ) -> types.CallToolResult:
        if name not in INPUTS:
            return error('unknown_tool', '工具不存在，请重新获取工具清单。')
        # 在 SDK 做参数转换前检查原始 JSON；整数字段不接受 1.0 或布尔值。
        if not INPUTS[name].is_valid(arguments) or any(type(value) is float for value in arguments.values()):
            return error('invalid_argument', '参数不符合此工具的输入契约，请核对必填项、类型和范围。')
        if not self.live:
            return error('live_disabled', '服务未启用真实取数（PORTRAIT_LIVE=0）；未发起请求。')
        product_id = PRODUCT_IDS[name]
        try:
            raw = await anyio.to_thread.run_sync(self.gateway.fetch, product_id, arguments)
            result = {'data': project_data(product_id, raw)}
            text = pack(result)
        except Fault as exc:
            return error(exc.code, exc.message)
        except Exception as exc:
            # 可能已经执行了上游查询。不得把异常当空结果或回显请求和响应。
            LOG.error('查询结果未知 tool=%s type=%s', name, type(exc).__name__)
            return error('billing_unknown', '未取得可确认的结果；是否计费待核，不得自动重发。')
        return types.CallToolResult(
            structuredContent=result, content=[types.TextContent(type='text', text=text)],
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


class Application:
    def __init__(self, *, gateway: Gateway | None = None, live: bool = False):
        allowed_hosts = ['127.0.0.1', '127.0.0.1:*', 'localhost', 'localhost:*', '[::1]', '[::1]:*']
        security = TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=allowed_hosts + _allowlist('PORTRAIT_ALLOWED_HOSTS'),
            allowed_origins=_allowlist('PORTRAIT_ALLOWED_ORIGINS'),
        )
        self.gateway = gateway if gateway is not None else Gateway()
        self.mcp = QueryMCP(self.gateway, live, security)

    def http_app(self):
        token = os.getenv('CUSTOMER_COHORT_PROFILE_MCP_TOKEN', '')
        if not token.isascii() or not token.isprintable() or len(token.strip()) < 32:
            raise Fault('configuration_error', 'HTTP 模式需要配置至少 32 字符的专用访问令牌。')
        app = self.mcp.streamable_http_app()
        app.add_middleware(BearerGuard, token=token.encode('ascii'))
        return app

    def close(self) -> None:
        self.gateway.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='企业客户画像洞察 MCP 服务', allow_abbrev=False)
    parser.add_argument('transport', nargs='?', default='stdio', choices=['stdio', 'streamable-http'])
    # 配套 Skill 连错旧服务时，旧入口会在加载配置或恢复任务前拒绝此参数。
    parser.add_argument('--query-contract', choices=(QUERY_CONTRACT,), help='要求指定的查询契约版本')
    args = parser.parse_args()
    load_dotenv(Path(__file__).resolve().parents[1] / '.env', override=False)
    for logger in ('httpx', 'httpcore', 'mcp'):
        logging.getLogger(logger).setLevel(logging.WARNING)
    live = os.getenv('PORTRAIT_LIVE', '0')
    if live not in ('0', '1'):
        raise Fault('configuration_error', 'PORTRAIT_LIVE 必须为 0 或 1。')
    app = Application(live=live == '1')
    try:
        if args.transport == 'stdio':
            asyncio.run(app.mcp.run_stdio_async())
        else:
            port = os.getenv('PORTRAIT_HTTP_PORT', '8000')
            if not port.isascii() or not port.isdigit() or len(port) > 5 or not 1 <= int(port) <= 65535:
                raise Fault('configuration_error', 'HTTP 端口无效。')
            uvicorn.run(app.http_app(), host=os.getenv('PORTRAIT_HTTP_HOST', '127.0.0.1'),
                        port=int(port), access_log=False)
    finally:
        app.close()


if __name__ == '__main__':
    try:
        main()
    except Fault as err:
        sys.exit(err.message)
