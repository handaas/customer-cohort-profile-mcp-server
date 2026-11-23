"""存量客户群体画像 MCP：七个工具，共用一份可发现、可验证的契约。"""
from __future__ import annotations

import argparse
import asyncio
import hmac
import logging
import os
import sys
from pathlib import Path

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import anyio
import uvicorn
from dotenv import load_dotenv
from jsonschema import Draft202012Validator, FormatChecker
from mcp import types
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse

from server.common import Fault, Settings, pack, uid
from server.execution import Engine
from server.gateway import Gateway
from server.mcp_contract import build_tools
from server.planning import Planner
from server.store import Store

LOG = logging.getLogger('portrait')
TOOLS = {s['name']: types.Tool.model_validate(s) for s in build_tools()}
FORMATS = FormatChecker()
INPUTS = {k: Draft202012Validator(t.inputSchema, format_checker=FORMATS) for k, t in TOOLS.items()}
OUTPUTS = {k: Draft202012Validator(t.outputSchema, format_checker=FORMATS) for k, t in TOOLS.items()}


def error(code, message):
    text = pack({'code': code, 'message': message})
    return types.CallToolResult(isError=True, content=[types.TextContent(type='text', text=text)])


class ContractMCP(FastMCP):
    async def list_tools(self):
        return list(TOOLS.values())

    async def call_tool(self, name, arguments):
        # FastMCP normally coerces parameters before validation. Validate the original
        # JSON first, including forbidden extra properties and per-topic product enums.
        if name not in INPUTS:
            return error('unknown_tool', '工具不存在，请重新获取工具清单。')
        if not INPUTS[name].is_valid(arguments):
            return error('invalid_arguments', '参数不符合此工具的输入契约。')
        return await super().call_tool(name, {'payload': arguments})


class BearerGuard:
    def __init__(self, app, token: bytes):
        self.app = app
        self.header = b'Bearer ' + token

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http':
            auth = [v for k, v in scope['headers'] if k == b'authorization']
            if len(auth) != 1 or not hmac.compare_digest(auth[0], self.header):
                response = JSONResponse({'error': 'unauthorized'}, status_code=401,
                                        headers={'WWW-Authenticate': 'Bearer'})
                return await response(scope, receive, send)
        return await self.app(scope, receive, send)


class Application:
    def __init__(self, settings: Settings, gateway=None):
        self.settings = settings
        store = Store(settings)
        self.gateway = gateway if gateway is not None else Gateway()
        self.engine = Engine(store, self.gateway)
        planner = Planner(store)
        self.operations = {
            'portrait_stage_roster': planner.stage,
            'portrait_plan_base': planner.base,
            'portrait_plan_resolution': planner.resolve,
            'portrait_plan_topics': planner.topics,
            'portrait_execute_plan': self.engine.execute,
            'portrait_get_run': self.engine.get,
            'portrait_read_page': store.get_page,
        }
        if self.operations.keys() != TOOLS.keys():
            raise RuntimeError('服务与工具契约不一致。')
        allowed_hosts = ['127.0.0.1', '127.0.0.1:*', 'localhost', 'localhost:*', '[::1]', '[::1]:*']
        allowed_hosts += _allowlist('PORTRAIT_ALLOWED_HOSTS')
        security = TransportSecuritySettings(enable_dns_rebinding_protection=True, allowed_hosts=allowed_hosts,
                                             allowed_origins=_allowlist('PORTRAIT_ALLOWED_ORIGINS'))
        self.mcp = ContractMCP('customer-cohort-profile', stateless_http=True, json_response=True,
                               transport_security=security, instructions='仅执行已确定分析范围的计划，只传 plan_id 和 plan_hash；读取快照不会重新取数。')
        for name, operation in self.operations.items():
            self._register(name, operation)

    def _register(self, name, operation):
        @self.mcp.tool(name=name, structured_output=False)
        async def handle(payload: dict) -> types.CallToolResult:
            try:
                result = await anyio.to_thread.run_sync(operation, self.settings.tenant, payload)
                invalid = next(OUTPUTS[name].iter_errors(result), None)
                if invalid is not None:
                    trace = uid('trace')
                    LOG.error('输出契约不符 tool=%s rule=%s trace=%s', name, invalid.validator, trace)
                    return error('internal_error', '结果未通过输出契约，追踪号：' + trace)
            except Fault as exc:
                return error(exc.code, exc.message)
            except Exception as exc:
                # ValidationError, SQL/HTTP exceptions may include full arguments.
                # Never include exception text, raw records, or a traceback in responses or logs.
                trace = uid('trace')
                LOG.error('工具执行失败 tool=%s type=%s trace=%s', name, type(exc).__name__, trace)
                return error('internal_error', '服务执行失败，追踪号：' + trace)
            return types.CallToolResult(structuredContent=result, content=[types.TextContent(type='text', text=pack(result))])

    def http_app(self):
        token = os.getenv('CUSTOMER_COHORT_PROFILE_MCP_TOKEN', '')
        if not token.isascii() or not token.isprintable() or len(token.strip()) < 32:
            raise Fault('configuration_error', 'HTTP 模式需要配置至少 32 字符的专用访问令牌。')
        app = self.mcp.streamable_http_app()
        app.add_middleware(BearerGuard, token=token.encode('ascii'))
        return app

    def close(self):
        self.engine.close()
        close = getattr(self.gateway, 'close', None)
        if close:
            close()


def _allowlist(name):
    values = [v.strip() for v in os.getenv(name, '').split(',') if v.strip()]
    if any('*' in v for v in values):
        raise Fault('configuration_error', f'{name} 只能配置明确的可信地址。')
    return values


def main():
    parser = argparse.ArgumentParser(description='存量客户群体画像 MCP 服务')
    parser.add_argument('transport', nargs='?', default='stdio', choices=['stdio', 'streamable-http'])
    args = parser.parse_args()
    load_dotenv(Path(__file__).resolve().parents[1] / '.env', override=False)
    # Avoid low-level SDK / client wire logs; they can contain identifiers or headers.
    for logger in ('httpx', 'httpcore', 'mcp'):
        logging.getLogger(logger).setLevel(logging.WARNING)
    app = Application(Settings.from_env())
    try:
        if args.transport == 'stdio':
            app.engine.recover()
            asyncio.run(app.mcp.run_stdio_async())
        else:
            http = app.http_app()
            port = int(os.getenv('PORTRAIT_HTTP_PORT', '8000'))
            if not 1 <= port <= 65535:
                raise Fault('configuration_error', 'HTTP 端口无效。')
            app.engine.recover()
            uvicorn.run(http, host=os.getenv('PORTRAIT_HTTP_HOST', '127.0.0.1'), port=port, access_log=False)
    finally:
        app.close()


if __name__ == '__main__':
    try:
        main()
    except Fault as err:
        sys.exit(err.message)
