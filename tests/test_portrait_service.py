"""Offline tests: real MCP transports, fixed signed gateway requests, synthetic data only."""

import asyncio
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs

import httpx
import uvicorn
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client

ROOT = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT / 'server' / 'mcp_server.py'
SPEC = importlib.util.spec_from_file_location('portrait_mcp_server', SERVER_PATH)
assert SPEC and SPEC.loader
mcp_server = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mcp_server)
Fault, PortraitMCP, ROUTES = mcp_server.Fault, mcp_server.PortraitMCP, mcp_server.ROUTES

NAME = '合成客户科技有限公司'
CONNECTOR_ENV = {
    'INTEGRATOR_ID': 'offline-integrator',
    'SECRET_ID': 'offline-secret-id',
    'SECRET_KEY': 'offline-sign-key',
}
TEST_ENV = {
    **CONNECTOR_ENV,
    'CUSTOMER_COHORT_PROFILE_MCP_TOKEN': 'offline-only-token-not-a-real-access-key',
    'PORTRAIT_ALLOWED_HOSTS': '',
    'PORTRAIT_ALLOWED_ORIGINS': '',
}
LEGACY_OPERATIONS = {
    'portrait_stage_roster', 'portrait_plan_base', 'portrait_plan_resolution', 'portrait_plan_topics',
    'portrait_execute_plan', 'portrait_get_run', 'portrait_read_page',
}


class ScriptedGateway:
    def __init__(self):
        self.calls = []
        self.reply = {'code': '10001', 'msgCN': '查询数据为空'}

    def __call__(self, request):
        wire = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
        expected = hashlib.md5(
            (wire['params'] + wire['product_id'] + wire['secret_id'] + TEST_ENV['SECRET_KEY']).encode()
        ).hexdigest()
        assert request.method == 'POST'
        assert request.url == 'https://console.handaas.com/api/v1/integrator/call_api/offline-integrator'
        assert set(wire) == {'product_id', 'secret_id', 'params', 'signature'}
        assert wire['signature'] == expected and wire['secret_id'] == TEST_ENV['SECRET_ID']
        pid = wire['product_id']
        self.calls.append((pid, json.loads(wire['params'])))
        reply = self.reply
        if reply == 'timeout':
            raise httpx.ReadTimeout('must not leak simulated upstream details', request=request)
        if reply == 'disconnect':
            raise httpx.ReadError('must not leak simulated upstream details', request=request)
        if reply == 'connection':
            raise httpx.ConnectError('must not leak simulated upstream details', request=request)
        if isinstance(reply, httpx.Response):
            return reply
        payload = dict(reply)
        status = payload.pop('_test_status', 200)
        return httpx.Response(status, json=payload)


class QueryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.provider = ScriptedGateway()
        env = patch.dict(os.environ, TEST_ENV)
        env.start()
        self.addCleanup(env.stop)
        self.app = PortraitMCP(transport=httpx.MockTransport(self.provider))
        self.addCleanup(self.app.close)

    async def call(self, slug, args, *, error=None):
        result = await self.app.call_tool('portrait_query_' + slug, args)
        body = json.loads(result.content[0].text)
        if error:
            self.assertTrue(result.isError)
            self.assertIsNone(result.structuredContent)
            self.assertEqual(body['code'], error)
            self.assertEqual(set(body), {'code', 'message'})
            self.assertNotIn(NAME, result.content[0].text)
            self.assertNotIn('must not leak', result.content[0].text)
            return body
        self.assertFalse(result.isError, body)
        self.assertEqual(result.structuredContent, body)
        self.assertEqual(set(body), {'data'})
        return body['data']

    async def test_one_signed_post_for_all_46_fixed_product_tools(self):
        tools = await self.app.list_tools()
        self.assertEqual(len(tools), 46)
        self.assertEqual(len({tool.name for tool in tools}), 46)
        self.assertEqual(len({tool.meta['portrait_product_id'] for tool in tools}), 46)
        self.assertFalse(LEGACY_OPERATIONS & {tool.name for tool in tools})
        self.assertEqual(len({route['topic'] for route in ROUTES.values()} - {'identity', 'base', 'supplement'}), 18)
        self.assertEqual(self.provider.calls, [])
        sample = {
            'matchKeyword': NAME, 'platform': '天猫商城', 'shopId': 'offline-shop',
            'statsMonth': '202505',
        }
        for tool in tools:
            with self.subTest(tool=tool.name):
                pid = tool.meta['portrait_product_id']
                self.assertEqual(tool.meta['portrait_query_contract'], 'v1')
                self.assertEqual(ROUTES[pid]['tool_name'], tool.name)
                self.assertFalse(tool.annotations.readOnlyHint)
                self.assertFalse(tool.annotations.idempotentHint)
                self.assertFalse(tool.inputSchema['additionalProperties'])
                self.assertNotIn('product_id', tool.inputSchema['properties'])
                cmd = {field: sample[field] for field in tool.inputSchema['required']}
                chain = ROUTES[pid].get('chain')
                if chain and 'matchKeyword' in cmd:
                    cmd['matchKeyword'] = 'offline-parent-key'
                self.assertEqual(await self.call(tool.name.removeprefix('portrait_query_'), cmd), {})
                self.assertEqual(self.provider.calls[-1], (pid, cmd), 'omit defaults rather than inventing parameters')
        self.assertEqual(Counter(pid for pid, _ in self.provider.calls), Counter({pid: 1 for pid in ROUTES}))

    async def test_required_page_enum_type_and_temporal_contracts_fail_before_dispatch(self):
        for slug, args in (
            ('enterprise_search', {}),
            ('enterprise_search', {'matchKeyword': NAME, 'keywordType': 'name'}),
            ('enterprise_search', {'matchKeyword': ' \t'}),
            ('enterprise_search', {'matchKeyword': NAME, 'pageSize': None}),
            ('enterprise_search', {'matchKeyword': NAME, 'pageSize': True}),
            ('enterprise_search', {'matchKeyword': NAME, 'pageSize': 1.0}),
            ('enterprise_search', {'matchKeyword': NAME, 'pageSize': '1'}),
            ('enterprise_search', {'matchKeyword': NAME, 'pageSize': 51}),
            ('enterprise_search', {'matchKeyword': NAME, 'pageIndex': 0}),
            ('office_details', {'matchKeyword': NAME, 'pageSize': 11}),
            ('listed_business_segments', {'matchKeyword': '000001', 'keywordType': 'regNumber'}),
            ('enterprise_base', {'matchKeyword': NAME, 'keywordType': 'reportStockCode'}),
            ('enterprise_base', {'matchKeyword': NAME, 'product_id': 'product'}),
            ('enterprise_base', {'matchKeyword': NAME, 'url': 'https://sample.invalid'}),
            ('enterprise_base', {'matchKeyword': NAME, 'headers': {'secret': 'offline'}}),
            ('enterprise_base', {'matchKeyword': NAME, 'SECRET_ID': 'offline'}),
            ('enterprise_base', {'roster': [NAME]}),
            ('enterprise_estore_shops', {'matchKeyword': NAME, 'statsMonth': '2025-02'}),
            ('enterprise_estore_shops', {'matchKeyword': NAME, 'statsMonth': '202513'}),
            ('enterprise_estore_shops', {'matchKeyword': NAME, 'statsMonth': '999912'}),
            ('annual_reports', {'matchKeyword': NAME, 'arYear': '9999'}),
            ('annual_reports', {'matchKeyword': NAME, 'arYear': 2024}),
            ('estore_monthly_sales', {'platform': '天猫商城', 'shopId': 'offline-shop'}),
        ):
            with self.subTest(slug=slug, args=args):
                await self.call(slug, args, error='invalid_argument')
        for name in LEGACY_OPERATIONS:
            result = await self.app.call_tool(name, {})
            self.assertTrue(result.isError)
        self.assertEqual(self.provider.calls, [])
        self.assertEqual(await self.call('annual_reports', {'matchKeyword': NAME, 'arYear': '2024'}), {})
        self.assertEqual(await self.call('listed_business_segments', {
            'matchKeyword': '000001', 'keywordType': 'reportStockCode', 'pageSize': 10, 'pageIndex': 2,
        }), {})
        self.assertEqual(len(self.provider.calls), 2)

    async def test_missing_or_placeholder_credentials_do_not_call_provider(self):
        for value in ('', 'your_secret_key'):
            with self.subTest(value=value), patch.dict(os.environ, {'SECRET_KEY': value}):
                await self.call('enterprise_base', {'matchKeyword': NAME}, error='configuration_error')
        self.assertEqual(self.provider.calls, [])

    async def test_unknown_results_and_valid_empty_answers_are_not_conflated(self):
        for reply, result, fault in (
            ({'code': '10001'}, {}, None),
            ({'code': '10000', 'data': {}}, {}, None),
            ({'code': '10000', 'data': {'total': 0, 'resultList': []}}, {'total': 0, 'resultList': []}, None),
            ({'code': '10000', 'data': {}, 'msgCN': '无查询权限'}, None, 'upstream_denied'),
            ({'code': '10000'}, None, 'billing_unknown'),
            ({'code': '10000', 'data': []}, None, 'billing_unknown'),
            ({'code': '10000', 'data': {'wrongList': []}}, None, 'billing_unknown'),
            ({'code': '10000', 'data': {'resultList': {}}}, None, 'billing_unknown'),
            ({'code': '10000', 'data': {'total': 3}}, None, 'billing_unknown'),
            ({'code': '10000', 'data': {'total': 'bad', 'resultList': []}}, None, 'billing_unknown'),
            ({'code': '10000', 'data': {'total': '0'}}, {'total': '0'}, None),
            ({'code': '10000', 'data': {'resultList': None, 'total': 0}}, None, 'billing_unknown'),
            ({'code': '10000', 'data': {'resultList': [{}]}}, None, 'billing_unknown'),
            ({'code': '10000', 'data': {'resultList': [None]}}, None, 'billing_unknown'),
            ({'code': '10000', 'data': {'resultList': [{'phone': 'hidden'}]}}, None, 'billing_unknown'),
            ({'code': '90000', 'message': 'must not leak details'}, None, 'upstream_rejected'),
            ({'_test_status': 429}, None, 'upstream_unavailable'),
            ({'_test_status': 307}, None, 'upstream_unavailable'),
            ({'_test_status': 503}, None, 'billing_unknown'),
            ('connection', None, 'upstream_unavailable'),
            ('timeout', None, 'billing_unknown'),
            ('disconnect', None, 'billing_unknown'),
        ):
            with self.subTest(fault=fault, kind=type(reply).__name__):
                self.provider.reply = reply
                before = len(self.provider.calls)
                actual = await self.call('enterprise_search', {'matchKeyword': NAME}, error=fault)
                if not fault:
                    self.assertEqual(actual, result)
                self.assertEqual(len(self.provider.calls), before + 1, 'no retry or pagination')

    async def test_nested_projection_preserves_selected_facts_not_hidden_fields(self):
        fixtures = (
            ('enterprise_base',
             {'entName': NAME, 'nameId': 'offline-id', 'addressValue': {'city': '上海市'}, 'socialCreditCode': 'offline-code'},
             {'contactPhone': 'hidden', 'addressValue': {'city': '上海市', 'phone': 'hidden'}}),
            ('enterprise_search',
             {'total': '2', 'resultList': [{
                 'nameId': 'candidate-A', 'entName': NAME, 'socialCreditCode': 'offline-code',
                 'catchReason': {'operBrandList': ['样本商号']},
             }]},
             {'resultList': [{
                 'nameId': 'candidate-A', 'entName': NAME, 'socialCreditCode': 'offline-code',
                 'catchReason': {'operBrandList': ['样本商号'], 'eMail': ['hidden']},
             }]}),
            ('enterprise_website',
             {'domainAppDownload': True, 'domainApplyTrial': False},
             {'domain400Phone': 'hidden', 'domainServiceOnLineList': ['hidden']}),
            ('enterprise_investments',
             {'resultList': [{'name': '样本被投主体', 'ratio': '12.3400%',
                              'regCapital': {'value': 0, 'coinType': '万元'}, 'isListed': 0}], 'total': 1},
             {'resultList': [{'name': '样本被投主体', 'ratio': '12.3400%', 'regCapital': {
                 'value': 0, 'coinType': '万元', 'secret_key': 'hidden'}, 'isListed': 0, 'phone': 'hidden'}]}),
            ('enterprise_operating_strength',
             {'incomeList': [{'year': 2024, 'growth_rate': '-12.3400%', 'min': {'value': -2.5, 'unit': '万'}}]},
             {'incomeList': [{'year': 2024, 'growth_rate': '-12.3400%', 'min': {
                 'value': -2.5, 'unit': '万', 'token': 'hidden'}}]}),
            ('patent_statistics',
             {'patentTypeStat': {'utilityModelPatentCount': 0}, 'patentTypeAppTimeStat': [
                 {'year': '2023', 'appearanceDesignPatentCount': 2},
             ]},
             {'authorization': 'hidden'}),
            ('enterprise_litigation',
             {'caKaitingList': [{'caPublishDate': '2024-02-29', 'caRelatedCaseNumber': 'SYNTHETIC'}],
              'caKaitingTotal': 1, 'caLianList': [], 'caLianTotal': 0, 'enforcementList': []},
             {'unsafe_field': 'hidden'}),
            ('enterprise_operating_anomalies',
             {'anomalyCount': 2, 'anomalyList': [{'createDate': '2019-01-01', 'removeDate': '2025-05-01'}]},
             {'name': 'undisclosed'}),
            ('trademark_details',
             {'resultList': [{'tmId': 'tm-offline', 'tmServiceContents': [{'detail': '样本产品'}]}], 'total': 1},
             {'resultList': [{'tmId': 'tm-offline', 'tmServiceContents': [
                 {'detail': '样本产品', 'phone': 'hidden'},
             ]}]}),
        )
        for slug, projected, extra in fixtures:
            with self.subTest(slug=slug):
                self.provider.reply = {'code': '10000', 'data': {**projected, **extra}}
                self.assertEqual(await self.call(slug, {'matchKeyword': NAME}), projected)

    async def test_child_parameters_come_from_project_brand_or_same_shop_month(self):
        # Numbers and IDs are intentionally unlike tool/product identifiers.
        self.provider.reply = {'code': '10000', 'data': {
            'resultList': [{'_id': 'project-X', 'projectName': '合成项目', 'totalInvestment': 0, 'tel': 'hidden'}],
            'total': 1,
        }}
        project = await self.call('construction_projects', {'matchKeyword': NAME})
        self.assertEqual(project['resultList'][0], {'_id': 'project-X', 'projectName': '合成项目', 'totalInvestment': 0})
        self.provider.reply = {'code': '10000', 'data': {
            'resultList': [{'contractId': 'contract-A', 'signingDate': '2025-04-03'}], 'total': 1,
        }}
        await self.call('construction_contracts', {'matchKeyword': project['resultList'][0]['_id']})
        self.assertEqual(self.provider.calls[-1], ('66aba795520b164ce252e60d', {'matchKeyword': 'project-X'}))

        self.provider.reply = {'code': '10000', 'data': {'storeList': [{'brandId': 'brand-X', 'brandName': '样本餐饮'}]}}
        restaurant = await self.call('restaurant_brands', {'matchKeyword': NAME})
        self.provider.reply = {'code': '10000', 'data': {
            'cityStatsList': [{'city': '上海市', 'count': 2}], 'cityStatsTotal': 1,
        }}
        await self.call('restaurant_brand_locations', {'matchKeyword': restaurant['storeList'][0]['brandId']})
        self.assertEqual(self.provider.calls[-1], ('66f3d8c064bd2be52d68a159', {'matchKeyword': 'brand-X'}))

        self.provider.reply = {'code': '10000', 'data': {'shopList': [
            {'platform': '天猫商城', 'shopId': 'shop-X', 'statsMonth': '202504', 'contactInfo': 'hidden'},
        ]}}
        shops = await self.call('enterprise_estore_shops', {'matchKeyword': NAME, 'statsMonth': '202504'})
        self.assertEqual(shops['shopList'], [{'platform': '天猫商城', 'shopId': 'shop-X', 'statsMonth': '202504'}])
        raw = {'platform': '天猫商城', 'shopId': 'shop-X', 'shopSalesAmountSumMth': 0,
               'shopSalesAmountSumMthYoy': '0.0000%'}
        self.provider.reply = {'code': '10000', 'data': raw}
        self.assertEqual(await self.call('estore_monthly_sales', shops['shopList'][0]), raw)
        self.assertEqual(self.provider.calls[-1], ('684ad781d4819127431f9ff8', shops['shopList'][0]))
        self.assertEqual(len(self.provider.calls), 6)

    async def test_project_brand_shop_tools_explain_parent_keys(self):
        tools = {t.name: t for t in await self.app.list_tools()}
        for slug, parent, field, source in (
            ('construction_contracts', 'construction_projects', 'matchKeyword', 'resultList[]._id'),
            ('restaurant_brand_locations', 'restaurant_brands', 'matchKeyword', 'storeList[].brandId'),
            ('estore_monthly_sales', 'enterprise_estore_shops', 'statsMonth', 'shopList[].statsMonth'),
        ):
            tool = tools['portrait_query_' + slug]
            self.assertIn('portrait_query_' + parent, tool.description)
            self.assertIn(source, tool.inputSchema['properties'][field]['description'])
            self.assertIn('可能计费', tool.description)
        self.assertEqual(self.provider.calls, [])

    async def test_stdio_launch_discovery_and_no_state_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = root / 'server'
            server.mkdir()
            shutil.copyfile(SERVER_PATH, server / SERVER_PATH.name)
            blocker = root / 'not-a-directory'
            blocker.write_text('unchanged', encoding='utf-8')
            shadow = root / 'external-pythonpath'
            shadow.mkdir()
            (shadow / 'server.py').write_text('raise AssertionError("unrelated module imported")\n', encoding='utf-8')
            env = {
                'HOME': str(root),
                'PORTRAIT_DB_PATH': str(blocker / 'legacy.sqlite3'),
                'PYTHONDONTWRITEBYTECODE': '1',
                'PYTHONPATH': str(shadow),
            }
            before = sorted(root.rglob('*'))
            root.chmod(0o500)
            try:
                for args in ([], ['stdio', '--query-contract', 'v1']):
                    with self.subTest(args=args):
                        child = StdioServerParameters(
                            command=sys.executable, args=[str(server / SERVER_PATH.name), *args], env=env,
                        )
                        async with stdio_client(child) as streams, ClientSession(*streams) as client:
                            await client.initialize()
                            tools = (await client.list_tools()).tools
                            self.assertEqual({t.name for t in tools}, {r['tool_name'] for r in ROUTES.values()})
                            self.assertTrue(all(t.meta['portrait_query_contract'] == 'v1' for t in tools))
                            result = await client.call_tool('portrait_query_enterprise_base', {'matchKeyword': NAME})
                            self.assertTrue(result.isError)
                            self.assertEqual(json.loads(result.content[0].text)['code'], 'configuration_error')
            finally:
                root.chmod(0o700)
            self.assertEqual(sorted(root.rglob('*')), before)
            self.assertEqual(blocker.read_text(encoding='utf-8'), 'unchanged')

    async def test_stdio_connector_credentials_are_sufficient_and_legacy_off_stays_disabled(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = root / 'server'
            server.mkdir()
            shutil.copyfile(SERVER_PATH, server / SERVER_PATH.name)
            (root / '.env').write_text(
                ''.join(f'{key}={value}\n' for key, value in CONNECTOR_ENV.items()), encoding='utf-8',
            )
            # Run the real CLI with a provider that counts every would-be HTTP request.
            probe = root / 'run_with_mock.py'
            probe.write_text(
                """import importlib.util
import json
from pathlib import Path
from urllib.parse import parse_qs
import httpx

root = Path(__file__).parent
spec = importlib.util.spec_from_file_location('portrait_mcp_server', root / 'server' / 'mcp_server.py')
assert spec and spec.loader
mcp_server = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mcp_server)
calls = root / 'calls.jsonl'
def fetch(request):
    assert request.method == 'POST' and request.url.host == 'console.handaas.com'
    form = parse_qs(request.content.decode())
    assert set(form) == {'product_id', 'secret_id', 'params', 'signature'}
    with calls.open('a', encoding='utf-8') as log:
        log.write(json.dumps(form) + '\\n')
    return httpx.Response(200, json={'code': 10000, 'data': {'name': '测试企业'}})

class MockMCP(mcp_server.PortraitMCP):
    def __init__(self, **kwargs):
        super().__init__(transport=httpx.MockTransport(fetch), **kwargs)

mcp_server.PortraitMCP = MockMCP
mcp_server.main()
""", encoding='utf-8',
            )
            calls = root / 'calls.jsonl'
            env = {'HOME': str(root), 'PYTHONDONTWRITEBYTECODE': '1'}
            legacy_env = {'PORTRAIT_LIVE': '0', 'PORTRAIT_ALLOWED_HOSTS': '*'}
            for legacy in (False, True):
                with self.subTest(legacy_disabled=legacy):
                    child = StdioServerParameters(
                        command=sys.executable,
                        args=[str(probe), 'stdio', '--query-contract', 'v1'],
                        env={**env, **(legacy_env if legacy else {})},
                    )
                    async with stdio_client(child) as streams, ClientSession(*streams) as client:
                        await client.initialize()
                        self.assertEqual(len((await client.list_tools()).tools), 46)
                        self.assertFalse(calls.exists(), 'discovery must not call the provider')
                        result = await client.call_tool('portrait_query_enterprise_base', {'matchKeyword': NAME})
                        if legacy:
                            self.assertTrue(result.isError)
                            self.assertEqual(json.loads(result.content[0].text)['code'], 'live_disabled')
                            self.assertFalse(calls.exists())
                        else:
                            self.assertFalse(result.isError)
                            self.assertEqual(result.structuredContent, {'data': {'name': '测试企业'}})
                            forms = [json.loads(row) for row in calls.read_text(encoding='utf-8').splitlines()]
                            self.assertEqual(len(forms), 1)
                            form = forms[0]
                            self.assertEqual(form['product_id'], ['66dbccbec7a7e3460f5e613f'])
                            self.assertEqual(form['secret_id'], [CONNECTOR_ENV['SECRET_ID']])
                            self.assertEqual(json.loads(form['params'][0]), {'matchKeyword': NAME})
                            expected = (
                                form['params'][0] + form['product_id'][0] + form['secret_id'][0]
                                + CONNECTOR_ENV['SECRET_KEY']
                            )
                            self.assertEqual(form['signature'], [hashlib.md5(expected.encode()).hexdigest()])
                            calls.unlink()

    async def test_guarded_http_transport_discovery_and_single_query(self):
        started = asyncio.Event()

        class TestServer(uvicorn.Server):
            async def startup(self, sockets=None):
                await super().startup(sockets=sockets)
                started.set()

        with socket.socket() as ready_socket:
            ready_socket.bind(('127.0.0.1', 0))
            ready_socket.listen()
            path = f'http://127.0.0.1:{ready_socket.getsockname()[1]}/mcp'
            with patch.dict(os.environ, {
                'PORTRAIT_ALLOWED_HOSTS': 'portrait.example',
                'PORTRAIT_ALLOWED_ORIGINS': 'https://client.example',
            }):
                http_app = self.app.streamable_http_app()
            server = TestServer(uvicorn.Config(http_app, ws='none', log_level='error', access_log=False))
            running = asyncio.create_task(server.serve(sockets=[ready_socket]))
            try:
                await asyncio.wait_for(started.wait(), 5)
                async with httpx.AsyncClient(trust_env=False) as http:
                    self.assertEqual((await http.post(path, json={})).status_code, 401)
                    self.assertEqual(
                        (await http.post(path, json={}, headers={'Authorization': 'Bearer wrong-token'})).status_code,
                        401,
                    )
                    http.headers['Authorization'] = 'Bearer ' + TEST_ENV['CUSTOMER_COHORT_PROFILE_MCP_TOKEN']
                    self.assertEqual((await http.post(path, headers={'Host': 'evil.invalid'}, json={})).status_code, 421)
                    self.assertEqual((await http.post(path, headers={'Origin': 'https://evil.invalid'}, json={})).status_code, 403)
                    http.headers.update({'Host': 'portrait.example', 'Origin': 'https://client.example'})
                    async with streamable_http_client(path, http_client=http) as (rd, wr, _), ClientSession(rd, wr) as client:
                        await client.initialize()
                        tool_names = {t.name for t in (await client.list_tools()).tools}
                        self.assertEqual(tool_names, {r['tool_name'] for r in ROUTES.values()})
                        self.assertEqual(self.provider.calls, [])
                        result = await client.call_tool('portrait_query_enterprise_base', {'matchKeyword': NAME})
                        self.assertEqual(result.structuredContent, {'data': {}})
                        self.provider.reply = 'timeout'
                        result = await client.call_tool('portrait_query_enterprise_base', {'matchKeyword': NAME})
                        self.assertTrue(result.isError)
                        self.assertEqual(json.loads(result.content[0].text)['code'], 'billing_unknown')
                        self.assertEqual(self.provider.calls, [
                            ('66dbccbec7a7e3460f5e613f', {'matchKeyword': NAME}),
                            ('66dbccbec7a7e3460f5e613f', {'matchKeyword': NAME}),
                        ])
            finally:
                server.should_exit = True
                await asyncio.wait_for(running, 10)

    async def test_http_requires_a_dedicated_token(self):
        with patch.dict(os.environ, {'CUSTOMER_COHORT_PROFILE_MCP_TOKEN': 'short'}):
            with self.assertRaises(Fault):
                self.app.streamable_http_app()


class RegistryTests(unittest.TestCase):
    def test_wrong_query_contract_exits_before_config_or_app(self):
        with patch.object(sys, 'argv', ['mcp_server.py', 'stdio', '--query-contract', 'v2']), \
                patch.object(mcp_server, 'load_dotenv') as load_config, \
                patch.object(mcp_server, 'PortraitMCP') as app, \
                patch('sys.stderr'):
            with self.assertRaises(SystemExit) as raised:
                mcp_server.main()
        self.assertEqual(raised.exception.code, 2)
        load_config.assert_not_called()
        app.assert_not_called()

    def test_invalid_legacy_live_flag_fails_before_constructing_server(self):
        for value in ('', 'yes'):
            with self.subTest(value=value), patch.dict(os.environ, {'PORTRAIT_LIVE': value}), \
                    patch.object(sys, 'argv', ['mcp_server.py']), patch.object(mcp_server, 'load_dotenv'), \
                    patch.object(mcp_server, 'PortraitMCP') as app:
                with self.assertRaises(Fault) as raised:
                    mcp_server.main()
                self.assertEqual(raised.exception.code, 'configuration_error')
                app.assert_not_called()

    def test_published_entrypoint_and_environment_need_no_extra_files(self):
        self.assertEqual(set((ROOT / '.env.example').read_text().splitlines()), {
            f'{key}=' for key in CONNECTOR_ENV
        })
        self.assertEqual(list(SERVER_PATH.parent.glob('*.py')), [SERVER_PATH])
        self.assertFalse((SERVER_PATH.parent / 'product-routes.json').exists())

    def test_trademarks_products_remain_public(self):
        expected = {
            '66d5b7df537c3f61d646c2dc': 'portrait_query_trademark_statistics',
            '671357d127ab3417e1f3f21b': 'portrait_query_trademark_overview',
            '6713537822fc5904b878fbcc': 'portrait_query_trademark_details',
        }
        self.assertEqual(
            {pid: r['tool_name'] for pid, r in ROUTES.items() if r['topic'] == 'trademark'},
            expected,
        )


if __name__ == '__main__':
    unittest.main()
