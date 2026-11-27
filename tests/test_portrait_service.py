"""Offline scenarios: real MCP, scope planner, SQLite ledger and gateway encoder.
Only the external data provider is replaced, at the HTTP transport boundary.
"""
import asyncio
import hashlib
import json
import os
import secrets
import socket
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs

import httpx
import uvicorn
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client

from server.common import ROUTES, Fault, Settings, _load_routes, digest
from server.gateway import Gateway
from server.mcp_server import Application
from server.store import Store

ROOT = Path(__file__).resolve().parents[1]
P = {p['name']: key for key, p in ROUTES.items()}
NAME = '离线样本商业服务有限公司'
OTHER = '离线样本零售有限公司'
LIMITS = dict(max_calls=5000, max_page_size=10, max_pages_per_view=2,
              max_child_keys=4, max_targets=20)
PERIOD = dict(start_month='2025-04', end_month='2025-05',
              observation_cutoff=datetime.now(timezone.utc).date().isoformat(), max_months_per_entity=2)


class LocalProvider:
    def __init__(self):
        self.calls = []
        self.timeout_on = None
        self.shop_ids = ['shop-A']

    def __call__(self, request):
        wire = {k: v[0] for k, v in parse_qs(request.content.decode()).items()}
        expected = hashlib.md5((''.join(wire[k] for k in sorted(('product_id','secret_id','params'))) +
                                os.environ['SECRET_KEY']).encode()).hexdigest()
        if not secrets.compare_digest(wire['signature'], expected) or request.url.host != 'console.handaas.com':
            raise AssertionError('Incorrect gateway envelope')
        pid, args = wire['product_id'], json.loads(wire['params'])
        self.calls.append((pid, args))
        if pid == self.timeout_on:
            raise httpx.ReadTimeout('isolated transport interruption', request=request)
        if pid == P['工商照面']:
            key = args['matchKeyword']
            if key in (NAME, 'a'*24, '91310000MA00000001'):
                data = {'name': NAME, 'socialCreditCode': '91310000MA00000001', 'addressValue':{'city':'上海市'},
                        'industry':{'firstIndustry':'批发和零售业','fourthIndustry':'其他综合零售'}}
            elif key in (OTHER, 'b'*24, '91310000MA00000002'):
                data = {'name': OTHER, 'socialCreditCode':'91310000MA00000002','industry':{'fourthIndustry':'零售'}}
            else:
                data = {}
        elif pid == P['关键词查询企业']:
            data = {'resultList':[{'name': NAME, 'nameId':'a'*24,'catchReason':{'operBrandList':['样本商号']}},
                                  {'name': OTHER, 'nameId':'b'*24,'catchReason':{'operBrandList':['样本商号']}}], 'total':2}
        elif pid == P['企业标签']:
            data = {'businessTags':['电商零售'], 'enterpriseClassification':'销售型'}
        elif pid == P['企业指标']:
            data = {'enterpriseSize':'小型企业'}
        elif pid == P['企业行业（国标）']:
            data = {'firstIndustry':'批发和零售业','fourthIndustry':'其他综合零售'}
        elif pid == P['企业产品标签']:
            data = {'tagNames':['零售']}
        elif pid == P['企业简介']:
            data = {'desc':'经营线下及电商零售业务。'}
        elif pid == P['企业店铺明细']:
            data = {'name': NAME, 'shopList': [
                {'platform': '天猫商城', 'shopId': key, 'statsMonth': args['statsMonth']}
                for key in self.shop_ids
            ]}
        elif pid == P['店铺月销售情况']:
            if set(args) != {'platform', 'shopId', 'statsMonth'} or args['shopId'] not in self.shop_ids or args['platform'] != '天猫商城':
                raise AssertionError('Child request did not use the three parent fields')
            data = {'platform':'天猫商城','shopId':args['shopId'],'shopSalesAmountSumMth':0,'shopSalesAmountSumMthYoy':'0.0000%'}
        elif pid == P['建筑工程项目明细']:
            data = {'resultList':[{'_id':'project-A','projectName':'历史项目'}], 'total':1}
        elif pid == P['建筑业项目合同信息']:
            if args['matchKeyword'] != 'project-A':
                raise AssertionError('Expected building project ID')
            data = {'resultList': [
                {'contractId': 'contract-A', 'signingDate': '2025-04-03'},
                {'contractId': 'outside-window', 'signingDate': '2025-06-15'},
            ], 'total': 2}
        elif pid == P['企业旗下餐饮品牌门店']:
            data = {'storeList':[{'brandId':'brand-A','brandName':'示例餐饮品牌'}]}
        elif pid == P['餐饮品牌门店统计']:
            if args['matchKeyword'] != 'brand-A':
                raise AssertionError('Expected restaurant brand ID')
            data = {'cityStatsList':[{'city':'上海市','count':2}], 'provinceStatsList':[{'province':'上海市','count':2}],
                    'cityStatsTotal':1,'provinceStatsTotal':1}
        elif pid == P['对外投资']:
            data = {'resultList': [
                {'name':'样本被投科技有限公司','scCode':'91310000MA00000003','ratio':51.0,
                 'subscriptionAmount':{'value':100,'coinType':'万元'},'regCapital':{'value':500,'coinType':'万元'},
                 'foundTime':'2018-06-01','operStatus':'存续','isListed':0,
                 'addressValue':{'province':'上海市','city':'上海市'},'business':'技术开发'},
                {'name':'样本参股企业','scCode':'91310000MA00000004','ratio':'12.3456%',
                 'subscriptionAmount':{'value':20,'coinType':'万元'},'regCapital':{'value':200,'coinType':'万元'},
                 'foundTime':'2020-01-02','operStatus':'存续','isListed':1,
                 'addressValue':{'province':'江苏省','city':'南京市'},'business':'零售'},
            ], 'total': 2}
        elif pid == P['推广情况']:
            data = {'lastPrmtTime':'2019-03-04','prmtCompetitorsNumber':3,'prmtLinksNumber':0,
                    'prmtSources':['搜索引擎','展会']}
        elif pid == P['域名信息']:
            data = {'resultList': [
                {'domainName':'sample.example','domainUrl':'https://www.sample.example',
                 'filingAuditTime':'2017-05-06','isHomePage':1,'websiteRecord':'沪ICP备00000001号'},
                {'domainName':'sample2.example','domainUrl':'https://sample2.example',
                 'filingAuditTime':'2026-01-02','isHomePage':0,'websiteRecord':'沪ICP备00000002号'},
            ], 'total': 2}
        elif pid == P['企业官网信息']:
            data = {'domain400Phone':'400-000-0000','domainAppDownload':True,'domainApplyTrial':False,
                    'domainCustomerCase':True,'domainEnglishVersion':False,'domainHttps':True,
                    'domainJoinUs':False,'domainLogin':False,'domainMerchant':True,'domainSolution':False,
                    'domainStatsModule':True,'domainServiceOnLineList':['客服甲']}
        elif pid == P['法律诉讼风险概况']:
            data = {
                'caKaitingList':[
                    {'_id':'k-1','caPublishDate':'2025-04-05','caRelatedCaseNumber':'CASE-SHARED','caType':'开庭公告'},
                    {'_id':'k-future','caPublishDate':'2099-01-01','caRelatedCaseNumber':'CASE-FUTURE','caType':'开庭公告'},
                ],
                'caLianList':[],
                'caList':[{'_id':'c-1','caPublishDate':'2025-04-10','caRelatedCaseNumber':'CASE-SHARED','caType':'法院公告'}],
                'edList':[{'_id':'e-1','efDishonestCaseCreateTime':'2024-01-01','efDishonestCaseNumber':'CASE-OLD'}],
                'enforcementList':[],
                'jdList':[{'_id':'j-1','jdCaseNumber':'CASE-SHARED','jdPublishDate':'2025-05-02'}],
                'limitedList':[],
                'caKaitingTotal':2,'caLianTotal':0,'caTotal':1,'edTotal':1,
                'enforcementTotal':0,'jdTotal':1,
            }
        elif pid == P['经营异常']:
            data = {'anomalyCount':4,'anomalyList':[
                {'createAuthority':'上海市监局','createDate':'2019-01-05','createReason':'未按规定公示年报',
                 'removeAuthority':'上海市监局','removeDate':'2025-04-15','removeReason':'已补报'},
                {'createAuthority':'上海市监局','createDate':'2025-05-01','createReason':'住所无法联系'},
                {'createAuthority':'上海市监局','createDate':'2018-02-01','createReason':'历史异常',
                 'removeAuthority':'上海市监局','removeDate':'2019-02-01','removeReason':'已移出'},
                {'createAuthority':'上海市监局','createDate':'2024-01-01','createReason':'公示信息隐瞒'},
            ]}
        else:
            raise AssertionError('Unselected product called')
        return httpx.Response(200, json={'code':'10000','data':data})


class ServiceWorkflow(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        cfg = dict(INTEGRATOR_ID=secrets.token_hex(12), SECRET_ID=secrets.token_hex(12), SECRET_KEY=secrets.token_hex(24),
                   PORTRAIT_DB_PATH=str(self.root/'state.sqlite3'),PORTRAIT_TENANT='offline',PORTRAIT_LIVE='1',
                   CUSTOMER_COHORT_PROFILE_MCP_TOKEN=secrets.token_urlsafe(32))
        env = patch.dict(os.environ, cfg)
        env.start()
        self.addCleanup(env.stop)
        self.provider = LocalProvider()
        self.settings = Settings.from_env()
        self.app = Application(self.settings, Gateway(transport=httpx.MockTransport(self.provider)))
        self.addCleanup(self.app.close)
        self.store = Store(self.settings)

    async def call(self, op, expect_error=False, **args):
        result = await self.app.mcp.call_tool('portrait_'+op, args)
        self.assertEqual(result.isError, expect_error, result.content[0].text if result.isError else op)
        return json.loads(result.content[0].text) if result.isError else result.structuredContent

    async def base(self, rows):
        receipt = await self.call('stage_roster', batch_key='batch_'+secrets.token_hex(8),chunk_index=1,chunk_count=1,rows=rows)
        return await self.call('plan_base',roster_id=receipt['roster_id'],business_scope_hash=digest('零售服务'),limits=LIMITS)

    async def execute(self, plan):
        self.assertTrue(plan['executable'], plan['blockers'])
        args = dict(plan_id=plan['plan_id'], plan_hash=plan['plan_hash'])
        run = await self.call('execute_plan', **args)
        async def wait_for_result():
            while True:
                result = await self.call('get_run',run_id=run['run_id'])
                if result['is_terminal'] or result['state']=='partial':
                    return result
                await asyncio.sleep(.02)
        return await asyncio.wait_for(wait_for_result(), 15), args

    async def test_confirmed_base_resolution_and_scoped_chains(self):
        plan = await self.base([{'row_id':'row_one','enterprise_name':NAME},{'row_id':'row_two','enterprise_name':NAME},
                                {'row_id':'row_brand','enterprise_name':'样本商号'}])
        self.assertFalse(self.provider.calls)
        await self.call('execute_plan',True,plan_id=plan['plan_id'],plan_hash=plan['plan_hash'],confirmed=True)
        await self.call('execute_plan', True, plan_id=plan['plan_id'], plan_hash=plan['plan_hash'], product_ids=[P['工商照面']])
        self.assertFalse(self.provider.calls)
        run, exec_args = await self.execute(plan)
        self.assertEqual(run['state'],'succeeded',run.get('error'))
        sid = run['result']['snapshot_id']
        calls = len(self.provider.calls)
        self.assertEqual((await self.call('execute_plan',**exec_args))['run_id'],run['run_id'])
        subjects=(await self.call('read_page',snapshot_id=sid,view='subjects'))['records']
        self.assertEqual(len(subjects),1)
        self.assertEqual(set(subjects[0]['source_row_ids']),{'row_one','row_two'})
        candidates = (await self.call('read_page',snapshot_id=sid,view='candidates'))['records']
        chosen = next(c for c in candidates if c['enterprise_name']==OTHER)
        await self.call('plan_resolution',True,snapshot_id=sid,limits=LIMITS,decisions=[{'row_id':'row_one','action':'select_candidate','candidate_id':chosen['candidate_id']}])
        fixed = await self.call('plan_resolution',snapshot_id=sid,limits=LIMITS,decisions=[{'row_id':'row_brand','action':'select_candidate','candidate_id':chosen['candidate_id']}])
        self.assertEqual(len(self.provider.calls),calls)
        proposed = await self.call('read_page', snapshot_id=fixed['snapshot_id'], view='subjects')
        self.assertEqual([s['subject_id'] for s in proposed['records']], [subjects[0]['subject_id']])
        resolved,_ = await self.execute(fixed)
        self.assertEqual(resolved['state'],'succeeded',resolved.get('error'))
        sid=resolved['result']['snapshot_id']
        self.assertEqual(len((await self.call('read_page',snapshot_id=sid,view='subjects'))['records']),2)
        newly_fetched = {pid for pid, _ in self.provider.calls[calls:]}
        self.assertTrue({P['企业标签'], P['企业指标']} <= newly_fetched)
        subject = subjects[0]['subject_id']
        choices = [dict(cohort_key='已确认客群',topic=t,subject_ids=[subject],product_ids=[P[p] for p in names]) for t,names in (
            ('estore',['企业店铺明细','店铺月销售情况']),('construction',['建筑工程项目明细','建筑业项目合同信息']),
            ('food_chain',['企业旗下餐饮品牌门店','餐饮品牌门店统计']))]
        scoped = await self.call('plan_topics',snapshot_id=sid,analysis_scope_hash=digest('历史覆盖'),period=PERIOD,limits=LIMITS,selections=choices)
        result,_ = await self.execute(scoped)
        self.assertEqual(result['state'],'succeeded',result.get('error'))
        last=result['result']['snapshot_id']; next_page={}; evidence=[]
        while True:
            page=await self.call('read_page',snapshot_id=last,view='evidence',limit=3,**next_page)
            evidence.extend(page['records'])
            if not page['has_more']:
                break
            next_page={'cursor':page['next_cursor']}
        sales = [e for e in evidence if e['source_field'].endswith('shopSalesAmountSumMth')
                 and e.get('raw_value') == 0 and e['status'] == 'zero']
        self.assertEqual({e['business_time'].get('value') for e in sales}, {'2025-04', '2025-05'})
        contracts = [e.get('raw_value') for e in evidence
                     if e['product_id'] == P['建筑业项目合同信息'] and e['source_field'].endswith('contractId')]
        self.assertEqual(contracts, ['contract-A'])
        self.assertTrue(any(e.get('raw_value')=='0.0000%' for e in evidence))
        self.assertEqual(sorted(q['statsMonth'] for pid,q in self.provider.calls if pid==P['店铺月销售情况']),['202504','202505'])
        for name in ('建筑业项目合同信息','餐饮品牌门店统计'):
            self.assertTrue(any(pid==P[name] for pid,_ in self.provider.calls),name)
        await self.call('read_page',True,snapshot_id=last,view='coverage',**next_page)
        with self.assertRaises(Fault):
            self.store.get_page('other-tenant',dict(snapshot_id=last,view='subjects'))

    async def test_store_expansion_limit_is_visible_as_partial_coverage(self):
        self.provider.shop_ids.append('shop-B')
        plan = await self.base([{'row_id': 'row_shop', 'enterprise_name': NAME}])
        base, _ = await self.execute(plan)
        sid = base['result']['snapshot_id']
        subject = (await self.call('read_page', snapshot_id=sid, view='subjects'))['records'][0]
        parent, child = P['企业店铺明细'], P['店铺月销售情况']
        topics = await self.call(
            'plan_topics', snapshot_id=sid, analysis_scope_hash=digest('门店上限'),
            period={**PERIOD, 'end_month': PERIOD['start_month']},
            limits={**LIMITS, 'max_child_keys': 1},
            selections=[dict(cohort_key='电商客群', topic='estore', subject_ids=[subject['subject_id']],
                             product_ids=[parent, child])],
        )
        run, _ = await self.execute(topics)
        self.assertEqual(run['state'], 'partial', run.get('error'))
        self.assertEqual(len([pid for pid, _ in self.provider.calls if pid == child]), 1)
        records = (await self.call('read_page', snapshot_id=run['result']['snapshot_id'],
                                   view='evidence', limit=200))['records']
        self.assertTrue(any(e['product_id'] == child and e['status'] == 'truncated' for e in records))

    async def test_lost_upstream_reply_is_not_dispatched_again(self):
        self.provider.timeout_on = P['工商照面']
        plan = await self.base([dict(row_id='row_uncertain',enterprise_name=NAME)])
        run,args=await self.execute(plan)
        self.assertIn(run['state'],('partial','failed'))
        self.assertEqual(run['error']['code'],'billing_unknown')
        self.assertEqual(sum(pid==self.provider.timeout_on for pid,_ in self.provider.calls),1)
        again = await self.call('execute_plan',**args)
        self.assertEqual(again['run_id'],run['run_id'])
        self.assertEqual(sum(pid==self.provider.timeout_on for pid,_ in self.provider.calls),1)
        restarted = Application(self.settings, Gateway(transport=httpx.MockTransport(self.provider)))
        self.addCleanup(restarted.close)
        restarted.engine.recover()
        recovered = restarted.engine.get(self.settings.tenant, {'run_id': run['run_id']})
        self.assertEqual(recovered['error']['code'], 'billing_unknown')
        self.assertEqual(restarted.engine.execute(self.settings.tenant, args)['run_id'], run['run_id'])
        self.assertEqual(sum(pid == self.provider.timeout_on for pid, _ in self.provider.calls), 1)
        changed = await self.call('plan_base', roster_id=plan['roster_id'],
                                  business_scope_hash=digest('第二份业务口径'), limits=LIMITS)
        distinct_run, _ = await self.execute(changed)
        self.assertNotEqual(distinct_run['run_id'], run['run_id'])
        self.assertEqual(distinct_run['error']['code'], 'billing_unknown')
        self.assertEqual(sum(pid == self.provider.timeout_on for pid, _ in self.provider.calls), 1)

    async def test_explicit_empty_and_damaged_envelope_are_distinct(self):
        pid = P['关键词查询企业']
        args = {'matchKeyword': '样本商号'}
        for envelope, error_code in (
            ({'code': '10001', 'msgCN': '查询数据为空'}, None),
            ({'code': '10000', 'data': {}, 'msgCN': '无查询权限'}, 'upstream_denied'),
            ({'code': '10000'}, 'billing_unknown'),
        ):
            gateway = Gateway(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=envelope)))
            try:
                if error_code:
                    with self.assertRaises(Fault) as cm:
                        gateway.fetch(pid, args)
                    self.assertEqual(cm.exception.code, error_code)
                else:
                    self.assertEqual(gateway.fetch(pid, args), {})
                with self.assertRaises(Fault):
                    gateway.validate_call(pid, {**args, 'keywordType': 'name'})
            finally:
                gateway.close()

    async def test_live_gate_hash_and_expiry_prevent_dispatch(self):
        plan = await self.base([dict(row_id='row_scope', enterprise_name=NAME)])
        args = {'plan_id': plan['plan_id'], 'plan_hash': plan['plan_hash']}
        changed = await self.call('execute_plan', True, **{**args, 'plan_hash': digest('不同分析范围')})
        self.assertEqual(changed['code'], 'plan_hash_mismatch')
        disabled = Application(replace(self.settings, live=False), Gateway(transport=httpx.MockTransport(self.provider)))
        self.addCleanup(disabled.close)
        result = await disabled.mcp.call_tool('portrait_execute_plan', args)
        self.assertTrue(result.isError)
        with patch('server.execution.now', return_value='2099-12-31T23:59:59Z'):
            expired = await self.call('execute_plan', True, **args)
        self.assertEqual(expired['code'], 'plan_expired')
        self.assertFalse(self.provider.calls)

    async def test_chunk_replay_preserves_scope(self):
        batch = dict(batch_key='batch_parttest', chunk_count=2)
        tail = dict(chunk_index=2, rows=[dict(row_id='row_b', enterprise_name=OTHER)])
        initial = await self.call('stage_roster', **batch, **tail)
        self.assertEqual(initial['status'], 'partial')
        self.assertNotIn('roster_id', initial)
        await self.call('stage_roster', True, **batch, chunk_index=2,
                        rows=[dict(row_id='row_changed', enterprise_name=NAME)])
        head = dict(chunk_index=1, rows=[dict(row_id='row_a', enterprise_name=NAME)])
        complete = await self.call('stage_roster', **batch, **head)
        replay = await self.call('stage_roster', **batch, **tail)
        self.assertEqual(replay['roster_id'], complete['roster_id'])
        plan = await self.call('plan_base', roster_id=complete['roster_id'],
                               business_scope_hash=digest('工艺'), limits=LIMITS)
        self.assertEqual(plan['scope']['row_count'], 2)
        self.assertFalse(self.provider.calls)

    async def test_parallel_direct_execution_and_cross_plan_reuse(self):
        plan = await self.base([{'row_id': 'row_direct', 'enterprise_name': NAME}])
        args = dict(plan_id=plan['plan_id'], plan_hash=plan['plan_hash'])
        left, right = await asyncio.gather(self.call('execute_plan', **args), self.call('execute_plan', **args))
        self.assertEqual(left['run_id'], right['run_id'])
        result, _ = await self.execute(plan)
        self.assertEqual(result['state'], 'succeeded', result.get('error'))
        calls = len(self.provider.calls)
        self.assertGreater(calls, 0)
        self.assertEqual(result['usage'], {'calls_used': calls, 'max_calls': plan['estimate']['max_calls']})
        second = await self.call('plan_base', roster_id=plan['roster_id'], limits=LIMITS,
                                 business_scope_hash=digest('同名单、不同业务问题'))
        reused, _ = await self.execute(second)
        self.assertEqual(reused['state'], 'succeeded', reused.get('error'))
        self.assertEqual(reused['usage']['calls_used'], 0)
        self.assertEqual(len(self.provider.calls), calls)

    async def test_new_topics_keep_dates_totals_shared_cases_and_anomaly_intervals(self):
        plan = await self.base([{'row_id': 'row_new_topics', 'enterprise_name': NAME}])
        base, _ = await self.execute(plan)
        sid = base['result']['snapshot_id']
        subject = (await self.call('read_page', snapshot_id=sid, view='subjects'))['records'][0]
        selections = [
            dict(cohort_key='新专题客群', topic='investment', subject_ids=[subject['subject_id']],
                 product_ids=[P['对外投资']]),
            dict(cohort_key='新专题客群', topic='promotion', subject_ids=[subject['subject_id']],
                 product_ids=[P['推广情况']]),
            dict(cohort_key='新专题客群', topic='website', subject_ids=[subject['subject_id']],
                 product_ids=[P['域名信息'], P['企业官网信息']]),
            dict(cohort_key='新专题客群', topic='risk', subject_ids=[subject['subject_id']],
                 product_ids=[P['法律诉讼风险概况'], P['经营异常']]),
        ]
        scoped = await self.call(
            'plan_topics', snapshot_id=sid, analysis_scope_hash=digest('新专题范围'),
            period=PERIOD, limits=LIMITS, selections=selections,
        )
        self.assertEqual(set(scoped['scope']['topic_keys']), {'investment', 'promotion', 'website', 'risk'})
        self.assertEqual(
            {p['routing_key'] for p in scoped['estimate']['products']},
            {'investment', 'promotion', 'website', 'risk'},
        )
        run, _ = await self.execute(scoped)
        self.assertEqual(run['state'], 'succeeded', run.get('error'))
        last = run['result']['snapshot_id']
        evidence, cursor = [], None
        while True:
            page = await self.call(
                'read_page', snapshot_id=last, view='evidence', limit=200,
                **({'cursor': cursor} if cursor else {}),
            )
            evidence.extend(page['records'])
            if not page['has_more']:
                break
            cursor = page['next_cursor']
        by_field = {}
        for item in evidence:
            by_field.setdefault((item['product_id'], item['source_field']), []).append(item)

        ratios = by_field[(P['对外投资'], 'resultList.ratio')]
        numeric_ratio = next(e for e in ratios if e['raw_value'] == 51.0)
        textual_ratio = next(e for e in ratios if e['raw_value'] == '12.3456%')
        self.assertEqual(numeric_ratio['unit'], 'none')
        self.assertEqual(textual_ratio['raw_value'], '12.3456%')
        self.assertEqual(textual_ratio['unit'], '%')
        investees = {e['raw_value'] for e in by_field[(P['对外投资'], 'resultList.name')]}
        self.assertEqual(investees, {'样本被投科技有限公司', '样本参股企业'})
        self.assertNotIn(NAME, investees)
        self.assertEqual(by_field[(P['对外投资'], 'total')][0]['raw_value'], 2)

        promotion_time = by_field[(P['推广情况'], 'lastPrmtTime')][0]
        self.assertEqual(promotion_time['raw_value'], '2019-03-04')
        self.assertEqual(promotion_time['status'], 'observed')
        self.assertEqual(by_field[(P['推广情况'], 'prmtLinksNumber')][0]['status'], 'zero')
        self.assertEqual(
            by_field[(P['推广情况'], 'prmtSources')][0]['raw_value'], ['搜索引擎', '展会']
        )

        audit_dates = {e['raw_value'] for e in by_field[(P['域名信息'], 'resultList.filingAuditTime')]}
        self.assertEqual(audit_dates, {'2017-05-06', '2026-01-02'})
        self.assertEqual(by_field[(P['域名信息'], 'total')][0]['raw_value'], 2)

        modules = {
            item['source_field']: item
            for item in evidence if item['product_id'] == P['企业官网信息']
        }
        self.assertEqual(len(modules), 10)
        self.assertNotIn('domain400Phone', modules)
        self.assertNotIn('domainServiceOnLineList', modules)
        self.assertIs(modules['domainApplyTrial']['raw_value'], False)
        self.assertEqual(modules['domainApplyTrial']['status'], 'observed')

        legal = P['法律诉讼风险概况']
        totals = {field: by_field[(legal, field)][0] for field in (
            'caKaitingTotal', 'caLianTotal', 'caTotal', 'edTotal',
            'enforcementTotal', 'jdTotal', 'limitedTotal',
        )}
        self.assertEqual(totals['caKaitingTotal']['raw_value'], 2)
        self.assertEqual(totals['caLianTotal']['status'], 'zero')
        self.assertEqual(totals['enforcementTotal']['status'], 'zero')
        self.assertEqual(totals['limitedTotal']['status'], 'not_returned')
        self.assertEqual(totals['edTotal']['raw_value'], 1)
        shared = [
            item for item in evidence
            if item['product_id'] == legal and item.get('raw_value') == 'CASE-SHARED'
        ]
        self.assertEqual(len(shared), 3)
        self.assertEqual(len({item['evidence_id'] for item in shared}), 3)
        self.assertTrue(all(item['topic'] == 'risk' for item in shared))
        self.assertFalse(any(item.get('raw_value') == 'CASE-FUTURE' for item in evidence))
        self.assertFalse(any(item.get('raw_value') == 'CASE-OLD' for item in evidence))

        anomaly = P['经营异常']
        reasons = {
            item['raw_value']
            for item in by_field[(anomaly, 'anomalyList.createReason')]
        }
        self.assertEqual(reasons, {'未按规定公示年报', '住所无法联系', '公示信息隐瞒'})
        self.assertNotIn('历史异常', reasons)
        removals = by_field[(anomaly, 'anomalyList.removeDate')]
        self.assertEqual(len(removals), 3)
        self.assertEqual(sum(e['status'] == 'not_returned' for e in removals), 2)
        self.assertEqual(by_field[(anomaly, 'anomalyCount')][0]['raw_value'], 4)
        anomaly_evidence = [e for e in evidence if e['product_id'] == anomaly]
        self.assertFalse(any('unresolved' in json.dumps(e, ensure_ascii=False) for e in anomaly_evidence))

        coverage = (await self.call('read_page', snapshot_id=last, view='coverage', limit=200))['records']
        legal_coverage = next(c for c in coverage if c['product_id'] == legal)
        self.assertIn('excluded_publish_after_cutoff:1', legal_coverage.get('reason') or '')
        self.assertNotIn('unresolved', legal_coverage.get('reason') or '')

    async def test_real_stdio_and_http(self):
        child_env=dict(os.environ,PORTRAIT_LIVE='0',PORTRAIT_DB_PATH=str(self.root/'stdio.sqlite3'))
        params=StdioServerParameters(command=sys.executable,args=[str(ROOT/'server/mcp_server.py'),'stdio'],env=child_env)
        async with stdio_client(params) as streams, ClientSession(*streams) as client:
            await client.initialize()
            tools=await client.list_tools()
            self.assertEqual({t.name for t in tools.tools},set(self.app.operations))
            public = {t.name: t for t in tools.tools}
            execute_input = public['portrait_execute_plan'].inputSchema
            self.assertEqual(set(execute_input['properties']), {'plan_id', 'plan_hash'})
            self.assertEqual(set(execute_input['required']), {'plan_id', 'plan_hash'})
            limits = public['portrait_plan_topics'].inputSchema['properties']['limits']
            self.assertEqual(set(limits['properties']), set(LIMITS))
            invalid=await client.call_tool('portrait_execute_plan',{'confirmed':True})
            self.assertTrue(invalid.isError)
        started=asyncio.Event()
        class TestServer(uvicorn.Server):
            async def startup(self,sockets=None):
                await super().startup(sockets=sockets)
                started.set()
        with socket.socket() as ready_socket:
            ready_socket.bind(('127.0.0.1',0)); ready_socket.listen()
            base=f'http://127.0.0.1:{ready_socket.getsockname()[1]}/mcp'
            http_app=self.app.http_app()
            svc=TestServer(uvicorn.Config(http_app,log_level='error',access_log=False))
            running=asyncio.create_task(svc.serve(sockets=[ready_socket]))
            try:
                await asyncio.wait_for(started.wait(),5)
                async with httpx.AsyncClient(trust_env=False) as http:
                    self.assertEqual((await http.post(base,json={})).status_code,401)
                    http.headers['Authorization']='Bearer '+os.environ['CUSTOMER_COHORT_PROFILE_MCP_TOKEN']
                    async with streamable_http_client(base,http_client=http) as (rd,wr,_), ClientSession(rd,wr) as client:
                        await client.initialize()
                        receipt=await client.call_tool('portrait_stage_roster',dict(batch_key='batch_httpdemo',chunk_index=1,chunk_count=1,rows=[dict(row_id='row_http',enterprise_name=NAME)]))
                        self.assertEqual(receipt.structuredContent['status'],'complete')
                        planned = await client.call_tool('portrait_plan_base', {
                            'roster_id': receipt.structuredContent['roster_id'],
                            'business_scope_hash': digest('线下渠道'), 'limits': LIMITS,
                        })
                        plan = planned.structuredContent
                        self.assertTrue(plan['executable'])
                        self.assertFalse(self.provider.calls)
                        dispatched = await client.call_tool('portrait_execute_plan', {
                            'plan_id': plan['plan_id'], 'plan_hash': plan['plan_hash'],
                        })
                        async def read_result():
                            while True:
                                response = await client.call_tool('portrait_get_run', {'run_id': dispatched.structuredContent['run_id']})
                                if response.structuredContent['state'] not in ('queued', 'running'):
                                    return response.structuredContent
                                await asyncio.sleep(.02)
                        run = await asyncio.wait_for(read_result(), 10)
                        self.assertEqual(run['state'], 'succeeded')
                        page = await client.call_tool('portrait_read_page', {
                            'snapshot_id': run['result']['snapshot_id'], 'view': 'subjects',
                        })
                        self.assertEqual([r['enterprise_name'] for r in page.structuredContent['records']], [NAME])
                        self.assertEqual(run['usage']['calls_used'], len(self.provider.calls))
            finally:
                svc.should_exit=True
                await asyncio.wait_for(running,10)


class ContractSurface(unittest.TestCase):
    def test_route_catalog_rejects_invalid_structure_and_duplicate_product_ids(self):
        cases = (
            ({"products": {}}, "must contain a products array"),
            ({"products": [None]}, r"products\[0\] must be an object"),
            ({"products": [{"product_id": ""}]}, "product_id must be a non-empty string"),
            ({"products": [
                {"product_id": "duplicate", "name": "first"},
                {"product_id": "duplicate", "name": "second"},
            ]}, "duplicate product_id"),
        )
        with tempfile.TemporaryDirectory() as directory:
            route_file = Path(directory) / "product-routes.json"
            for document, message in cases:
                with self.subTest(message=message):
                    route_file.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaisesRegex(RuntimeError, message):
                        _load_routes(route_file)

    def test_new_topics_and_products_are_publicly_allowlisted(self):
        from server import mcp_contract

        expected = {
            'investment': ('669e5ee54efb02e6f96c7c9c',),
            'promotion': ('66c33eff3c0917a9a02febdd',),
            'website': ('66a0f258ce3408eb23b56706', '66bf124bf134a4c21b4fc2cb'),
            'risk': ('66bb1aa834d3cd3e43928163', '66a248f381d41651f2689d95'),
            'trademark': ('66d5b7df537c3f61d646c2dc', '671357d127ab3417e1f3f21b',
                          '6713537822fc5904b878fbcc'),
        }
        self.assertEqual(len(mcp_contract.ALL_PRODUCT_IDS), 46)
        self.assertEqual(len(mcp_contract.TOPIC_KEYS), 18)
        tools = {tool['name']: tool for tool in mcp_contract.build_tools()}
        selections = tools['portrait_plan_topics']['inputSchema']['properties']['selections']['items']['oneOf']
        by_topic = {variant['properties']['topic']['const']: variant for variant in selections}
        for topic, product_ids in expected.items():
            self.assertEqual(
                set(by_topic[topic]['properties']['product_ids']['items']['enum']),
                set(product_ids),
            )
        progress = tools['portrait_get_run']['outputSchema']['properties']['progress']['properties']
        self.assertEqual(progress['products_total']['maximum'], 46)
        self.assertEqual(progress['products_completed']['maximum'], 46)
