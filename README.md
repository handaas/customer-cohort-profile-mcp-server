# 存量客户画像数据 MCP

这个服务回答：名单中每行是哪家企业？在用户选定的客群、产品和历史期间内能查到哪些事实？每条证据如何复核？**只确认分析范围，不确认价格**。业务表单、分群、建模和 Excel/HTML 交付由 Skill 在本地完成；服务不会向名单外拓客。

## 启动

```sh
python3 -m venv mcp_env
. mcp_env/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
./start_mcp_server.sh stdio
```

Python 3.10+。默认 `PORTRAIT_LIVE=0`：可接收名单、生成本地计划和读取已发布快照；不能派发付费请求。stdio 的 stdout 专供 JSON-RPC。服务只加载本目录的 `.env`，不会借用相邻服务的账号。

HTTP：在服务端私密配置中设置 `CUSTOMER_COHORT_PROFILE_MCP_TOKEN`（至少 32 个随机 ASCII 字符），运行 `./start_mcp_server.sh streamable-http`。默认端点 `http://127.0.0.1:8000/mcp`，客户端发送 `Authorization: Bearer …`；令牌不要放 URL 或日志中。外部访问走可信 TLS 反向代理，按需设置 `PORTRAIT_ALLOWED_HOSTS` 和 `PORTRAIT_ALLOWED_ORIGINS` 精确值。每个服务实例绑定 `PORTRAIT_TENANT`，客户端不能传参数切换租户；不同客户组织应分别配置访问凭据和租户，不能共用同一个身份。

## 七个工具

| 工具 | 输入要点 | 效果 |
|---|---|---|
| `portrait_stage_roster` | `batch_key`、从 **1** 开始的 `chunk_index`、`chunk_count`、`rows`；每行 `row_id` 和企业名称／信用代码至少一项 | 本地暂存；全部分块完成才返回 `roster_id`。不收 CRM 金额、联系人、文件路径。 |
| `portrait_plan_base` | `roster_id`、`business_scope_hash`、`limits` | 估算身份查证、工商照面、企业标签、企业指标及条件补查；无上游请求。 |
| `portrait_plan_resolution` | `snapshot_id`、逐行 `decisions`、`limits` | 只接受该行已有候选或排除。需要工商核对时，冻结必要的补查范围。 |
| `portrait_plan_topics` | `snapshot_id`、`analysis_scope_hash`、`selections`、`period`、`limits` | 冻结每个专题实际主体与所选产品，不以整份名单扩张查询。 |
| `portrait_execute_plan` | 仅 `plan_id`、`plan_hash` | 启用真实查询后直接执行冻结范围。相同计划只建一次运行。 |
| `portrait_get_run` | `run_id` | 查询进度、用量、终止原因和结果快照，不触发补查。 |
| `portrait_read_page` | `snapshot_id`、`view`、可选 `cursor`、`limit` | 只读不可变快照；`identities`、`candidates`、`subjects`、`evidence`、`coverage`。游标不能跨视图、快照或租户。 |

完整的 `tools/list` 输入/输出契约可本地导出：`python server/mcp_contract.py`。结构化成功结果使用 MCP `structuredContent`；业务错误使用 `isError` 与脱敏的 `code`、`message`。不要以“空结果”代替权限、覆盖或计费错误。

### 只确认分析范围

- 从用户提交的名单开始。确切企业用工商照面核实；简称、曾用名和歧义用 **关键词查询企业**（`675cea1f0e009a9ea37edaa1`）。品牌命中不自动计入客户；用户选择候选后，工商核实成功才生成主体。
- 基础概览足够时即可结束，不必请求专题。深查前，Skill 只与用户确认业务目标、客群、企业、产品、期间和展开范围；数据 MCP 不收价目，不询价、不核算费用，也不要求另一个审批人或回执。
- `limits` 是调用次数 (`max_calls`)、企业数 (`max_targets`)、每页条数 (`max_page_size`)、页数 (`max_pages_per_view`) 和子项数 (`max_child_keys`)。专题另有历史月份范围、`observation_cutoff` 和 `max_months_per_entity`。计划返回 `executable`、`expires_at`、逐产品调用估计和上限，超过上限不追加查询。
- 在部署端设置 `PORTRAIT_LIVE=1` 并配置 `INTEGRATOR_ID`、`SECRET_ID`、`SECRET_KEY` 后，受信任的 Skill 可直接调用 `portrait_execute_plan`。例如将刚取得的 `plan` 传为 `{"plan_id": plan["plan_id"], "plan_hash": plan["plan_hash"]}`。生产服务仍按旷湖账户原约定计费，**免确认不代表接口免费**。
- 计划变更或过期要重做计划；不同租户不能互用。`billing_unknown` 表示请求已发出但结果不明，保留调用记录、不自动重发。不要向 Skill 提供上游凭证或服务数据库写权限。

### 证据和恢复

43 个数据产品的允许参数、时间和来源字段见 [product-routes.json](server/product-routes.json)。建筑合同只接受已查项目的 `_id`，餐饮门店统计只接受父记录的 `brandId`，店铺月销售只接受同一父记录、同一月份的 `platform + shopId + statsMonth`。新增对外投资、推广情况、域名信息／企业官网信息、法律诉讼风险概况／经营异常：前四个产品是当前快照，日期字段只是字段日期，不用于过滤；诉讼七类累计数各自独立、不相加，明细按各自公告/立案/发布日期进入观察窗口；经营异常按列入日至移出日（未移出为开口区间）与窗口相交纳入，缺少移出日期只表示接口未返回，不能写成已确认未移出或仍异常。没有日期筛选的原始接口不得虚构历史时点；历史事件、报告期和资料取得时间是不同概念。

客户端用 `run_id` 读取状态和结果快照。部分覆盖、真实为零、未披露、未查和失败不混记。`portrait_read_page` 只翻阅已取得的数据。重启后沿用同一 SQLite 账本，不要删除数据库来重试状态不明的请求。默认路径 `~/.local/share/handaas/customer-cohort-profile/portrait.sqlite3`，可用 `PORTRAIT_DB_PATH` 改为服务账户专用路径。数据库、WAL 和备份均包含客户数据，应独立保护。快照到期不代表部署备份已删除。

## 离线验收

在本服务目录运行 `python -m unittest discover -s tests -v`。用真实 MCP stdio/HTTP、隔离 SQLite 和本地 HTTP 测试传输验证工作流；不会连接旷湖生产 API，不读取真实账户、名单或历史验收文件。