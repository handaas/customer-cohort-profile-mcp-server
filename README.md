# 存量客户画像数据 MCP

[该 MCP 服务提供企业身份核验、基础画像、专题取数计划和证据查询，帮助用户了解已有客户，并复核画像结论来自哪些数据。](https://www.handaas.com/) 服务只处理用户提供的名单，不向名单外拓客；业务表单、客户分群、建模及 Excel / HTML 报告由客户端 Skill 在本地完成。

## 主要功能

- 📋 分块暂存企业名单，只接收核验身份所需的字段
- 🔍 核验企业全称和统一社会信用代码，处理简称、曾用名及同名歧义
- 🏢 规划工商照面、企业标签和企业指标等基础画像查询
- 📊 按客群、企业、产品及观察期间规划专题查询，覆盖对外投资、商标、企业官网和经营风险等主题
- 🧾 跟踪执行进度和调用用量，分页读取来源、业务时间及数据覆盖情况

服务提供 **7 个 MCP 工具**，使用 **46 个数据产品**（7 个基础、补充及身份核验产品，39 个产品分属 18 个专题）。产品和字段清单见 [product-routes.json](server/product-routes.json)。

## 环境要求

- Python 3.10+
- 依赖包：`mcp>=1.26,<2`、`python-dotenv`、`httpx`、`jsonschema[format-nongpl]`、`uvicorn`，具体版本范围见 [requirements.txt](requirements.txt)
- 运行账户需能读写自己的 SQLite 数据目录

## 本地快速启动

### 1. 克隆项目

GitHub（公开仓库）：

```bash
git clone https://github.com/handaas/customer-cohort-profile-mcp-server.git
cd customer-cohort-profile-mcp-server
```

也可从 [Gitee 公开镜像](https://gitee.com/handaas/customer-cohort-profile-mcp-server) 克隆。已在 HandaaS 工作区时，直接进入已有的服务目录即可。

### 2. 创建虚拟环境&安装依赖

```bash
python3 -m venv mcp_env
source mcp_env/bin/activate
python -m pip install -r requirements.txt
```

### 3. 环境配置

首次配置时复制模板（已有 `.env` 不覆盖）：

```bash
test -e .env || cp .env.example .env
```

| 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `PORTRAIT_LIVE` | `0` | `0` 仅暂存名单、生成计划及读取已有结果，不派发真实请求；`1` 允许执行符合条件的取数计划，可能计费 |
| `PORTRAIT_TENANT` | `local` | 服务实例绑定的租户；客户组织之间使用不同实例和访问凭据 |
| `PORTRAIT_DB_PATH` | `~/.local/share/handaas/customer-cohort-profile/portrait.sqlite3` | 名单、计划、运行记录及快照的存储路径 |
| `PORTRAIT_PLAN_TTL_SECONDS` / `PORTRAIT_SNAPSHOT_TTL_SECONDS` | `900` / `604800` | 计划及快照有效期，单位为秒 |
| `PORTRAIT_HTTP_HOST` / `PORTRAIT_HTTP_PORT` | `127.0.0.1` / `8000` | HTTP 监听地址及端口 |
| `CUSTOMER_COHORT_PROFILE_MCP_TOKEN` | 未设置 | **HTTP 模式必填**，使用至少 32 个可打印 ASCII 字符的随机专用令牌；STDIO 不使用该令牌 |
| `PORTRAIT_ALLOWED_HOSTS` / `PORTRAIT_ALLOWED_ORIGINS` | 不添加额外允许项 | 反向代理场景按需加入明确可信的值，以英文逗号分隔，不使用通配符 |
| `INTEGRATOR_ID` / `SECRET_ID` / `SECRET_KEY` | 未设置 | HandaaS 网关凭据，仅在启用真实取数时填写 |

服务只自动加载本目录的 `.env`，同名进程环境变量优先。不借用相邻服务的账号。完整配置见 [`.env.example`](.env.example)。

### 4. streamable-http启动服务

先在服务端配置 `CUSTOMER_COHORT_PROFILE_MCP_TOKEN`，再启动：

```bash
./start_mcp_server.sh streamable-http
```

也可直接运行：

```bash
python server/mcp_server.py streamable-http
```

默认端点：`http://127.0.0.1:8000/mcp`。HTTP 请求必须携带 `Authorization: Bearer <token>`，不要将令牌附在 URL 上。服务支持 `stdio` 和 `streamable-http`，不支持 SSE；直接运行或使用启动脚本不传参数时均默认使用 `stdio`。

### 5. Cursor / Cherry Studio MCP配置

服务启动后，在客户端配置 Streamable HTTP 连接：

```json
{
  "mcpServers": {
    "customer-cohort-profile-mcp-server": {
      "url": "http://127.0.0.1:8000/mcp",
      "headers": {
        "Authorization": "Bearer {service_token}"
      }
    }
  }
}
```

将 `{service_token}` 换成本实例的 `CUSTOMER_COHORT_PROFILE_MCP_TOKEN`，令牌仅存于私有配置或客户端密钥设置。Cherry Studio 可选择「可流式传输的 HTTP」，填入相同 URL 和请求头。修改监听端口后，同步修改客户端 URL。配置方法参考 [Cursor MCP 文档](https://cursor.com/docs/mcp) 和 [Cherry Studio MCP 文档](https://github.com/CherryHQ/cherry-studio-website/blob/main/docs-site/content/advanced-basic/extensions/mcp/README.md)。

## STDIO版安装部署

### 设置Cursor / Cherry Studio MCP配置

先按本地快速启动的第 1–3 步安装，再将以下两处 `{workdir}` 换成服务目录的绝对路径。STDIO 由客户端启动子进程，无需先跑 HTTP 服务或配置 HTTP 令牌：

```json
{
  "mcpServers": {
    "customer-cohort-profile-mcp-server": {
      "command": "{workdir}/mcp_env/bin/python",
      "args": ["{workdir}/server/mcp_server.py", "stdio"]
    }
  }
}
```

手动启动可用 `./start_mcp_server.sh stdio`。STDIO 的标准输出专供 MCP JSON-RPC，不用于打印日志或直接显示查询结果。

## 使用官方Remote服务

本文档未提供已核验的官方 Remote 地址。GitHub / Gitee 为源码仓库，不是 MCP 端点；不要复用其他服务的 Remote URL 或将平台通用 token 当成本实例令牌。

如需在其他机器接入自建实例，应通过可信 TLS 反向代理提供 HTTPS 地址，沿用 Streamable HTTP 配置和 Bearer 请求头。为不同客户组织分别配置租户、实例和访问凭据，不在 URL、日志或共享客户端配置中暴露令牌。

## 可用工具

建议调用顺序：**暂存名单 → 生成并执行基础计划 → 读取身份与候选 → 按需复核候选或生成专题计划 → 执行所需计划并读取证据**。生成计划不调用上游；每次执行前先核对用户确认的范围。

成功结果优先读取 MCP 的 `structuredContent`，也可解析 `content[0].text` 中的 JSON。若 `isError=true`，读取脱敏后的 `code`、`message`，不要当作空结果；工具调用成功后，也要检查运行的 `state` 和 `error`。完整输入、输出和枚举以运行时 `tools/list` 为准，可用 `python server/mcp_contract.py` 离线导出。

### 1. portrait_stage_roster
**功能**: 分块暂存企业名单，不查询上游。

**参数**:
- `batch_key` (必需): 同一批名单的唯一批次键，格式为 `batch_` 加 8–64 位字母、数字、下划线或连字符；各分块使用同一个键。
- `chunk_index` (必需): 分块序号，从 1 开始，不大于 `chunk_count`。
- `chunk_count` (必需): 分块总数，1–1000。
- `rows` (必需): 每块 1–500 行。每行必须有 `row_id`，并至少提供 `enterprise_name` 或 `social_credit_code` 之一；可附 `source_row_number`。不传联系人、CRM 金额、备注或文件路径。

**返回值**:
- `status`: `partial` 为尚有缺块，`complete` 为名单暂存完成。
- `accepted_rows`、`duplicate_rows`、`rejected_rows`、`rejected`: 分块的处理统计和被拒原因。
- `next_chunk_index` / `roster_id`: 未完成时取前者；仅完成后才返回可用于计划的 `roster_id`。同一分块原样重传不会创建第二份名单。

### 2. portrait_plan_base
**功能**: 根据完整名单生成身份核验及基础画像计划，估算必要的调用量，不执行取数。

**参数**:
- `roster_id` (必需): 名单暂存完成后返回的 ID。
- `business_scope_hash` (必需): 由 Skill / 调用方根据已确定的业务范围生成，格式为 `sha256:` 加 64 位小写十六进制字符；不传原始 CRM 文本。
- `limits` (必需): 调用量、主体和分页等硬上限，见「使用注意事项」。

**返回值**:
- `plan_id`、`plan_hash`: 对应同一计划的标识和摘要，执行时原样回传。
- `scope`、`estimate`: 冻结的范围及逐产品调用估计，不是报价或账单。
- `executable`、`expires_at`、`blockers`: 可执行性、有效期和阻断项。

### 3. portrait_plan_resolution
**功能**: 为身份未确认的名单行选择已有候选或排除，生成必要的复核计划，不自动选择第一个候选。

**参数**:
- `snapshot_id` (必需): 含该名单行及其候选记录的已有快照 ID。
- `decisions` (必需): 1–2000 条行级决定，每个 `row_id` 只能出现一次。选择时用 `action=select_candidate` 和该行的 `candidate_id`；排除时用 `action=exclude` 与 `reason_code`，可附 `note`。原因只接受 `not_a_customer`、`duplicate_subject`、`no_reliable_match`、`out_of_scope` 或 `user_excluded`。未处理的行保持未知。
- `limits` (必需): 本计划的硬上限；不能用来追加名单外企业。

**返回值**:
- `plan_id`、`plan_hash`: 候选复核计划引用。
- `scope`、`estimate`、`executable`、`expires_at`、`blockers`: 复核范围、调用估计及执行条件。选择候选不代表身份已经核验成功。

### 4. portrait_plan_topics
**功能**: 基于已核验主体生成专题取数计划，不把一个客群或专题的范围扩展到全名单。

**参数**:
- `snapshot_id` (必需): 含已核验主体的当前快照 ID；主体集合变更后应重新生成专题计划。
- `analysis_scope_hash` (必需): 已确定的分析范围摘要，格式为 `sha256:` 加 64 位小写十六进制字符。
- `selections` (必需): 1–100 组选择，每组包含 `cohort_key`、`topic`、`subject_ids`、`product_ids`。主体取自当前快照的 `subjects`；产品只选本专题所需的子集。相同主体的重复查询会去重，客群归属仍保留。
- `period` (必需): `start_month`、`end_month` 用 `YYYY-MM`，起月不晚于止月；`observation_cutoff` 用 `YYYY-MM-DD`；`max_months_per_entity` 为 1–120。没有历史期间口径的产品仍按快照处理。
- `limits` (必需): 本次查询的硬上限，按任务填入最小必要值。

**返回值**:
- `plan_id`、`plan_hash`: 冻结专题计划的引用。
- `scope`、`period`、`estimate`: 专题选择、观察期间及调用估计。
- `executable`、`expires_at`、`blockers`: 执行前需检查的状态及有效期。

### 5. portrait_execute_plan
**功能**: 在后台执行已有计划。它是唯一可能触发真实上游取数和计费的工具。

**参数**:
- `plan_id` (必需): 取自生成计划工具的返回值，计划必须可执行、未过期且属于当前租户。
- `plan_hash` (必需): 与同一个 `plan_id` 对应的原始返回值。不要传 `confirmed`、追加企业、改期间或指定其他产品；需要改变范围时重新规划。

**返回值**:
- `run_id`、`state`: 运行 ID 和当前状态；返回不代表取数已完成。用 `portrait_get_run` 查询进度。
- `progress`、`usage`: 已处理的主体、产品及调用量等，不是费用金额。
- `result` / `error`: 结果快照引用或运行错误（按状态返回）。同一计划和摘要重复调用复用已有运行，不以重试为由新增一轮取数。

### 6. portrait_get_run
**功能**: 查询已有运行的状态、用量和结果，不发起补查。

**参数**:
- `run_id` (必需): 执行计划时返回的运行 ID。

**返回值**:
- `state`、`is_terminal`、`is_resumable`: 当前状态及后续处理条件。`queued` / `running` 表示进行中；`partial` 表示已有部分结果且原计划不再继续派发，不要只看 `is_terminal` 循环等待。
- `progress`、`usage.calls_used`、`usage.max_calls`: 进度、已用调用次数及上限。
- `result.snapshot_id`、`result.views_available`: 有结果时返回可读快照及可选视图。出现 `error` 时保留原因，不将失败写成零结果。

### 7. portrait_read_page
**功能**: 分页读取已发布快照，不触发上游查询。

**参数**:
- `snapshot_id` (必需): 用运行结果中的 `result.snapshot_id`，不要传 `run_id` 或计划 ID。
- `view` (必需): `identities`（名单核验）、`candidates`（歧义候选）、`subjects`（已核验主体）、`evidence`（证据）、`coverage`（覆盖情况）之一。
- `cursor` (可选): 首次省略；后续原样回传同一租户、同一快照、同一视图的 `next_cursor`，不要拼接、改写或跨视图复用。
- `limit` (可选，默认 `50`): 单页记录上限，1–200；不改变计划的上游查询范围。

**返回值**:
- `records`、`returned`: 当前页记录和实际条数。证据应保留来源、原始值、数据状态、业务时间及取得时间。
- `has_more`、`next_cursor`: 有下一页时返回游标；没有更多记录不代表上游全覆盖，应结合 `coverage` 视图解读。

## 使用注意事项

1. **先核对名单主体**：确切名称或信用代码走工商核实；简称、曾用名和歧义用关键词找候选。品牌命中不能自动计入客户，候选也必须通过工商核实才作为主体。
2. **只确认分析范围，不确认价格**：由 Skill 与用户确认业务目标、客群、企业、产品、期间及展开范围。基础结果已够用时无需生成专题计划。服务不接收价目、不报价、无需额外的价格确认或审批回执；启用真实取数后，仍按 HandaaS 账户原约定计费。
3. **计划上限必须完整**：`limits` 的五项均必填：`max_targets` / `max_calls` 各 1–100000，`max_page_size` 1–200，`max_pages_per_view` / `max_child_keys` 各 1–10000。服务会按各产品限制进一步收紧，每个计划不会自动增加上限。
4. **只用已取得的业务 ID**：建筑合同使用父记录的项目 `_id`；餐饮门店使用来源记录的 `brandId`；店铺月销售使用同一父记录、同一月份的 `platform + shopId + statsMonth`。不得从关键词猜测标识或跨名单复用。
5. **按证据口径解读**：对外投资、推广、域名、企业官网和商标资料是快照，不同于历史时点或经营结果。诉讼七类累计数独立，不相加；经营异常按列入至移出日期与观察窗口是否重叠筛选。未返回移出日期时按无结束日期筛选，但不能据此确认仍在异常名录。部分覆盖、真实为零、未披露、未查和失败分别保留。
6. **不要自动重发不明请求**：`billing_unknown` 表示已派发但结果不明，应保留运行记录，不自动重发。计划过期需重新规划；快照过期不会自动取数。不要靠删除数据库或跨租户复用快照来补查。
7. **保护凭据和客户名单**：不要把网关凭据发给 Skill，也不要给它数据库写权限。SQLite 数据库、WAL 和备份可能含客户数据，应分别保护；快照过期不等于备份已删除。不要提交 `.env`、运行数据或真实业务响应。

## 使用提问示例

### portrait_stage_roster (暂存客户名单)
1. 把我提供的名单按块暂存，只提交行标识、企业名称和统一社会信用代码，不查上游。
2. 检查名单是否还有缺块或被拒的行；完整后再使用返回的名单 ID。

### portrait_plan_base (基础画像计划)
1. 根据这份已暂存名单，规划身份核验和基础画像查询，先只列出范围与预计调用次数。
2. 只为这份完整名单生成基础计划；如果超出上限，先说明阻断项，不自动缩减名单。

### portrait_plan_resolution (候选复核计划)
1. 用快照中第 3 行已有的候选编号生成复核计划，不要把第一个候选默认当成该客户。
2. 按我确认的原因排除重复行，其余未确认的行继续标为未知。

### portrait_plan_topics (专题取数计划)
1. 只为已核验的制造业客户规划 2025 年经营风险查询，写明所选产品、观察截止日期和调用上限。
2. 只为我指定的客群规划商标统计和商标明细；商标记录按资料快照展示，不写成历史经营指标。

### portrait_execute_plan (执行已确认范围的计划)
1. 按刚才核对过的范围执行基础计划，原样使用它的计划 ID 和摘要，不追加企业。
2. 如果计划已过期或上游未启用，告诉我原因，不自动发起其他查询。

### portrait_get_run (查看取数进度)
1. 查询这次运行处理了多少主体、用了多少次调用；暂时不要发起补查。
2. 如果只完成了部分查询，返回已有的快照和覆盖限制，不把缺失写成零。

### portrait_read_page (读取证据快照)
1. 分页读取本次运行的证据，按企业列出来源、原始值和取得时间。
2. 只读取覆盖视图，区分未查、未披露及查询失败，不为填齐报告而重新取数。

## 测试验证

在本服务目录运行：

```bash
python -m py_compile server/mcp_server.py
python -m unittest discover -s tests -v
```

离线测试覆盖 MCP STDIO / Streamable HTTP、工具契约、名单分块及候选复核、调用上限与去重、结果快照和失败处理。测试使用隔离 SQLite 和模拟上游数据，不连接 HandaaS 生产接口，不读取真实账户、客户名单或历史验收数据。
