# 企业客户画像洞察

[该 MCP 服务为客户画像提供企业身份核验、基础画像和专题数据查询。](https://www.handaas.com/) 服务只负责查数；名单、计划、进度、结果及报告由客户端 Skill 或任务系统管理。

## 主要功能

- 核验企业全称、统一社会信用代码，按关键词查找待复核候选
- 查询工商照面、企业标签、企业指标、国标行业、产品标签和企业简介
- 查询工厂、渠道、招投标、建筑、外贸、电商、知识产权、经营风险等 18 个专题
- 按业务记录 ID 查询建筑项目合同、餐饮品牌门店分布及店铺月销售

共 **46 个工具对应 46 个数据产品**（基础、身份核验及补充类 7 项，专题 39 项）。每次调用查单个固定产品的一页，不自动翻页或重试，不接收整份客户名单，不保存计划、运行状态或查询结果。

## 环境要求

- Python 3.10+
- 依赖包：`mcp>=1.26,<2`、`python-dotenv`、`httpx`、`jsonschema[format-nongpl]`、`uvicorn`，具体版本范围见 [requirements.txt](requirements.txt)
- 仅需读取服务配置和产品目录，无数据库或任务存储要求

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
| `PORTRAIT_LIVE` | `0` | `0` 允许发现工具、不查上游；`1` 允许发起单次查询，需要网关凭据，可能计费 |
| `PORTRAIT_HTTP_HOST` / `PORTRAIT_HTTP_PORT` | `127.0.0.1` / `8000` | HTTP 监听地址及端口 |
| `CUSTOMER_COHORT_PROFILE_MCP_TOKEN` | 未设置 | **HTTP 模式必填**，使用至少 32 个可打印 ASCII 字符的随机专用令牌；STDIO 不使用该令牌 |
| `PORTRAIT_ALLOWED_HOSTS` / `PORTRAIT_ALLOWED_ORIGINS` | 不添加额外允许项 | 反向代理场景填写可信 Host（需要时带端口）或 Origin（含协议与端口），以英文逗号分隔，不使用通配符 |
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

将 `{service_token}` 换成本实例的 `CUSTOMER_COHORT_PROFILE_MCP_TOKEN`，令牌仅存于私有配置或客户端密钥设置。Cherry Studio 可选择「可流式传输的 HTTP」，填入相同 URL 和请求头。修改监听端口后，同步修改客户端 URL。

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

手动启动可用 `./start_mcp_server.sh stdio`。STDIO 的标准输出专供 MCP JSON-RPC，不用于打印日志或直接显示查询结果。配套 Skill 启动 STDIO 时指定 `--query-contract v1`；旧版不接受该参数，会在启动任务前退出。普通客户端可省略。

## 使用官方Remote服务

本文档未提供已核验的官方 Remote 地址。GitHub / Gitee 为源码仓库，不是 MCP 端点；不要复用其他服务的 Remote URL 或将平台通用 token 当成本实例令牌。

如需在其他机器接入自建实例，应通过可信 TLS 反向代理提供 HTTPS 地址，沿用 Streamable HTTP 配置和 Bearer 请求头。不同客户组织使用各自的实例和访问凭据，不在 URL、日志或共享客户端配置中暴露令牌。

## 可用工具

客户端先用 `tools/list` 发现工具，以工具的参数说明、必填项、默认值和枚举为准。机器可读的产品、输入参数及来源字段见 [产品目录](server/product-routes.json)。不能自选 `product_id`、网关地址或鉴权参数。

- 企业标识查询通常使用 `matchKeyword`；缺省 `keywordType` 时填企业全称。如果用企业 ID、注册号或统一社会信用代码，明确指定对应枚举（以工具为准）。关键词查询只返回候选，不接受 `keywordType`，不要自动认定第一条。上市主营构成另外支持 `reportStockCode`。
- 分页工具支持 `pageIndex` / `pageSize`，页码从 1 开始。不要把一页认作全量，也不要把 `total` 当成已取得记录数。可选字段不使用时省略，不发 `null`。
- 建筑合同使用建筑工程项目明细返回的 `data.resultList[]._id`；餐饮品牌门店统计用企业旗下餐饮品牌门店返回的 `data.storeList[].brandId`。两者都用 `matchKeyword`，不能传企业 ID 或产品 ID。
- 企业店铺明细需要 `statsMonth`（`YYYYMM` 字符串）；店铺月销售的 `platform`、`shopId`、`statsMonth` 必须来自同一条店铺记录。工商年报和社保的 `arYear` 用四位年份字符串；不传时分别按来源已有年度、最新一年处理，不能补零。
- 成功返回 `{"data": {...}}`，优先读取 `structuredContent`，也可解析 `content[0].text`。结果只含已声明业务字段，保留上游数值、零和比例精度，不是原始网关信封。`{"data": {}}` 或明确空列表是已返回的空结果；未返回字段和真实零不同。

| 工具 | 上游产品 | 分类 | 分页 |
| --- | --- | --- | --- |
| `portrait_query_enterprise_base` | 工商照面 | 基础画像 | — |
| `portrait_query_enterprise_tags` | 企业标签 | 基础画像 | — |
| `portrait_query_enterprise_metrics` | 企业指标 | 基础画像 | — |
| `portrait_query_enterprise_search` | 关键词查询企业 | 身份核验 | 每页最多 50 条 |
| `portrait_query_enterprise_industry` | 企业行业（国标） | 基础补充 | — |
| `portrait_query_enterprise_product_tags` | 企业产品标签 | 基础补充 | — |
| `portrait_query_enterprise_summary` | 企业简介 | 基础补充 | — |
| `portrait_query_factory_info` | 工厂信息 | 工厂 | — |
| `portrait_query_factory_products` | 工厂产品统计 | 工厂 | — |
| `portrait_query_enterprise_channels` | 企业渠道信息 | 渠道 | — |
| `portrait_query_enterprise_bids` | 企业招投标信息 | 招投标与经营关系 | 每页最多 50 条 |
| `portrait_query_enterprise_business_relations` | 企业经营关系图谱 | 招投标与经营关系 | — |
| `portrait_query_construction_projects` | 建筑工程项目明细 | 建筑 | 每页最多 50 条 |
| `portrait_query_construction_contracts` | 建筑业项目合同信息 | 建筑 | 每页最多 50 条 |
| `portrait_query_trade_details` | 企业外贸数据明细 | 外贸 | 每页最多 50 条 |
| `portrait_query_trade_destinations` | 企业外贸订单地区 | 外贸 | 每页最多 50 条 |
| `portrait_query_trade_export_trends` | 企业外贸出口趋势 | 外贸 | — |
| `portrait_query_restaurant_brands` | 企业旗下餐饮品牌门店 | 餐饮 | — |
| `portrait_query_restaurant_brand_locations` | 餐饮品牌门店统计 | 餐饮 | 每页最多 50 条 |
| `portrait_query_enterprise_estore_overview` | 企业国内外网店概况 | 电商 | — |
| `portrait_query_enterprise_estore_shops` | 企业店铺明细 | 电商 | — |
| `portrait_query_estore_monthly_sales` | 店铺月销售情况 | 电商 | — |
| `portrait_query_software_copyrights` | 软著明细 | 技术与资质 | 每页最多 50 条 |
| `portrait_query_patent_statistics` | 专利统计 | 技术与资质 | — |
| `portrait_query_industry_standards` | 行业标准信息 | 技术与资质 | 每页最多 50 条 |
| `portrait_query_qualification_certificates` | 资质证书明细 | 技术与资质 | 每页最多 50 条 |
| `portrait_query_office_statistics` | 办公地址统计 | 办公 | — |
| `portrait_query_office_details` | 办公地址明细 | 办公 | 每页最多 10 条 |
| `portrait_query_parent_enterprise` | 企业所属总公司查询 | 组织 | — |
| `portrait_query_enterprise_branches` | 分支机构 | 组织 | 每页最多 50 条 |
| `portrait_query_enterprise_scale` | 企业经营规模 | 规模用工 | — |
| `portrait_query_annual_reports` | 工商年报 | 规模用工 | — |
| `portrait_query_social_insurance` | 社保信息 | 规模用工 | — |
| `portrait_query_recruitment_details` | 招聘明细 | 经营变化 | 每页最多 50 条 |
| `portrait_query_enterprise_operating_strength` | 经营实力 | 经营变化 | — |
| `portrait_query_enterprise_financing` | 融资信息 | 经营变化 | — |
| `portrait_query_listed_business_segments` | 上市公司主营构成分析表详情 | 上市业务 | 每页最多 10 条 |
| `portrait_query_enterprise_investments` | 对外投资 | 对外投资 | 每页最多 50 条 |
| `portrait_query_enterprise_promotion` | 推广情况 | 推广 | — |
| `portrait_query_enterprise_domains` | 域名信息 | 官网和域名 | 每页最多 50 条 |
| `portrait_query_enterprise_website` | 企业官网信息 | 官网和域名 | — |
| `portrait_query_enterprise_litigation` | 法律诉讼风险概况 | 法律与经营风险 | — |
| `portrait_query_enterprise_operating_anomalies` | 经营异常 | 法律与经营风险 | — |
| `portrait_query_trademark_statistics` | 商标统计 | 商标 | — |
| `portrait_query_trademark_overview` | 商标概况 | 商标 | — |
| `portrait_query_trademark_details` | 商标明细 | 商标 | 每页最多 50 条 |

## 使用注意事项

- 默认关闭真实查询，仅可发现工具。启用后每次查询最多向上游发起一次请求，可能扣费；翻页和重新提交都是新调用，不自动重试。
- `isError=true` 时查看脱敏的 `code/message`，不能当作零或空结果。常见错误：`invalid_argument`（参数不符）、`live_disabled`（未启用真查）、`configuration_error`（服务配置问题）、`upstream_denied`（权限拒绝）、`upstream_unavailable`（未取得结果）、`upstream_rejected`（上游明确拒绝）。`billing_unknown` 表示超时、中断或结果格式异常造成的计费不确定，不自动重发。
- 当前快照和累计量不能替代历史期间。没有期间过滤的产品，只能按返回记录日期另行判断，不保证历史覆盖。
- 客户端如需处理名单、确认范围、追踪任务、去重及交付报告，使用配套的 `customer-cohort-profile-skill`。旧版 7 个工作流工具已移除，服务不再使用旧账本、不迁移或续跑旧任务；保留旧数据库和归档，已保存报告仍可离线查看。请为新查询使用新批次，不用新批次绕过旧请求的未知计费状态。

## 使用提问示例

### portrait_query_enterprise_search（寻找待核验企业）

> 用这个简称查第 1 页候选，每页最多 10 条。列出候选，不选第一条作为确权结果。

### portrait_query_enterprise_base（核验企业登记信息）

> 按我提供的统一社会信用代码核验这家企业的名称、登记行业和成立日期，只查工商照面。

### portrait_query_construction_contracts（查询指定建筑项目合同）

> 用已核实项目的 `_id` 查第 1 页建筑业项目合同。区分签约日期和登记日期，不把一页当成全量。

### portrait_query_estore_monthly_sales（查询已核实店铺的某月销售）

> 用店铺明细同一条记录里的平台、店铺 ID 和统计月份查月销售，保留来源单位和同比精度，不把店铺销售额当成企业总营收。

## 测试验证

在项目目录运行：

```bash
python -m py_compile server/*.py
python -m unittest discover -s tests -v
```

离线测试使用合成参数与 MockTransport，检查 46 个产品各自的签名请求次数、输入约束、空值与未知错误、父子记录业务键、响应字段筛选和 STDIO / Streamable HTTP。测试不访问真实旷湖上游，不能证明线上订阅权限或计费。
