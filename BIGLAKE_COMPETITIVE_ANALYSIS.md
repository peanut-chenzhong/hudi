# Google Cloud BigLake / Lakehouse 竞品洞察分析报告

## 1. 报告说明

本文基于 Google Cloud 当前官方公开资料整理，重点分析 Google Cloud BigLake 及其演进形态 **Lakehouse for Apache Iceberg (formerly BigLake)**。

主要依据：

- [Lakehouse for Apache Iceberg / formerly BigLake](https://cloud.google.com/products/lakehouse)
- [Introduction to BigLake external tables](https://cloud.google.com/bigquery/docs/biglake-intro)
- [Apache Iceberg managed tables](https://cloud.google.com/bigquery/docs/iceberg-tables)
- [BigQuery pricing](https://cloud.google.com/bigquery/pricing)
- [Introduction to data governance in BigQuery](https://cloud.google.com/bigquery/docs/data-governance)
- [Knowledge Catalog (formerly Dataplex)](https://cloud.google.com/dataplex)

## 2. 产品定位

Google Cloud 对 BigLake 的最新定位已经从“BigQuery 外部表能力”升级为开放湖仓产品线。官方当前主推名称是 **Lakehouse for Apache Iceberg (formerly BigLake)**，定位为：

> Open, cross-cloud lakehouse for the agentic era.

从产品形态看，可以分为两层：

| 层次 | 产品形态 | 核心定位 |
| --- | --- | --- |
| BigLake external tables | 查询 Cloud Storage、Amazon S3、Azure Blob 上的外部数据 | 通过 BigQuery 统一 SQL 与安全模型访问外部数据 |
| Lakehouse for Apache Iceberg / Iceberg managed tables | 托管 Iceberg 表，数据存储在用户 Cloud Storage bucket | 面向开放 lakehouse、多引擎互操作、实时分析与 AI 工作流 |

Google 的战略不是只做“外部数据查询”，而是把 Iceberg、BigQuery、Spark、治理、安全和 AI 统一为企业级开放湖仓底座。

## 3. 产品架构

### 3.1 总体架构

```text
                 ┌──────────────────────────────────────┐
                 │ BigQuery / GoogleSQL / BigQuery AI   │
                 └──────────────────┬───────────────────┘
                                    │
     ┌──────────────────────────────┼──────────────────────────────┐
     │                              │                              │
┌────▼─────┐                 ┌──────▼──────┐                ┌──────▼──────┐
│ Spark    │                 │ Flink/Trino │                │ 3P Engines  │
│ Managed  │                 │ OSS Engines │                │ Snowflake/DB│
└────┬─────┘                 └──────┬──────┘                └──────┬──────┘
     │                              │                              │
     └──────────────┬───────────────┴───────────────┬──────────────┘
                    │ Lakehouse REST Catalog /       │
                    │ BigLake Metastore              │
                    └───────────────┬────────────────┘
                                    │
                 ┌──────────────────▼───────────────────┐
                 │ Apache Iceberg metadata / snapshots  │
                 └──────────────────┬───────────────────┘
                                    │
      ┌─────────────────────────────┼─────────────────────────────┐
      │                             │                             │
┌─────▼─────┐               ┌───────▼───────┐             ┌───────▼───────┐
│ GCS       │               │ Amazon S3      │             │ Azure Blob    │
│ Parquet   │               │ BigQuery Omni  │             │ BigQuery Omni │
└───────────┘               └───────────────┘             └───────────────┘
```

### 3.2 架构关键点

- **存算分离**：数据存储在 Cloud Storage、Amazon S3、Azure Blob 等对象存储，计算由 BigQuery、Spark、Flink、Trino 等引擎完成。
- **统一元数据层**：Lakehouse REST Catalog / BigLake metastore 作为 Iceberg 表的中心元数据服务。
- **多引擎互操作**：官方强调 BigQuery、Managed Service for Apache Spark、Spark、Trino、Flink 可读写同一 Iceberg 表；Snowflake、Databricks 互操作处于 Preview。
- **统一安全治理**：通过 BigQuery IAM、连接服务账号、Knowledge Catalog、行列级权限、数据脱敏等能力统一管控。
- **自动表维护**：Iceberg managed tables 支持 adaptive file sizing、automatic clustering、garbage collection、metadata optimization。

## 4. 核心能力

### 4.1 BigLake External Tables

BigLake external tables 支持通过 BigQuery 查询外部数据源：

- Cloud Storage
- Amazon S3
- Azure Blob Storage

其核心机制是 **access delegation**。用户访问 BigLake table，底层对象存储访问由 external connection 关联的 service account 完成。这样可以把“数据分析权限”和“对象存储权限”解耦，便于在 BigQuery 表层执行统一安全策略。

官方文档明确说明 BigLake tables 支持表级细粒度安全，包括 row-level security、column-level security；Cloud Storage BigLake tables 还支持 dynamic data masking。

### 4.2 Apache Iceberg Managed Tables

Apache Iceberg managed tables 是 Google 当前重点投入方向。官方说明其“formerly BigLake tables for Apache Iceberg in BigQuery”，提供类似标准 BigQuery 表的全托管体验，但数据存储在用户自己的 Cloud Storage bucket 中。

核心能力包括：

- 使用 GoogleSQL DML 修改表数据
- 通过 BigQuery Storage Write API 支持批流统一和高吞吐流式写入
- 导出 Iceberg V2 snapshot，并在表变更后自动刷新
- Schema evolution
- Time travel
- Column-level security
- Data masking
- Automatic storage optimization
- Multi-statement transactions（Preview）
- Table partitioning（Preview）

## 5. 产品优势

### 5.1 BigQuery 原生体验 + 开放格式

BigLake/Iceberg managed tables 的最大优势是：用户获得 BigQuery 的 Serverless、SQL、治理和 AI 能力，同时保留 Iceberg/Parquet 开放格式和用户自有 bucket。

这比传统数据湖产品更强：它不是要求所有数据进入封闭仓库，而是把开放格式接入 BigQuery 的性能、安全和 AI 体系。

### 5.2 多云与跨云访问

BigLake external tables 支持 Cloud Storage、S3、Azure Blob，并通过 BigQuery Omni 支持跨云分析。官方说明 cross-cloud join 可以跨 Google Cloud 和 Omni regions 运行，并尽量只传输查询引用的列和行，而非整表复制。

### 5.3 自动优化降低湖仓运维成本

Iceberg managed tables 自动执行：

- adaptive file sizing
- automatic clustering
- garbage collection
- metadata optimization

这直击 Iceberg/Hudi/Delta 在企业落地中的核心痛点：小文件、元数据膨胀、compaction、clustering、过期数据清理等运维复杂度。

### 5.4 AI 与 Agentic Data Platform 绑定

Google 当前产品页大量强调 “agentic era”“AI agents”“Gemini Enterprise”“BigQuery AI”“ObjectRefs”。这说明 BigLake/Lakehouse 不只是数据湖仓产品，而是 Google AI 数据底座的一部分。

竞品启示：Google 正在把 lakehouse 从“分析基础设施”升级为“AI agent 可治理上下文层”。

## 6. 高光特性

### 6.1 Lakehouse REST Catalog

官方将 Lakehouse REST Catalog 定义为 Iceberg tables 的 central hub，提供跨 BigQuery、Managed Spark、OSS engines、partners 的 universal read/write access。

这相当于把 Iceberg catalog 服务产品化，降低客户自建 Hive Metastore、Nessie、Polaris 等 catalog 的复杂度。

### 6.2 BigQuery + Iceberg Streaming

Iceberg managed tables 支持通过 BigQuery Storage Write API 做统一 batch 和 high throughput streaming，解决传统湖仓“写入后可见性延迟高”的问题。

### 6.3 自动存储优化

BigQuery 自动对 Iceberg managed tables 做文件大小、聚簇、GC、元数据优化。对竞品来说，这是非常强的托管化差异点。

### 6.4 Knowledge Catalog 深度治理

Knowledge Catalog 提供：

- metadata harvesting
- semantic search
- business glossary
- lineage
- profiling
- data quality
- policy-based governance

官方还强调它能跨 Google 和 partner data platforms 汇聚上下文，为 AI agents 提供安全检索。

### 6.5 跨云 Iceberg 与第三方引擎互操作

Google 产品页提到 Spark、Trino、Flink，以及 Snowflake、Databricks 互操作（Preview）。这代表 Google 试图用 Iceberg 标准打穿封闭数据平台边界。

## 7. 成本模型

### 7.1 Lakehouse / BigLake 成本

官方 Lakehouse 产品页给出的 BigLake/Lakehouse 计费项包括：

| 成本项 | 官方口径 | 公开价格起点 |
| --- | --- | --- |
| Table management | 自动表存储优化使用的 compute resources | $0.12 / DCU-Hour |
| Metadata storage | Lakehouse runtime catalog 元数据存储 | $0.04 / GiB / month，含每月 1 GiB free tier |
| Metadata access Class A | writes、updates、list、create、config 等操作 | $6.00 / million operations，含每月 5,000 operations free tier |
| Metadata access Class B | reads、get、delete 等操作 | $0.90 / million operations，含每月 50,000 operations free tier |

### 7.2 BigQuery 查询成本

BigQuery 计算成本有两类：

- On-demand：按查询扫描数据量计费
- Capacity pricing：按 slot-hour 计费

官方价格页显示 on-demand 查询：每月前 1 TiB 免费，超出后示例区域价格为 $6.25 / TiB。

### 7.3 外部存储与跨云成本

需要额外关注：

- Iceberg managed tables 的数据存储在用户 Cloud Storage bucket，Cloud Storage 成本另算。
- Cross-cloud join 可能产生数据传输成本。
- BigLake external table 文档说明：跨云 join 会把 remote part 转成 CTAS 临时表，成功传输即计费。
- Cross-cloud join 每次 transfer 有 60 GB 限制。

## 8. 治理能力

BigLake/Lakehouse 治理主要依托 BigQuery governance 与 Knowledge Catalog。

### 8.1 元数据治理

Knowledge Catalog 是集中式数据资产 inventory，保存 business、technical、operational metadata，并通过 AI/ML 发现元数据关系和语义。

能力包括：

- 数据发现
- 元数据采集
- 业务术语表
- 数据画像
- 数据质量
- 数据血缘
- 语义搜索

### 8.2 多引擎治理

官方 Knowledge Catalog 页面强调其与 Google Cloud Lakehouse 深度集成，可以跨 BigQuery 和 Managed Spark 等多引擎统一治理策略。

这对开放湖仓非常关键，因为 Iceberg 的痛点往往不是格式，而是多引擎读写后的权限、血缘、质量、审计一致性。

## 9. 安全能力

### 9.1 Access Delegation

BigLake external tables 的关键安全机制是 access delegation。用户访问的是 BigLake table，底层对象存储访问由 external connection 对应的 service account 完成。

价值：

- 用户无需直接持有 bucket 权限
- 可以在 BigQuery 表层执行统一权限策略

### 9.2 细粒度权限

官方明确支持：

- IAM
- Row-level security
- Column-level security
- Dynamic data masking
- Audit logs
- VPC Service Controls
- Encryption at rest and in transit

### 9.3 Iceberg Managed Tables 权限模型

创建 Iceberg managed tables 需要：

- BigQuery Data Owner
- BigQuery Connection Admin

查询需要：

- BigQuery Data Viewer
- BigQuery User

连接服务账号需要 Cloud Storage 权限：

- Storage Object User
- Storage Legacy Bucket Reader

这说明 Google 的安全模型是 “BigQuery 权限 + Connection Service Account + Storage IAM” 的组合。

## 10. 限制与风险

### 10.1 Managed Iceberg 的写入边界

官方 Iceberg managed tables 文档明确警告：不要直接在 bucket 中外部添加、修改、替换文件，否则可能造成数据丢失或表不可读。

这意味着 Iceberg managed tables 虽然是开放格式，但在 managed 模式下，Google 希望 BigQuery 成为主控写入方。对真正多引擎写入场景，这是重要边界。

### 10.2 Cross-cloud join 限制明显

BigLake external tables 的跨云 join 有明显限制：

- 不支持 BigQuery free tier / sandbox
- transfer size 每次限制 60 GB
- 某些聚合无法下推
- 临时表不复用
- 成功传输即计费

因此它适合“跨云按需分析”，不一定适合高频大规模跨云 join。

### 10.3 Preview 能力较多

当前产品页中一些高光能力仍处于 Preview，例如：

- Snowflake / Databricks 互操作
- cross-cloud interconnect and caching
- REST catalog 的部分自动优化能力
- multi-statement transactions
- table partitioning

竞品分析时需要区分 GA 与 Preview，避免把未来能力当成已成熟能力。

## 11. 竞争洞察

### 11.1 Google 的核心打法

Google 不是单纯卖一个湖仓表格式，而是在做三件事：

- 用 Iceberg 解决开放性和多引擎互操作
- 用 BigQuery 解决 Serverless 分析、性能和 AI 集成
- 用 Knowledge Catalog 解决治理、语义和 agent 上下文

这形成了“开放格式 + 云原生托管 + AI 治理”的组合拳。

### 11.2 对 Databricks / Snowflake / 自建湖仓的竞争点

| 对手 | Google BigLake/Lakehouse 的攻击点 |
| --- | --- |
| Databricks | 强调 BigQuery Serverless + Iceberg 开放互操作 + Google AI |
| Snowflake | 强调用户自有 bucket、开放格式、BigQuery AI 和跨云访问 |
| 自建 Iceberg 湖仓 | 强调全托管 catalog、自动优化、统一治理、安全与低运维 |

### 11.3 最大差异化

最大差异化不是 Iceberg 本身，而是：

- BigQuery 的成熟查询引擎和 Serverless 体验
- Knowledge Catalog 的治理与语义能力
- Google AI/Gemini/Agent 平台绑定
- 自动优化和 managed table 运维托管

## 12. 对我们产品的启示

- **开放格式不是充分条件**：必须配套 catalog、权限、血缘、质量、自动优化。
- **多引擎一致治理是关键卖点**：尤其是 Spark/Flink/Trino/BI/AI 同时访问同一份数据。
- **自动优化能力会成为湖仓产品标配**：小文件、compaction、clustering、GC、metadata optimization 都需要产品化。
- **AI 场景正在倒逼湖仓升级**：治理、语义、血缘、实时上下文会比单纯查询性能更重要。
- **跨云能力是高端客户入口，但成本与限制要透明**：BigLake 的跨云查询有明确限制，竞品可以从成本可控性和开放性切入。

## 13. 一句话结论

Google BigLake 已经从“BigQuery 外部表”演进为 **Google Cloud Lakehouse for Apache Iceberg**：它以 Iceberg 为开放格式核心，以 BigQuery 为计算与 AI 引擎，以 Lakehouse REST Catalog 为多引擎元数据中心，以 Knowledge Catalog 提供治理和语义上下文，目标是成为多云、开放、AI-ready 的企业湖仓底座。
