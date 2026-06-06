# Hudi 0.15.0 vs master：Flink 使用 Hudi Catalog 写表问题分析

## 1. 分析范围与方法

- 对比基线：`origin/release-0.15.0` 与 `origin/master`。
- 关注范围：Flink Catalog 写表链路（`CREATE TABLE`/`ALTER TABLE`/`DROP PARTITION`）：
  - `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/catalog/HoodieCatalog.java`
  - `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/catalog/HoodieHiveCatalog.java`
  - `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/catalog/HoodieCatalogUtil.java`
  - `hudi-flink-datasource/hudi-flink/src/main/java/org/apache/hudi/table/catalog/TableOptionProperties.java`
- 证据来源：上述文件分支 diff + master 新增/修改的对应测试 + 关键修复提交（HUDI-*）。

---

## 2. 结论概览（按优先级）

### P0/P1（建议优先处理）

1. DFS Catalog 下 `ALTER TABLE` 在 0.15.0 不可用（直接 `UnsupportedOperationException`）。
2. Hive/DFS Catalog 的 schema evolution 能力缺失或不完整（内部 schema 与 catalog 元数据更新链路不完整）。
3. Hive Catalog 在建表时缺少主键/分区定义一致性校验，容易“建表成功但语义错误”。
4. Hive Catalog 不校验分区字段顺序一致性，可能造成 HMS 与 Hoodie Meta 语义错位。
5. `DROP PARTITION` 使用不安全的 instant 生成方式（`createNewInstantTime`），在新回滚调度模型下存在冲突风险。

### P2（功能/兼容性问题）

6. Partition Bucket Index 建表时未初始化 hashing 元数据，导致配置写入但运行时不生效。
7. 排序字段配置键在 0.15.0 仍以 `precombine` 体系为主，新键 `ordering.fields` 兼容性不足。
8. 表选项持久化能力不足（缺少覆盖更新能力），导致部分 ALTER 场景无法可靠刷新表属性文件。

---

## 3. 详细问题清单

## 问题 1：DFS Catalog 的 `ALTER TABLE` 在 0.15.0 直接不支持（P0）

- **0.15.0 现状**
  - `HoodieCatalog.alterTable(...)` 直接抛 `UnsupportedOperationException`。
- **master 变化**
  - 接入 `HoodieCatalogUtil.alterTable(...)`，并支持携带 `tableChanges` 的路径。
  - 关键提交：`8930e8ced1be`（HUDI-7270）。
- **影响**
  - Flink SQL 在 DFS Catalog 下无法进行 schema 变更（加列/改列等）。
  - 建表后演进能力缺失，生产上只能“重建+迁移”。
- **建议**
  - 若业务依赖 `ALTER TABLE`，建议升级到包含 HUDI-7270 的版本。
  - 在 0.15.0 临时规避：避免在 DFS Catalog 上做在线 schema 变更。

## 问题 2：schema evolution 写入链路不完整（P0/P1）

- **0.15.0 现状**
  - Hive Catalog 的 `alterTable` 主要是 HMS 表定义重写，缺少统一的内部 schema 变更提交流程。
  - DFS Catalog 更是直接不支持 `ALTER TABLE`（见问题 1）。
- **master 变化**
  - 抽象出 `HoodieCatalogUtil.alterTable`，在有 `tableChanges` 时通过 `WriteOperationType.ALTER_SCHEMA` 提交内部 schema。
  - 然后再执行 catalog/HMS 侧刷新回写。
  - 关键提交：`c4f985921044`（HUDI-7265）、`8930e8ced1be`（HUDI-7270）。
- **影响**
  - 易出现“Catalog schema 变了，但 Hoodie 内部 schema 演进不完整”的风险。
  - 下游读写（尤其跨引擎）可能触发字段解析异常或演进不一致。
- **建议**
  - 对需要长期演进的表，优先使用 master 对应修复后的版本。
  - 0.15.0 上尽量将 schema 固化，减少在线变更频次。

## 问题 3：Hive Catalog 缺少主键/分区参数一致性校验（P1）

- **0.15.0 现状**
  - 允许 SQL 声明主键/分区与 options 中 `hoodie.datasource.write.recordkey.field`、`hoodie.datasource.write.partitionpath.field` 不一致。
  - 可能“建表成功但真实写入 key 语义错误”。
- **master 变化**
  - 新增 `validateParameterConsistency(...)`，建表阶段直接拒绝不一致定义。
  - 关键提交：`bd2644632b59`（HUDI-8497）。
- **影响**
  - 主键冲突、更新覆盖异常、分区路径异常等隐患会延后到运行期暴露。
- **建议**
  - 0.15.0 使用规范：SQL PK/Partition 与 options 完全一致，避免双重定义。
  - 升级后可由框架提前兜底校验。

## 问题 4：Hive 分区字段顺序不校验，存在元数据错位风险（P1）

- **0.15.0 现状**
  - 仅处理分区字段集合，不强校验顺序。
- **master 变化**
  - 对分区字段顺序做显式一致性校验，不一致直接失败。
  - 关键提交：`483473246b5e`（HUDI-9266）。
- **影响**
  - HMS 与 Hoodie Meta 对分区字段解释顺序不一致时，可能导致分区裁剪/写入路径语义偏差。
- **建议**
  - 0.15.0 上严格约束 DDL 字段顺序与分区定义顺序一致。

## 问题 5：`DROP PARTITION` instant 生成方式存在调度冲突风险（P1）

- **0.15.0 现状**
  - 分区删除使用 `createNewInstantTime()`。
- **master 变化**
  - 改为 `startDeletePartitionCommit()`，与新的 rollback/调度语义对齐。
  - 关键提交：`18c098fbdb51`（HUDI-9421 Part2）。
- **影响**
  - 在更严格的回滚/TTL 调度语义下，存在 instant 冲突或异常处理风险。
- **建议**
  - 高并发/频繁分区运维场景建议升级。
  - 0.15.0 上减少并发 `DROP PARTITION` 操作并加强失败重试观察。

## 问题 6：Partition Bucket Index 建表初始化不完整（P2）

- **0.15.0 现状**
  - Catalog 建表后未初始化 partition-bucket hashing 元数据。
- **master 变化**
  - `createTable` 后新增 `initPartitionBucketIndexMeta(...)`，持久化 hashing 配置。
  - 关键提交：`3440b8038e1e`（HUDI-8990）。
- **影响**
  - 用户声明了 bucket index 分区表达式/规则，但运行时可能未按预期生效。
- **建议**
  - 如依赖 partition-level bucket index，建议升级；或避免在 0.15.0 通过 Catalog 路径启用该能力。

## 问题 7：排序字段配置键体系在 0.15.0 与新版本不一致（P2）

- **0.15.0 现状**
  - Catalog 仍围绕 `preCombineField` / `PRECOMBINE_FIELD` 做映射和校验。
- **master 变化**
  - 迁移至 `ordering.fields` / `ORDERING_FIELDS`（并处理兼容）。
  - 关键提交：`c530d4196588`（HUDI-9619）。
- **影响**
  - 与新版本文档/配置习惯混用时，可能出现配置不生效或语义偏差。
- **建议**
  - 在 0.15.0 严格使用旧键体系，避免跨版本复制 DDL/options。

## 问题 8：表选项文件缺少“覆盖更新”能力，ALTER 后属性刷新不可靠（P2）

- **0.15.0 现状**
  - `TableOptionProperties` 仅有 `createProperties`，缺少 `overwriteProperties`。
- **master 变化**
  - 增加覆盖写能力，用于 ALTER 后刷新 table option 文件。
  - 与 schema evolution 路径联动（见问题 1/2）。
- **影响**
  - 表属性（主键、分区、注释、schema 相关 option）在变更后的持久化一致性不足。
- **建议**
  - 0.15.0 上避免频繁 ALTER 依赖 table_option.properties 的场景。

---

## 4. 风险评估与升级建议

- **如果你当前重点是“稳定建表+写入，不做 schema 演进”**
  - 0.15.0 可用，但需严格执行建表规范（PK/分区/options 一致、字段顺序一致、控制分区删除并发）。
- **如果你需要“Flink SQL 持续演进表结构”**
  - 建议升级到包含 HUDI-7265/7270 及后续修复（至少 master 对应能力）。
- **如果你使用 partition bucket index 或复杂分区运维**
  - 建议升级并重点验证 HUDI-8990、HUDI-9421 相关链路。

---

## 5. 最小验证清单（升级后回归）

1. `CREATE TABLE`：主键、分区、ordering fields 配置是否落盘且可回读。
2. `ALTER TABLE ADD COLUMN/CHANGE COLUMN`：Catalog schema 与 Hoodie internal schema 是否同步生效。
3. `DROP PARTITION`：并发执行下是否存在 instant 冲突/回滚异常。
4. bucket index：hashing 配置文件是否生成并按规则生效。
5. 跨引擎（Flink 写、Spark 读/写）一致性：主键、分区、schema 演进行为是否一致。

