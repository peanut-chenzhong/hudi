# Hudi 0.15.0 vs master：Flink Hudi Catalog 写表问题分析（完善版）

## 1. 范围与术语（先澄清边界）

本文分析的是 **Flink 使用 Hudi Catalog 写 Hudi 表**，包含两种实现：

- `HoodieCatalog`：DFS 模式（文件系统 Catalog）
- `HoodieHiveCatalog`：HMS 模式（Hive Metastore Catalog）

对比基线：`origin/release-0.15.0` vs `origin/master`。  
关注写表链路：`CREATE TABLE` / `ALTER TABLE` / `DROP PARTITION`。  
证据来源：源码 diff + 对应测试变更 + 关键修复提交（HUDI-*）。

---

## 2. 问题归属矩阵（你最关心：到底是哪个 catalog）

| 编号 | 问题 | 影响 Catalog | 优先级 |
| --- | --- | --- | --- |
| 1 | DFS 下 `ALTER TABLE` 不支持 | `HoodieCatalog` | P0 |
| 2 | schema evolution 链路不完整 | 两者（DFS 更严重） | P0/P1 |
| 3 | PK/分区与 options 不一致未拦截 | `HoodieHiveCatalog` | P1 |
| 4 | 分区字段顺序不一致未拦截 | `HoodieHiveCatalog` | P1 |
| 5 | `DROP PARTITION` instant 生成方式风险 | 两者 | P1 |
| 6 | Partition Bucket Index 元数据未初始化 | 主要 `HoodieCatalog` | P2 |
| 7 | `precombine` 与 `ordering.fields` 键体系差异 | 两者 | P2 |
| 8 | 表属性文件缺少覆盖更新能力 | 主要 `HoodieCatalog`（也影响统一演进链路） | P2 |

---

## 3. 关键结论（按实现拆分）

### 3.1 `HoodieCatalog`（DFS）

- **高优先问题**
  - 0.15.0 的 `ALTER TABLE` 不可用（直接 `UnsupportedOperationException`）。
  - 这使 DFS Catalog 下 schema evolution 在生产几乎不可用。
- **中优先问题**
  - bucket index 建表后未初始化 hashing 元数据。
  - 表属性文件缺少覆盖更新能力，ALTER 后属性刷新能力不足。

### 3.2 `HoodieHiveCatalog`（HMS）

- **高优先问题**
  - 建表时 PK/分区声明与 options 不一致未强校验，存在“建表成功但语义错误”风险。
  - 分区字段顺序不一致未强校验，可能引入 HMS 与 Hoodie Meta 语义错位。
- **中优先问题**
  - schema evolution 早期仅偏 HMS 侧刷新，内部 schema 变更提交链路不统一（master 已补齐）。

### 3.3 两者共同问题

- `DROP PARTITION` 使用 `createNewInstantTime()` 在新 rollback/scheduling 语义下有冲突风险。
- 排序字段配置从 `precombine` 向 `ordering.fields` 迁移，0.15.0 与新版本配置习惯存在错配风险。

---

## 4. 详细问题（带修复依据）

## 问题 1：DFS Catalog 的 `ALTER TABLE` 在 0.15.0 不支持（P0）

- **归属**：仅 `HoodieCatalog`
- **0.15.0 现状**
  - `HoodieCatalog.alterTable(...)` 直接抛 `UnsupportedOperationException`。
- **master 修复**
  - 接入 `HoodieCatalogUtil.alterTable(...)`，支持 `tableChanges`。
  - 提交：`8930e8ced1be`（HUDI-7270）。
- **影响**
  - DFS Catalog 下无法做在线 schema 演进，必须重建迁移。

## 问题 2：schema evolution 写入链路不完整（P0/P1）

- **归属**：两者（DFS 更严重）
- **0.15.0 现状**
  - DFS：ALTER 不支持（问题 1）。
  - HMS：更多是 HMS 表定义更新，内部 schema 提交流程不统一。
- **master 修复**
  - 统一走 `HoodieCatalogUtil.alterTable`，对 `tableChanges` 触发 `WriteOperationType.ALTER_SCHEMA`。
  - 提交：`c4f985921044`（HUDI-7265）、`8930e8ced1be`（HUDI-7270）。
- **影响**
  - 可能出现 catalog schema 与 Hoodie 内部 schema 演进不同步。

## 问题 3：HMS 建表缺少 PK/分区参数一致性校验（P1）

- **归属**：仅 `HoodieHiveCatalog`
- **0.15.0 现状**
  - SQL PK/Partition 与 options 中 key 定义不一致也可能建表成功。
- **master 修复**
  - 新增 `validateParameterConsistency(...)`，不一致直接失败。
  - 提交：`bd2644632b59`（HUDI-8497）。
- **影响**
  - 主键/分区语义错误延迟到运行期暴露，排障成本高。

## 问题 4：HMS 分区字段顺序不一致未拦截（P1）

- **归属**：仅 `HoodieHiveCatalog`
- **0.15.0 现状**
  - 只看字段集合，不强校验顺序一致性。
- **master 修复**
  - 增加顺序校验，不一致直接报错。
  - 提交：`483473246b5e`（HUDI-9266）。
- **影响**
  - HMS 与 Hoodie Meta 对分区语义解释可能错位。

## 问题 5：`DROP PARTITION` instant 生成方式风险（P1）

- **归属**：两者
- **0.15.0 现状**
  - 删除分区使用 `createNewInstantTime()`。
- **master 修复**
  - 改为 `startDeletePartitionCommit()`。
  - 提交：`18c098fbdb51`（HUDI-9421 Part2）。
- **影响**
  - 在更严格调度语义下，可能产生 instant 冲突或回滚异常。

## 问题 6：Partition Bucket Index 元数据未初始化（P2）

- **归属**：主要 `HoodieCatalog`
- **0.15.0 现状**
  - 建表后未初始化 partition bucket hashing config。
- **master 修复**
  - `createTable` 后调用 `initPartitionBucketIndexMeta(...)`。
  - 提交：`3440b8038e1e`（HUDI-8990）。
- **影响**
  - 配置声明与实际运行行为不一致（“配了但不生效”）。

## 问题 7：排序字段配置键迁移导致兼容风险（P2）

- **归属**：两者
- **0.15.0 现状**
  - 仍以 `precombine` 相关键为主。
- **master 修复**
  - 迁移到 `ordering.fields` 体系并做兼容。
  - 提交：`c530d4196588`（HUDI-9619）。
- **影响**
  - 跨版本复制 DDL/options 时，排序字段可能不生效。

## 问题 8：表选项文件缺少覆盖更新能力（P2）

- **归属**：主要 `HoodieCatalog`
- **0.15.0 现状**
  - `TableOptionProperties` 仅 `createProperties`，无 `overwriteProperties`。
- **master 修复**
  - 新增覆盖更新接口用于 ALTER 后刷新属性。
- **影响**
  - ALTER 后 table_option.properties 的一致性不足。

---

## 5. 建议（按你的使用模式）

- **你主要用 DFS Catalog（`HoodieCatalog`）**
  - 核心痛点是 ALTER 与 schema evolution，建议优先升级。
- **你主要用 HMS Catalog（`HoodieHiveCatalog`）**
  - 核心痛点是一致性校验与分区顺序校验，建议至少合入 HUDI-8497、HUDI-9266 之后版本。
- **你两种都用**
  - 统一建议升级到包含 HUDI-7265/7270/8497/9266/9421/9619/8990 的版本区间。

---

## 6. 最小回归清单（升级后必须做）

1. `CREATE TABLE`：PK/分区与 options 冲突是否能被正确拦截。
2. `ALTER TABLE ADD COLUMN/CHANGE COLUMN`：catalog schema 与 internal schema 是否同步。
3. `DROP PARTITION`：并发和失败重试下是否有 instant/rollback 异常。
4. bucket index：hashing 配置文件是否生成且生效。
5. 跨引擎（Flink 写 + Spark 读/写）：主键、分区、schema 演进是否一致。
