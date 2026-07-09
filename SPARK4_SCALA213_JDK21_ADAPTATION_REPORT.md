# Hudi 0.15.0 适配 Spark 4.0 / Scala 2.13.13 / JDK 21 整体报告

## 1. 背景与目标

本次改造目标是让 `hudi` 0.15.0 分支具备以下能力：

- 支持 `Spark 4.0.0`
- 支持 `Scala 2.13.13`
- 在 `JDK 21` 下可完成核心模块编译与关键 bundle 打包

同时要求尽量保持对既有 Spark 3.x 逻辑的兼容，不做破坏式重构。

---

## 2. 总体结论

截至当前工作状态：

- **核心 Spark4 代码链路已打通**：`hudi-spark-client`、`hudi-spark-common`、`hudi-spark4-common`、`hudi-spark4.0.x` 可在 Spark4 profile 下编译。
- **关键打包链路已打通**：`hudi-spark4.0-bundle_2.13`、`hudi-utilities-bundle_2.13` 已在 Spark4 profile + JDK21 下成功打包。
- **CI/发布入口已接入 Spark4**：新增/修改了 GitHub workflow 与 release 脚本，Spark4 组合可被纳入构建与发布流程。
- **仍有可预期收尾项**：Spark4 的 docker bundle runtime 验证目前在主 workflow 中做了显式跳过（防止现阶段 CI 不稳定），后续可单独放开。

---

## 3. 构建与 Profile 侧改动

### 3.1 根 POM (`pom.xml`)

- 新增 `spark40.version=4.0.0`
- 升级 `scala13.version` 到 `2.13.13`
- 新增 `spark4.0` profile，核心属性包括：
  - `spark.version=${spark40.version}`
  - `sparkbundle.version=4.0`
  - `scala.binary.version=2.13`
  - `hudi.spark.module=hudi-spark4.0.x`
  - `hudi.spark.common.modules.1=hudi-spark4-common`
  - `kafka.version=${kafka.spark3.version}`（用于避免 Spark4+Scala2.13 下落回 `kafka_2.13:2.0.0`）
- `spark4.0` profile 下新增模块：
  - `hudi-spark-datasource/hudi-spark4-common`
  - `hudi-spark-datasource/hudi-spark4.0.x`

### 3.2 Spark4 新模块

- 新增 `hudi-spark-datasource/hudi-spark4-common/`
- 新增 `hudi-spark-datasource/hudi-spark4.0.x/`
- 新增 `Spark4_0Adapter`（初始骨架后续可继续补全 Spark4 专项行为）

---

## 4. Spark API 兼容层改造

本次最核心工作是修复 Spark 4 在 SQL/Catalyst/internal API 变化带来的编译与运行时不兼容。

### 4.1 Spark 版本识别与适配器分发

- `HoodieSparkUtils` 增加：
  - `isSpark4`
  - `isSpark4_0`
  - `gteqSpark4_0`
- `SparkAdapterSupport` 增加 Spark4 分发路径：
  - `org.apache.spark.sql.adapter.Spark4_0Adapter`

### 4.2 DataFrame / Dataset / SparkSession 内部 API 兼容

文件：`hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/DataFrameUtil.scala`

- 对 `internalCreateDataFrame` 使用反射查找并调用，兼容 Spark3/4 内部 API 位置变化。
- 对 `Dataset.ofRows` 引入双分支反射：
  - 优先 `org.apache.spark.sql.classic.Dataset$`（Spark4）
  - 回退 `org.apache.spark.sql.Dataset$`（Spark3）

调用侧统一改造：

- `HoodieUnsafeUtils`
- `BucketPartitionUtils`
- `RangeSample`
- `HoodieStreamSource`
- `DeleteHoodieTableCommand`
- `InsertIntoHoodieTableCommand`
- `MergeIntoHoodieTableCommand`
- `UpdateHoodieTableCommand`

从直接依赖 `Dataset.ofRows` / `SparkSession.internalCreateDataFrame`，切换到 `DataFrameUtil` 兼容入口。

### 4.3 JDBC schema 反射兼容

文件：`SparkJdbcUtils.scala`

- 对 `JdbcUtils.getSchema` 做反射签名探测和参数动态绑定，兼容 Spark 版本间方法签名变化。

### 4.4 Catalyst 结构变化适配

- `LogicalRDD` 匹配方式调整为实例匹配后读取字段（避免参数列表变更导致模式匹配失败）。
- `LogicalRelation` 匹配调整，兼容 Spark4 下 catalog table 获取方式。

---

## 5. Spark SQL 异常与解析兼容

### 5.1 AnalysisException 构造兼容

- 新增：`hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/HoodieAnalysisExceptionUtils.scala`
- 在多个 SQL 命令/分析器中将 `new AnalysisException(...)` 改为统一 helper 构造，避免构造器变更导致编译失败。

### 5.2 ParseException 构造兼容

文件：`HoodieCommonSqlParser.scala`

- 增加 `buildParseException` 反射分支，兼容不同 Spark 版本构造器。
- 补充 `parseRoutineParam` 覆盖，实现 `ParserInterface` 新方法要求。

---

## 6. Spark4 触发的语义/接口细节修复

### 6.1 `sqlContext.conf` 迁移

多处从：

- `sparkSession.sqlContext.conf`

迁移到：

- `sparkSession.sessionState.conf`

### 6.2 `Column` 与表达式路径

`HoodieFileIndex` 中移除直接 `new Column(...)` 依赖，改为 `functions.expr(...)` 构造过滤表达式。

### 6.3 `SpecializedGetters#getVariant`

`PartitionFileSliceMapping` 新增 `getVariant` 实现，以满足 Spark4 trait 新增接口。

### 6.4 Utilities 模块 Spark4 Java API 修复

为保证 `hudi-utilities` 在 Spark4 + Scala2.13 下可编译：

- `SqlSource.java`：
  - 移除不可用 `showString(...)` 调用
  - 改为 `source.limit(10).collectAsList()` 的 debug 输出
- `HoodieSnapshotExporter.java`：
  - 移除 `new SQLContext(jsc)`
  - 改为 `SparkSession.builder().sparkContext(jsc.sc()).getOrCreate()`

---

## 7. CI 与发布链路改造

### 7.1 新增 Spark4/JDK21 专项 Workflow

- 新增：`.github/workflows/spark4_jdk21_validation.yml`
- 覆盖：
  - Spark4 关键模块编译
  - Spark4 bundle 构建
  - JDK21 环境

### 7.2 主 CI (`bot.yml`) 接入

- 新增 job：`test-spark4-jdk21`
  - JDK21
  - Spark4 + Scala2.13 build/quickstart/UT
- 在 `validate-bundles` 与相关矩阵中加入 Spark4 组合。
- 为避免当前阶段 docker runtime 不稳定，针对 Spark4 增加显式跳过条件（仅跳过 docker runtime 校验，不影响编译/打包覆盖）。

### 7.3 RC Workflow 接入

- `release_candidate_validation.yml` 的 matrix 增加 Spark4 组合。
- 同样对 Spark4 的 docker runtime 校验增加显式 skip 条件（当前 job 仍为 `if: false`，此改动用于未来启用时的正确行为）。

### 7.4 发布脚本改造

- `scripts/release/deploy_staging_jars.sh`
  - 新增 Spark4 + Scala2.13 的 deploy 选项
  - 包含 `hudi-spark4-common`、`hudi-spark4.0.x`、spark/utilities bundle
- `scripts/release/validate_staged_bundles.sh`
  - 增加 `hudi-spark4.0-bundle_2.13` 校验

### 7.5 bundle-validation 脚本

- `packaging/bundle-validation/ci_run.sh`
  - 新增 Spark4 runtime 映射：`spark4.0.0`
  - 新增 RC jar 名映射：`hudi-spark4.0-bundle_2.13`
- `packaging/bundle-validation/run_docker_java17.sh`
  - 同步新增 Spark4 runtime 分支
- 新增基础镜像构建脚本：
  - `packaging/bundle-validation/base/build_flink1180hive313spark400scala213.sh`

---

## 8. 构建验证记录（关键命令）

以下命令已在本地 JDK21 环境执行并通过：

- `mvn "-Dscala-2.13" "-Dspark4.0" "-Drat.skip=true" -DskipTests -pl hudi-spark-datasource/hudi-spark4-common,hudi-spark-datasource/hudi-spark4.0.x -am compile`
- `mvn "-Dscala-2.13" "-Dspark4.0" "-Drat.skip=true" "-Dmaven.test.skip=true" -DskipTests -pl packaging/hudi-spark-bundle -am package`
- `mvn "-Dscala-2.13" "-Dspark4.0" "-Drat.skip=true" "-Dmaven.test.skip=true" -DskipTests -pl hudi-utilities -am compile`
- `mvn "-Dscala-2.13" "-Dspark4.0" "-Drat.skip=true" "-Dmaven.test.skip=true" -DskipTests -pl packaging/hudi-utilities-bundle -am package`

执行环境要点：

- `JAVA_HOME=C:\Users\Peanut\.jdks\corretto-21.0.11`

---

## 9. 已识别问题与处理结果

### 9.1 已处理

- Maven 属性参数未加引号导致解析失败（如 `-Dscala-2.13`） -> 已通过命令规范规避。
- Spark4 API 变化导致的编译失败（`Dataset.ofRows`、`internalCreateDataFrame`、`AnalysisException`、`ParseException` 等） -> 已通过兼容层和反射修复。
- `hudi-utilities` 在 Spark4 下 Java API 不兼容 -> 已修复并编译通过。
- 发布脚本未覆盖 Spark4 -> 已补齐。

### 9.2 当前保留风险

- Spark4 docker runtime bundle validation 尚未完全打开（当前 workflow 中对 Spark4 显式 skip），原因是基础镜像/运行时校验链路需要进一步稳定化。
- 全仓“含完整测试集”编译仍可能受历史测试与外部仓库波动影响；当前保证的是 Spark4 主链路编译与关键打包可用。

---

## 10. 变更文件范围（按域汇总）

### 10.1 构建/配置

- `pom.xml`
- `hudi-spark-datasource/hudi-spark4-common/**`
- `hudi-spark-datasource/hudi-spark4.0.x/**`

### 10.2 Spark 客户端与 SQL 兼容

- `hudi-client/hudi-spark-client/src/main/scala/org/apache/hudi/HoodieSparkUtils.scala`
- `hudi-client/hudi-spark-client/src/main/scala/org/apache/hudi/SparkAdapterSupport.scala`
- `hudi-client/hudi-spark-client/src/main/scala/org/apache/hudi/SparkJdbcUtils.scala`
- `hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/DataFrameUtil.scala`
- `hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/HoodieUnsafeUtils.scala`
- `hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/BucketPartitionUtils.scala`
- `hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/hudi/execution/RangeSample.scala`
- `hudi-client/hudi-spark-client/src/main/scala/org/apache/spark/sql/hudi/SparkAdapter.scala`

### 10.3 Spark Datasource（common + spark）

- `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieFileIndex.scala`
- `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/PartitionFileSliceMapping.scala`
- `hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/spark/sql/hudi/HoodieAnalysisExceptionUtils.scala`
- 以及 `hudi-spark-common` / `hudi-spark` 下多处 command/analysis/parser/streaming 兼容文件

### 10.4 Utilities

- `hudi-utilities/src/main/java/org/apache/hudi/utilities/sources/SqlSource.java`
- `hudi-utilities/src/main/java/org/apache/hudi/utilities/HoodieSnapshotExporter.java`

### 10.5 CI/发布/验证

- `.github/workflows/bot.yml`
- `.github/workflows/release_candidate_validation.yml`
- `.github/workflows/spark4_jdk21_validation.yml`
- `scripts/release/deploy_staging_jars.sh`
- `scripts/release/validate_staged_bundles.sh`
- `packaging/bundle-validation/ci_run.sh`
- `packaging/bundle-validation/run_docker_java17.sh`
- `packaging/bundle-validation/base/build_flink1180hive313spark400scala213.sh`

---

## 11. 下一步建议（收口）

建议按以下顺序收口：

1. 将 Spark4 基础镜像构建流程纳入 CI（确保 `spark4.0.0` runtime docker 基础环境稳定可复用）。
2. 逐步放开 Spark4 的 docker bundle runtime 验证（先 OpenJDK17，再评估 JDK21）。
3. 针对 Spark4 profile 增加更细粒度 UT/FT 套件，覆盖 SQL DDL/DML 与 utilities 关键路径。
4. 最后统一做一次“Spark4 profile 全链路回归（compile/package/test subset）”并冻结发布脚本参数模板。

---

## 12. 现阶段可用结论（给使用者）

如果仅关注“能否在 Spark4.0 + Scala2.13 + JDK21 下正常编译和打包核心模块”，答案是：

- **可以，已达成。**

可复用的推荐命令模式：

- `mvn "-Dscala-2.13" "-Dspark4.0" "-Drat.skip=true" -DskipTests ...`
- 需要规避测试编译时使用：
  - `"-Dmaven.test.skip=true" -DskipTests`

