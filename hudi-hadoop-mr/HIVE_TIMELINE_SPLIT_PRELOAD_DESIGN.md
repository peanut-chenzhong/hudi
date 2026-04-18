# Hive 侧 Timeline 预计算并下发 Task 方案说明

## 1. 背景与问题

在 Hive 读取 Hudi MOR 表时，`AbstractHoodieLogRecordReader` 在扫描日志块（`scanInternalV1`/`scanInternalV2`）过程中，会依赖 timeline 过滤逻辑判断某个 block instant 是否可读。

传统路径下，Task 侧 reader 会执行：

- `metaClient.getCommitsTimeline()`
- `filterCompletedInstants()`
- `filterInflights()`

当同一个 map 任务内读取多个文件或 split 时，会出现重复 timeline 初始化，造成额外开销（CPU + 元数据读取）。

### 1.1 修改前是否“每读一个 parquet 文件都读一次 timeline”

在 MOR realtime 读取场景下，基本可以近似理解为“每个 realtime split 都会读一次 timeline”，而一个 realtime split 通常对应一个 base parquet 文件（或日志文件组）。

因此在常见执行形态下，会观察到“每读一个 parquet 文件都加载一次 timeline”的现象。

说明：

- 这是 MOR realtime 路径中的行为，不是所有 Hudi 读取路径（例如 COW 纯 parquet 读取）都如此。
- 在 combine split 等特殊执行形态下，粒度更准确是“每个 reader/split 实例一次”，本质仍是重复初始化。

### 1.2 读取 timeline 的目的

读取 timeline 不是冗余动作，它的核心目的是保证日志块可见性与快照一致性，避免脏读。

Task 侧需要借助 timeline 回答：

- 某个 log block 对应 instant 是否已 completed（可读）；
- 是否处于 inflight（需要过滤）；
- 是否满足当前查询的 instant 可见性边界。

在代码语义上对应两类判断：

- committed 判断：`containsOrBeforeTimelineStarts`
- inflight 判断：`containsInstant`

如果不做 timeline 判定，失败写入或未完成写入的日志块可能被误读，导致查询结果不一致。

## 2. 目标

- 在 Driver 侧预计算 timeline 视图并随 split 下发到 Task。
- Task 侧优先使用预计算结果，减少/避免重复 timeline 扫描。
- 保持语义不变（可见性判断一致）。
- 提供显式开关，支持灰度与快速回退。

## 3. 方案概览

### 3.1 总体流程

1. Driver（`HoodieMergeOnReadTableInputFormat#getSplits`）按 `basePath` 预计算 timeline 状态。
2. 预计算状态写入 `RealtimeSplit` 并序列化到 split。
3. Task 侧 reader 从 split 取出状态，转换为 `PrecomputedTimelineState`。
4. `AbstractHoodieLogRecordReader` 优先使用预计算状态完成 instant 可见性判断。
5. 若未提供预计算状态，则自动回退旧路径（Task 自行读取 timeline）。

### 3.2 关键设计点

- 传递的是“轻量可序列化状态”，不是传 `HoodieTimeline` 对象本体：
  - `completedTimelineStartInstant`
  - `completedInstants`（Set）
  - `inflightInstants`（Set）
- 可见性判断仍与原逻辑一致：
  - committed 判断：`containsOrBeforeTimelineStarts`
  - inflight 判断：`containsInstant`
- split 反序列化兼容旧格式：读取新增字段遇到 EOF 自动兜底为 `Option.empty()`。

## 4. 代码改动清单

## 4.1 hudi-common

- 新增：`hudi-common/src/main/java/org/apache/hudi/common/table/log/PrecomputedTimelineState.java`
  - Task 侧使用的预计算 timeline 视图封装。

- 修改：`hudi-common/src/main/java/org/apache/hudi/common/table/log/AbstractHoodieLogRecordReader.java`
  - 新增 `Option<PrecomputedTimelineState>` 入参。
  - 在 `scanInternalV1/V2` 中：
    - 有预计算状态：直接按预计算集合判断
    - 无预计算状态：走原有 `metaClient.getCommitsTimeline()` 路径

- 修改：
  - `hudi-common/src/main/java/org/apache/hudi/common/table/log/HoodieMergedLogRecordScanner.java`
  - `hudi-common/src/main/java/org/apache/hudi/common/table/log/HoodieUnMergedLogRecordScanner.java`
  - builder 增加 `withPrecomputedTimelineState(...)` 并向下透传。

- 新增测试：`hudi-common/src/test/java/org/apache/hudi/common/table/log/TestPrecomputedTimelineStateInLogRecordReader.java`
  - 验证有预计算状态时不调用 `metaClient.getCommitsTimeline()`
  - 验证无预计算状态时仍调用 timeline（回退逻辑）

## 4.2 hudi-hadoop-mr

- 新增：`hudi-hadoop-mr/src/main/java/org/apache/hudi/hadoop/realtime/RealtimeSplitTimelineState.java`
  - Driver 与 Task 间 split 透传结构。

- 修改：`hudi-hadoop-mr/src/main/java/org/apache/hudi/hadoop/realtime/RealtimeSplit.java`
  - 接口新增 `get/setRealtimeSplitTimelineState`
  - `writeToOutput/readFromInput` 新增 timeline 状态序列化
  - `readFromInput` 加 EOF 兼容处理

- 修改：
  - `HoodieRealtimeFileSplit`
  - `HoodieRealtimeBootstrapBaseFileSplit`
  - `HoodieRealtimePath`
  - `RealtimeFileStatus`
  - 完整承载并传递 split timeline 状态

- 修改：`hudi-hadoop-mr/src/main/java/org/apache/hudi/hadoop/realtime/HoodieMergeOnReadTableInputFormat.java`
  - `getSplits` 阶段按开关执行预计算并下发到 split
  - 以 `basePath` 为维度做一次加载，多 split 复用

- 修改：Reader 接入点
  - `AbstractRealtimeRecordReader`：把 split 状态转换为 `PrecomputedTimelineState`
  - `RealtimeCompactedRecordReader` / `RealtimeUnmergedRecordReader` / `HoodieMergeOnReadSnapshotReader`
    - 将预计算状态传入 log scanner builder

- 修改配置：`hudi-hadoop-mr/src/main/java/org/apache/hudi/hadoop/config/HoodieRealtimeConfig.java`
  - `hoodie.realtime.split.timeline.preload.enabled`
  - 默认值：`false`

## 5. 配置说明

### 5.1 开关

- Key: `hoodie.realtime.split.timeline.preload.enabled`
- 类型: `boolean`
- 默认: `false`

### 5.2 建议

- 灰度期：先在小范围任务开启。
- 大分区/超长 timeline 场景：关注 split 大小与 Driver 内存占用。

## 6. 行为与兼容性

## 6.1 语义

- 开关关闭：完全旧行为。
- 开关开启但预加载失败（例如某 basePath 读取异常）：
  - 单 basePath 降级为 `Option.empty()`
  - Task 侧自动回退旧逻辑，不影响正确性。

## 6.2 序列化兼容

- 新版本写出的 split 包含 timeline 状态字段。
- 读取旧 split 时，EOF 兜底，视为无预计算状态。

## 6.3 风险点

- split 体积增大：
  - `completedInstants/inflightInstants` 过大时会增加网络与反序列化开销。
- Driver 侧额外预加载开销：
  - 通过 basePath 缓存避免重复计算。

## 7. 验证与测试

## 7.1 编译验证

```bash
mvn -pl hudi-common -Dtest=TestPrecomputedTimelineStateInLogRecordReader test -DskipITs
mvn -pl hudi-hadoop-mr -am -DskipTests compile
```

## 7.2 建议压测

- 场景：
  - 单 map 读取多文件
  - timeline 很长（大量 commits）
- 指标：
  - Task CPU 时间
  - map 耗时
  - Task 侧 timeline 初始化次数（日志/埋点）

## 8. 回滚方案

若出现异常或性能不符合预期，直接关闭开关：

```properties
hoodie.realtime.split.timeline.preload.enabled=false
```

关闭后恢复原有“Task 侧自行读取 timeline”行为。

## 9. 后续可选优化

- 对 split 中 instant 集合做压缩编码（减少序列化体积）。
- Driver 侧增加统计埋点（每个 basePath 的 instant 数量、预加载耗时）。
- 进一步把 `containsOrBeforeTimelineStarts` 判断路径做更紧凑结构优化（如 bitset / sorted array + binary search）。
