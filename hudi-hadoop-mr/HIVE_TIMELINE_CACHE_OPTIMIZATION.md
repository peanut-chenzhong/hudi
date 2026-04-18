# Hive 读 Hudi Timeline 扫描优化说明

## 1. 背景

在 Hive 读取 Hudi MOR（Merge-On-Read）表时，`AbstractHoodieLogRecordReader` 在 `scanInternalV1`/`scanInternalV2` 路径中会初始化 timeline 视图：

- `getCommitsTimeline()`
- `filterCompletedInstants()`
- `filterInflights()`

当同一个 map 任务内读取多个文件/split 时，reader 可能被重复创建，导致 timeline 被重复扫描，带来额外 CPU 与元数据访问开销。

## 2. 优化目标

- 在任务 JVM 内复用 timeline 视图，减少重复初始化。
- 对语义零侵入：不改变增量/快照读取结果。
- 支持开关与 TTL，确保可回退、可按场景调优。

## 3. 实现概览

### 3.1 任务内 timeline 缓存

在 `hudi-common` 的 `AbstractHoodieLogRecordReader` 中新增了 timeline 缓存逻辑：

- **缓存 key**：`basePath + latestInstantTime + instantRange + timelineLayoutVersion`
- **缓存 value**：
  - `completedInstantsTimeline`
  - `inflightInstantsTimeline`

`scanInternalV1` 和 `scanInternalV2` 都改为通过统一的缓存访问入口获取 timeline 视图。

### 3.2 缓存刷新策略

- 默认支持 JVM 内长期复用（TTL = 0，表示不按时间过期）。
- 可配置 TTL（毫秒）后，超时自动刷新。

## 4. 配置项

配置定义位于 `hudi-hadoop-mr/src/main/java/org/apache/hudi/hadoop/config/HoodieRealtimeConfig.java`。

### 4.1 启用开关

- **key**: `hoodie.realtime.reader.timeline.cache.enabled`
- **default**: `true`
- **说明**: 是否启用 timeline 缓存。

### 4.2 TTL 配置

- **key**: `hoodie.realtime.reader.timeline.cache.ttl.ms`
- **default**: `0`
- **说明**:
  - `0`：不按时间过期
  - `> 0`：超过 TTL 后刷新缓存

## 5. 接入范围

以下 MOR 读路径已接入上述配置并透传至 scanner builder：

- `RealtimeCompactedRecordReader`
- `RealtimeUnmergedRecordReader`
- `HoodieMergeOnReadSnapshotReader`

同时，`HoodieMergedLogRecordScanner` 与 `HoodieUnMergedLogRecordScanner` 的 builder 增加了：

- `withTimelineCacheEnabled(boolean)`
- `withTimelineCacheTtlMs(long)`

## 6. 回归测试

新增测试类：

- `hudi-common/src/test/java/org/apache/hudi/common/table/log/TestAbstractHoodieLogRecordReaderTimelineCache.java`

覆盖点：

- cache 开启（V1）：两次扫描仅加载 1 次 timeline。
- cache 关闭：两次扫描加载 2 次 timeline。
- cache 开启（V2）：两次扫描仅加载 1 次 timeline。

## 7. 构建与验证

可使用以下命令验证：

```bash
mvn -pl hudi-common -Dtest=TestAbstractHoodieLogRecordReaderTimelineCache test -DskipITs
mvn -pl hudi-hadoop-mr -am -DskipTests compile
```

建议在实际 Hive 环境增加压测对比：

- 场景：单 map 读取多文件 + 长 timeline（大量 commits）
- 指标：
  - map 耗时
  - timeline 初始化次数
  - CPU 时间

## 8. 回退策略

若线上需要快速回退行为，可直接关闭缓存：

```properties
hoodie.realtime.reader.timeline.cache.enabled=false
```

关闭后会恢复为旧逻辑（每次 reader 初始化时重新获取并过滤 timeline）。
