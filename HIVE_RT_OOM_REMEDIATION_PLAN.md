# Hive RT OOM 修复方案（修正版）

## 1. 结论先行（根因重排）

本方案按最新复盘结论调整优先级：

1. **主因**：`HoodieRecordSizeEstimator` 低估导致 `ExternalSpillableMap` 溢写触发过晚或失效，`inMemoryMap` 持续增长。
2. **放大器**：高堆占用下，`HoodieLogBlock.inflate` 分配连续 `byte[]` 失败，引发 OOM。
3. **关键结构问题**：分段 merge 若复用同一个 scanner 且 records 不释放，内存仍是 O(U)，无法降到 O(S)。

因此修复顺序必须是：**先修 Spill/分段生命周期，再做 inflate 侧增强**。

---

## 2. 修复目标

### 2.1 功能目标

- 避免 Hive RT 读取 OOM，优先消除 `ExternalSpillableMap` 常驻膨胀问题。
- 分段读取内存复杂度从 O(U) 降到 O(S)（S 为单段 key 数）。
- 保持与 legacy 一致的 merge 语义（insert/update/delete/preCombine）。

### 2.2 非功能目标

- 保持默认行为兼容，通过开关灰度启用。
- 新增指标支持定位“估算偏差”和“分段释放是否生效”。

---

## 3. 修复优先级（最终版）

### P0：止血（必须先做）

1. `SizeEstimator` 引入安全系数（8~10x，可配）；
2. 分段 merge 改为 **每段独立 scanner + 段结束 close 并释放 records**；
3. 禁止 segment 路径回落到全量 `performScan()`。

### P1：稳态优化

1. spill 触发改为“估算值 + Runtime 内存水位”双条件；
2. 大记录直落盘；
3. 指标完善与参数调优。

### P2：补充优化（次优先）

1. inflate 防护（谨慎使用，默认建议关闭或高阈值）；
2. 流式解压优化（收益次于 P0/P1，不作为主修复路径）。

---

## 4. 详细改造设计

## 4.1 P0-1：修复 `SizeEstimator` 低估（最高优先）

### 改动点

- `hudi-common/.../HoodieRecordSizeEstimator.java`（value 侧）
- `hudi-common/.../DefaultSizeEstimator.java` 或 key 估算调用点（key 侧）
- `hudi-common/.../ExternalSpillableMap.java`

### 方案

1. 在现有估算结果上引入安全系数（同时覆盖 value 和 key）：
   - `effectiveValueSize = rawValueEstimatedSize * valueSafetyFactor`
   - `effectiveKeySize = rawKeyEstimatedSize * keySafetyFactor`
2. 新增配置：
   - `hoodie.spill.record.size.safety.factor`（默认 `8`，可调 `8~10`）
   - `hoodie.spill.key.size.safety.factor`（默认 `2`~`3`，按 key 长度分布调优）
3. 增加最小估算下限（防止极小值误判）：
   - `hoodie.spill.record.size.min.bytes`（默认例如 `1024`）
   - `hoodie.spill.key.size.min.bytes`（默认例如 `64`）

### 为什么先做这个

- 改动小、见效快，可直接让 spill 提前触发，快速抑制 `inMemoryMap` 无界增长。
- 比 EWMA 采样方案更直接，适合线上止血。

### 安全系数依据（补充）

- `HoodieAvroIndexedRecord` 浅堆通常在 `40~60B`；
- 实际深堆（含 `GenericRecord` 子对象）常见在 `600~700B`；
- value 侧比值通常在 `10~17x`；
- 叠加 `HashMap.Entry` 等结构开销后，综合倍率经验上落在 `7~10x`；
- 因此默认 `8x` 是合理起步值，但必须按 schema 复杂度和线上指标调参。

---

## 4.2 P0-2：分段 merge 生命周期重构（独立 scanner）

### 目标

确保内存真正按段释放，而不是全局累积。

### 关键要求（必须满足）

1. **每个 segment 创建独立 `HoodieMergedLogRecordScanner`**；
2. 仅扫描当前 segment 的 key 集（`scanByFullKeys` 优先）；
3. segment merge 完成后：
   - 清空段内 map；
   - `scanner.close()`；
   - 释放临时结构；
4. 下一个 segment 重新创建 scanner，不复用上一段状态。

### 改动点

- `hudi-hadoop-mr/.../SegmentedRealtimeCompactedRecordReader.java`
- 相关分段执行器/装载器（若已有拆分类则分别调整）

### 复杂度目标

- 从全局 O(U) 转为段级 O(S)。

---

## 4.3 P0-3：禁止 segment 路径触发全量 scan

### 问题

若 segment 流程末尾再次执行 `scan()`/`performScan()`，会回到全量 materialize，抵消分段收益。

### 要求

- segment 模式下全程禁用全量 scan；
- log-only 输出也必须分段或按增量策略处理，不能追加一次全量扫描兜底。

### log-only 输出闭环方案（新增）

禁止全量 value scan 后，log-only 记录需要独立发现机制，建议如下：

1. 引入轻量 `logAllKeysSet`（仅存 key，不存 value）；
2. 来源优先级：
   - 优先：扫描 log block header / key 索引（若可用）构建 key 集；
   - 退化：执行一次“仅提取 key 的轻量扫描”（不 materialize value）；
3. base 侧按 segment 读取时，把命中的 key 写入 `matchedBaseKeysSet`；
4. 结束时输出 `logOnlyKeys = logAllKeysSet - matchedBaseKeysSet`；
5. 对 `logOnlyKeys` 再按 segment 批量 `scanByFullKeys` 拉取 value 并输出，避免全量 value scan。

### 改动点

- `hudi-hadoop-mr/.../SegmentedRealtimeCompactedRecordReader.java`
- 与 `HoodieMergedLogRecordScanner` 调用处相关的控制逻辑。

---

## 4.4 P0-4：消除 `deltaRecordKeys` 冗余拷贝

### 问题

legacy 路径中 `deltaRecordKeys = new HashSet<>(deltaRecordMap.keySet())` 会造成 key 双份存储，放大内存占用。

### 方案

1. 分段重构中不再维护全局 `deltaRecordKeys` 副本；
2. 优先使用段内 map 的 `keySet()` 或迭代视图；
3. 需要删除语义时，使用可回收的段级结构，而非全局 `HashSet` 常驻。

### 改动点

- `hudi-hadoop-mr/.../RealtimeCompactedRecordReader.java`（legacy 可选优化）
- `hudi-hadoop-mr/.../SegmentedRealtimeCompactedRecordReader.java`（必须）

---

## 4.5 P1：spill 触发策略升级（基于运行时内存）

### 问题

仅基于 `currentInMemoryMapSize` 的比例阈值不可靠，因为该值本身受估算偏差影响。

### 方案

触发条件改为：

- 条件 A：估算值达到阈值；
- 条件 B：JVM 运行时内存水位达到阈值（例如 used/max）；
- 满足任一条件即可 spill。

### 运行时水位计算与采样节奏（补充）

1. 使用 `used = totalMemory - freeMemory`，`usage = used / maxMemory`；
2. 避免每次 `put()` 都检查，按固定采样节奏检查（建议与现有估算采样一致，如每 100 条一次）；
3. 使用短窗口平滑（例如最近 3 次采样取 max）降低 GC 抖动带来的误判。

### 建议配置

- `hoodie.spill.runtime.heap.usage.trigger`（默认 `0.70`~`0.80`）
- `hoodie.spill.large.record.direct.to.disk.enabled`（默认 `true`）
- `hoodie.spill.large.record.threshold.bytes`（默认 `1MB`）
- `hoodie.spill.runtime.heap.sample.interval.records`（默认 `100`）

---

## 4.6 P2：inflate 侧优化（降级处理）

### 定位

inflate 是放大器，不是主修复路径。

### 建议策略

1. 仅保留保守的防护开关，不作为默认强策略；
2. 流式解压作为可选优化推进；
3. 避免激进低阈值导致正常大 block 被误杀。

---

## 5. 测试与验收

## 5.1 单测（P0 必须）

- 安全系数生效后，spill 提前触发；
- segment 独立 scanner 生效，段结束后 records 释放；
- segment 模式下不触发全量 `performScan()`。

## 5.2 集成测试（P0/P1）

- 构造“高 unique key + 大 log 文件”场景；
- 验证内存峰值随 segment 大小变化，而非随全量 U 增长；
- 验证结果与 legacy 一致。

## 5.3 验收门槛

- OOM 消失或显著下降；
- `inMemoryMap` 峰值显著下降；
- 语义无回归。

---

## 6. 指标与可观测性（重点调整）

新增或强化：

- `spill_estimated_record_size_bytes`
- `spill_effective_record_size_bytes`（含安全系数后的值）
- `spill_effective_key_size_bytes`（含 key 安全系数后的值）
- `spill_safety_factor`
- `spill_key_safety_factor`
- `spill_inmemory_entries_peak`
- `spill_disk_entries`
- `segment_scanner_create_count`
- `segment_scanner_close_count`
- `segment_records_released_count`
- `full_scan_invocation_count`（segment 模式下应为 0）
- `log_only_keys_count`
- `log_only_scan_mode`（header_index / key_only_scan）

---

## 7. 发布与回滚

## 7.1 灰度顺序

1. 仅启用 `safetyFactor`；
2. 启用 segment 独立 scanner 生命周期 + log-only 闭环；
3. 启用 runtime heap 水位触发 spill；
4. 最后评估 inflate 侧补充优化。

## 7.2 回滚策略

- 每一步均可独立关闭开关回退；
- 保留 legacy 路径；
- 如出现性能异常，先调系数与 segment 参数，再考虑回滚。

---

## 8. PR 拆分建议

- **PR-1（P0）**：value/key 双侧安全系数 + 最小估算下限 + 指标
- **PR-2（P0）**：segment 独立 scanner + 段结束释放 + log-only 闭环 + 禁止全量 scan
- **PR-3（P0/P1）**：移除 `deltaRecordKeys` 冗余拷贝 + runtime heap 触发 spill + 大记录直落盘
- **PR-4（P2）**：inflate 防护与流式解压优化（可选）

---

## 9. 风险与缓解

- 风险：安全系数过高导致过度 spill、IO 上升  
  缓解：按表分级配置，先 8x，再根据指标调到 6x/10x。

- 风险：segment 太小导致重复扫描 IO 增加  
  缓解：通过 `segment.max.keys/max.bytes` 调优，建立压测基线。

- 风险：实现不当导致 segment 仍共享 scanner 状态  
  缓解：以 `create_count == close_count` 和 `full_scan_invocation_count==0` 作为强校验指标。

