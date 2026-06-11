# Hive RT OOM 修复方案（Inflate + Spill 估算双路径）

## 1. 背景与问题定义

在 Hive 读取 Hudi MOR realtime（RT）链路中出现 `java.lang.OutOfMemoryError: Java heap space`，典型堆栈如下：

- `HoodieLogBlock.inflate`
- `HoodieDataBlock.readRecordsFromBlockPayload`
- `AbstractHoodieLogRecordReader.processDataBlock`
- `HoodieMergedLogRecordScanner.performScan`
- `RealtimeCompactedRecordReader.getMergedLogRecordScanner`

从该堆栈可确认：

1. 触发点是 **log block 解压（inflate）阶段**，属于大块内存分配失败。
2. 即便 `ExternalSpillableMap` 存在，OOM 仍可能在进入 map 前发生。
3. `SizeEstimator` 低估不是唯一根因，但会将堆推向高水位，显著放大 OOM 风险。

因此修复应采用“双路径”：

- 路径 A（优先）：降低 `inflate` 大数组分配风险；
- 路径 B（并行/后续）：修复 spill 内存估算偏低与触发滞后。

---

## 2. 修复目标

### 2.1 功能目标

- 避免 Hive RT 读取在 log block 解压阶段发生 OOM。
- 保持 merge 语义与现有实现一致（insert/update/delete/preCombine）。
- 保持默认行为兼容，通过配置渐进启用新逻辑。

### 2.2 非功能目标

- 将读取链路峰值内存从“随 block 大小增长”降为“随流式 buffer 增长”。
- 使 spill 触发更接近真实内存使用，降低高水位堆积。
- 任何异常可 fail-open 降级（可配置）。

---

## 3. 总体策略与优先级

### P0：先止血 `inflate` OOM

1. 增加 inflate 上限保护；
2. 将整块解压改为流式解压；
3. 在 reader 链路上增加 fail-open 降级能力。

### P1：修正 spill 估算与触发

1. 增加估算与真实采样的动态校正；
2. 前移 spill 触发阈值；
3. 大记录直接落盘策略。

### P2：验证与灰度

1. 指标打通；
2. 压测与回归；
3. 分批开关灰度发布。

---

## 4. 详细改造设计

## 4.1 路径 A：`inflate` 风险治理（主因）

### A1. 解压上限保护

#### 目标

将不可控 OOM 转为可观测、可降级的受控异常。

#### 建议改动点

- `hudi-common/.../HoodieLogBlock.java`
  - `inflate(...)`（或等效解压入口）

#### 建议配置

- `hoodie.realtime.logblock.inflate.max.bytes`（默认建议 `64MB` 或 `128MB`）
- `hoodie.realtime.logblock.inflate.failopen.enabled`（默认建议 `true`）

#### 行为

- 解压过程中若累计输出字节数超过阈值，抛出 `HoodieLogBlockTooLargeException`（新异常）。
- 上层 reader 根据 fail-open 策略决定降级或失败。

---

### A2. 流式解压替代整块解压

#### 目标

避免一次性分配超大连续 `byte[]`。

#### 建议改动点

- `hudi-common/.../HoodieDataBlock.java`
  - `readRecordsFromBlockPayload(...)`
  - `getRecordIterator(...)`
- 相关 block 实现（Avro/Parquet/HFile）的 payload 读取路径。

#### 设计要点

1. 使用流式解压（例如 `InflaterInputStream`）读取压缩 payload；
2. 按固定 chunk（例如 64KB/256KB）推进反序列化；
3. 记录迭代器保持惰性消费，不在内存中整体展开 block。

#### 预期收益

- 内存峰值由 O(block_size) 降到 O(chunk_size + record_window)；
- 在高堆占用时，显著降低连续大数组分配失败概率。

---

### A3. Reader 级 fail-open 降级

#### 目标

发生大块异常时不中断所有任务，提高整体可用性。

#### 建议改动点

- `hudi-hadoop-mr/.../HoodieRealtimeRecordReader.java`
- `hudi-hadoop-mr/.../RealtimeCompactedRecordReader.java`
- 已有分段 reader 路径（如 `SegmentedRealtimeCompactedRecordReader`）

#### 行为

- 捕获 `HoodieLogBlockTooLargeException` 或解压相关受控异常；
- 若 `failopen=true`：降级到 legacy/可跳过策略并打印告警；
- 若 `failopen=false`：显式失败并附带诊断信息。

---

## 4.2 路径 B：Spill 估算与触发优化（放大器）

### B1. `SizeEstimator` 动态校正

#### 目标

降低估算偏差导致 spill 触发偏晚。

#### 建议改动点

- `hudi-common/.../HoodieRecordSizeEstimator.java`
- `hudi-common/.../ExternalSpillableMap.java`

#### 方案

1. 保留当前轻量估算；
2. 每 N 条进行一次真实采样（序列化后取字节长度）；
3. 通过 EWMA 动态更新估算值；
4. 设置估算下限（防止被低估拖垮）。

---

### B2. 提前 spill 与大记录直落盘

#### 目标

避免内存接近极限才触发 spill。

#### 建议配置

- `hoodie.spill.early.trigger.fraction`（默认 `0.75`~`0.80`）
- `hoodie.spill.large.record.direct.to.disk.enabled`（默认 `true`）
- `hoodie.spill.large.record.threshold.bytes`（默认例如 `1MB`）

#### 行为

- 达到提前阈值即进入 spill 模式；
- 超大单条记录不进 in-memory map，直接写 disk map。

---

## 4.3 与现有分段 Merge 的协同

已实现的 segmented merge（opt-in）可以降低 key map 常驻规模，但仍需注意：

1. 若仍触发全量 `scan()`，末尾阶段仍可能放大 inflate 风险；
2. 应优先使用按 segment 的增量扫描策略，避免回退到 `performScan()`；
3. 与本方案 A/B 结合后，才能形成完整闭环。

---

## 5. 测试与验收

## 5.1 单元测试

- Inflate 上限触发：超阈值时抛受控异常，不触发 OOM；
- 流式解压语义一致性：与旧实现输出记录一致；
- SizeEstimator 校正：估算误差随采样收敛；
- 提前 spill / 大记录直落盘：行为符合预期。

## 5.2 集成测试

- 构造“单 file group + 大 log block + 高 unique key”场景；
- 验证 Hive RT 不再 OOM；
- 验证结果与 legacy 一致（集合与语义一致）。

## 5.3 性能验收门槛

- `inflate_oom_count == 0`
- 峰值堆内存下降（建议目标 >= 30%）
- 查询耗时回归可控（建议目标 <= 15%）
- 无语义回归（delete/preCombine 规则一致）

---

## 6. 指标与可观测性

建议新增（日志或 metrics）：

- `inflate_attempt_count`
- `inflate_block_compressed_bytes`
- `inflate_block_uncompressed_bytes`
- `inflate_guard_reject_count`
- `spill_count`
- `spill_bytes`
- `estimated_record_size_bytes`
- `sampled_record_size_bytes`
- `estimation_error_ratio`
- `fallback_to_legacy_count`

---

## 7. 发布与回滚

## 7.1 灰度顺序

1. 仅开启 inflate 上限保护（最小风险）；
2. 小流量开启流式解压；
3. 开启 spill 估算动态校正；
4. 开启提前 spill 与大记录直落盘。

## 7.2 回滚策略

- 所有新行为均通过配置开关控制；
- 任一阶段异常可单独回退，不影响其他优化项；
- 默认保留 legacy 路径作为兜底。

---

## 8. 推荐实施拆分（PR 规划）

- **PR-1（P0）**：inflate 上限 + 受控异常 + fail-open 降级
- **PR-2（P0）**：流式解压与迭代读取改造
- **PR-3（P1）**：SizeEstimator 动态校正 + spill 策略优化
- **PR-4（P2）**：压测基准、指标完善、文档与默认参数建议

---

## 9. 风险与缓解

- 风险：流式解压带来 CPU 开销上升  
  缓解：通过 chunk size 与并发配置调优。

- 风险：spill 过早导致 IO 压力  
  缓解：提供可调阈值，按表类型分层配置。

- 风险：fail-open 掩盖数据问题  
  缓解：告警分级 + 指标阈值触发后自动 fail-close（可选）。

