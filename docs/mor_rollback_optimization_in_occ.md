# MOR 表 OCC 场景下 Rollback 写 Log 文件优化设计

## 1. 背景

### 1.1 OCC 场景下的 Rollback 触发机制

在 Hudi 的乐观并发控制（OCC）场景下，`HoodieFailedWritesCleaningPolicy` 会被自动推断为 `LAZY`：

```java
// HoodieCleanConfig.java
public static final ConfigProperty<String> FAILED_WRITES_CLEANER_POLICY = ConfigProperty
    .key("hoodie.cleaner.policy.failed.writes")
    .defaultValue(HoodieFailedWritesCleaningPolicy.EAGER.name())
    .withInferFunction(cfg -> {
        // ...
        if (!mode.supportsOptimisticConcurrencyControl()) {
            return Option.empty();
        }
        return Option.of(HoodieFailedWritesCleaningPolicy.LAZY.name());
    })
```

在 `LAZY` 策略下，只有 `CLEAN_ACTION` 才会触发 `rollbackFailedWrites`（参见 `CleanerUtils.rollbackFailedWrites`）。
这意味着在 OCC 多 writer 并发场景中，**rollback 只在 clean 执行时发生**，而不是在每次 commit 前。

### 1.2 MOR 表 Rollback 的特殊性

对于 MOR（Merge-On-Read）表，rollback 操作不能简单删除 log 文件，因为一个 log 文件中可能包含多个 instant 的数据 block。
因此，MOR 表的 rollback 通过 **append 一个 `ROLLBACK_BLOCK`（`HoodieCommandBlock`）** 到 log 文件来逻辑失效已回滚的数据。

当前实现位于 `BaseRollbackHelper.maybeDeleteAndCollectStats()`：

```java
writer = HoodieLogFormat.newWriterBuilder()
    .onParentPath(FSUtils.constructAbsolutePath(metaClient.getBasePathV2().toString(), partitionPath))
    .withFileId(fileId)
    .overBaseCommit(latestBaseInstant)
    .withStorage(metaClient.getStorage())
    .withLogWriteCallback(getRollbackLogMarkerCallback(writeMarkers, partitionPath, fileId))
    .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

// append ROLLBACK_BLOCK to the existing log file
filePath = writer.appendBlock(new HoodieCommandBlock(header)).logFile().getPath();
```

这意味着 **rollback 会尝试 append 到当前 file group 已有的最新 log 文件**。

## 2. 问题分析

### 2.1 问题一：并发写冲突

在 OCC 场景下，clean 触发的 rollback 和正常 writer 可能**同时 append 同一个 log 文件**。

**时序示例：**

```
时间线: ─────────────────────────────────────────────────────>

Writer A (正常写入):
  [获取 log 文件句柄] ──────── [append 数据 block] ──────── [关闭]

Writer B (clean 触发 rollback):
             [获取同一 log 文件句柄] ── [append ROLLBACK_BLOCK] ── [关闭]
                      ^
                      │
                 两个 writer 同时写同一个 log 文件！
```

**现有保护层及其局限：**

| 保护层 | 机制 | 对 rollback 场景的局限 |
|--------|------|------------------------|
| Marker 文件 | `createIfNotExists` 原子操作 | Rollback 的 `preLogFileOpen` **总返回 `true`**，旁路了 marker 冲突检测 |
| HDFS Lease | 文件级排他锁 | 后来者抛出 `AlreadyBeingCreatedException`，被动 rollover |
| 云存储 | 不支持 append | 天然是 rollover 模式，无此问题 |
| 过期心跳检测 | 检测假死 writer 并触发 rollover | 仅保护正常 writer，不保护 rollback 操作 |

当前 rollback 的 marker callback 关键代码：

```java
// BaseRollbackHelper.getRollbackLogMarkerCallback()
public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
    // there may be existed marker file if fs support append. So always return true;
    createAppendMarker(logFileToAppend);
    return true;  // ← 总是返回 true，跳过冲突检测！
}
```

**风险**：在 HDFS 上，虽然 lease 机制会物理保护文件不被同时写入（后来者被动 rollover），但这是一种被动容错，依赖底层存储特性，不够健壮。

### 2.2 问题二：幽灵数据复活（Ghost Data Resurrection）

如果为了避免并发写冲突而选择**不写 `ROLLBACK_BLOCK`**（仅删除元数据文件），则存在严重的数据正确性风险。

**Log Reader 的 timeline 过滤逻辑：**

```java
// AbstractHoodieLogRecordReader.scanInternalV1()
if (logBlock.getBlockType() != CORRUPT_BLOCK && logBlock.getBlockType() != COMMAND_BLOCK) {
    if (!completedInstantsTimeline.containsOrBeforeTimelineStarts(instantTime)
        || inflightInstantsTimeline.containsInstant(instantTime)) {
        // 跳过未提交的 block
        continue;
    }
}
```

其中 `containsOrBeforeTimelineStarts` 的实现：

```java
// HoodieDefaultTimeline.java
public boolean containsOrBeforeTimelineStarts(String instant) {
    return containsInstant(instant) || isBeforeTimelineStarts(instant);
}

public boolean isBeforeTimelineStarts(String instant) {
    Option<HoodieInstant> firstNonSavepointCommit = getFirstNonSavepointCommit();
    return firstNonSavepointCommit.isPresent()
        && compareTimestamps(instant, LESSER_THAN, firstNonSavepointCommit.get().getTimestamp());
}
```

**`isBeforeTimelineStarts` 假设：所有早于 timeline 起始点的 instant 都是已归档的有效 commit。**

**幽灵数据复活场景：**

```
阶段 1: Active Timeline = [D1, D2, D3, ..., D30]
        D5 写入失败，需要 rollback

阶段 2: Rollback D5（仅删除元数据，不写 ROLLBACK_BLOCK）
        Active Timeline = [D1, D2, D3, D4, D6, ..., D30]
        D5 的数据 block 仍然在 log 文件中

阶段 3: Archival 运行（timeline instants > maxInstantsToKeep）
        归档 D1 ~ D10 到 archived timeline
        Active Timeline = [D11, D12, ..., D30]
        timeline 起始点 = D11

阶段 4: Log Reader 读取包含 D5 数据的 log 文件
        检查 D5: containsOrBeforeTimelineStarts("D5")
        → containsInstant("D5") = false（D5 已被删除）
        → isBeforeTimelineStarts("D5") = true（D5 < D11）
        → 结果：D5 的数据被错误地视为有效！ ❌ 幽灵数据复活！
```

### 2.3 问题三：Archival 不感知 File Slice 物理状态

用户可能认为"如果 log 文件未 compact，archival 不应该归档其 deltacommit"。但实际上，**archival 的决策完全基于 timeline 级别的属性，不关心 file slice 的物理状态**。

`HoodieTimelineArchiver.getCommitInstantsToArchive()` 的归档约束仅包括：

| 约束 | 说明 | 是否感知 file slice |
|------|------|---------------------|
| `maxInstantsToKeep` / `minInstantsToKeep` | Timeline 大小限制 | ❌ |
| `oldestPendingInstant` | 最早的 pending/inflight instant | ❌ |
| Savepoint | 不归档 savepoint 之后的 commit | ❌ |
| `oldestInstantToRetainForCompaction` | 保留足够 delta commit 触发 compaction 调度（**仅 NUM_COMMITS/NUM_AND_TIME 策略**） | ❌ |
| `oldestInstantToRetainForClustering` | Clustering 相关约束 | ❌ |
| Metadata Table Compaction | 不归档 metadata table 最后 compaction 之后的 instant | ❌ |

特别注意 `oldestInstantToRetainForCompaction`：

```java
// HoodieTimelineArchiver.getCommitInstantsToArchive()
Option<HoodieInstant> oldestInstantToRetainForCompaction =
    (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ
        && (config.getInlineCompactTriggerStrategy() == CompactionTriggerStrategy.NUM_COMMITS
        || config.getInlineCompactTriggerStrategy() == CompactionTriggerStrategy.NUM_AND_TIME))
        ? CompactionUtils.getOldestInstantToRetainForCompaction(...)
        : Option.empty();  // ← 其他策略下完全不保护！
```

这意味着：
1. 当 compaction 触发策略**不是** `NUM_COMMITS` 或 `NUM_AND_TIME` 时，**没有任何 compaction 相关的归档保护**
2. 即使有保护，也只是基于 delta commit **数量**，不是 file slice 物理状态
3. **Archival 完全可以在 file slice 未 clean 前归档其 deltacommit**

## 3. 解决方案

### 3.1 核心思路

**在 OCC 场景下 clean 触发的 rollback 写 `ROLLBACK_BLOCK` 时，强制 rollover 到新的 log 文件，而不是 append 到已有的 log 文件。**

这一方案同时解决了上述三个问题：

| 问题 | 解决方式 |
|------|---------|
| 并发写冲突 | Rollback 写新文件，不与正常 writer 争抢同一 log 文件 |
| 幽灵数据复活 | ROLLBACK_BLOCK 仍然被写入（只是写到新文件），archival 后数据仍被正确失效 |
| Marker 旁路 | 新文件不存在已有 marker，不需要旁路检查 |

### 3.2 可行性验证：Log Reader 对跨文件 ROLLBACK_BLOCK 的处理

#### 3.2.1 V1 扫描路径

V1 使用 `currentInstantLogBlocks`（`ArrayDeque`）跨所有 log 文件累积 block。当读到 ROLLBACK_BLOCK 时，通过 `removeIf` 从整个 deque 中移除匹配的 block，**不区分来源文件**：

```java
// AbstractHoodieLogRecordReader.scanInternalV1()
case ROLLBACK_BLOCK:
    currentInstantLogBlocks.removeIf(block -> {
        if (targetInstantForCommandBlock.contentEquals(
                block.getLogBlockHeader().get(INSTANT_TIME))) {
            return true;  // 移除匹配的 block，不管它来自哪个 log 文件
        }
        return false;
    });
    break;
```

#### 3.2.2 V2 扫描路径

V2 使用 `instantToBlocksMap` 和 `targetRollbackInstants` 全局管理。遇到 ROLLBACK_BLOCK 时直接从 map 中移除目标 instant 的所有 block：

```java
// AbstractHoodieLogRecordReader.scanInternalV2()
if (commandBlock.getType().equals(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK)) {
    targetRollbackInstants.add(targetInstantForCommandBlock);
    orderedInstantsList.remove(targetInstantForCommandBlock);
    instantToBlocksMap.remove(targetInstantForCommandBlock);  // 全局移除，不区分文件
}
```

V2 路径在设计注释中明确说明了这种多 writer 场景：

```
* With multi-writer mode the blocks can be out of sync. An example scenario.
* B1, B2, B3, B4, R1(B3), B5
* In this case, rollback block R1 is invalidating the B3 which is not the previous block.
```

#### 3.2.3 COMMAND_BLOCK 免于 Timeline 过滤

`COMMAND_BLOCK`（包括 ROLLBACK_BLOCK）不受 timeline 有效性检查，确保 **archival 后 ROLLBACK_BLOCK 仍被处理**：

```java
// V1 路径
if (logBlock.getBlockType() != CORRUPT_BLOCK && logBlock.getBlockType() != COMMAND_BLOCK) {
    if (!completedInstantsTimeline.containsOrBeforeTimelineStarts(instantTime)
        || inflightInstantsTimeline.containsInstant(instantTime)) {
        continue;  // ← COMMAND_BLOCK 不受此过滤
    }
}

// V2 路径
if (logBlock.getBlockType() != COMMAND_BLOCK) {
    if (!completedInstantsTimeline.containsOrBeforeTimelineStarts(instantTime)
        || inflightInstantsTimeline.containsInstant(instantTime)) {
        continue;  // ← COMMAND_BLOCK 不受此过滤
    }
}
```

#### 3.2.4 读取顺序保证

`HoodieLogFormatReader` 按列表顺序逐个读取 log 文件：

```java
// HoodieLogFormatReader 构造函数
if (!logFiles.isEmpty()) {
    HoodieLogFile nextLogFile = logFiles.remove(0);  // 按顺序读取
    this.currentReader = new HoodieLogFileReader(storage, nextLogFile, ...);
}
```

File group 的 log 文件按版本号升序排列（`.log.1` → `.log.2` → ...），因此：

```
.log.N   (旧文件): 包含 D5 的数据 block → 被读入
.log.N+1 (rollover 新文件): 包含 ROLLBACK_BLOCK(target=D5) → 使 D5 数据失效
```

**结论：两种扫描路径 + timeline 豁免 + 读取顺序，全部正确支持跨 log 文件的 ROLLBACK_BLOCK。**

### 3.3 实现方案

#### 3.3.1 修改 `BaseRollbackHelper` 中的 log writer 创建逻辑

在 OCC（LAZY）场景下，rollback 写 `ROLLBACK_BLOCK` 时强制使用 rollover 方式创建新的 log 文件，避免 append 到已有文件。

**修改点 1：`BaseRollbackHelper.maybeDeleteAndCollectStats()`**

```java
// 现有代码
writer = HoodieLogFormat.newWriterBuilder()
    .onParentPath(FSUtils.constructAbsolutePath(metaClient.getBasePathV2().toString(), partitionPath))
    .withFileId(fileId)
    .overBaseCommit(latestBaseInstant)
    .withStorage(metaClient.getStorage())
    .withLogWriteCallback(getRollbackLogMarkerCallback(writeMarkers, partitionPath, fileId))
    .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

// 修改为：在 OCC 场景下强制使用新 log 文件
writer = HoodieLogFormat.newWriterBuilder()
    .onParentPath(FSUtils.constructAbsolutePath(metaClient.getBasePathV2().toString(), partitionPath))
    .withFileId(fileId)
    .overBaseCommit(latestBaseInstant)
    .withStorage(metaClient.getStorage())
    .withLogWriteCallback(getRollbackLogMarkerCallback(writeMarkers, partitionPath, fileId))
    .withRolloverLogWriteToken(FSUtils.makeWriteToken(
        TaskContextSupplier.getPartitionIdFromTaskId(),
        TaskContextSupplier.getStageIdFromTaskId(),
        TaskContextSupplier.getAttemptIdFromTaskId()))
    .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
    .build();

// 在 OCC/多 writer 场景下，构建 writer 后强制 rollover 到新文件
if (isMultiWriterMode) {
    // 不 append 到已有 log 文件，而是创建新版本的 log 文件
    // 这样 ROLLBACK_BLOCK 会被写入 .log.N+1 而不是 .log.N
}
```

**修改点 2：`HoodieLogFormat.WriterBuilder` 增加 `forceNewFile` 选项**

```java
// HoodieLogFormat.WriterBuilder
private boolean forceNewFile = false;

public WriterBuilder withForceNewFile(boolean forceNewFile) {
    this.forceNewFile = forceNewFile;
    return this;
}
```

在 `HoodieLogFormatWriter` 初始化逻辑中，如果 `forceNewFile` 为 `true`，则跳过 append 尝试，直接创建新版本的 log 文件。

#### 3.3.2 修改调用链

```
BaseRollbackHelper.maybeDeleteAndCollectStats()
  └─→ HoodieLogFormat.newWriterBuilder()
        .withForceNewFile(isOCCMode)   // ← 新增：OCC 模式下强制新文件
        .build()
  └─→ writer.appendBlock(new HoodieCommandBlock(header))
        └─→ HoodieLogFormatWriter.appendBlock()
              └─→ 如果 forceNewFile，不尝试 open existing，直接 rollOver + createNewFile
```

#### 3.3.3 判断是否为 OCC/多 Writer 模式

通过 `HoodieWriteConfig` 中的并发模式配置来判断：

```java
boolean isMultiWriterMode = config.getWriteConcurrencyMode()
    .supportsOptimisticConcurrencyControl();
```

或者通过 `HoodieFailedWritesCleaningPolicy` 来判断：

```java
boolean isLazyCleanPolicy = config.getFailedWritesCleanPolicy()
    .equals(HoodieFailedWritesCleaningPolicy.LAZY);
```

### 3.4 与现有行为的兼容性

| 场景 | 现有行为 | 修改后行为 | 兼容性 |
|------|---------|-----------|--------|
| **单 Writer（EAGER）** | Append 到已有 log 文件 | 不变（不受影响） | ✅ 完全兼容 |
| **OCC 多 Writer（LAZY）+ HDFS** | 尝试 append，遇到 lease 冲突被动 rollover | 主动 rollover 到新文件 | ✅ 更安全 |
| **OCC 多 Writer（LAZY）+ 云存储** | 天然不支持 append，已是新文件模式 | 行为一致 | ✅ 完全兼容 |
| **Log Reader V1** | 跨文件处理 ROLLBACK_BLOCK | 不变 | ✅ 已支持 |
| **Log Reader V2** | 全局 map 管理，跨文件处理 | 不变 | ✅ 已支持 |
| **Compaction** | 读取所有 log 文件并合并 | 多一个小 log 文件参与合并 | ✅ 兼容 |

## 4. 正确性论证

### 4.1 Rollback 正确性

**Rollover 后 ROLLBACK_BLOCK 仍能正确失效目标数据 block：**

```
File Group: fg-001
├── fg-001_001.20230101.log.1  (包含 D3 的数据 block, D5 的数据 block)
├── fg-001_001.20230101.log.2  (包含 D7 的数据 block)
└── fg-001_002.20230101.log.3  (rollover 新文件，包含 ROLLBACK_BLOCK(target=D5))

Log Reader 读取顺序:
  .log.1 → 读到 D3 block (有效) ✅
  .log.1 → 读到 D5 block (有效，暂时加入)
  .log.2 → 读到 D7 block (有效) ✅
  .log.3 → 读到 ROLLBACK_BLOCK(target=D5) → 从内存中移除 D5 ✅

最终结果: D3 ✅, D7 ✅, D5 ❌ (被正确回滚)
```

### 4.2 防止幽灵数据复活

ROLLBACK_BLOCK 写入新 log 文件后：
1. **Archival 前**：timeline 中没有 D5 的 completed instant → 数据 block 被 timeline 过滤跳过
2. **Archival 后**：即使 D5 通过 `isBeforeTimelineStarts` 检查变为"看似有效"，ROLLBACK_BLOCK 仍会被处理（因为 `COMMAND_BLOCK` 免于 timeline 过滤），正确移除 D5 数据
3. **Compaction 后**：compaction 读取 log 时也会处理 ROLLBACK_BLOCK，D5 数据不会被合并到 base file

### 4.3 并发安全性

```
Writer A (正常写入 D8):           Writer B (rollback D5):
  │                                  │
  ├─ append to .log.2                ├─ rollover → 创建 .log.3
  │  (继续写入已有文件)                 ├─ 写 ROLLBACK_BLOCK 到 .log.3
  │                                  │  (写全新文件，无冲突)
  ├─ 关闭 .log.2                     ├─ 关闭 .log.3
  │                                  │
  ✅ 两个 writer 操作不同文件，无并发冲突
```

## 5. 潜在影响与注意事项

### 5.1 小文件增多

每次 rollback 每个 file group 会多产生一个仅包含 `ROLLBACK_BLOCK` 的小 log 文件（几乎没有数据，仅有 header 信息）。

**缓解措施：**
- 这些小文件在下次 compaction 时会被自然合并
- Rollback 本身是低频操作（仅在 clean 时触发，且仅针对失败的写入）
- 对整体文件数量的影响极小

### 5.2 Log 文件版本号跳跃

Rollover 会导致 log 文件版本号不连续（例如 `.log.1` → `.log.3`，跳过 `.log.2`），但这在 Hudi 中是已有的、被支持的行为（HDFS lease 冲突时的 rollover 也会导致版本号跳跃）。

### 5.3 不影响 COW 表

本优化仅影响 MOR 表的 rollback 行为。COW 表的 rollback 通过删除文件实现，不涉及 log 文件 append。

### 5.4 不影响单 Writer 模式

在单 Writer（EAGER）模式下，rollback 发生在每次 commit 前（inline），不存在并发写冲突问题。本优化仅在 OCC（LAZY）模式下生效。

## 6. 影响的代码范围

### 6.1 需要修改的文件

| 文件 | 修改内容 |
|------|---------|
| `HoodieLogFormat.WriterBuilder` | 增加 `forceNewFile` 选项 |
| `HoodieLogFormatWriter` | 支持 `forceNewFile` 模式，构建时直接创建新文件 |
| `BaseRollbackHelper` | 在 OCC 模式下使用 `forceNewFile` 选项构建 writer |

### 6.2 不需要修改的文件

| 文件 | 原因 |
|------|------|
| `AbstractHoodieLogRecordReader` | 已天然支持跨 log 文件的 ROLLBACK_BLOCK |
| `HoodieLogFormatReader` | 按顺序读取所有 log 文件，无需修改 |
| `HoodieTimelineArchiver` | Archival 逻辑不受影响 |
| `CleanPlanner` / `CleanerUtils` | Clean 逻辑不受影响 |

## 7. 测试计划

### 7.1 单元测试

1. **基本功能测试**：验证 rollover 模式写入的 ROLLBACK_BLOCK 能正确失效目标数据 block
2. **跨文件 ROLLBACK_BLOCK 测试**：验证数据 block 在 `.log.N`、ROLLBACK_BLOCK 在 `.log.N+1` 的场景下 log reader 行为正确
3. **V1 和 V2 扫描路径覆盖**：分别测试 `scanInternalV1` 和 `scanInternalV2` 的正确性

### 7.2 集成测试

1. **OCC 并发测试**：模拟多 writer 并发写入，其中一个 writer 失败，验证 clean 触发的 rollback 使用 rollover 方式
2. **Archival 后读取测试**：验证 archival 归档 rolled-back instant 后，log reader 仍能通过 ROLLBACK_BLOCK 正确过滤幽灵数据
3. **Compaction 集成测试**：验证 compaction 正确处理 rollover 方式写入的 ROLLBACK_BLOCK

### 7.3 兼容性测试

1. **单 Writer 模式不受影响**：验证 EAGER 策略下 rollback 行为不变
2. **COW 表不受影响**：验证 COW 表 rollback 行为不变
3. **不同存储后端**：验证 HDFS 和云存储（S3/GCS）上的行为一致性

## 8. 总结

本方案通过在 OCC 场景下 rollback 写 ROLLBACK_BLOCK 时**强制 rollover 到新的 log 文件**，以最小的改动代价同时解决了三个问题：

1. ✅ **并发写冲突**：rollback 和正常 writer 不再争抢同一 log 文件
2. ✅ **幽灵数据复活**：ROLLBACK_BLOCK 仍然存在，archival 后数据仍被正确失效
3. ✅ **Marker 旁路风险**：写新文件不需要旁路 marker 检查

方案与现有 log reader 的 V1/V2 扫描路径完全兼容，与云存储的已有行为一致，实现简洁且风险可控。
