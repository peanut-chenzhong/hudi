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

### 1.3 哪些场景需要 append ROLLBACK_BLOCK

并非所有 rollback 都需要 append `ROLLBACK_BLOCK`。只有当数据被 **追加** 到一个已存在的 log 文件中时（即 `baseInstantTime < rollbackInstant`），才需要写入 `ROLLBACK_BLOCK`。以下是完整分类：

| 表类型 | Action 类型 | 是否需要 ROLLBACK_BLOCK | 说明 |
|--------|------------|------------------------|------|
| COW | COMMIT_ACTION | ❌ | 直接删除 base 文件 |
| MOR | COMMIT_ACTION | ❌ | 直接删除 base 文件（compaction 产出） |
| MOR | COMPACTION_ACTION | ❌ | 直接删除 compaction 产出文件 |
| MOR | REPLACE_COMMIT_ACTION | ❌ | 直接删除相关文件 |
| MOR | DELTA_COMMIT - 首写 log | ❌ | `baseCommitTime == rollbackInstant`，整个文件可删除 |
| MOR | DELTA_COMMIT - 追加 log | ✅ | `baseCommitTime < rollbackInstant`，只能追加 ROLLBACK_BLOCK |

**本文关注的正是最后一种场景：MOR 表 DELTA_COMMIT 追加数据到已有 log 文件后需要 rollback 的情况。**

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
| OBS 等对象存储 | 无跨进程 append 租约保护 | **无法阻止并发 append，数据可能损坏** |

当前 rollback 的 marker callback 关键代码：

```java
// BaseRollbackHelper.getRollbackLogMarkerCallback()
public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
    // there may be existed marker file if fs support append. So always return true;
    createAppendMarker(logFileToAppend);
    return true;  // ← 总是返回 true，跳过冲突检测！
}
```

### 2.2 OBS 跨进程并发 Append 验证

通过实际测试验证了 OBS（华为云对象存储）的 append 租约行为：

| 测试场景 | 结果 | 说明 |
|---------|------|------|
| 同进程内并发 append | ✅ 有保护 | OBS 客户端级别的锁，阻止同一进程内并发 |
| 跨进程并发 append | ❌ 无保护 | **两个进程可同时 append 同一文件，数据可能损坏** |
| HDFS 并发 append | ✅ 有保护 | 文件级租约机制（`AlreadyBeingCreatedException`） |

**结论：OBS 等对象存储缺乏跨进程 append 互斥保护，rollback 与正常 writer 存在真实的并发写冲突风险。**

### 2.3 问题二：幽灵数据复活（Ghost Data Resurrection）

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

### 2.4 问题三：Archival 不感知 File Slice 物理状态

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

## 3. 解决方案：固定 Rollback Log 版本号

### 3.1 核心思路

**在 OCC 场景下，rollback 产生的 `ROLLBACK_BLOCK` 统一写入一个固定超高版本号（`999999999`）的 log 文件，而非 append 到现有的 log 文件。**

```
正常数据文件:    .fileId_baseTime.log.1_0-0-0      (数据 block)
正常数据文件:    .fileId_baseTime.log.2_1-0-0      (数据 block)
Rollback 文件:   .fileId_baseTime.log.999999999_0-0-0  (所有 ROLLBACK_BLOCK)
```

这一设计同时解决了上述所有问题：

| 问题 | 解决方式 |
|------|---------|
| 并发写冲突 | Rollback 写入固定高版本文件，正常 writer 写低版本文件，物理上是不同文件 |
| 幽灵数据复活 | ROLLBACK_BLOCK 仍然被写入，archival 后数据仍被正确失效 |
| Marker 旁路 | 写新文件不需要旁路 marker 检查 |
| 小文件数量 | 每个 file group 仅一个 rollback 文件，多次 rollback 复用同一文件 |

### 3.2 为什么使用固定版本号 999999999

#### 3.2.1 读取排序天然保证

`LogFileComparator` 的比较逻辑：先比 `baseCommitTime`，再比 `logVersion`，最后比 `writeToken`：

```java
// HoodieLogFile.LogFileComparator
public int compare(HoodieLogFile o1, HoodieLogFile o2) {
    if (baseInstantTime1.equals(baseInstantTime2)) {
        if (o1.getLogVersion() == o2.getLogVersion()) {
            return getWriteTokenComparator().compare(o1.getLogWriteToken(), o2.getLogWriteToken());
        }
        // compare by log-version when base-commit is same
        return Integer.compare(o1.getLogVersion(), o2.getLogVersion());
    }
    return baseInstantTime1.compareTo(baseInstantTime2);
}
```

`logVersion=999999999` 在同一 `baseCommitTime` 下**永远排最后**，保证 ROLLBACK_BLOCK 在所有数据 block 之后被读取。

#### 3.2.2 每个 file group 仅一个 rollback 文件

多次 rollback 操作都 append 到同一个 `.log.999999999_0-0-0` 文件，**减少小文件数量**。对比递增版本号方案：

```
方案对比（3 次 rollback 后）:

  递增版本号方案:                        固定版本号方案:
  .log.1_0-0-0   (数据)                 .log.1_0-0-0   (数据)
  .log.2_1-0-0   (数据)                 .log.2_1-0-0   (数据)
  .log.3_rollback-0-0  (ROLLBACK D5)    .log.999999999_0-0-0  (ROLLBACK D5 + D7 + D9)
  .log.4_rollback-0-0  (ROLLBACK D7)
  .log.5_rollback-0-0  (ROLLBACK D9)
  共 5 个文件                            共 3 个文件
```

#### 3.2.3 过滤简单直接

正常 Writer 只需判断 `logVersion < ROLLBACK_LOG_VERSION` 即可排除 rollback 文件，比基于 writeToken 的字符串匹配更高效。

#### 3.2.4 writeToken `0-0-0` 完全匹配现有正则

```java
// FSUtils.java
public static final Pattern LOG_FILE_PATTERN =
    Pattern.compile("^\\.(.+)_(.*)\\.(log|archive)\\.(\\d+)(_((\\d+)-(\\d+)-(\\d+))(.cdc)?)?");
```

`0-0-0` 完全匹配 `(\d+)-(\d+)-(\d+)` 模式，不会导致任何解析异常。

#### 3.2.5 版本号安全性

`logVersion` 存储为 `int` 类型，`Integer.MAX_VALUE = 2147483647`，`999999999 << 2147483647`，无溢出风险。
正常 log 文件版本从 `1` 开始递增，即使每秒 rollover 一次，达到 999999999 需要约 31 年，实际中不可能与 rollback 版本冲突。

### 3.3 正常 Writer 隔离机制

**核心问题：** 正常 Writer 在查找"最新 log 文件"时，必须排除 rollback 文件。否则会得到 `logVersion=999999999`，错误地尝试 append 到 rollback 文件。

#### 3.3.1 当前版本查找逻辑（需修改）

```java
// FSUtils.getLatestLogVersion()
public static Option<Pair<Integer, String>> getLatestLogVersion(
    HoodieStorage storage, StoragePath partitionPath,
    final String fileId, final String logFileExtension,
    final String baseCommitTime) throws IOException {
  Option<HoodieLogFile> latestLogFile =
      getLatestLogFile(getAllLogFiles(storage, partitionPath, fileId, logFileExtension, baseCommitTime));
  // ...
}

// 底层实现：使用反向比较器取最大值
private static Option<HoodieLogFile> getLatestLogFile(Stream<HoodieLogFile> logFiles) {
    return Option.fromJavaOptional(logFiles.min(HoodieLogFile.getReverseLogFileComparator()));
}
```

如果存在 `.log.999999999` 文件，`getLatestLogVersion()` 会返回 `999999999`，导致正常 Writer 错误地定位到 rollback 文件。

#### 3.3.2 版本过滤方案

在 `FSUtils` 中增加过滤方法，排除 rollback 版本的文件：

```java
// FSUtils.java - 新增方法
public static Stream<HoodieLogFile> getAllNonRollbackLogFiles(
    HoodieStorage storage, StoragePath partitionPath,
    final String fileId, final String logFileExtension,
    final String baseCommitTime) throws IOException {
  return getAllLogFiles(storage, partitionPath, fileId, logFileExtension, baseCommitTime)
      .filter(f -> f.getLogVersion() < HoodieLogFile.ROLLBACK_LOG_VERSION);
}

public static Option<Pair<Integer, String>> getLatestNonRollbackLogVersion(
    HoodieStorage storage, StoragePath partitionPath,
    final String fileId, final String logFileExtension,
    final String baseCommitTime) throws IOException {
  Option<HoodieLogFile> latestLogFile =
      getLatestLogFile(getAllNonRollbackLogFiles(storage, partitionPath, fileId, logFileExtension, baseCommitTime));
  if (latestLogFile.isPresent()) {
    return Option.of(Pair.of(latestLogFile.get().getLogVersion(), latestLogFile.get().getLogWriteToken()));
  }
  return Option.empty();
}
```

#### 3.3.3 WriterBuilder 修改

在 `WriterBuilder.build()` 中，正常 Writer 使用过滤后的版本查询：

```java
// HoodieLogFormat.WriterBuilder.build() - 修改后
if (logVersion == null) {
    // 使用过滤后的方法，排除 rollback 文件
    Option<Pair<Integer, String>> versionAndWriteToken =
        FSUtils.getLatestNonRollbackLogVersion(storage, parentPath, logFileId, fileExtension, instantTime);
    if (versionAndWriteToken.isPresent()) {
        logVersion = versionAndWriteToken.get().getKey();
        logWriteToken = versionAndWriteToken.get().getValue();
    } else {
        logVersion = HoodieLogFile.LOGFILE_BASE_VERSION;
        logWriteToken = rolloverLogWriteToken;
    }
}
```

正常 Writer 的行为变化：

```
修改前:
  getAllLogFiles() → [.log.1, .log.2, .log.999999999] → latest = .log.999999999 ❌

修改后:
  getAllNonRollbackLogFiles() → [.log.1, .log.2] → latest = .log.2 ✅
  正常 Writer append 到 .log.2，与 rollback 文件无交集
```

### 3.4 可行性验证：Log Reader 对跨文件 ROLLBACK_BLOCK 的处理

#### 3.4.1 V1 扫描路径

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

#### 3.4.2 V2 扫描路径

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

#### 3.4.3 COMMAND_BLOCK 免于 Timeline 过滤

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

#### 3.4.4 读取顺序保证

`HoodieLogFormatReader` 按列表顺序逐个读取 log 文件，file group 的 log 文件按版本号升序排列：

```
.log.1           (数据 block D3, D5)    → 先读
.log.2           (数据 block D7)        → 次读
.log.999999999   (ROLLBACK_BLOCK D5)    → 最后读，使 D5 失效
```

**由于版本号 999999999 远大于任何正常版本，ROLLBACK_BLOCK 保证在所有数据 block 之后被处理。** 这对 V1 路径（使用 deque 的 `removeIf`）和 V2 路径（使用全局 map）都是正确的。

**结论：两种扫描路径 + timeline 豁免 + 版本号天然排序，全部正确支持固定版本号 rollback 文件中的 ROLLBACK_BLOCK。**

### 3.5 多次 Rollback 复用同一文件

同一个 file group 的多次 rollback，所有 `ROLLBACK_BLOCK` 都 append 到同一个 `.log.999999999_0-0-0` 文件：

```
第一次 rollback D5:
  .log.999999999_0-0-0 不存在 → 创建，写入 ROLLBACK_BLOCK(target=D5)

第二次 rollback D7:
  .log.999999999_0-0-0 已存在 → append，写入 ROLLBACK_BLOCK(target=D7)

文件最终内容:
  .log.999999999_0-0-0:
    ├── ROLLBACK_BLOCK { TARGET_INSTANT_TIME: "D5" }
    └── ROLLBACK_BLOCK { TARGET_INSTANT_TIME: "D7" }
```

每个 `ROLLBACK_BLOCK` 有独立的 `TARGET_INSTANT_TIME` header，Log Reader 会逐个处理，互不干扰。

**OCC 下 clean 是串行的**（同一时间只有一个 clean 操作），因此多次 rollback 不会并发 append 到 `.log.999999999` 文件。

## 4. 实现方案

### 4.1 新增常量

```java
// HoodieLogFile.java
public static final int ROLLBACK_LOG_VERSION = 999999999;
public static final String ROLLBACK_WRITE_TOKEN = "0-0-0";
```

### 4.2 修改 `BaseRollbackHelper` 中的 log writer 创建逻辑

在 OCC（LAZY）场景下，rollback 写 `ROLLBACK_BLOCK` 时指定固定版本号：

```java
// BaseRollbackHelper.maybeDeleteAndCollectStats() - 修改后
writer = HoodieLogFormat.newWriterBuilder()
    .onParentPath(FSUtils.constructAbsolutePath(metaClient.getBasePathV2().toString(), partitionPath))
    .withFileId(fileId)
    .overBaseCommit(latestBaseInstant)
    .withStorage(metaClient.getStorage())
    .withLogWriteCallback(getRollbackLogMarkerCallback(writeMarkers, partitionPath, fileId))
    .withLogVersion(HoodieLogFile.ROLLBACK_LOG_VERSION)       // ← 固定版本号 999999999
    .withLogWriteToken(HoodieLogFile.ROLLBACK_WRITE_TOKEN)    // ← 固定 writeToken 0-0-0
    .withSizeThreshold(Long.MAX_VALUE)                        // ← 禁止 size-based rollover
    .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
    .build();
```

关键变化：
- `withLogVersion(999999999)`：显式指定版本号，跳过 `getLatestLogVersion()` 的自动计算
- `withLogWriteToken("0-0-0")`：使用固定 writeToken，文件名确定
- `withSizeThreshold(Long.MAX_VALUE)`：禁止 rollback 文件因大小触发 rollover

由于 `WriterBuilder.build()` 中 `logVersion != null` 时跳过版本计算：

```java
// HoodieLogFormat.WriterBuilder.build()
if (logVersion == null) {
    // 版本计算逻辑...
}
// logVersion 已设置，直接跳过
```

构造出的文件路径为 `.fileId_baseTime.log.999999999_0-0-0`。如果文件已存在则 append，不存在则创建。

### 4.3 修改 `FSUtils` 版本查找方法

增加排除 rollback 版本的方法，供正常 Writer 使用：

```java
// FSUtils.java - 新增

/**
 * Get all non-rollback log files for the passed in file-id.
 * Excludes log files with ROLLBACK_LOG_VERSION used by rollback operations in OCC mode.
 */
public static Stream<HoodieLogFile> getAllNonRollbackLogFiles(
    HoodieStorage storage, StoragePath partitionPath,
    final String fileId, final String logFileExtension,
    final String baseCommitTime) throws IOException {
  return getAllLogFiles(storage, partitionPath, fileId, logFileExtension, baseCommitTime)
      .filter(f -> f.getLogVersion() < HoodieLogFile.ROLLBACK_LOG_VERSION);
}

/**
 * Get the latest non-rollback log version for the fileId in the partition path.
 */
public static Option<Pair<Integer, String>> getLatestNonRollbackLogVersion(
    HoodieStorage storage, StoragePath partitionPath,
    final String fileId, final String logFileExtension,
    final String baseCommitTime) throws IOException {
  Option<HoodieLogFile> latestLogFile =
      getLatestLogFile(getAllNonRollbackLogFiles(storage, partitionPath, fileId, logFileExtension, baseCommitTime));
  if (latestLogFile.isPresent()) {
    return Option.of(Pair.of(latestLogFile.get().getLogVersion(), latestLogFile.get().getLogWriteToken()));
  }
  return Option.empty();
}
```

### 4.4 修改 `WriterBuilder.build()` 版本查找逻辑

正常 Writer 使用过滤后的版本查询：

```java
// HoodieLogFormat.WriterBuilder.build() - 修改后
if (logVersion == null) {
    LOG.info("Computing the next log version for " + logFileId + " in " + parentPath);
    Option<Pair<Integer, String>> versionAndWriteToken =
        FSUtils.getLatestNonRollbackLogVersion(storage, parentPath, logFileId, fileExtension, instantTime);
    if (versionAndWriteToken.isPresent()) {
        logVersion = versionAndWriteToken.get().getKey();
        logWriteToken = versionAndWriteToken.get().getValue();
    } else {
        logVersion = HoodieLogFile.LOGFILE_BASE_VERSION;
        logWriteToken = rolloverLogWriteToken;
    }
}
```

> **注意**：rollback 调用时通过 `.withLogVersion(ROLLBACK_LOG_VERSION)` 显式设置了 `logVersion`，因此 `logVersion != null`，不会进入此分支。正常 Writer 的 `logVersion == null`，使用过滤后的查询。两条路径互不影响。

### 4.5 修改 `computeNextLogVersion`

确保 rollover 时也排除 rollback 版本：

```java
// FSUtils.java - 修改
public static int computeNextLogVersion(HoodieStorage storage, StoragePath partitionPath,
    final String fileId, final String logFileExtension,
    final String baseCommitTime) throws IOException {
  Option<Pair<Integer, String>> currentVersionWithWriteToken =
      getLatestNonRollbackLogVersion(storage, partitionPath, fileId, logFileExtension, baseCommitTime);
  return (currentVersionWithWriteToken.isPresent()) ? currentVersionWithWriteToken.get().getKey() + 1
      : HoodieLogFile.LOGFILE_BASE_VERSION;
}
```

### 4.6 完整调用链

```
正常 Writer 路径:
  HoodieLogFormat.newWriterBuilder()
    .build()  // logVersion == null
      └─→ FSUtils.getLatestNonRollbackLogVersion()  // 排除 .log.999999999
            └─→ 返回最新的正常 log 版本（如 version=2）
      └─→ append 到 .log.2_x-x-x

Rollback 路径 (OCC):
  HoodieLogFormat.newWriterBuilder()
    .withLogVersion(ROLLBACK_LOG_VERSION)     // 999999999
    .withLogWriteToken(ROLLBACK_WRITE_TOKEN)  // "0-0-0"
    .build()  // logVersion != null, 跳过版本计算
      └─→ 直接定位 .log.999999999_0-0-0
            ├─ 文件已存在 → append ROLLBACK_BLOCK
            └─ 文件不存在 → 创建并写入 ROLLBACK_BLOCK
```

### 4.7 判断是否为 OCC/多 Writer 模式

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

仅在 OCC 模式下使用固定版本号，单 Writer 模式保持原有行为不变。

## 5. 正确性论证

### 5.1 Rollback 正确性

```
File Group: fg-001
├── .fg-001_20230101.log.1_0-0-0  (包含 D3 的数据 block, D5 的数据 block)
├── .fg-001_20230101.log.2_1-0-0  (包含 D7 的数据 block)
└── .fg-001_20230101.log.999999999_0-0-0  (ROLLBACK_BLOCK(target=D5))

Log Reader 读取顺序（按版本号升序）:
  .log.1         → 读到 D3 block (有效) ✅
  .log.1         → 读到 D5 block (有效，暂时加入)
  .log.2         → 读到 D7 block (有效) ✅
  .log.999999999 → 读到 ROLLBACK_BLOCK(target=D5) → 从内存中移除 D5 ✅

最终结果: D3 ✅, D7 ✅, D5 ❌ (被正确回滚)
```

### 5.2 防止幽灵数据复活

ROLLBACK_BLOCK 写入 `.log.999999999` 文件后：
1. **Archival 前**：timeline 中没有 D5 的 completed instant → 数据 block 被 timeline 过滤跳过
2. **Archival 后**：即使 D5 通过 `isBeforeTimelineStarts` 检查变为"看似有效"，ROLLBACK_BLOCK 仍会被处理（因为 `COMMAND_BLOCK` 免于 timeline 过滤），正确移除 D5 数据
3. **Compaction 后**：compaction 读取 log 时也会处理 ROLLBACK_BLOCK，D5 数据不会被合并到 base file

### 5.3 并发安全性

```
Writer A (正常写入 D8):               Writer B (rollback D5):
  │                                      │
  ├─ 查询最新版本                          ├─ 使用固定版本 999999999
  │  getLatestNonRollbackLogVersion()     │
  │  → version=2                          │
  │                                       │
  ├─ append 到 .log.2                     ├─ 写入 .log.999999999
  │  (正常数据文件)                         │  (ROLLBACK_BLOCK)
  │                                       │
  ├─ 关闭 .log.2                          ├─ 关闭 .log.999999999
  │                                       │
  ✅ 两个 writer 操作完全不同的文件，无并发冲突
```

### 5.4 多次 Rollback 正确性

```
File Group: fg-001

初始状态:
  .log.1_0-0-0  (D3, D5, D7 的数据)

第一次 Rollback (D5):
  .log.999999999_0-0-0 创建，写入 ROLLBACK_BLOCK(target=D5)

第二次 Rollback (D7):
  .log.999999999_0-0-0 存在，append ROLLBACK_BLOCK(target=D7)

Log Reader 读取:
  .log.1         → D3 ✅, D5 (暂存), D7 (暂存)
  .log.999999999 → ROLLBACK(D5) → 移除 D5 ✅
                 → ROLLBACK(D7) → 移除 D7 ✅

最终结果: D3 ✅, D5 ❌, D7 ❌ (均被正确回滚)
```

## 6. 与现有行为的兼容性

| 场景 | 现有行为 | 修改后行为 | 兼容性 |
|------|---------|-----------|--------|
| **单 Writer（EAGER）** | Append 到已有 log 文件 | 不变（不受影响） | ✅ 完全兼容 |
| **OCC 多 Writer（LAZY）+ HDFS** | 尝试 append，遇到 lease 冲突被动 rollover | 主动写入固定版本号文件 | ✅ 更安全 |
| **OCC 多 Writer（LAZY）+ OBS/S3** | 可能并发 append 同一文件导致损坏 | 写入不同文件，无并发冲突 | ✅ 修复问题 |
| **Log Reader V1** | 跨文件处理 ROLLBACK_BLOCK | 不变 | ✅ 已支持 |
| **Log Reader V2** | 全局 map 管理，跨文件处理 | 不变 | ✅ 已支持 |
| **Compaction** | 读取所有 log 文件并合并 | 多一个 rollback log 文件参与合并 | ✅ 兼容 |

## 7. 潜在影响与注意事项

### 7.1 Compaction 后的清理

Compaction 执行时会读取所有 log 文件（包括 `.log.999999999`），生成新 base file 后旧 log 文件（包括 rollback 文件）被 clean 清理。如果 compaction 后又发生新的 rollback，会重新创建 `.log.999999999`。这是正常行为。

### 7.2 Rollback 文件的 Size Rollover

Rollback 文件设置 `sizeThreshold = Long.MAX_VALUE`，不会因文件大小触发 rollover。每个 `ROLLBACK_BLOCK` 仅包含几十字节的 header 信息，即使数百次 rollback 也只占用极小空间。

### 7.3 Rollback 崩溃重试

如果 rollback 过程崩溃：
- **HDFS**：可能存在 lease 未释放的问题。但 Hudi 已有 `RecoveryInProgressException` 和 `AlreadyBeingCreatedException` 的处理逻辑（参见 `HoodieLogFormatWriter.handleAppendExceptionOrRecoverLease`），会自动恢复。
- **OBS**：同进程内有客户端级保护；跨进程无 lease 问题，崩溃后新进程直接 append 即可。
- **OCC 下 clean 是串行的**：不会出现两个 clean 同时尝试 rollback 到同一个 `.log.999999999` 的情况。

### 7.4 不影响 COW 表

本优化仅影响 MOR 表的 rollback 行为。COW 表的 rollback 通过删除文件实现，不涉及 log 文件 append。

### 7.5 不影响单 Writer 模式

在单 Writer（EAGER）模式下，rollback 发生在每次 commit 前（inline），不存在并发写冲突问题。本优化仅在 OCC（LAZY）模式下生效。

## 8. 影响的代码范围

### 8.1 需要修改的文件

| 文件 | 修改内容 |
|------|---------|
| `HoodieLogFile` | 新增 `ROLLBACK_LOG_VERSION = 999999999` 和 `ROLLBACK_WRITE_TOKEN = "0-0-0"` 常量 |
| `FSUtils` | 新增 `getAllNonRollbackLogFiles()`、`getLatestNonRollbackLogVersion()` 方法；修改 `computeNextLogVersion()` 排除 rollback 版本 |
| `HoodieLogFormat.WriterBuilder` | 修改 `build()` 方法，正常 Writer 使用 `getLatestNonRollbackLogVersion()` |
| `BaseRollbackHelper` | 在 OCC 模式下使用 `.withLogVersion(ROLLBACK_LOG_VERSION).withLogWriteToken(ROLLBACK_WRITE_TOKEN)` 构建 writer |

### 8.2 不需要修改的文件

| 文件 | 原因 |
|------|------|
| `AbstractHoodieLogRecordReader` | 已天然支持跨 log 文件的 ROLLBACK_BLOCK |
| `HoodieLogFormatReader` | 按顺序读取所有 log 文件，无需修改 |
| `HoodieLogFormatWriter` | 现有 append / create 逻辑已满足需求 |
| `HoodieTimelineArchiver` | Archival 逻辑不受影响 |
| `CleanPlanner` / `CleanerUtils` | Clean 逻辑不受影响 |
| `ListingBasedRollbackStrategy` | Rollback 策略层面不受影响 |
| `MarkerBasedRollbackStrategy` | Rollback 策略层面不受影响 |

## 9. 测试计划

### 9.1 单元测试

1. **基本功能测试**：验证 rollback 写入 `.log.999999999_0-0-0` 的 ROLLBACK_BLOCK 能正确失效目标数据 block
2. **跨文件 ROLLBACK_BLOCK 测试**：验证数据 block 在 `.log.N`、ROLLBACK_BLOCK 在 `.log.999999999` 的场景下 Log Reader 行为正确
3. **多次 Rollback 复用文件测试**：验证多次 rollback 的 ROLLBACK_BLOCK 都正确 append 到同一个 `.log.999999999` 文件
4. **V1 和 V2 扫描路径覆盖**：分别测试 `scanInternalV1` 和 `scanInternalV2` 的正确性
5. **版本过滤测试**：验证 `getLatestNonRollbackLogVersion()` 正确排除 `.log.999999999` 文件
6. **正常 Writer 隔离测试**：验证正常 Writer 不会 append 到 `.log.999999999` 文件

### 9.2 集成测试

1. **OCC 并发测试**：模拟多 writer 并发写入，其中一个 writer 失败，验证 clean 触发的 rollback 写入固定版本号文件
2. **Archival 后读取测试**：验证 archival 归档 rolled-back instant 后，Log Reader 仍能通过 ROLLBACK_BLOCK 正确过滤幽灵数据
3. **Compaction 集成测试**：验证 compaction 正确处理 `.log.999999999` 文件中的 ROLLBACK_BLOCK
4. **Compaction 后新 Rollback 测试**：验证 compaction 清理旧 rollback 文件后，新 rollback 能正确创建新的 `.log.999999999`

### 9.3 兼容性测试

1. **单 Writer 模式不受影响**：验证 EAGER 策略下 rollback 行为不变
2. **COW 表不受影响**：验证 COW 表 rollback 行为不变
3. **不同存储后端**：验证 HDFS、OBS、S3 上的行为一致性
4. **存量数据兼容**：验证升级后能正确读取不含 `.log.999999999` 的旧数据

## 10. 方案对比

### 10.1 vs. 递增版本号 + 特殊 WriteToken 方案

| 维度 | 固定版本号 999999999 (本方案) | 递增版本号 + 特殊 WriteToken |
|------|---------------------------|--------------------------|
| **文件数量** | ✅ 每个 file group 仅一个 rollback 文件 | ⚠️ 每次 rollback 产生一个新文件 |
| **读取排序** | ✅ 版本号天然保证最后读到 | ✅ 版本号递增也保证在后 |
| **过滤方式** | ✅ 按版本号过滤（整数比较，高效） | ⚠️ 按 writeToken 过滤（字符串匹配） |
| **rollback 间并发** | ⚠️ 多次 rollback append 同一文件（但 clean 串行） | ✅ 每次新建文件无冲突 |
| **实现复杂度** | 中等 | 中等 |
| **parseWriteToken 兼容** | ✅ `0-0-0` 完全兼容现有正则 | ⚠️ 特殊 token 需验证所有解析路径 |

### 10.2 vs. Null WriteToken 方案

| 维度 | 固定版本号 999999999 (本方案) | Null WriteToken |
|------|---------------------------|----------------|
| **文件数量** | ✅ 每个 file group 仅一个 rollback 文件 | ⚠️ 每次 rollback 产生新文件 |
| **正则兼容** | ✅ 完全匹配 LOG_FILE_PATTERN | ⚠️ null token 匹配旧格式，可能触发 rollover |
| **识别方式** | ✅ 版本号直接判断 | ⚠️ 需检查 writeToken 是否为 null |
| **writer 隔离** | ✅ 版本号过滤，简单直接 | ⚠️ 需额外逻辑跳过 null token 文件 |

## 11. 总结

本方案通过在 OCC 场景下 rollback 写 ROLLBACK_BLOCK 时**使用固定超高版本号 `999999999`**，以最小的改动代价同时解决了所有已知问题：

1. ✅ **并发写冲突**：rollback 和正常 writer 天然写入不同版本号的文件，物理隔离
2. ✅ **幽灵数据复活**：ROLLBACK_BLOCK 仍然存在于 `.log.999999999` 中，archival 后数据仍被正确失效
3. ✅ **Marker 旁路风险**：写新版本文件不涉及已有 marker
4. ✅ **小文件问题**：每个 file group 仅一个 rollback 文件，多次 rollback 复用
5. ✅ **OBS 等对象存储兼容**：正常 writer 和 rollback 操作不同文件，不依赖存储层租约机制

方案与现有 Log Reader 的 V1/V2 扫描路径完全兼容，修改范围小（4 个文件），实现简洁且风险可控。
