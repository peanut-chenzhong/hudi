# 分区级并发早期冲突检测机制

## 概述

本文档描述了 Hudi 分区级并发写入的完整冲突检测与保护机制，包括：

1. **分区级早期冲突检测（ECD）** — 检测活跃 writer 之间的分区冲突，抛异常阻止并发写入
2. **Task 重试文件保护** — 通过 marker 文件防止 Spark/Flink task 重试导致的文件损坏
3. **过期心跳分区冲突检测（新增）** — 检测"心跳假死"writer 的分区冲突，触发 rollover 写新文件

---

## 一、分区级早期冲突检测（ECD）

### 核心设计目标

1. **分区级冲突检测**：不同 instant（作业）写同一分区时检测冲突
2. **减少 ZK 压力**：只在首次创建分区目录时获取锁，后续 task 跳过锁
3. **高效并发**：不同分区的写入完全并行，无锁竞争

### 详细流程图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        分区级并发早期冲突检测流程                                  │
│                 PartitionTransactionDirectMarkerBasedDetectionStrategy           │
│            （含过期心跳分区冲突检测，与活跃心跳检测共享同一次 .temp listing）        │
└─────────────────────────────────────────────────────────────────────────────────┘

                              ┌──────────────────┐
                              │   Task 开始写入   │
                              │  (Executor 侧)    │
                              └────────┬─────────┘
                                       │
                                       ▼
                    ┌─────────────────────────────────────┐
                    │ DirectWriteMarkers                   │
                    │ .createWithEarlyConflictDetection()  │
                    └────────────────┬────────────────────┘
                                     │
                                     ▼
              ┌──────────────────────────────────────────────────┐
              │ PartitionTransactionDirectMarkerBasedDetection   │
              │ Strategy.detectAndResolveConflictIfNecessary()   │
              └──────────────────────┬───────────────────────────┘
                                     │
                                     ▼
                ╔════════════════════════════════════════════╗
                ║  检查分区目录是否存在？                      ║
                ║  .temp/{instantTime}/{partitionPath}/       ║
                ║  【第一次检查，无需获取锁】                   ║
                ╚════════════════════╤═══════════════════════╝
                                     │
                   ┌─────────────────┴─────────────────┐
                   │                                   │
                   ▼                                   ▼
        ┌─────────────────────┐              ┌─────────────────────┐
        │   目录已存在         │              │    目录不存在        │
        │ (快速路径)           │              │   (慢速路径)         │
        │                     │              └──────────┬──────────┘
        │  仍需检查过期心跳：  │                         │
        │  hasExpiredHeartbeat │                         ▼
        │  PartitionConflict  │      ┌───────────────────────────────┐
        │  (独立 list .temp)  │      │   获取 ZK 分区锁               │
        │                     │      │   Lock Key: partition_lock_   │
        │  结果 → 存入标志位   │      │   {partitionPath}             │
        └────────┬────────────┘      └───────────────┬───────────────┘
                 │                                   │
                 │                                   ▼
                 │                   ╔═══════════════════════════════╗
                 │                   ║  Double-Check:                 ║
                 │                   ║  分区目录是否存在？            ║
                 │                   ║  【持有锁状态下检查】          ║
                 │                   ╚═══════════════╤═══════════════╝
                 │                                   │
                 │                   ┌───────────────┴───────────────┐
                 │                   │                               │
                 │                   ▼                               ▼
                 │        ┌─────────────────┐            ┌──────────────────────┐
                 │        │   目录已存在     │            │    目录仍不存在       │
                 │        │(其他task已创建)  │            │   (需要冲突检测)      │
                 │        └────────┬────────┘            └───────────┬──────────┘
                 │                 │                                 │
                 │                 │                                 ▼
                 │                 │              ┌──────────────────────────────────────┐
                 │                 │              │  List .temp 目录（仅一次 IO）         │
                 │                 │              │         │                             │
                 │                 │              │         ▼                             │
                 │                 │              │  classifyInstantsByHeartbeat()        │
                 │                 │              │  单次遍历，每个instant心跳只读一次    │
                 │                 │              │  ┌────────────┐  ┌────────────────┐  │
                 │                 │              │  │活跃心跳组   │  │过期心跳组       │  │
                 │                 │              │  │→分区冲突检测│  │→hasExpiredHB   │  │
                 │                 │              │  │             │  │ InPartition()  │  │
                 │                 │              │  └──────┬─────┘  └──────┬─────────┘  │
                 │                 │              │         │               │             │
                 │                 │              │         ▼               ▼             │
                 │                 │              │     有/无冲突      结果→标志位        │
                 │                 │              └─────────┬───────────────┬─────────────┘
                 │                 │                        │               │
                 │                 │            ┌───────────┴──────┐        │
                 │                 │            │                  │        │
                 │                 │            ▼                  ▼        │
                 │                 │  ┌─────────────────┐  ┌────────────┐  │
                 │                 │  │  检测到冲突！    │  │  无冲突    │  │
                 │                 │  │ (活跃instant     │  │            │  │
                 │                 │  │  在写同一分区)   │  └─────┬──────┘  │
                 │                 │  └────────┬────────┘        │         │
                 │                 │           │                 ▼         │
                 │                 │           │    ┌──────────────────┐   │
                 │                 │           │    │ 创建分区目录      │   │
                 │                 │           │    │ .temp/{instant}/ │   │
                 │                 │           │    │ {part}/ 占位符   │   │
                 │                 │           │    └────────┬─────────┘   │
                 │                 │           │             │             │
                 │                 │           ▼             ▼             │
                 │                 │  ┌────────────────┐  ┌────────────┐  │
                 │                 │  │ 释放锁          │  │ 释放锁     │  │
                 │                 │  │ 抛出冲突异常    │  └─────┬──────┘  │
                 │                 │  │ 任务失败重试    │        │         │
                 │                 │  └────────────────┘        │         │
                 │                 │                             │         │
                 │                 ▼                             │         │
                 │    ┌───────────────────┐                     │         │
                 │    │    释放锁          │                     │         │
                 │    └─────────┬─────────┘                     │         │
                 │              │                                │         │
                 └──────────────┼────────────────────────────────┘         │
                                │                                          │
                                │◄─────────────────────────────────────────┘
                                │     (expiredHeartbeatPartitionConflictDetected
                                │      标志位已在扫描中设置)
                                ▼
              ╔══════════════════════════════════════════════════════╗
              ║  检查过期心跳标志位                                    ║
              ║  strategy.isExpiredHeartbeatPartitionConflict        ║
              ║  Detected() == true ?                                ║
              ╚═══════════════════════╤════════════════════════════╝
                                      │
                    ┌─────────────────┴─────────────────┐
                    │                                   │
                    ▼                                   ▼
         ┌──────────────────────┐          ┌───────────────────────────┐
         │  有过期心跳分区冲突   │          │  无过期心跳冲突            │
         │  return Option.empty │          │                           │
         │  → rollover 写新文件 │          │  DirectWriteMarkers       │
         └──────────────────────┘          │  .create() 创建 marker    │
                                           │  {file}.marker.{IOType}   │
                                           └─────────────┬─────────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │    开始写数据文件    │
                                              └─────────────────────┘
```

### Marker 文件目录结构

```
/data/hudi/my_table/                           # 表基础路径 (basePath)
├── .hoodie/                                   # 元数据目录
│   ├── .temp/                                 # 临时目录
│   │   ├── 20240115120000000/                 # Flink 作业的 instant
│   │   │   └── year=2024/month=01/            # 分区目录（首次写入时创建）
│   │   │       ├── file-abc.parquet.marker.CREATE
│   │   │       └── file-def.parquet.marker.MERGE
│   │   │
│   │   └── 20240115120000100/                 # Spark 作业的 instant
│   │       └── year=2024/month=02/            # 不同分区，无冲突
│   │           └── file-xyz.parquet.marker.CREATE
│   │
│   └── .heartbeat/                            # 心跳目录
│       ├── 20240115120000000                  # Flink 作业心跳文件
│       └── 20240115120000100                  # Spark 作业心跳文件
```

### ZK 锁优化效果

| 场景 | 锁获取次数 |
|-----|----------|
| 100 个 task 写同一分区 | **1 次** |
| 100 个 task 写 10 个分区 | **10 次** |
| 1000 个 task 写 100 个分区 | **100 次** |

### getCandidateInstants 过滤逻辑

`MarkerUtils.getCandidateInstants()` 过滤候选 instant 的规则：

1. 跳过当前 writer 自身的 instant
2. 跳过 `currentInstantTime` 之后的 instant
3. **跳过心跳超时的 instant**（认为 writer 已死）
4. 跳过 pending compaction / pending replace instant

> **注意**：第 3 步的"跳过心跳超时"导致了一个安全漏洞——"心跳假死"场景。详见第三章。

---

## 二、Task 重试文件保护

### 问题场景

在 Spark/Flink 中，task 失败后会被调度器重试。但"假死" task 可能仍在写入同一个 log 文件，
与重试 task 并发写入导致文件损坏。

### Spark 保护机制（HoodieWriteHandle）

```java
// HoodieWriteHandle.AppendLogWriteCallback
public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
    // createIfNotExists: 如果 marker 已存在 → 返回 empty → 返回 false → rollover
    return createAppendMarker(logFileToAppend);
}
```

- marker 文件的 `createIfNotExists` 语义天然提供了原子性保护
- 第一个创建 marker 的 task attempt 获得写入权
- 后续 attempt 发现 marker 已存在 → `preLogFileOpen` 返回 `false` → 触发 rollover

### Flink 保护机制（FlinkAppendHandle）

```java
// FlinkAppendHandle - 通过 createdMarkers Set 区分自身 vs 其他 attempt
public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
    String markerKey = partitionPath + "/" + logFileToAppend.getFileName();

    if (createdMarkers != null) {
        if (createdMarkers.contains(markerKey)) {
            return true;  // 自己在之前 mini-batch 创建的，安全追加
        }
        Option<StoragePath> result = writeMarkers.createIfNotExists(...);
        if (result.isPresent()) {
            createdMarkers.add(markerKey);  // 记录为自己创建
            return true;
        } else {
            return false;  // 其他 attempt 创建的，触发 rollover
        }
    }
    // 兼容模式：无 task retry 保护
    writeMarkers.createIfNotExists(...);
    return true;
}
```

- `createdMarkers`（`Set<String>`）跟踪当前 task 实例创建的 marker
- 区分"自己在之前 mini-batch 创建的 marker"（允许追加）vs"其他 attempt 创建的 marker"（rollover）

---

## 三、过期心跳分区冲突检测（新增）

### 问题背景

现有 ECD 机制中 `getCandidateInstants` 会**跳过心跳超时的 instant**（认为 writer 已死）。
但存在"心跳假死"场景：

```
Writer A:  心跳因 GC pause / 网络抖动超时，但 task 实际还在运行并写入 log 文件
Writer B:  开始写入同一分区
           ↓
           ECD 扫描 → 发现 Writer A 心跳超时 → 跳过 → 认为无冲突
           ↓
           Writer B 追加到与 Writer A 相同的 log 文件 → 数据损坏！
```

### 解决方案：集成到 ECD 扫描点

过期心跳检测**直接集成到 ECD 的 `.temp` 目录扫描流程中**，
与活跃心跳冲突检测复用同一次 `listDirectEntries` 调用，避免重复 IO。

```
createWithEarlyConflictDetection() 调用链:
  │
  ├─ Step 1: strategy.detectAndResolveConflictIfNecessary()
  │   └─ 内部 checkMarkerConflict() / checkPartitionMarkerConflict()
  │       ├─ List .temp 目录（仅一次 IO）
  │       ├─ classifyInstantsByHeartbeat()  ← 单次遍历，每个心跳只读一次
  │       │   ├─ 活跃心跳组 → fileId/partition 冲突检测
  │       │   │   └─ 有冲突 → 抛异常
  │       │   └─ 过期心跳组 → hasExpiredHeartbeatInPartition()
  │       │       └─ 结果存入 expiredHeartbeatPartitionConflictDetected 标志位
  │
  ├─ Step 2: 检查标志位
  │   └─ strategy.isExpiredHeartbeatPartitionConflictDetected()
  │       └─ true → 返回 Option.empty() → preLogFileOpen 返回 false → rollover
  │
  └─ Step 3: 无冲突 → 创建 marker 文件 → preLogFileOpen 返回 true
```

**关键优化点**：
- `checkMarkerConflict` 只调用一次 `storage.listDirectEntries(.temp)`
- `classifyInstantsByHeartbeat` 单次遍历所有 instant，每个 instant 的心跳文件**只读一次**
- 分类后活跃组和过期组各自处理，**零重复心跳 IO**

### Spark 与 Flink 的不同路径

| 引擎 | 过期心跳检测位置 | 原因 |
|------|----------------|------|
| **Spark** | `DirectWriteMarkers.createWithEarlyConflictDetection()` | Spark 走 ECD 流程，复用 `.temp` listing |
| **Flink** | `FlinkAppendHandle.preLogFileOpen()` | Flink 不走 ECD，需要独立检测 |

### 与现有 ECD 的关系

| 维度 | 现有 ECD（活跃心跳） | 过期心跳分区冲突检测 |
|------|---------|-------------------|
| 检测对象 | 心跳**未**超时的 instant | 心跳**已**超时的 instant |
| 检测粒度 | 文件级（fileId）/ 分区级 | 分区级（partitionPath） |
| 冲突处理 | **抛异常**，写入失败 | **返回 empty**，rollover 写新文件 |
| 执行位置 | `detectAndResolveConflictIfNecessary()` | 同一方法内（共享 `.temp` listing） |
| IO 开销 | 共享 list | **零额外 list**（复用） |
| 配置开关 | `early.conflict.detection.enable` | `expired.heartbeat.partition.conflict.check.enable` |

### 核心代码

**MarkerUtils.classifyInstantsByHeartbeat()（单次遍历分类）**

```java
// 一次遍历所有 instant，每个 instant 只调用一次 isHeartbeatExpired，
// 按心跳状态分为活跃/过期两组
public static InstantClassification classifyInstantsByHeartbeat(
    HoodieActiveTimeline activeTimeline, List<StoragePath> instants,
    String currentInstantTime, long maxAllowableHeartbeatIntervalInMs,
    HoodieStorage storage, String basePath) {

  List<String> activeHeartbeatInstants = new ArrayList<>();
  List<StoragePath> expiredHeartbeatInstants = new ArrayList<>();

  for (StoragePath instantPath : instants) {
    String instantTime = markerDirToInstantTime(instantPath.toString());
    if (instantTime >= currentInstantTime) continue;             // 跳过当前及之后的
    if (pendingCompaction/pendingReplace) continue;              // 跳过 compaction/replace

    boolean expired = isHeartbeatExpired(instantTime, ...);      // 每个 instant 只读一次心跳
    if (!expired) activeHeartbeatInstants.add(instantPath);
    else          expiredHeartbeatInstants.add(instantPath);
  }
  return new InstantClassification(activeHeartbeatInstants, expiredHeartbeatInstants);
}
```

**DirectMarkerBasedDetectionStrategy.checkMarkerConflict()（使用分类结果）**

```java
public boolean checkMarkerConflict(String basePath, long maxAllowableHeartbeatIntervalInMs) {
    // 1. List .temp 目录（仅一次 IO）
    List<StoragePath> allInstantPaths = storage.listDirectEntries(.temp);

    // 2. 单次遍历分类（每个 instant 的心跳只读一次）
    InstantClassification classification = MarkerUtils.classifyInstantsByHeartbeat(
        activeTimeline, allInstantPaths, instantTime, maxHeartbeat, storage, basePath);

    // 3. 活跃心跳 instant → fileId/partition 冲突检测
    long res = classification.activeHeartbeatInstants.stream().flatMap(/* 检查 */).count();

    // 4. 过期心跳 instant → 分区冲突检测（零额外心跳 IO）
    this.expiredHeartbeatPartitionConflictDetected =
        MarkerUtils.hasExpiredHeartbeatInPartition(storage, classification.expiredHeartbeatInstants, partitionPath);

    return res != 0L;
}
```

**DirectWriteMarkers.createWithEarlyConflictDetection()（读取标志位）**

```java
strategy.detectAndResolveConflictIfNecessary();

// ECD 通过 → 检查过期心跳标志位
if (config.isExpiredHeartbeatPartitionConflictCheckEnabled()
    && strategy.isExpiredHeartbeatPartitionConflictDetected()) {
    LOG.warn("Expired heartbeat partition conflict detected, returning empty to trigger rollover.");
    return Option.empty();  // → preLogFileOpen 返回 false → rollover
}

return create(getMarkerPath(partitionPath, dataFileName, type), checkIfExists);
```

**MarkerUtils — ECD 路径 vs Flink 路径**

```java
// ECD 路径（Spark）：使用 classifyInstantsByHeartbeat + hasExpiredHeartbeatInPartition
//   → 与活跃心跳检测共享单次遍历，每个 instant 心跳只读一次

// Flink 路径（独立调用）：hasExpiredHeartbeatPartitionConflict
//   → 自行 list .temp 并遍历，适用于不走 ECD 的场景
```

**FlinkAppendHandle（Flink，独立检测）**

Flink 不走 ECD 流程，在 `preLogFileOpen` 中独立调用原始方法：

```java
// createdMarkers.contains(markerKey) → return !hasExpiredHeartbeatPartitionConflict();
// result.isPresent()                 → return !hasExpiredHeartbeatPartitionConflict();
// 兼容模式                            → return !hasExpiredHeartbeatPartitionConflict();
```

### PartitionTransactionDirectMarkerBasedDetectionStrategy 的特殊处理

```
detectAndResolveConflictIfNecessary():
  │
  ├─ Fast path (分区目录已存在，跳过 ZK 锁)
  │   └─ 仍然独立检查过期心跳 (hasExpiredHeartbeatPartitionConflict)
  │      因为条件可能在首次检查后发生变化
  │
  └─ Slow path (需要获取 ZK 锁)
      └─ 调用 super.detectAndResolveConflictIfNecessary()
         └─ checkPartitionMarkerConflict() 中已复用 listing 做过期心跳检测
```

### HoodieLogFormatWriter 中的 rollover 行为

当 `preLogFileOpen` 返回 `false` 时，`HoodieLogFormatWriter.getOutputStream()` 的处理：

```java
boolean canAppend = isAppendSupported ? logFileWriteCallback.preLogFileOpen(logFile) : false;
if (!isAppendSupported || !canAppend) {
    rollOver();      // logVersion + 1，生成新文件名
    createNewFile(); // 调用 preLogFileCreate → 为新文件创建 marker
}
```

- `rollOver()`: 通过 `FSUtils.computeNextLogVersion` 计算下一个可用版本号
- `createNewFile()`: 创建新的物理文件，`preLogFileCreate` 会为新文件创建 marker

---

## 四、配置参数

```properties
# ===== 基础 OCC 配置 =====
hoodie.write.concurrency.mode=optimistic_concurrency_control

# ===== ZooKeeper 锁配置 =====
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
hoodie.write.lock.zookeeper.url=zk1:2181,zk2:2181,zk3:2181
hoodie.write.lock.zookeeper.base_path=/hudi/locks

# ===== 分区级早期冲突检测 =====
hoodie.write.concurrency.early.conflict.detection.enable=true
hoodie.write.lock.early.conflict.detection.strategy=org.apache.hudi.table.marker.PartitionTransactionDirectMarkerBasedDetectionStrategy

# ===== 过期心跳分区冲突检测（新增）=====
hoodie.write.concurrency.expired.heartbeat.partition.conflict.check.enable=true

# ===== 心跳配置（影响超时判断）=====
hoodie.client.heartbeat.interval_in_ms=60000       # 心跳间隔 60s
hoodie.client.heartbeat.tolerable.misses=2          # 容忍 2 次丢失
# → 超时阈值 = 60000 * 2 = 120000ms = 2 分钟
```

---

## 五、使用示例

### 场景说明

假设有一张 MOR 表 `hudi_db.user_events`，按 `dt` 字段做日期分区。
两个作业并发写入不同分区：
- **Spark 作业 A**：每小时批量更新 `dt=2024-01-15` 分区
- **Flink 作业 B**：实时写入 `dt=2024-01-16` 分区

需要保证：
1. 不同分区可以并发写入，互不阻塞
2. 如果意外写同一分区，能检测冲突
3. 如果某个 writer 心跳假死，另一个 writer 自动 rollover 避免文件损坏

### 5.1 Spark 使用示例

#### 方式一：Spark DataSource API（推荐 — 一键模式）

只需设置 `hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT`，
所有分区级冲突检测参数将**自动配置**：

```scala
import org.apache.spark.sql.SaveMode

val df = spark.read.parquet("/data/source/user_events_20240115")

df.write.format("hudi")
  .option("hoodie.table.name", "user_events")
  .option("hoodie.datasource.write.recordkey.field", "user_id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.partitionpath.field", "dt")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")

  // ===== 只需这一个参数即可开启分区级并发控制 =====
  .option("hoodie.write.concurrency.mode", "OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT")

  // ===== ZooKeeper 分布式锁（仍需手动配置）=====
  .option("hoodie.write.lock.provider",
    "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider")
  .option("hoodie.write.lock.zookeeper.url", "zk1:2181,zk2:2181,zk3:2181")
  .option("hoodie.write.lock.zookeeper.base_path", "/hudi/locks")

  // 以下参数已自动设置，无需手动指定：
  // - hoodie.write.concurrency.early.conflict.detection.enable = true
  // - hoodie.write.concurrency.early.conflict.detection.strategy = PartitionTransactionDirectMarkerBasedDetectionStrategy
  // - hoodie.write.concurrency.expired.heartbeat.partition.conflict.check.enable = true
  // - hoodie.cleaner.policy.failed.writes = LAZY
  // - hoodie.write.lock.conflict.resolution.strategy = PartitionBasedConcurrentWritesConflictResolutionStrategy
  // - hoodie.write.markers.type = DIRECT

  .mode(SaveMode.Append)
  .save("/data/hudi/user_events")
```

> **自动配置列表**：设置 `OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT` 后，以下参数会被自动设置：
>
> | 参数 | 自动设置值 |
> |------|-----------|
> | `hoodie.write.concurrency.early.conflict.detection.enable` | `true` |
> | `hoodie.write.concurrency.early.conflict.detection.strategy` | `PartitionTransactionDirectMarkerBasedDetectionStrategy` |
> | `hoodie.write.lock.conflict.resolution.strategy` | `PartitionBasedConcurrentWritesConflictResolutionStrategy` |
> | `hoodie.write.concurrency.expired.heartbeat.partition.conflict.check.enable` | `true` |
> | `hoodie.cleaner.policy.failed.writes` | `LAZY` |
> | `hoodie.write.markers.type` | `DIRECT` |

#### 方式一（备选）：手动配置模式

如果需要更精细的控制，也可以使用 `OPTIMISTIC_CONCURRENCY_CONTROL` + 手动指定各参数：

```scala
import org.apache.spark.sql.SaveMode

val df = spark.read.parquet("/data/source/user_events_20240115")

df.write.format("hudi")
  .option("hoodie.table.name", "user_events")
  .option("hoodie.datasource.write.recordkey.field", "user_id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.partitionpath.field", "dt")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")

  // ===== 并发控制：OCC 模式 =====
  .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")

  // ===== ZooKeeper 分布式锁 =====
  .option("hoodie.write.lock.provider",
    "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider")
  .option("hoodie.write.lock.zookeeper.url", "zk1:2181,zk2:2181,zk3:2181")
  .option("hoodie.write.lock.zookeeper.base_path", "/hudi/locks")

  // ===== 分区级早期冲突检测 =====
  .option("hoodie.write.concurrency.early.conflict.detection.enable", "true")
  .option("hoodie.write.concurrency.early.conflict.detection.strategy",
    "org.apache.hudi.table.marker.PartitionTransactionDirectMarkerBasedDetectionStrategy")

  // ===== 过期心跳分区冲突检测 =====
  .option("hoodie.write.concurrency.expired.heartbeat.partition.conflict.check.enable", "true")

  // ===== 心跳配置（可选，使用默认值即可）=====
  .option("hoodie.client.heartbeat.interval_in_ms", "60000")    // 60s
  .option("hoodie.client.heartbeat.tolerable.misses", "2")      // 容忍2次

  .mode(SaveMode.Append)
  .save("/data/hudi/user_events")
```

#### 方式二：Spark SQL

```sql
-- 建表时指定（一键模式）
CREATE TABLE hudi_db.user_events (
  user_id STRING,
  event_type STRING,
  ts TIMESTAMP,
  dt STRING
) USING hudi
PARTITIONED BY (dt)
TBLPROPERTIES (
  'type' = 'mor',
  'primaryKey' = 'user_id',
  'preCombineField' = 'ts',

  -- 一键开启分区级并发控制
  'hoodie.write.concurrency.mode' = 'OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT',

  -- ZK 锁
  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider',
  'hoodie.write.lock.zookeeper.url' = 'zk1:2181,zk2:2181,zk3:2181',
  'hoodie.write.lock.zookeeper.base_path' = '/hudi/locks'
  -- 其他参数已自动配置，无需手动指定
);

-- 写入数据
INSERT INTO hudi_db.user_events
SELECT user_id, event_type, ts, dt
FROM source_table
WHERE dt = '2024-01-15';
```

#### 方式三：spark-submit 参数

```bash
spark-submit \
  --class com.example.HudiWriter \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  your-app.jar \
  --hoodie-conf hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT \
  --hoodie-conf hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider \
  --hoodie-conf hoodie.write.lock.zookeeper.url=zk1:2181,zk2:2181,zk3:2181 \
  --hoodie-conf hoodie.write.lock.zookeeper.base_path=/hudi/locks
```

### 5.2 Flink 使用示例

#### 方式一：Flink SQL（推荐 — 一键模式）

```sql
CREATE TABLE user_events (
  user_id STRING,
  event_type STRING,
  ts TIMESTAMP(3),
  dt STRING,
  PRIMARY KEY (user_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'connector' = 'hudi',
  'path' = '/data/hudi/user_events',
  'table.type' = 'MERGE_ON_READ',

  'hoodie.datasource.write.recordkey.field' = 'user_id',
  'hoodie.datasource.write.precombine.field' = 'ts',

  -- 一键开启分区级并发控制
  'hoodie.write.concurrency.mode' = 'OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT',

  -- ZK 锁
  'write.lock.provider' = 'org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider',
  'hoodie.write.lock.zookeeper.url' = 'zk1:2181,zk2:2181,zk3:2181',
  'hoodie.write.lock.zookeeper.base_path' = '/hudi/locks'

  -- 注意：Flink 不走 ECD 流程，但过期心跳检测在 FlinkAppendHandle 中独立生效
  -- 其他参数已自动配置
);

-- 实时写入
INSERT INTO user_events
SELECT user_id, event_type, ts, dt
FROM kafka_source;
```

#### 方式二：Flink DataStream API

```java
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.flink.configuration.Configuration;

Configuration conf = new Configuration();
// 基础配置
conf.setString(FlinkOptions.PATH, "/data/hudi/user_events");
conf.setString(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");
conf.setString(FlinkOptions.RECORD_KEY_FIELD, "user_id");
conf.setString(FlinkOptions.PRECOMBINE_FIELD, "ts");
conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "dt");

// 一键开启分区级并发控制
conf.setString("hoodie.write.concurrency.mode",
    "OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT");

// ZK 锁
conf.setString("hoodie.write.lock.provider",
    "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider");
conf.setString("hoodie.write.lock.zookeeper.url", "zk1:2181,zk2:2181,zk3:2181");
conf.setString("hoodie.write.lock.zookeeper.base_path", "/hudi/locks");

// 其他参数已由 OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT 自动配置

// 构建 pipeline 并提交
HoodiePipeline.builder("user_events")
    .column("user_id STRING")
    .column("event_type STRING")
    .column("ts TIMESTAMP(3)")
    .column("dt STRING")
    .pk("user_id")
    .partition("dt")
    .options(conf)
    .sink(dataStream, false);

env.execute("Flink Hudi Writer");
```

### 5.3 Spark + Flink 混合并发写入示例

以下是典型的生产环境部署拓扑（使用一键模式）：

```
┌──────────────────────────────────────────────────────────────────────┐
│                          ZooKeeper 集群                              │
│                   zk1:2181, zk2:2181, zk3:2181                      │
│                   /hudi/locks (锁根路径)                              │
└────────────────────────────┬─────────────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
┌──────────────────────────┐  ┌──────────────────────────┐
│   Spark 批处理作业        │  │   Flink 实时流作业        │
│   每小时写 dt=2024-01-15 │  │   实时写 dt=2024-01-16   │
│                          │  │                          │
│   concurrency.mode:      │  │   concurrency.mode:      │
│   OPTIMISTIC_CONCURRENCY │  │   OPTIMISTIC_CONCURRENCY │
│   _CONTROL_PARTITION     │  │   _CONTROL_PARTITION     │
│   _LIMIT                 │  │   _LIMIT                 │
│                          │  │                          │
│   (ECD + 过期心跳检测     │  │   (过期心跳检测           │
│    全部自动配置)          │  │    自动配置，             │
│                          │  │    FlinkAppendHandle     │
│                          │  │    独立执行)              │
└──────────────────────────┘  └──────────────────────────┘
              │                             │
              ▼                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│   HDFS / S3 — /data/hudi/user_events/                               │
│   ├── .hoodie/.temp/                                                │
│   │   ├── {spark_instant}/dt=2024-01-15/  ← Spark 的 marker        │
│   │   └── {flink_instant}/dt=2024-01-16/  ← Flink 的 marker        │
│   ├── .hoodie/.heartbeat/                                           │
│   │   ├── {spark_instant}  ← Spark 心跳（60s 更新一次）              │
│   │   └── {flink_instant}  ← Flink 心跳                             │
│   ├── dt=2024-01-15/  ← Spark 写入                                  │
│   └── dt=2024-01-16/  ← Flink 写入                                  │
└──────────────────────────────────────────────────────────────────────┘
```

**不同分区** → 互不干扰，两个作业完全并行
**相同分区** → ECD 检测到冲突，后到的 writer 抛异常重试
**心跳假死** → 过期心跳检测触发 rollover，自动写新 log 文件避免损坏

### 5.4 两种配置方式对比

| 对比项 | 一键模式（推荐） | 手动模式 |
|--------|:---------------:|:--------:|
| `hoodie.write.concurrency.mode` | `OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT` | `OPTIMISTIC_CONCURRENCY_CONTROL` |
| 需要手动设置 ECD？ | 否（自动） | 是 |
| 需要手动设置 ECD 策略？ | 否（自动） | 是 |
| 需要手动设置冲突解析策略？ | 否（自动） | 是 |
| 需要手动设置过期心跳检测？ | 否（自动） | 是 |
| 需要手动设置 LAZY 策略？ | 否（自动） | 否（OCC 也自动设置） |
| 需要配置锁？ | 是 | 是 |
| 适用场景 | 快速上手，标准分区级并发 | 需要自定义策略组合 |

### 5.5 配置参数速查表

| 配置项 | 默认值 | 说明 | 一键模式 | 手动模式(Spark) | Flink |
|--------|--------|------|:--------:|:---------------:|:-----:|
| `hoodie.write.concurrency.mode` | `SINGLE_WRITER` | 并发模式 | 设为`OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT` | 设为`OPTIMISTIC_CONCURRENCY_CONTROL` | 二选一 |
| `hoodie.write.lock.provider` | - | 分布式锁实现类 | 必需 | 必需 | 必需 |
| `hoodie.write.lock.zookeeper.url` | - | ZK 连接地址 | 必需 | 必需 | 必需 |
| `hoodie.write.lock.zookeeper.base_path` | - | ZK 锁根路径 | 必需 | 必需 | 必需 |
| `hoodie.write.concurrency.early.conflict.detection.enable` | `false` | 启用 ECD | **自动** | 手动设置 | 不涉及 |
| `hoodie.write.concurrency.early.conflict.detection.strategy` | - | ECD 策略类名 | **自动** | 手动设置 | 不涉及 |
| `hoodie.write.lock.conflict.resolution.strategy` | `SimpleConcurrent...` | 冲突解析策略 | **自动** | 手动设置 | **自动** |
| `hoodie.write.concurrency.expired.heartbeat.partition.conflict.check.enable` | `false` | 过期心跳分区冲突检测 | **自动** | 手动设置 | **自动** |
| `hoodie.cleaner.policy.failed.writes` | `EAGER` | 失败写入清理策略 | **自动** | **自动**(OCC) | **自动** |
| `hoodie.write.markers.type` | 引擎相关 | Marker 类型 | **自动**(DIRECT) | 手动设置 | 默认DIRECT |
| `hoodie.client.heartbeat.interval_in_ms` | `60000` | 心跳间隔（ms） | 可选 | 可选 | 可选 |
| `hoodie.client.heartbeat.tolerable.misses` | `2` | 容忍丢失次数 | 可选 | 可选 | 可选 |

> **注意**：
> - **一键模式**只需设置 `hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT` + 锁配置，其余参数自动推导
> - Spark 走 ECD 流程，过期心跳检测集成在 ECD 扫描点，与活跃心跳检测共享单次遍历
> - Flink 不走 ECD 流程，过期心跳检测在 `FlinkAppendHandle.preLogFileOpen()` 中独立执行
> - 心跳超时阈值 = `interval_in_ms` × `tolerable.misses`，建议不要设置过小以避免频繁误判

---

## 六、三层保护机制总览（Spark ECD 路径）

三层检测在 `createWithEarlyConflictDetection` 的**同一个调用**中完成，共享 `.temp` listing：

```
┌─────────────────────────────────────────────────────────────────────────┐
│              createWithEarlyConflictDetection() 完整流程                │
│                     (preLogFileOpen → createAppendMarker 触发)          │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
                ┌────────────────────────────────────────────────┐
                │  storage.listDirectEntries(.temp)              │
                │  ── 仅一次 IO，三层检测共享 ──                  │
                └───────────────────┬────────────────────────────┘
                                    │
                                    ▼
                ┌────────────────────────────────────────────────┐
                │  classifyInstantsByHeartbeat()                 │
                │  单次遍历，每个 instant 心跳只读一次            │
                │  ── 分为活跃组 / 过期组 ──                      │
                └───────────────────┬────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
       ┌────────────────────────┐     ┌────────────────────────────┐
       │ 第一层: 活跃心跳冲突    │     │ 第三层: 过期心跳分区冲突     │
       │ activeHeartbeatInstants│     │ hasExpiredHeartbeat         │
       │ → fileId/partition检测 │     │ InPartition()              │
       └──────────┬─────────────┘     └──────────────┬─────────────┘
                  │                                   │
                  ▼                                   ▼
          冲突 → 抛异常                      结果 → 存入标志位
          无冲突 → 继续                  expiredHeartbeatPartition
                  │                      ConflictDetected
                  │                                   │
                  ├───────────────────────────────────┘
                  │
                  ▼
       ┌────────────────────────────────────────────────┐
       │  检查标志位                                      │
       │  strategy.isExpiredHeartbeatPartitionConflict   │
       │  Detected() == true?                            │
       │  ├─ YES → return Option.empty() → rollover      │
       │  └─ NO  → 继续                                  │
       └──────────────────┬─────────────────────────────┘
                          │
                          ▼
       ┌──────────────────────────────────────┐
       │ 第二层: Marker 文件保护               │
       │ create(markerPath, checkIfExists)     │
       │ ├─ marker 已存在 → empty → rollover   │
       │ └─ marker 创建成功 → path → 追加文件   │
       └──────────────────────────────────────┘
```

---

## 七、关键代码路径

| 组件 | 文件 | 方法 |
|------|------|------|
| ECD 入口 | `DirectWriteMarkers` | `createWithEarlyConflictDetection()` |
| ECD 策略(Simple) | `SimpleDirectMarkerBasedDetectionStrategy` | `detectAndResolveConflictIfNecessary()` |
| ECD 策略(分区级) | `PartitionBasedDirectMarkerDetectionStrategy` | `checkPartitionMarkerConflict()` |
| ECD 策略(分区+ZK锁) | `PartitionTransactionDirectMarkerBasedDetectionStrategy` | `detectAndResolveConflictIfNecessary()` |
| 候选 instant 过滤 | `MarkerUtils` | `getCandidateInstants()` |
| 单次遍历分类(ECD) | `MarkerUtils` | `classifyInstantsByHeartbeat()` |
| 过期心跳分区检测(ECD) | `MarkerUtils` | `hasExpiredHeartbeatInPartition()` |
| 过期心跳分区检测(Flink) | `MarkerUtils` | `hasExpiredHeartbeatPartitionConflict()` |
| 过期心跳标志位 | `DirectMarkerBasedDetectionStrategy` | `isExpiredHeartbeatPartitionConflictDetected()` |
| Spark preLogFileOpen | `HoodieWriteHandle.AppendLogWriteCallback` | `preLogFileOpen()` → `createAppendMarker()` |
| Flink preLogFileOpen | `FlinkAppendHandle` (匿名内部类) | `preLogFileOpen()` |
| Rollover 执行 | `HoodieLogFormatWriter` | `getOutputStream()` → `rollOver()` |
| 心跳超时判断 | `HoodieHeartbeatUtils` | `isHeartbeatExpired()` |
| 配置项 | `HoodieWriteConfig` | `isExpiredHeartbeatPartitionConflictCheckEnabled()` |
