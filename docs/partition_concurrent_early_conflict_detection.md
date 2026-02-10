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
        ┌─────────────────┐                 ┌─────────────────────┐
        │   目录已存在     │                 │    目录不存在        │
        │ (快速路径)       │                 │   (慢速路径)         │
        └────────┬────────┘                 └──────────┬──────────┘
                 │                                     │
                 │                                     ▼
                 │                     ┌───────────────────────────────┐
                 │                     │   获取 ZK 分区锁               │
                 │                     │   Lock Key: partition_lock_   │
                 │                     │   {partitionPath}             │
                 │                     └───────────────┬───────────────┘
                 │                                     │
                 │                                     ▼
                 │                     ╔═══════════════════════════════╗
                 │                     ║  Double-Check:                 ║
                 │                     ║  分区目录是否存在？            ║
                 │                     ║  【持有锁状态下检查】          ║
                 │                     ╚═══════════════╤═══════════════╝
                 │                                     │
                 │                     ┌───────────────┴───────────────┐
                 │                     │                               │
                 │                     ▼                               ▼
                 │          ┌─────────────────┐            ┌──────────────────────┐
                 │          │   目录已存在     │            │    目录仍不存在       │
                 │          │(其他task已创建)  │            │   (需要冲突检测)      │
                 │          └────────┬────────┘            └───────────┬──────────┘
                 │                   │                                 │
                 │                   │                                 ▼
                 │                   │                 ┌────────────────────────────────┐
                 │                   │                 │  扫描其他 instant 的 marker    │
                 │                   │                 │  目录，检查是否有同分区写入     │
                 │                   │                 │  .temp/{otherInstant}/{partition}│
                 │                   │                 └───────────────┬────────────────┘
                 │                   │                                 │
                 │                   │                   ┌─────────────┴─────────────┐
                 │                   │                   │                           │
                 │                   │                   ▼                           ▼
                 │                   │        ┌─────────────────┐        ┌───────────────────┐
                 │                   │        │  检测到冲突！    │        │   无冲突          │
                 │                   │        │ (其他instant     │        │                   │
                 │                   │        │  在写同一分区)   │        └─────────┬─────────┘
                 │                   │        └────────┬────────┘                  │
                 │                   │                 │                           ▼
                 │                   │                 │           ┌──────────────────────────┐
                 │                   │                 │           │  创建分区目录             │
                 │                   │                 │           │  .temp/{instant}/{part}/ │
                 │                   │                 │           │  作为"占位符"             │
                 │                   │                 │           └────────────┬─────────────┘
                 │                   │                 │                        │
                 │                   │                 ▼                        ▼
                 │                   │    ┌───────────────────┐    ┌───────────────────┐
                 │                   │    │  释放锁            │    │   释放锁           │
                 │                   │    │  抛出冲突异常      │    └─────────┬─────────┘
                 │                   │    │  任务失败重试      │              │
                 │                   │    └───────────────────┘              │
                 │                   │                                       │
                 │                   ▼                                       │
                 │      ┌───────────────────┐                                │
                 │      │    释放锁          │                                │
                 │      └─────────┬─────────┘                                │
                 │                │                                          │
                 └────────────────┼──────────────────────────────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────────┐
                    │  DirectWriteMarkers.create()    │
                    │  创建实际的 marker 文件          │
                    │  {file}.marker.{IOType}         │
                    └───────────────┬─────────────────┘
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
  │       ├─ 活跃心跳 instant → fileId/partition 冲突检测
  │       │   └─ 有冲突 → 抛异常
  │       └─ 过期心跳 instant → 分区冲突检测（复用同一 listing）
  │           └─ 结果存入 expiredHeartbeatPartitionConflictDetected 标志位
  │
  ├─ Step 2: 检查标志位
  │   └─ strategy.isExpiredHeartbeatPartitionConflictDetected()
  │       └─ true → 返回 Option.empty() → preLogFileOpen 返回 false → rollover
  │
  └─ Step 3: 无冲突 → 创建 marker 文件 → preLogFileOpen 返回 true
```

**关键优化点**：`checkMarkerConflict` 只调用一次 `storage.listDirectEntries(.temp)`，
同时完成活跃心跳和过期心跳两种检测，显著减少 IO 开销。

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

**DirectMarkerBasedDetectionStrategy.checkMarkerConflict()（复用 listing）**

```java
public boolean checkMarkerConflict(String basePath, long maxAllowableHeartbeatIntervalInMs) throws IOException {
    // List .temp 目录——仅一次 IO，两种检测共享
    List<StoragePath> allInstantPaths = storage.listDirectEntries(new StoragePath(tempFolderPath))
        .stream().map(StoragePathInfo::getPath).collect(Collectors.toList());

    // 1. 活跃心跳 → fileId 级冲突（原有逻辑）
    List<String> candidateInstants = MarkerUtils.getCandidateInstants(
        activeTimeline, allInstantPaths, instantTime, maxAllowableHeartbeatIntervalInMs, storage, basePath);
    long res = candidateInstants.stream().flatMap(/* fileId 检查 */).count();

    // 2. 过期心跳 → 分区级冲突（新增，复用同一 listing）
    this.expiredHeartbeatPartitionConflictDetected =
        MarkerUtils.hasExpiredHeartbeatPartitionConflictFromPaths(
            storage, allInstantPaths, basePath, instantTime,
            maxAllowableHeartbeatIntervalInMs, partitionPath);

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

**MarkerUtils — 两个重载方法**

```java
// 方法 1: 自行 list .temp 目录（供 Flink 使用）
public static boolean hasExpiredHeartbeatPartitionConflict(
    HoodieStorage storage, String basePath, String currentInstantTime,
    long maxAllowableHeartbeatIntervalInMs, String partitionPath)

// 方法 2: 接受预列路径（供 ECD 扫描点使用，零额外 IO）
public static boolean hasExpiredHeartbeatPartitionConflictFromPaths(
    HoodieStorage storage, List<StoragePath> allInstantPaths, String basePath,
    String currentInstantTime, long maxAllowableHeartbeatIntervalInMs, String partitionPath)
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

## 五、三层保护机制总览

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
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
       ┌────────────────────────┐     ┌────────────────────────────┐
       │ 第一层: 活跃心跳冲突    │     │ 第三层: 过期心跳分区冲突     │
       │ getCandidateInstants   │     │ hasExpiredHeartbeat         │
       │ → fileId/partition检测 │     │ PartitionConflictFromPaths │
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

## 六、关键代码路径

| 组件 | 文件 | 方法 |
|------|------|------|
| ECD 入口 | `DirectWriteMarkers` | `createWithEarlyConflictDetection()` |
| ECD 策略(Simple) | `SimpleDirectMarkerBasedDetectionStrategy` | `detectAndResolveConflictIfNecessary()` |
| ECD 策略(分区级) | `PartitionBasedDirectMarkerDetectionStrategy` | `checkPartitionMarkerConflict()` |
| ECD 策略(分区+ZK锁) | `PartitionTransactionDirectMarkerBasedDetectionStrategy` | `detectAndResolveConflictIfNecessary()` |
| 候选 instant 过滤 | `MarkerUtils` | `getCandidateInstants()` |
| 过期心跳检测(复用listing) | `MarkerUtils` | `hasExpiredHeartbeatPartitionConflictFromPaths()` |
| 过期心跳检测(独立) | `MarkerUtils` | `hasExpiredHeartbeatPartitionConflict()` |
| 过期心跳标志位 | `DirectMarkerBasedDetectionStrategy` | `isExpiredHeartbeatPartitionConflictDetected()` |
| Spark preLogFileOpen | `HoodieWriteHandle.AppendLogWriteCallback` | `preLogFileOpen()` → `createAppendMarker()` |
| Flink preLogFileOpen | `FlinkAppendHandle` (匿名内部类) | `preLogFileOpen()` |
| Rollover 执行 | `HoodieLogFormatWriter` | `getOutputStream()` → `rollOver()` |
| 心跳超时判断 | `HoodieHeartbeatUtils` | `isHeartbeatExpired()` |
| 配置项 | `HoodieWriteConfig` | `isExpiredHeartbeatPartitionConflictCheckEnabled()` |
