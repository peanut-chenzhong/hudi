# Hudi 0.15.0 OCC 并发机制详细分析

## 一、概述

Hudi（Hadoop Upserts Deletes and Incrementals）实现了基于时间线（Timeline）的乐观并发控制（Optimistic Concurrency Control，OCC）机制，用于支持多写入者（Multi-writer）场景下的数据一致性、完整性和正确性。

### 1.1 核心设计理念

- **文件组级别冲突检测**：OCC 在 Hudi 的文件组（File Group）级别检测冲突，即两个并发写入者更新同一个文件组时会被检测为冲突
- **基于时间线的协调**：使用 Hudi Timeline 作为中央协调机制，通过事件日志实现进程间协调
- **延迟冲突检测**：传统 OCC 在提交元数据前、数据写入完成后进行冲突检测
- **早期冲突检测**：通过 Marker 机制在数据写入阶段进行早期冲突检测，提前终止冲突的写入操作

## 二、OCC 模式配置

### 2.1 写入并发模式

Hudi 定义了两种写入并发模式（`WriteConcurrencyMode`）：

```java
public enum WriteConcurrencyMode {
  // 单写入者模式：只有一个活跃的写入者，最大化吞吐量
  SINGLE_WRITER,
  
  // 乐观并发控制模式：多个写入者可以操作表，使用延迟冲突解决
  // 如果多个写入者写入同一个文件组，只有一个会成功
  OPTIMISTIC_CONCURRENCY_CONTROL;
}
```

### 2.2 配置方式

通过配置项 `hoodie.write.concurrency.mode` 设置为 `optimistic_concurrency_control` 来启用 OCC。

## 三、核心组件架构

### 3.1 主要类结构

```
TransactionUtils (冲突解决入口)
    ├── ConflictResolutionStrategy (冲突解决策略接口)
    │   ├── SimpleConcurrentFileWritesConflictResolutionStrategy (简单文件写入冲突解决)
    │   └── BucketIndexConcurrentFileWritesConflictResolutionStrategy (Bucket索引冲突解决)
    ├── ConcurrentOperation (并发操作表示)
    └── EarlyConflictDetectionStrategy (早期冲突检测策略)
        ├── DirectMarkerBasedDetectionStrategy (直接Marker检测)
        │   ├── SimpleDirectMarkerBasedDetectionStrategy
        │   └── SimpleTransactionDirectMarkerBasedDetectionStrategy
        └── TimelineServerBasedDetectionStrategy (时间线服务器检测)
            └── AsyncTimelineServerBasedDetectionStrategy
```

## 四、冲突检测机制

### 4.1 传统冲突检测（提交时检测）

#### 4.1.1 检测时机

冲突检测在以下时机进行：
1. **数据写入完成后**
2. **提交元数据前**
3. **在 `TransactionUtils.resolveWriteConflictIfAny()` 方法中执行**

#### 4.1.2 检测流程

```java
// TransactionUtils.java
public static Option<HoodieCommitMetadata> resolveWriteConflictIfAny(
    final HoodieTable table,
    final Option<HoodieInstant> currentTxnOwnerInstant,
    final Option<HoodieCommitMetadata> thisCommitMetadata,
    final HoodieWriteConfig config,
    Option<HoodieInstant> lastCompletedTxnOwnerInstant,
    boolean reloadActiveTimeline,
    Set<String> pendingInstants) throws HoodieWriteConflictException {
    
  if (config.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
    // 1. 获取在当前写入操作期间完成的 instant
    Stream<HoodieInstant> completedInstantsDuringCurrentWriteOperation = 
        getCompletedInstantsDuringCurrentWriteOperation(table.getMetaClient(), pendingInstants);
    
    // 2. 获取冲突解决策略
    ConflictResolutionStrategy resolutionStrategy = config.getWriteConflictResolutionStrategy();
    
    // 3. 重新加载活跃时间线以获取最新更新
    if (reloadActiveTimeline) {
      table.getMetaClient().reloadActiveTimeline();
    }
    
    // 4. 获取候选 instant 流（需要检查冲突的 instant）
    Stream<HoodieInstant> instantStream = Stream.concat(
        resolutionStrategy.getCandidateInstants(
            table.getMetaClient(), 
            currentTxnOwnerInstant.get(), 
            lastCompletedTxnOwnerInstant),
        completedInstantsDuringCurrentWriteOperation);
    
    // 5. 创建当前操作对象
    final ConcurrentOperation thisOperation = new ConcurrentOperation(
        currentTxnOwnerInstant.get(), 
        thisCommitMetadata.orElseGet(HoodieCommitMetadata::new));
    
    // 6. 遍历所有候选 instant，检查冲突
    instantStream.forEach(instant -> {
      try {
        ConcurrentOperation otherOperation = new ConcurrentOperation(instant, table.getMetaClient());
        if (resolutionStrategy.hasConflict(thisOperation, otherOperation)) {
          LOG.info("Conflict encountered between current instant = " + thisOperation 
              + " and instant = " + otherOperation + ", attempting to resolve it...");
          resolutionStrategy.resolveConflict(table, thisOperation, otherOperation);
        }
      } catch (IOException io) {
        throw new HoodieWriteConflictException("Unable to resolve conflict, if present", io);
      }
    });
    
    LOG.info("Successfully resolved conflicts, if any");
    return thisOperation.getCommitMetadataOption();
  }
  return thisCommitMetadata;
}
```

#### 4.1.3 候选 Instant 选择策略

`SimpleConcurrentFileWritesConflictResolutionStrategy` 的候选 instant 选择逻辑：

```java
@Override
public Stream<HoodieInstant> getCandidateInstants(
    HoodieTableMetaClient metaClient, 
    HoodieInstant currentInstant,
    Option<HoodieInstant> lastSuccessfulInstant) {
  
  HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
  
  // 1. 获取自上次成功写入以来的所有已完成提交
  Stream<HoodieInstant> completedCommitsInstantStream = activeTimeline
      .getCommitsTimeline()
      .filterCompletedInstants()
      .findInstantsAfter(
          lastSuccessfulInstant.isPresent() 
              ? lastSuccessfulInstant.get().getTimestamp() 
              : HoodieTimeline.INIT_INSTANT_TS)
      .getInstantsAsStream();
  
  // 2. 获取在当前 instant 之后开始的压缩和聚类操作的待处理时间线
  Stream<HoodieInstant> compactionAndClusteringPendingTimeline = activeTimeline
      .getTimelineOfActions(CollectionUtils.createSet(REPLACE_COMMIT_ACTION, COMPACTION_ACTION))
      .findInstantsAfter(currentInstant.getTimestamp())
      .filterInflightsAndRequested()
      .getInstantsAsStream();
  
  return Stream.concat(completedCommitsInstantStream, compactionAndClusteringPendingTimeline);
}
```

### 4.2 冲突判断逻辑

#### 4.2.1 ConcurrentOperation 类

`ConcurrentOperation` 用于表示一个并发操作，包含以下关键信息：

```java
public class ConcurrentOperation {
  private WriteOperationType operationType;  // 操作类型（INSERT, UPSERT, DELETE等）
  private final HoodieMetadataWrapper metadataWrapper;  // 元数据包装器
  private final Option<HoodieCommitMetadata> commitMetadataOption;  // 提交元数据
  private final String actionState;  // 操作状态（REQUESTED, INFLIGHT, COMPLETED）
  private final String actionType;  // 操作类型（COMMIT, DELTA_COMMIT, COMPACTION等）
  private final String instantTime;  // Instant 时间戳
  private Set<Pair<String, String>> mutatedPartitionAndFileIds;  // 修改的分区和文件ID集合
}
```

#### 4.2.2 冲突检测方法

```java
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
  // 获取两个操作修改的分区和文件ID集合
  Set<Pair<String, String>> partitionAndFileIdsSetForFirstInstant = 
      thisOperation.getMutatedPartitionAndFileIds();
  Set<Pair<String, String>> partitionAndFileIdsSetForSecondInstant = 
      otherOperation.getMutatedPartitionAndFileIds();
  
  // 计算交集：如果两个操作修改了相同的 (分区路径, 文件ID) 对，则存在冲突
  Set<Pair<String, String>> intersection = new HashSet<>(partitionAndFileIdsSetForFirstInstant);
  intersection.retainAll(partitionAndFileIdsSetForSecondInstant);
  
  if (!intersection.isEmpty()) {
    LOG.info("Found conflicting writes between first operation = " + thisOperation
        + ", second operation = " + otherOperation + " , intersecting file ids " + intersection);
    return true;
  }
  return false;
}
```

**关键点**：
- 冲突检测基于 **(分区路径, 文件ID)** 对
- 如果两个操作修改了相同的文件组，则判定为冲突
- 对于 Bucket Index，冲突检测基于 **(分区路径, Bucket ID)** 对

### 4.3 冲突解决策略

#### 4.3.1 简单冲突解决策略

```java
@Override
public Option<HoodieCommitMetadata> resolveConflict(
    HoodieTable table,
    ConcurrentOperation thisOperation, 
    ConcurrentOperation otherOperation) {
  
  // 特殊情况1：如果另一个操作是压缩操作
  if (otherOperation.getOperationType() == WriteOperationType.COMPACT) {
    // 如果压缩操作的时间戳小于当前操作，则当前操作可以继续
    if (HoodieTimeline.compareTimestamps(
        otherOperation.getInstantTimestamp(), 
        HoodieTimeline.LESSER_THAN, 
        thisOperation.getInstantTimestamp())) {
      return thisOperation.getCommitMetadataOption();
    }
  } 
  // 特殊情况2：如果当前操作是日志压缩
  else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(thisOperation.getInstantActionType())) {
    // 日志压缩可以与其他 delta commit 一起提交
    return thisOperation.getCommitMetadataOption();
  }
  
  // 默认情况：发现冲突时，中止当前写入
  throw new HoodieWriteConflictException(
      new ConcurrentModificationException("Cannot resolve conflicts for overlapping writes"));
}
```

**冲突解决原则**：
- **默认策略**：发现冲突时，抛出 `HoodieWriteConflictException`，当前写入操作被中止
- **压缩操作特殊处理**：如果压缩操作时间戳更早，当前操作可以继续
- **日志压缩特殊处理**：日志压缩可以与 delta commit 并发执行

#### 4.3.2 重试机制

在 Spark 写入客户端中，实现了自动重试机制：

```scala
// HoodieSparkSqlWriter.scala
def write(...) {
  var counter = 0
  val maxRetry: Integer = Integer.parseInt(
    optParams.getOrElse(
      HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.key(), 
      HoodieWriteConfig.NUM_RETRIES_ON_CONFLICT_FAILURES.defaultValue().toString))
  
  while (counter <= maxRetry && !succeeded) {
    try {
      toReturn = writeInternal(...)
      succeeded = true
    } catch {
      case e: HoodieWriteConflictException =>
        val writeConcurrencyMode = optParams.getOrElse(...)
        if (writeConcurrencyMode.equalsIgnoreCase(
            WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name()) 
            && counter < maxRetry) {
          counter += 1
          log.warn(s"Conflict found. Retrying again for attempt no $counter")
        } else {
          throw e
        }
    }
  }
}
```

## 五、早期冲突检测机制（Early Conflict Detection）

### 5.1 设计背景

传统 OCC 的局限性：
- 冲突检测发生在数据写入完成后，导致计算资源浪费
- 如果检测到冲突，整个写入过程需要重做

早期冲突检测的优势：
- 在数据写入阶段就检测冲突
- 提前终止冲突的写入操作，释放计算资源
- 基于 Marker 机制实现

### 5.2 Marker 机制

#### 5.2.1 Marker 的作用

- **文件追踪**：每个数据文件对应一个 Marker 文件，标记该文件是活跃写入的一部分
- **清理机制**：在失败和回滚场景中，使用 Marker 自动清理未提交的数据
- **冲突检测**：通过检查 Marker 文件是否存在来判断是否有并发写入

#### 5.2.2 Marker 类型

Hudi 支持两种 Marker 维护方式：

1. **DirectWriteMarkers**：写入者直接创建和维护 Marker 文件
2. **TimelineServerBasedWriteMarkers**：Marker 操作由 Timeline Service 作为代理处理

### 5.3 早期冲突检测策略

#### 5.3.1 接口定义

```java
public interface EarlyConflictDetectionStrategy {
  // 检测并解决冲突（如果需要）
  void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException;
  
  // 检查是否存在 Marker 冲突
  boolean hasMarkerConflict();
  
  // 解决 Marker 冲突
  void resolveMarkerConflict(String basePath, String partitionPath, String dataFileName);
}
```

#### 5.3.2 Direct Marker 检测策略

**SimpleDirectMarkerBasedDetectionStrategy**：

```java
@Override
public boolean hasMarkerConflict() {
  try {
    // 1. 检查 Marker 文件冲突
    boolean markerConflict = checkMarkerConflict(basePath, maxAllowableHeartbeatIntervalInMs);
    
    // 2. 可选：检查提交冲突
    boolean commitConflict = checkCommitConflict && 
        MarkerUtils.hasCommitConflict(
            activeTimeline, 
            Stream.of(fileId).collect(Collectors.toSet()), 
            completedCommitInstants);
    
    return markerConflict || commitConflict;
  } catch (IOException e) {
    throw new HoodieIOException("Exception occurs during create marker file in eager conflict detection mode.", e);
  }
}
```

**Marker 冲突检测逻辑**：

```java
public boolean checkMarkerConflict(String basePath, long maxAllowableHeartbeatIntervalInMs) 
    throws IOException {
  String tempFolderPath = basePath + StoragePath.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME;
  
  // 1. 获取候选 instant（活跃的、未过期的写入操作）
  List<String> candidateInstants = MarkerUtils.getCandidateInstants(
      activeTimeline,
      storage.listDirectEntries(new StoragePath(tempFolderPath)).stream()
          .map(StoragePathInfo::getPath)
          .collect(Collectors.toList()),
      instantTime, 
      maxAllowableHeartbeatIntervalInMs, 
      storage,
      basePath);
  
  // 2. 检查每个候选 instant 的 marker 目录中是否存在相同 fileId 的 marker
  long res = candidateInstants.stream().flatMap(currentMarkerDirPath -> {
    StoragePath markerPartitionPath = new StoragePath(currentMarkerDirPath, partitionPath);
    if (!storage.exists(markerPartitionPath)) {
      return Stream.empty();
    } else {
      return storage.listDirectEntries(markerPartitionPath).stream().parallel()
          .filter((path) -> path.toString().contains(fileId));
    }
  }).count();
  
  // 3. 如果找到相同 fileId 的 marker，则存在冲突
  if (res != 0L) {
    LOG.warn("Detected conflict marker files: " + partitionPath + "/" + fileId + " for " + instantTime);
    return true;
  }
  return false;
}
```

**关键优化**：
- **路径剪枝**：只检查特定分区路径下的 marker，而不是列出所有 `.temp` 目录
- **并行检查**：使用并行流提高检查效率
- **心跳过滤**：只检查未过期（有活跃心跳）的写入操作

#### 5.3.3 Timeline Server 检测策略

**AsyncTimelineServerBasedDetectionStrategy**：

- 在 Timeline Server 端异步、定期检查冲突
- 写入者在创建 marker 前获取冲突检查结果
- 通过后台线程周期性检查 marker 冲突

### 5.4 早期冲突检测流程

```
写入开始
    ↓
创建 Marker 前
    ↓
执行早期冲突检测 (detectAndResolveConflictIfNecessary)
    ↓
    ├─→ 检测到冲突 → 抛出 HoodieEarlyConflictDetectionException → 写入中止
    ↓
未检测到冲突
    ↓
创建 Marker 文件
    ↓
写入数据文件
    ↓
提交时进行传统冲突检测（双重保障）
```

### 5.5 心跳机制（Heartbeat）

#### 5.5.1 心跳的作用

- **活跃性检测**：判断写入操作是否仍在进行
- **过期检测**：如果心跳超时，认为写入操作已失败或卡住
- **冲突检测过滤**：只检查有活跃心跳的写入操作

#### 5.5.2 心跳实现

```java
public class HoodieHeartbeatClient {
  private final Long heartbeatIntervalInMs;  // 心跳间隔
  private final Long maxAllowableHeartbeatIntervalInMs;  // 最大允许心跳间隔
  
  // 启动心跳
  public void start(String instantTime) {
    updateHeartbeat(instantTime);  // 立即更新一次
    newHeartbeat.getTimer().scheduleAtFixedRate(
        new HeartbeatTask(instantTime), 
        this.heartbeatIntervalInMs,
        this.heartbeatIntervalInMs);
  }
  
  // 更新心跳
  private void updateHeartbeat(String instantTime) {
    Long newHeartbeatTime = System.currentTimeMillis();
    // 创建/更新心跳文件
    this.storage.create(new StoragePath(heartbeatFolderPath, instantTime), true);
    heartbeat.setLastHeartbeatTime(newHeartbeatTime);
  }
}
```

**心跳文件位置**：`$basePath/.hoodie/.heartbeat/<instantTime>`

## 六、冲突检测的调用时机

### 6.1 提交流程中的冲突检测

```java
// BaseCommitActionExecutor.java
protected void autoCommit(...) {
  final Option<HoodieInstant> inflightInstant = Option.of(new HoodieInstant(
      State.INFLIGHT, getCommitActionType(), instantTime));
  
  TransactionManager txnManager = this.txnManagerOption.get();
  txnManager.beginTransaction(inflightInstant, ...);
  
  try {
    setCommitMetadata(result);
    // 重新加载活跃时间线以获取所有更新
    TransactionUtils.resolveWriteConflictIfAny(
        table, 
        txnManager.getCurrentTransactionOwner(),
        result.getCommitMetadata(), 
        config, 
        txnManager.getLastCompletedTransactionOwner(), 
        true,  // reloadActiveTimeline = true
        pendingInflightAndRequestedInstants);
    commit(extraMetadata, result);
  } finally {
    txnManager.endTransaction(inflightInstant);
  }
}
```

### 6.2 Marker 创建时的早期冲突检测

```java
// WriteMarkers.java
public Option<StoragePath> createIfNotExists(
    String partitionPath, String fileName, IOType type, 
    HoodieWriteConfig writeConfig, String fileId, 
    HoodieActiveTimeline activeTimeline) {
  
  if (writeConfig.isEarlyConflictDetectionEnable()
      && writeConfig.getWriteConcurrencyMode().supportsOptimisticConcurrencyControl()) {
    
    // 对于压缩和聚类操作，跳过早期冲突检测
    HoodieTimeline pendingCompactionTimeline = activeTimeline.filterPendingCompactionTimeline();
    HoodieTimeline pendingReplaceTimeline = activeTimeline.filterPendingReplaceTimeline();
    if (pendingCompactionTimeline.containsInstant(instantTime) 
        || pendingReplaceTimeline.containsInstant(instantTime)) {
      return create(partitionPath, fileName, type, true);
    }
    
    // 执行早期冲突检测
    return createWithEarlyConflictDetection(
        partitionPath, fileName, type, false, writeConfig, fileId, activeTimeline);
  }
  return create(partitionPath, fileName, type, true);
}
```

## 七、特殊场景处理

### 7.1 压缩操作（Compaction）

- 压缩操作最终会以 COMMIT 操作出现在时间线上
- 在冲突解决时，需要特殊处理压缩操作，避免将其视为普通提交
- 如果压缩操作时间戳更早，当前写入可以继续

### 7.2 聚类操作（Clustering）

- REPLACE 操作中的 CLUSTER 操作不支持并发更新
- 如果看到重叠的文件 ID，视为冲突
- 未来计划支持 CLUSTER 的并发更新（HUDI-1042）

### 7.3 日志压缩（Log Compaction）

- 日志压缩是重写操作，可以与其他 delta commit 一起提交
- 提交顺序由 `AbstractHoodieLogRecordReader` 的 scan 方法处理
- 只有当日志压缩提交的时间戳小于压缩提交时才会产生冲突

### 7.4 Bucket Index

对于使用 Bucket Index 的表，冲突检测基于 Bucket ID 而非文件 ID：

```java
// BucketIndexConcurrentFileWritesConflictResolutionStrategy
@Override
public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
  // 将文件 ID 转换为 Bucket ID
  Set<String> partitionBucketIdSetForFirstInstant = thisOperation
      .getMutatedPartitionAndFileIds()
      .stream()
      .map(partitionAndFileId ->
          BucketIdentifier.partitionBucketIdStr(
              partitionAndFileId.getLeft(), 
              BucketIdentifier.bucketIdFromFileId(partitionAndFileId.getRight())))
      .collect(Collectors.toSet());
  
  // 检查 Bucket ID 交集
  Set<String> intersection = new HashSet<>(partitionBucketIdSetForFirstInstant);
  intersection.retainAll(partitionBucketIdSetForSecondInstant);
  return !intersection.isEmpty();
}
```

## 八、配置参数

### 8.1 OCC 相关配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.concurrency.mode` | `SINGLE_WRITER` | 写入并发模式 |
| `hoodie.write.lock.provider` | - | 锁提供者类（OCC 需要） |
| `hoodie.write.lock.wait_time_ms` | - | 锁等待时间 |
| `hoodie.write.lock.num_retries` | - | 锁重试次数 |

### 8.2 早期冲突检测配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.concurrency.early.conflict.detection.enable` | `false` | 是否启用早期冲突检测 |
| `hoodie.write.concurrency.early.conflict.detection.strategy` | 根据 Marker 类型自动选择 | 早期冲突检测策略类名 |
| `hoodie.write.concurrency.early.conflict.check.commit` | `false` | 是否在早期检测中检查提交冲突 |
| `hoodie.write.concurrency.early.conflict.detection.max.heartbeat.interval.ms` | - | 最大允许心跳间隔 |

### 8.3 心跳配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.client.heartbeat.interval.inms` | `30 * 1000` | 心跳间隔（毫秒） |
| `hoodie.client.heartbeat.tolerable.misses` | `3` | 可容忍的心跳丢失次数 |

### 8.4 冲突重试配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `hoodie.write.num.retries.on.conflict.failures` | `0` | 冲突失败时的重试次数 |

## 九、总结

### 9.1 OCC 机制特点

1. **文件组级别冲突检测**：基于 (分区路径, 文件ID) 对进行冲突检测
2. **时间线驱动**：使用 Hudi Timeline 作为中央协调机制
3. **延迟检测 + 早期检测**：双重保障机制
4. **心跳机制**：用于活跃性检测和过期过滤
5. **可插拔策略**：支持不同的冲突解决策略

### 9.2 优势

- **无锁设计**：不需要分布式锁（可选锁提供者用于早期检测的事务保护）
- **高吞吐量**：在低冲突场景下性能优异
- **资源节约**：早期冲突检测可以提前终止冲突的写入操作

### 9.3 局限性

- **高冲突场景性能下降**：OCC 在高冲突场景下性能较差，需要重试
- **INSERT 冲突无法检测**：如果两个并发写入者执行 INSERT 操作写入相同的记录键，但写入不同的文件组，OCC 无法检测
- **需要外部锁提供者**：某些场景（如早期检测的事务模式）需要外部锁服务

### 9.4 最佳实践

1. **避免高冲突场景**：通过输入流序列化更新/删除/插入操作
2. **启用早期冲突检测**：在资源敏感的场景下启用早期冲突检测
3. **合理配置心跳**：根据作业执行时间合理设置心跳间隔
4. **监控冲突率**：监控写入冲突率，评估是否需要调整写入策略

---

**参考文档**：
- RFC-56: Early Conflict Detection For Multi-writer
- RFC-69: Non-blocking Concurrency Control
- Hudi 官方文档
