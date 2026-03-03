# 分区级早期冲突检测（ECD）测试计划

## 概述

本文档定义了分区级早期冲突检测（ECD）机制的完整测试计划，覆盖 48 个测试场景，
涵盖基础功能验证、边界条件、发散场景和交叉影响分析。

### 三层保护机制

| 层级 | 名称 | 检测对象 | 冲突处理 |
|------|------|---------|---------|
| 第一层 | 活跃心跳 ECD | 心跳活跃的其他 instant | **抛异常** → task 失败重试 |
| 第二层 | Marker 文件保护 | 同 instant 内的重复 task | **返回 empty** → rollover 写新文件 |
| 第三层 | 过期心跳分区冲突检测 | 心跳过期但可能"假死"的 instant | **返回 empty** → rollover 写新文件 |

### 测试环境前提

```properties
# 基础配置（一键模式）
hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
hoodie.write.lock.zookeeper.url=zk1:2181,zk2:2181,zk3:2181
hoodie.write.lock.zookeeper.base_path=/hudi/locks

# 以下参数由一键模式自动配置（无需手动设置）：
# hoodie.write.concurrency.early.conflict.detection.enable=true
# hoodie.write.concurrency.early.conflict.detection.strategy=...PartitionTransactionDirectMarkerBasedDetectionStrategy
# hoodie.write.lock.conflict.resolution.strategy=...PartitionBasedConcurrentWritesConflictResolutionStrategy
# hoodie.write.concurrency.expired.heartbeat.partition.conflict.check.enable=true
# hoodie.cleaner.policy.failed.writes=LAZY
# hoodie.write.markers.type=DIRECT
```

---

## 一、自动配置验证（场景 1-4）

验证 `OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT` 模式下参数自动配置的正确性。

### 场景 1：一键模式自动参数配置

| 项目 | 说明 |
|------|------|
| **目标** | 验证设置 `OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT` 后，所有相关参数被自动设置 |
| **前置条件** | 仅设置 `hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT` 和 ZK 锁配置 |
| **操作步骤** | 1. 创建 MOR 表并使用一键模式配置<br>2. 通过日志或反射获取最终生效的 WriteConfig<br>3. 验证自动配置参数 |
| **预期结果** | 以下参数被自动设置：<br>- `early.conflict.detection.enable=true`<br>- `early.conflict.detection.strategy=PartitionTransactionDirectMarkerBasedDetectionStrategy`<br>- `conflict.resolution.strategy=PartitionBasedConcurrentWritesConflictResolutionStrategy`<br>- `expired.heartbeat.partition.conflict.check.enable=true`<br>- `cleaner.policy.failed.writes=LAZY`<br>- `write.markers.type=DIRECT` |
| **验证方法** | 检查 driver 日志中的 `Automatically set` 信息 |
| **优先级** | P0 |

```scala
// 测试脚本
val df = spark.range(1).toDF("id")
  .withColumn("ts", lit(System.currentTimeMillis()))
  .withColumn("dt", lit("2024-01-15"))

df.write.format("hudi")
  .option("hoodie.table.name", "test_auto_config")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.partitionpath.field", "dt")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.write.concurrency.mode", "OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT")
  .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider")
  .option("hoodie.write.lock.zookeeper.url", "${ZK_URL}")
  .option("hoodie.write.lock.zookeeper.base_path", "/hudi/locks")
  .mode(SaveMode.Overwrite)
  .save("/tmp/hudi/test_auto_config")

// 验证：检查 driver 日志是否包含以下 "Automatically set" 信息
// grep "Automatically set" driver.log
```

### 场景 2：手动配置覆盖自动配置

| 项目 | 说明 |
|------|------|
| **目标** | 验证手动指定某参数时，自动配置不会覆盖用户设定 |
| **前置条件** | 一键模式 + 手动设置 `hoodie.client.heartbeat.interval_in_ms=30000` |
| **操作步骤** | 同场景 1，额外指定心跳间隔 |
| **预期结果** | 心跳间隔为 30s（用户设定值），其他参数仍为自动值 |
| **优先级** | P1 |

### 场景 3：Spark SQL CREATE TABLE 配置验证

| 项目 | 说明 |
|------|------|
| **目标** | 验证通过 Spark SQL 建表时一键模式配置生效 |
| **前置条件** | 无 |
| **操作步骤** | 1. 通过 `CREATE TABLE ... TBLPROPERTIES(...)` 建表<br>2. 执行 `INSERT INTO`<br>3. 检查 `.temp` 目录是否生成 marker 文件 |
| **预期结果** | `.temp/{instantTime}/{partition}/` 分区目录和 marker 文件正确创建 |
| **优先级** | P1 |

```sql
CREATE TABLE test_db.test_ecd_sql (
  id BIGINT,
  ts BIGINT,
  dt STRING
) USING hudi
PARTITIONED BY (dt)
TBLPROPERTIES (
  'type' = 'mor',
  'primaryKey' = 'id',
  'preCombineField' = 'ts',
  'hoodie.write.concurrency.mode' = 'OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT',
  'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider',
  'hoodie.write.lock.zookeeper.url' = '${ZK_URL}',
  'hoodie.write.lock.zookeeper.base_path' = '/hudi/locks'
);

INSERT INTO test_db.test_ecd_sql VALUES (1, 1000, '2024-01-15');

-- 验证 marker：
-- hadoop fs -ls /tmp/hudi/test_ecd_sql/.hoodie/.temp/
```

### 场景 4：缺失 ZK 锁配置时的报错

| 项目 | 说明 |
|------|------|
| **目标** | 验证使用 `PARTITION_LIMIT` 模式但未配置 ZK 锁时能给出明确错误 |
| **前置条件** | 只设置 `hoodie.write.concurrency.mode=OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT`，不设置锁配置 |
| **操作步骤** | 尝试写入数据 |
| **预期结果** | 在获取分区锁时抛出明确的异常提示需要配置 ZookeeperBasedLockProvider |
| **优先级** | P1 |

---

## 二、不同分区并发写入（场景 5-8）

验证不同分区的 writer 可以完全并行，互不干扰。

### 场景 5：两个 Spark 作业写不同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证两个 Spark 作业并发写不同分区，无冲突 |
| **前置条件** | 同一 MOR 表，开启 ECD |
| **操作步骤** | 1. spark-submit 启动 Job A 写分区 `dt=2024-01-15`<br>2. spark-submit 启动 Job B 写分区 `dt=2024-01-16`<br>3. 两个作业并行执行 |
| **预期结果** | 两个作业均成功提交，无冲突异常 |
| **验证方法** | 1. 两个作业的日志均无冲突警告<br>2. 查询两个分区数据完整<br>3. `.hoodie` 目录有两条 commit |
| **优先级** | P0 |

```scala
// === Job A (spark-submit #1) ===
// 写入分区 dt=2024-01-15
val dfA = spark.range(1, 10000).toDF("id")
  .withColumn("name", concat(lit("user_"), col("id")))
  .withColumn("ts", lit(System.currentTimeMillis()))
  .withColumn("dt", lit("2024-01-15"))

dfA.write.format("hudi")
  .option("hoodie.table.name", "test_diff_partition")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.partitionpath.field", "dt")
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.write.concurrency.mode", "OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT")
  .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider")
  .option("hoodie.write.lock.zookeeper.url", "${ZK_URL}")
  .option("hoodie.write.lock.zookeeper.base_path", "/hudi/locks")
  .mode(SaveMode.Append)
  .save("/tmp/hudi/test_diff_partition")

// === Job B (spark-submit #2, 同时执行) ===
// 写入分区 dt=2024-01-16（脚本同上，仅改 dt 值）
```

### 场景 6：Spark 和 Flink 写不同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Spark 和 Flink 混合引擎并发写不同分区 |
| **前置条件** | 同一 MOR 表，两个引擎均开启 `PARTITION_LIMIT` 模式 |
| **操作步骤** | 1. Spark 写分区 `dt=2024-01-15`<br>2. Flink 实时写分区 `dt=2024-01-16` |
| **预期结果** | 两个引擎均成功写入，互不干扰 |
| **验证方法** | 查询两个分区数据均完整 |
| **优先级** | P0 |

### 场景 7：三个 Spark 作业分别写三个不同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证多路并发写不同分区的扩展性 |
| **前置条件** | 同一 MOR 表 |
| **操作步骤** | 3 个 spark-submit 分别写 `dt=2024-01-13/14/15` |
| **预期结果** | 3 个作业均成功 |
| **优先级** | P2 |

### 场景 8：单作业连续多次写不同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证单个 writer 连续多批次写不同分区 |
| **前置条件** | 同一 Spark 作业 |
| **操作步骤** | 先写 `dt=2024-01-15`，再写 `dt=2024-01-16`，再写 `dt=2024-01-17` |
| **预期结果** | 3 次写入均成功，产生 3 条 deltacommit |
| **优先级** | P2 |

---

## 三、同分区并发冲突检测（场景 9-13）

验证同分区写入时 ECD 正确检测并阻止冲突。

### 场景 9：两个 Spark 作业写同一分区（核心场景）

| 项目 | 说明 |
|------|------|
| **目标** | 验证两个 Spark 作业写同一分区时，后到的 writer 被 ECD 阻止 |
| **前置条件** | 同一 MOR 表，开启 ECD |
| **操作步骤** | 1. 启动 Job A 写分区 `dt=2024-01-15`（大数据量，执行时间较长）<br>2. 在 Job A 执行过程中启动 Job B 写同一分区 `dt=2024-01-15` |
| **预期结果** | Job B 的 task 抛出 `HoodieEarlyConflictDetectionException`，包含信息 "Another writer is writing to the same partition" |
| **验证方法** | 1. Job A 成功<br>2. Job B 失败，日志包含分区冲突信息<br>3. 数据一致性验证：只有 Job A 的数据 |
| **优先级** | P0 |

```scala
// === Job A (先启动) ===
// 写大量数据到 dt=2024-01-15，确保执行时间较长
val dfA = spark.range(1, 1000000).toDF("id")
  .withColumn("name", concat(lit("user_a_"), col("id")))
  .withColumn("ts", lit(System.currentTimeMillis()))
  .withColumn("dt", lit("2024-01-15"))

dfA.write.format("hudi")
  .option("hoodie.table.name", "test_same_partition")
  // ... 同上配置 ...
  .mode(SaveMode.Append)
  .save("/tmp/hudi/test_same_partition")

// === Job B (Job A 执行过程中启动) ===
// 写数据到同一分区 dt=2024-01-15
val dfB = spark.range(1000001, 1010000).toDF("id")
  .withColumn("name", concat(lit("user_b_"), col("id")))
  .withColumn("ts", lit(System.currentTimeMillis()))
  .withColumn("dt", lit("2024-01-15"))

dfB.write.format("hudi")
  .option("hoodie.table.name", "test_same_partition")
  // ... 同上配置 ...
  .mode(SaveMode.Append)
  .save("/tmp/hudi/test_same_partition")
// 预期：Job B 在 ECD 阶段抛出异常
```

### 场景 10：Spark 先写，Flink 后写同一分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证跨引擎写同一分区的冲突检测 |
| **前置条件** | Spark 正在写分区 P1 |
| **操作步骤** | Flink 启动写同一分区 P1 |
| **预期结果** | Flink 检测到 Spark 心跳活跃 → ECD 检测失败<br>**注意**：Flink 走独立路径而非 ECD，但心跳和 marker 仍能互斥 |
| **优先级** | P0 |

### 场景 11：Job A 写完后 Job B 立即写同一分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Job A 完成提交后，Job B 可以正常写同一分区 |
| **前置条件** | Job A 已成功提交 |
| **操作步骤** | 等待 Job A 完成 → 启动 Job B 写同一分区 |
| **预期结果** | Job A 的 marker 和心跳已清理，Job B 正常写入成功 |
| **验证方法** | 1. Job B 日志无冲突<br>2. 查询数据包含两批数据 |
| **优先级** | P1 |

### 场景 12：同分区冲突后 Job B 重试成功

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Job B 冲突失败后，等待 Job A 完成，自动重试可成功 |
| **前置条件** | 场景 9 的续，Job A 已完成 |
| **操作步骤** | 1. Job B 第一次尝试失败<br>2. Job A 完成提交<br>3. Job B 重试（手动或自动） |
| **预期结果** | Job B 重试成功 |
| **优先级** | P1 |

### 场景 13：同分区 INSERT_OVERWRITE 与 UPSERT 冲突

| 项目 | 说明 |
|------|------|
| **目标** | 验证 INSERT_OVERWRITE 操作也受 ECD 保护 |
| **前置条件** | Job A 正在 UPSERT 到分区 P1 |
| **操作步骤** | Job B 尝试 INSERT_OVERWRITE 同一分区 |
| **预期结果** | Job B 被 ECD 阻止 |
| **优先级** | P1 |

---

## 四、ZK 锁优化（场景 14-17）

验证 ZK 分区锁的快路径/慢路径优化。

### 场景 14：同 instant 多 task 写同一分区（快路径验证）

| 项目 | 说明 |
|------|------|
| **目标** | 验证同一 instant 下多个 task 写同一分区时，仅第一个 task 获取锁 |
| **前置条件** | 单个 Spark 作业，数据分布在同一分区 |
| **操作步骤** | 1. 多个 executor 的 task 并发写 `dt=2024-01-15`<br>2. 观察 ZK 锁获取次数 |
| **预期结果** | 1. 仅 1 次 ZK 锁获取<br>2. 后续 task 走快路径（`Partition directory already exists, skipping lock acquisition`）<br>3. 数据正确写入 |
| **验证方法** | 搜索 executor 日志：<br>- 仅 1 条 `Acquiring partition lock`<br>- 多条 `already exists...skipping lock` |
| **优先级** | P0 |

### 场景 15：同 instant 多 task 写不同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证不同分区的 task 各自独立获取锁 |
| **前置条件** | 单个 Spark 作业，数据覆盖 10 个分区 |
| **操作步骤** | 多个 task 写 10 个不同分区 |
| **预期结果** | 10 次 ZK 锁获取，每个分区 1 次 |
| **优先级** | P1 |

### 场景 16：两个 instant 竞争同一分区锁

| 项目 | 说明 |
|------|------|
| **目标** | 验证两个不同 instant 的 task 竞争同一分区的 ZK 锁时，冲突检测正确 |
| **前置条件** | Job A 和 Job B 几乎同时启动写同一分区 |
| **操作步骤** | 两者的第一个 task 竞争获取锁 |
| **预期结果** | 获得锁的 task 通过冲突检测并创建分区目录，另一个 task 在锁释放后进入 → double-check 发现目录已存在（由另一个 instant 创建）→ 但该 task 走快路径时又发现另一个 instant 的 marker → ECD 仍可能在后续检测中发现冲突 |
| **验证方法** | 两个作业中一个最终被 ECD 阻止 |
| **优先级** | P1 |

### 场景 17：ZK 连接中断恢复

| 项目 | 说明 |
|------|------|
| **目标** | 验证 ZK 连接短暂中断后的锁获取行为 |
| **前置条件** | ZK 集群短暂重启 |
| **操作步骤** | ZK 恢复后启动写入 |
| **预期结果** | 锁获取可能失败并抛出异常，task 重试后成功 |
| **优先级** | P2 |

---

## 五、过期心跳检测（场景 18-22）

验证"心跳假死"场景下的 rollover 保护。

### 场景 18：模拟心跳过期 → rollover（Spark）

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Spark 检测到过期心跳分区冲突时触发 rollover |
| **前置条件** | 设置短心跳超时 `heartbeat.interval_in_ms=5000`, `tolerable.misses=1`（超时 5s） |
| **操作步骤** | 1. Job A 写分区 P1，模拟心跳停止（kill 心跳线程或 sleep 超过超时时间）<br>2. Job B 写同一分区 P1 |
| **预期结果** | 1. ECD 第一层不阻止（Job A 心跳已过期，被分到过期组）<br>2. 第三层检测到过期心跳 → `expiredHeartbeatPartitionConflictDetected=true`<br>3. 返回 `Option.empty()` → rollover 写新 log 文件 |
| **验证方法** | 1. Job B 日志包含 `Expired heartbeat partition conflict detected`<br>2. Job B 写入了新版本的 log 文件（非 Job A 追加的那个） |
| **优先级** | P0 |

### 场景 19：心跳正常 → 无 rollover

| 项目 | 说明 |
|------|------|
| **目标** | 验证所有 writer 心跳正常时，不触发过期心跳 rollover |
| **前置条件** | Job A 和 Job B 写不同分区，心跳均正常 |
| **操作步骤** | 正常并发写入 |
| **预期结果** | 无 `Expired heartbeat partition conflict detected` 日志 |
| **优先级** | P1 |

### 场景 20：Flink 检测到过期心跳 → rollover

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Flink 的独立过期心跳检测路径 |
| **前置条件** | Spark Job A 心跳过期，Flink 写同一分区 |
| **操作步骤** | 同场景 18，但 Writer B 是 Flink |
| **预期结果** | Flink 在 `FlinkAppendHandle.preLogFileOpen()` 中检测到过期心跳 → `hasExpiredHeartbeatPartitionConflict()` 返回 true → `preLogFileOpen` 返回 false → rollover |
| **验证方法** | Flink 日志包含 `Detected expired heartbeat partition conflict for partition` |
| **优先级** | P1 |

### 场景 21：快路径中的过期心跳检测

| 项目 | 说明 |
|------|------|
| **目标** | 验证走快路径（分区目录已存在）时仍然检测过期心跳 |
| **前置条件** | 同一 instant 的第 2 个 task 走快路径 |
| **操作步骤** | 1. Task 1（慢路径）创建分区目录<br>2. 在此期间另一个 instant 的 writer 心跳过期<br>3. Task 2（快路径）进入 |
| **预期结果** | Task 2 虽然跳过了锁，但仍调用 `hasExpiredHeartbeatPartitionConflict` 独立检查 → 检测到过期心跳 → rollover |
| **验证方法** | 快路径 task 也触发了 rollover |
| **优先级** | P1 |

### 场景 22：心跳恰好在检查时过期（边界竞态）

| 项目 | 说明 |
|------|------|
| **目标** | 验证心跳在 ECD 检查过程中过期的边界情况 |
| **前置条件** | Job A 心跳即将超时（距超时<1s） |
| **操作步骤** | Job B 启动写同一分区 |
| **预期结果** | 两种可能均安全：<br>- 如果检查时 A 仍活跃 → 第一层阻止 → 抛异常（过度保守但安全）<br>- 如果检查时 A 已过期 → 第三层检测 → rollover（正确处理） |
| **优先级** | P2 |

---

## 六、Task 重试保护（场景 23-25）

验证 Spark/Flink task 重试时的 marker 互斥和 rollover。

### 场景 23：Spark task 重试 → marker 存在 → rollover

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Spark task 失败重试时，通过 marker 互斥触发 rollover |
| **前置条件** | Spark 作业中某 task 需要追加 log 文件 |
| **操作步骤** | 1. Task attempt 0 创建 marker 并开始写入<br>2. 模拟 attempt 0 假死（OOM、GC 等）<br>3. Spark 重试 attempt 1 |
| **预期结果** | attempt 1 发现 marker 已存在 → `preLogFileOpen` 返回 false → rollover 写新 log 文件 |
| **优先级** | P1 |

### 场景 24：Flink task 重试 → createdMarkers 区分自身 vs 其他

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Flink 通过 `createdMarkers` Set 正确区分自己 vs 其他 attempt 创建的 marker |
| **前置条件** | Flink 作业中某 task 需要追加 log 文件 |
| **操作步骤** | 1. Task 实例 A 创建 marker 并写入<br>2. Task 实例 A 假死<br>3. Task 实例 B（重试实例）启动 |
| **预期结果** | 实例 B 的 `createdMarkers` 为空 → `createIfNotExists` 返回 empty → `preLogFileOpen` 返回 false → rollover |
| **优先级** | P1 |

### 场景 25：Flink 连续 mini-batch 写同一 log 文件

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Flink 连续 mini-batch 写同一 log 文件时，`createdMarkers` 正确追踪 |
| **前置条件** | Flink 作业持续运行 |
| **操作步骤** | 同一 task 连续多个 checkpoint 写同一分区 |
| **预期结果** | 第一次 `createIfNotExists` 成功 → 加入 `createdMarkers` → 后续 mini-batch 直接 contains → return true → 正常追加 |
| **优先级** | P1 |

---

## 七、Commit 级冲突解析（场景 26-28）

验证 `PartitionBasedConcurrentWritesConflictResolutionStrategy` 在提交阶段的兜底检测。

### 场景 26：ECD 漏检 → commit 级兜底

| 项目 | 说明 |
|------|------|
| **目标** | 验证当 ECD 未能检测到冲突（理论上不应发生），commit 级检查能兜底 |
| **前置条件** | 禁用 ECD（`early.conflict.detection.enable=false`），但保留分区级冲突解析策略 |
| **操作步骤** | 两个 Job 同时写同一分区并尝试提交 |
| **预期结果** | 后提交的 Job 在 commit 阶段被 `PartitionBasedConcurrentWritesConflictResolutionStrategy` 拒绝，抛出 `HoodieWriteConflictException` |
| **优先级** | P0 |

### 场景 27：Compaction 与普通写入的 commit 兼容

| 项目 | 说明 |
|------|------|
| **目标** | 验证 compaction 操作在 commit 级冲突解析中的特殊处理 |
| **前置条件** | Writer A 正在写分区 P1，同时 compaction 也涉及 P1 |
| **操作步骤** | compaction 先提交，Writer A 后提交 |
| **预期结果** | `resolveConflict` 中 compaction 检查特殊逻辑：如果 compaction instant < thisOperation instant → 允许通过 |
| **优先级** | P1 |

### 场景 28：Log Compaction 与 deltacommit 的 commit 兼容

| 项目 | 说明 |
|------|------|
| **目标** | 验证 log compaction 在 commit 级解析中被允许 |
| **前置条件** | Log compaction 和 delta commit 同分区 |
| **操作步骤** | 两者同时提交 |
| **预期结果** | `resolveConflict` 中 `LOG_COMPACTION_ACTION` 直接返回 commitMetadata → 允许通过 |
| **优先级** | P1 |

---

## 八、多分区写入 & 部分重叠（场景 29-31）

验证单 writer 跨多分区时，与其他 writer 的部分重叠场景。

### 场景 29：Writer A 写 P1+P2，Writer B 写 P2+P3（部分重叠）

| 项目 | 说明 |
|------|------|
| **目标** | 验证多分区写入中部分分区重叠时的冲突检测 |
| **前置条件** | 同一 MOR 表 |
| **操作步骤** | 1. Job A 写分区 `dt=2024-01-15` 和 `dt=2024-01-16`<br>2. Job B 同时写分区 `dt=2024-01-16` 和 `dt=2024-01-17` |
| **预期结果** | Job B 的 task 在写分区 `dt=2024-01-16` 时触发 ECD 冲突检测 → 抛异常。<br>Job B 对 `dt=2024-01-17` 的写入取决于 task 失败是否导致整个 job 失败 |
| **验证方法** | 1. Job A 成功<br>2. Job B 失败（至少 `dt=2024-01-16` 相关 task 失败）<br>3. 验证 `dt=2024-01-17` 分区状态 |
| **优先级** | P1 |

```scala
// === Job A ===
val dfA = spark.range(1, 10000).toDF("id")
  .withColumn("ts", lit(System.currentTimeMillis()))
  .withColumn("dt", when(col("id") % 2 === 0, lit("2024-01-15")).otherwise(lit("2024-01-16")))

// === Job B ===
val dfB = spark.range(10001, 20000).toDF("id")
  .withColumn("ts", lit(System.currentTimeMillis()))
  .withColumn("dt", when(col("id") % 2 === 0, lit("2024-01-16")).otherwise(lit("2024-01-17")))
```

### 场景 30：Writer 写 100 个分区，第 51 个分区与另一 writer 冲突

| 项目 | 说明 |
|------|------|
| **目标** | 验证大规模多分区写入中的部分冲突 |
| **前置条件** | Job A 正在写分区 `dt=2024-02-20` |
| **操作步骤** | Job B 写 100 个分区（`dt=2024-01-01` ~ `dt=2024-04-10`），其中 `dt=2024-02-20` 与 Job A 冲突 |
| **预期结果** | Job B 在写 `dt=2024-02-20` 的 task 上失败，其他分区的 task 可能已成功。<br>**关键问题**：已成功 task 的 marker 残留是否影响下次重试？ |
| **验证方法** | 1. 检查 `.temp` 目录 marker 状态<br>2. Job B 重试后（Job A 完成后）是否成功 |
| **优先级** | P1 |

### 场景 31：同一 writer 单 task 写多分区的冲突行为

| 项目 | 说明 |
|------|------|
| **目标** | 验证单个 task 内写多个分区时，每个分区独立检测冲突 |
| **前置条件** | repartition 导致单 task 处理多个分区数据 |
| **操作步骤** | 单 task 按顺序写 P1, P2, P3，其中 P2 被其他 writer 占用 |
| **预期结果** | P1 写入成功，P2 触发冲突异常，该 task 失败，P3 未执行 |
| **优先级** | P2 |

---

## 九、Writer 崩溃 & 恢复链路（场景 32-35）

验证 writer 异常终止后的检测和恢复行为。

### 场景 32：Writer A 崩溃，心跳未过期 → Writer B 写同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证 writer 刚崩溃（心跳尚未过期）时的冲突检测 |
| **前置条件** | Job A 正在写分区 P1，突然 kill -9 |
| **操作步骤** | 立即启动 Job B 写分区 P1（在 A 心跳过期之前） |
| **预期结果** | ECD 第一层：A 心跳仍活跃 → 分区冲突 → 抛异常阻止 B |
| **验证方法** | Job B 抛出 `HoodieEarlyConflictDetectionException` |
| **优先级** | P0 |

### 场景 33：Writer A 崩溃 → 心跳过期 → Writer B rollover → LAZY clean rollback

| 项目 | 说明 |
|------|------|
| **目标** | 验证完整的崩溃恢复链路：崩溃 → 过期 → rollover → rollback |
| **前置条件** | 设置短心跳超时（5s），LAZY clean 已配置 |
| **操作步骤** | 1. Job A 写分区 P1 后 kill -9<br>2. 等待心跳过期（>5s）<br>3. 启动 Job B 写分区 P1<br>4. Job B 完成后，LAZY clean 触发对 Job A 的 rollback |
| **预期结果** | 1. Job B 检测到 A 的过期心跳 → rollover 写新 log 文件<br>2. LAZY clean rollback 对 A 的数据写 ROLLBACK_BLOCK 到 log.999999999<br>3. rollback 的 log.999999999 不影响 B 的正常 log 文件<br>4. 最终查询数据只包含 Job B 的数据 |
| **验证方法** | 1. 查看分区下的 log 文件列表<br>2. 查询数据一致性 |
| **优先级** | P0（两个机制交叉点） |

### 场景 34：Writer A 崩溃后恢复（心跳重新活跃）

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Writer A 心跳过期后又恢复活跃（网络恢复）的情况 |
| **前置条件** | Job A 因网络问题心跳暂停后恢复 |
| **操作步骤** | 1. A 心跳过期 → B 进入 rollover 写新 log<br>2. A 网络恢复，继续写旧 log |
| **预期结果** | A 写旧 log，B 写新 log → 物理上不冲突 → 但 A 最终提交时 commit 级冲突解析可能拒绝 A（如果 B 已提交） |
| **优先级** | P2 |

### 场景 35：Writer A 创建分区目录但未创建 marker 就崩溃

| 项目 | 说明 |
|------|------|
| **目标** | 验证分区目录存在但无 marker 的边界状态 |
| **前置条件** | Job A 在 `createDirectory(markerPartitionDirPath)` 之后、marker 创建之前崩溃 |
| **操作步骤** | Job B 写同一分区 |
| **预期结果** | Job B 的 task 走快路径（目录已存在）→ 但仍检查过期心跳 → A 心跳过期 → rollover<br>**关键**：快路径不执行活跃心跳 ECD 检测，仅检查过期心跳 |
| **验证方法** | Job B 正常写入（rollover 到新 log 文件） |
| **优先级** | P1 |

---

## 十、三层保护交叉 & 优先级（场景 36-38）

验证三层保护在不同组合下的行为。

### 场景 36：Layer1 通过 + Layer3 检测到过期心跳 → rollover

| 项目 | 说明 |
|------|------|
| **目标** | 验证第一层通过（活跃心跳无冲突）但第三层检测到过期心跳的场景 |
| **前置条件** | Writer A（活跃）写分区 P1，Writer C（过期心跳）在分区 P2 有 marker。当前 writer 写 P2 |
| **操作步骤** | 当前 writer 写分区 P2 |
| **预期结果** | Layer1：A 在 P1 → 不同分区 → 通过<br>Layer3：C 过期心跳在 P2 → 检测到冲突 → rollover |
| **优先级** | P1 |

### 场景 37：Layer1 通过 + Layer3 通过 + Layer2 marker 已存在 → rollover

| 项目 | 说明 |
|------|------|
| **目标** | 验证 Layer2 独立触发 rollover（task 重试场景） |
| **前置条件** | 无其他 writer（Layer1、Layer3 通过），但 marker 已由前一个 attempt 创建 |
| **操作步骤** | Task attempt 1 尝试创建 marker |
| **预期结果** | marker 创建返回 empty → rollover。不会因 Layer1/Layer3 通过就跳过 marker 检查 |
| **优先级** | P1 |

### 场景 38：Layer1 阻止时不执行 Layer2/Layer3

| 项目 | 说明 |
|------|------|
| **目标** | 验证第一层阻止后直接抛异常，不进入后续层级 |
| **前置条件** | Writer A 活跃心跳在同一分区 |
| **操作步骤** | 当前 writer 尝试写入 |
| **预期结果** | Layer1 抛出 `HoodieEarlyConflictDetectionException`，Layer2 和 Layer3 不执行 |
| **优先级** | P2 |

---

## 十一、配置不一致 & 混合模式（场景 39-42）

验证不同 writer 配置不一致时的行为。

### 场景 39：Writer A 用 PARTITION_LIMIT，Writer B 用普通 OCC

| 项目 | 说明 |
|------|------|
| **目标** | 验证配置不一致的两个 writer 是否能互相检测 |
| **前置条件** | Writer A：`OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT`<br>Writer B：`OPTIMISTIC_CONCURRENCY_CONTROL`（未开 ECD） |
| **操作步骤** | 两者写同一分区 |
| **预期结果** | **A 能检测到 B**（因为 B 有心跳和 marker）<br>**B 不能检测到 A**（B 未开 ECD）<br>冲突在 commit 级别被 B 的冲突解析策略兜底（如果 B 用 SimpleConcurrent...） |
| **验证方法** | 至少一方在 commit 阶段失败 |
| **优先级** | P0（生产环境最可能出现） |

### 场景 40：不同心跳超时配置的混合 writer

| 项目 | 说明 |
|------|------|
| **目标** | 验证心跳间隔配置不一致时的误判风险 |
| **前置条件** | Writer A：`heartbeat.interval=30s, misses=2`（超时 60s）<br>Writer B：`heartbeat.interval=120s, misses=2`（超时 240s） |
| **操作步骤** | Writer A 检查 B 的心跳：B 的更新间隔 120s，A 认为 60s 就过期 |
| **预期结果** | **A 可能误判 B 心跳过期**（因为 A 用自己的超时阈值 60s，但 B 实际以 120s 间隔更新）→ 触发 rollover |
| **验证方法** | A 日志出现 `Expired heartbeat` 但 B 实际仍活跃 |
| **风险等级** | ⚠️ 已知限制：不同 writer 应使用相同心跳配置 |
| **优先级** | P1 |

### 场景 41：DIRECT marker 与 TIMELINE_SERVER marker 混用

| 项目 | 说明 |
|------|------|
| **目标** | 验证 marker 类型不一致时 ECD 是否能互检 |
| **前置条件** | Writer A：`DIRECT` marker，Writer B：`TIMELINE_SERVER` marker |
| **操作步骤** | 两者写同一分区 |
| **预期结果** | **ECD 可能无法检测到冲突**，因为 DIRECT 和 TIMELINE_SERVER 的 marker 存储位置不同。<br>需要 commit 级冲突解析兜底 |
| **风险等级** | ⚠️ 已知限制：`PARTITION_LIMIT` 模式强制使用 DIRECT marker |
| **优先级** | P2 |

### 场景 42：InProcessLockProvider 在多 JVM 环境

| 项目 | 说明 |
|------|------|
| **目标** | 验证使用 InProcessLockProvider 替代 ZK 锁时的行为 |
| **前置条件** | 配置 `hoodie.write.lock.provider=InProcessLockProvider` |
| **操作步骤** | 两个 JVM 进程同时写同一分区 |
| **预期结果** | InProcess 锁只在 JVM 内互斥 → 跨 JVM 无法保护分区目录创建 → 可能出现两个进程都创建了目录但只有一个做了冲突检测 |
| **风险等级** | ⚠️ 应使用 ZookeeperBasedLockProvider 或其他分布式锁 |
| **优先级** | P2 |

---

## 十二、与 Table Service 的交互（场景 43-46）

验证 ECD 与 compaction、clean、rollback 等 table service 的交互。

### 场景 43：Compaction 运行时 Writer 写同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证 compaction 不阻塞普通写入 |
| **前置条件** | Compaction 正在处理分区 P1 |
| **操作步骤** | Writer 写分区 P1 |
| **预期结果** | `getCandidateInstants` 中 compaction instant 被跳过（pending compaction 过滤）→ ECD 不阻止 writer<br>Writer 写新 log 文件，compaction 读旧 log 文件 → 互不影响 |
| **优先级** | P1 |

### 场景 44：Clean 运行时 Writer 写同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证 clean 操作与普通写入的兼容性 |
| **前置条件** | Clean 正在清理分区 P1 中的旧 file slice |
| **操作步骤** | Writer 写分区 P1 |
| **预期结果** | Clean 不创建 marker → ECD 不检测到 clean → 不冲突<br>Clean 只删除旧文件，writer 写新文件 → 互不影响 |
| **优先级** | P1 |

### 场景 45：LAZY Clean 触发 Rollback + Writer 写同分区

| 项目 | 说明 |
|------|------|
| **目标** | 验证 LAZY clean 触发 rollback 时的 log 隔离 |
| **前置条件** | Writer A 之前崩溃，LAZY clean 检测到需要 rollback |
| **操作步骤** | LAZY clean rollback Writer A 的同时，Writer B 正在写同一分区 |
| **预期结果** | rollback 写 log.999999999_0-0-0（隔离文件）<br>Writer B 写正常 log（如 log.2_xxx）<br>两者物理上写不同文件 → 无冲突 |
| **验证方法** | 1. 分区下存在 `log.999999999_0-0-0` 文件<br>2. Writer B 的 log 文件版本号正常递增<br>3. 查询结果正确（rollback 的数据被逻辑删除） |
| **优先级** | P0（与 rollback log 隔离交叉） |

### 场景 46：Metadata Table 更新与数据写入

| 项目 | 说明 |
|------|------|
| **目标** | 验证 metadata table 更新不触发 ECD |
| **前置条件** | Metadata table 已开启 |
| **操作步骤** | 正常写入触发 metadata table 同步更新 |
| **预期结果** | Metadata table 的更新是内部操作，不参与 ECD，不创建 `.temp` marker |
| **优先级** | P2 |

---

## 十三、存储层特殊行为（场景 47-48）

验证不同存储系统对 ECD 的影响。

### 场景 47：OBS/S3 最终一致性对 .temp listing 的影响

| 项目 | 说明 |
|------|------|
| **目标** | 验证对象存储最终一致性是否导致 ECD 漏检 |
| **前置条件** | 使用 OBS/S3 存储 |
| **操作步骤** | Writer A 刚创建 marker，Writer B 立即 list `.temp` 目录 |
| **预期结果** | **可能漏检**：如果 listing 有延迟，B 看不到 A 的 marker → ECD 通过 → 两者都写<br>最终由 commit 级冲突解析兜底 |
| **风险等级** | ⚠️ 已知限制：对象存储 ECD 可能有短暂窗口期 |
| **缓解措施** | commit 级冲突解析保证最终一致性 |
| **优先级** | P1 |

### 场景 48：HDFS HA Failover 期间的行为

| 项目 | 说明 |
|------|------|
| **目标** | 验证 HDFS NameNode 切换对 ECD 的影响 |
| **前置条件** | HDFS 集群发生 HA 切换 |
| **操作步骤** | 切换期间 writer 尝试 list `.temp` 或创建分区目录 |
| **预期结果** | IO 操作可能短暂失败 → ECD 抛出 `IOException` → task 失败重试 → HA 恢复后重试成功 |
| **优先级** | P2 |

---

## 附录 A：场景优先级总览

### P0（必须测试，核心功能）

| 编号 | 场景 | 关键词 |
|------|------|--------|
| 1 | 一键模式自动参数配置 | 配置正确性 |
| 5 | 两个 Spark 写不同分区 | 基础并发 |
| 6 | Spark + Flink 写不同分区 | 跨引擎并发 |
| 9 | 两个 Spark 写同一分区 | 核心冲突检测 |
| 14 | 同 instant 多 task 快路径 | ZK 锁优化 |
| 18 | 过期心跳 rollover | 第三层保护 |
| 26 | ECD 漏检 → commit 级兜底 | 兜底机制 |
| 32 | 崩溃后心跳未过期 → ECD 阻止 | 崩溃恢复 |
| 33 | 崩溃 → 过期 → rollover → rollback | 机制交叉 |
| 39 | PARTITION_LIMIT vs OCC 混配 | 配置兼容 |
| 45 | LAZY Clean rollback + 写同分区 | rollback 隔离 |

### P1（建议测试，重要场景）

| 编号 | 场景 | 关键词 |
|------|------|--------|
| 2 | 手动覆盖自动配置 | 配置 |
| 3 | SQL CREATE TABLE 配置 | SQL |
| 4 | 缺失 ZK 配置报错 | 容错 |
| 10 | Spark→Flink 同分区冲突 | 跨引擎 |
| 11 | A 完后 B 写同分区 | 串行写 |
| 12 | 冲突后重试成功 | 恢复 |
| 13 | INSERT_OVERWRITE 冲突 | 操作类型 |
| 15 | 多 task 写多分区 | ZK 锁 |
| 16 | 两 instant 竞争锁 | 并发 |
| 19 | 心跳正常无 rollover | 正常路径 |
| 20 | Flink 过期心跳 rollover | Flink |
| 21 | 快路径过期心跳检测 | 快路径 |
| 23 | Spark task 重试 rollover | Task 重试 |
| 24 | Flink task 重试 | Flink |
| 25 | Flink mini-batch | Flink |
| 27 | Compaction commit 兼容 | Compaction |
| 28 | Log Compaction commit 兼容 | LogCompaction |
| 29 | 多分区部分重叠 | 多分区 |
| 30 | 100 分区第 51 个冲突 | 规模 |
| 35 | 目录存在但无 marker | 边界 |
| 36 | Layer1 通过 + Layer3 rollover | 三层交叉 |
| 37 | Layer2 独立 rollover | 三层交叉 |
| 40 | 心跳配置不一致 | 配置 |
| 43 | Compaction + 写同分区 | 服务交互 |
| 44 | Clean + 写同分区 | 服务交互 |
| 47 | S3/OBS 最终一致性 | 存储 |

### P2（可选测试，边界场景）

| 编号 | 场景 | 关键词 |
|------|------|--------|
| 7 | 三路并发写不同分区 | 扩展性 |
| 8 | 单作业连续多批次 | 连续写 |
| 17 | ZK 连接中断恢复 | ZK |
| 22 | 心跳边界竞态 | 边界 |
| 31 | 单 task 写多分区冲突 | 边界 |
| 34 | 崩溃后恢复活跃 | 恢复 |
| 38 | Layer1 阻止后不执行后续 | 优先级 |
| 41 | Marker 类型混用 | 配置 |
| 42 | InProcessLock 跨 JVM | 锁 |
| 46 | Metadata Table 更新 | 内部 |
| 48 | HDFS HA Failover | 存储 |

---

## 附录 B：测试脚本模板

### B.1 建表脚本

```scala
// ========================================
// 通用建表脚本（Spark SQL）
// ========================================
val zkUrl = "zk1:2181,zk2:2181,zk3:2181"  // 修改为实际 ZK 地址
val tablePath = "/tmp/hudi/test_ecd"        // 修改为实际路径
val tableName = "test_ecd"

spark.sql(s"""
  CREATE TABLE IF NOT EXISTS test_db.${tableName} (
    id BIGINT,
    name STRING,
    ts BIGINT,
    dt STRING
  ) USING hudi
  PARTITIONED BY (dt)
  TBLPROPERTIES (
    'type' = 'mor',
    'primaryKey' = 'id',
    'preCombineField' = 'ts',
    'hoodie.write.concurrency.mode' = 'OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT',
    'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider',
    'hoodie.write.lock.zookeeper.url' = '${zkUrl}',
    'hoodie.write.lock.zookeeper.base_path' = '/hudi/locks'
  )
  LOCATION '${tablePath}'
""")

println(s"Table created at: ${tablePath}")
```

### B.2 写入数据脚本

```scala
// ========================================
// 通用写入脚本（可在不同 spark-submit 中运行）
// ========================================
val tableName = "test_ecd"
val partition = "2024-01-15"  // 修改分区值
val idStart = 1               // 修改起始 ID
val idEnd = 10000             // 修改结束 ID

import spark.implicits._
val df = (idStart to idEnd).map(i => (i.toLong, s"user_${i}", System.currentTimeMillis(), partition))
  .toDF("id", "name", "ts", "dt")

df.createOrReplaceTempView("tmp_data")

spark.sql(s"""
  INSERT INTO test_db.${tableName}
  SELECT * FROM tmp_data
""")

println(s"Inserted ${idEnd - idStart + 1} records into partition dt=${partition}")
```

### B.3 并发写入检测脚本

```bash
#!/bin/bash
# ========================================
# 并发写入测试脚本
# 同时启动两个 spark-submit 作业
# ========================================

ZK_URL="zk1:2181,zk2:2181,zk3:2181"
TABLE_PATH="/tmp/hudi/test_ecd_concurrent"

# 先建表（使用其中一个 spark-submit）
spark-submit \
  --class com.example.CreateTable \
  --master yarn \
  test-app.jar \
  --table-path ${TABLE_PATH}

echo "=== Starting Job A (partition dt=2024-01-15) ==="
spark-submit \
  --class com.example.WriteData \
  --master yarn \
  --name "ECD_Test_JobA" \
  test-app.jar \
  --table-path ${TABLE_PATH} \
  --partition "2024-01-15" \
  --id-start 1 \
  --id-end 100000 \
  2>&1 | tee /tmp/ecd_test_jobA.log &

JOB_A_PID=$!
sleep 5  # 等 Job A 启动

echo "=== Starting Job B (same partition dt=2024-01-15) ==="
spark-submit \
  --class com.example.WriteData \
  --master yarn \
  --name "ECD_Test_JobB" \
  test-app.jar \
  --table-path ${TABLE_PATH} \
  --partition "2024-01-15" \
  --id-start 100001 \
  --id-end 110000 \
  2>&1 | tee /tmp/ecd_test_jobB.log &

JOB_B_PID=$!

echo "=== Waiting for both jobs ==="
wait $JOB_A_PID
JOB_A_EXIT=$?
wait $JOB_B_PID
JOB_B_EXIT=$?

echo ""
echo "========================================="
echo "Job A exit code: ${JOB_A_EXIT}"
echo "Job B exit code: ${JOB_B_EXIT}"
echo "========================================="

# 检查结果
echo ""
echo "=== Checking for ECD conflicts ==="
grep -i "conflict\|EarlyConflictDetection\|expired heartbeat" /tmp/ecd_test_jobA.log
grep -i "conflict\|EarlyConflictDetection\|expired heartbeat" /tmp/ecd_test_jobB.log

echo ""
echo "=== Checking marker directories ==="
hadoop fs -ls ${TABLE_PATH}/.hoodie/.temp/

echo ""
echo "=== Checking data ==="
spark-sql -e "SELECT dt, count(*) FROM test_db.test_ecd_concurrent GROUP BY dt"
```

### B.4 过期心跳测试脚本

```scala
// ========================================
// 过期心跳 rollover 测试
// 需要两个 spark-shell 窗口配合执行
// ========================================

// === 窗口 1：启动一个写入然后模拟心跳停止 ===
// 设置短心跳超时
val zkUrl = "zk1:2181,zk2:2181,zk3:2181"
val tablePath = "/tmp/hudi/test_expired_hb"

spark.sql(s"""
  CREATE TABLE IF NOT EXISTS test_db.test_expired_hb (
    id BIGINT,
    name STRING,
    ts BIGINT,
    dt STRING
  ) USING hudi
  PARTITIONED BY (dt)
  TBLPROPERTIES (
    'type' = 'mor',
    'primaryKey' = 'id',
    'preCombineField' = 'ts',
    'hoodie.write.concurrency.mode' = 'OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT',
    'hoodie.write.lock.provider' = 'org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider',
    'hoodie.write.lock.zookeeper.url' = '${zkUrl}',
    'hoodie.write.lock.zookeeper.base_path' = '/hudi/locks',
    'hoodie.client.heartbeat.interval_in_ms' = '5000',
    'hoodie.client.heartbeat.tolerable.misses' = '1'
  )
  LOCATION '${tablePath}'
""")

// 写入第一批数据
import spark.implicits._
val df1 = (1 to 100).map(i => (i.toLong, s"user_${i}", System.currentTimeMillis(), "2024-01-15"))
  .toDF("id", "name", "ts", "dt")
df1.createOrReplaceTempView("tmp1")
spark.sql("INSERT INTO test_db.test_expired_hb SELECT * FROM tmp1")
println("Batch 1 done")

// 此时不要关闭 spark-shell，让它保持运行但不写入
// 心跳会在 5s 后超时
println("Now waiting... heartbeat will expire in 5 seconds.")
println("Please start Job B in another window within 10 seconds.")
Thread.sleep(60000)  // 等待 60s，期间心跳已过期

// === 窗口 2：在窗口 1 的心跳过期后启动 ===
// （大约在窗口 1 打印 "Please start Job B" 后 6-10 秒启动）
//
// 使用另一个 spark-submit / spark-shell 写同一分区
// 预期：检测到过期心跳 → rollover → 写新 log 文件
//
// 检查日志：
// grep "Expired heartbeat partition conflict detected" executor.log
// grep "rollover" executor.log
```

### B.5 marker 目录检查脚本

```bash
#!/bin/bash
# ========================================
# 检查 marker 目录和心跳文件
# ========================================

TABLE_PATH=$1

if [ -z "$TABLE_PATH" ]; then
  echo "Usage: $0 <table_path>"
  exit 1
fi

echo "=== Marker directories (.temp) ==="
hadoop fs -ls -R ${TABLE_PATH}/.hoodie/.temp/

echo ""
echo "=== Heartbeat files ==="
hadoop fs -ls ${TABLE_PATH}/.hoodie/.heartbeat/

echo ""
echo "=== Active timeline ==="
hadoop fs -ls ${TABLE_PATH}/.hoodie/ | grep -E "\.commit|\.deltacommit|\.inflight|\.requested"

echo ""
echo "=== Data partitions ==="
hadoop fs -ls ${TABLE_PATH}/ | grep -v "\.hoodie"

echo ""
echo "=== Log files in each partition ==="
for part in $(hadoop fs -ls ${TABLE_PATH}/ | grep -v "\.hoodie" | awk '{print $NF}'); do
  echo "--- Partition: $(basename $part) ---"
  hadoop fs -ls $part/ | grep "\.log\."
done
```

---

## 附录 C：关键日志搜索模式

| 关键词 | 含义 | 预期场景 |
|--------|------|---------|
| `Automatically set` | 自动配置参数 | 场景 1-3 |
| `Acquiring partition lock` | 获取分区 ZK 锁 | 场景 14-16（慢路径） |
| `already exists...skipping lock` | 分区目录已存在，跳过锁 | 场景 14（快路径） |
| `Performing partition-level conflict detection` | 执行 ECD 检测 | 场景 9-13 |
| `Early partition-level conflict detected` | ECD 发现冲突 | 场景 9-10, 29-30 |
| `Expired heartbeat partition conflict detected` | 过期心跳冲突 | 场景 18-21, 33 |
| `Rolling over to a new log file` | rollover 触发 | 场景 18, 23-24 |
| `HoodieWriteConflictException` | commit 级冲突 | 场景 26 |
| `Cannot resolve conflicts for overlapping writes at partition level` | 分区级 commit 冲突 | 场景 26 |
| `Creating marker partition directory` | 创建 marker 分区目录 | 场景 14（慢路径成功） |
| `Partition lock released` | 释放 ZK 锁 | 场景 14-16 |

---

## 附录 D：测试矩阵

### 引擎 × 功能矩阵

| 功能 | Spark | Flink |
|------|:-----:|:-----:|
| 第一层（活跃心跳 ECD） | ✅ `DirectWriteMarkers.createWithEarlyConflictDetection` | ❌ 不走 ECD |
| 第二层（Marker 保护） | ✅ `HoodieWriteHandle.createAppendMarker` | ✅ `FlinkAppendHandle.createdMarkers` |
| 第三层（过期心跳） | ✅ 集成在 ECD 扫描中 | ✅ `FlinkAppendHandle.hasExpiredHeartbeatPartitionConflict` |
| ZK 分区锁 | ✅ `PartitionTransactionDirectMarkerBasedDetectionStrategy` | ❌ 不走 ECD |
| Commit 级冲突解析 | ✅ `PartitionBasedConcurrentWritesConflictResolutionStrategy` | ✅ 同上 |

### 存储 × 功能矩阵

| 功能 | HDFS | OBS/S3 | 本地文件系统 |
|------|:----:|:------:|:----------:|
| `.temp` listing 一致性 | ✅ 强一致 | ⚠️ 最终一致 | ✅ 强一致 |
| 心跳文件读写 | ✅ | ✅ | ✅ |
| Marker 文件原子创建 | ✅ | ✅ | ✅ |
| Log 文件 append 租约 | ✅ HDFS 租约 | ❌ 无跨进程租约 | ✅ 文件锁 |
| Rollback log 隔离必要性 | ⚠️ 有租约但建议使用 | ✅ 必须 | ⚠️ 建议使用 |
