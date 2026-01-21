# Task 重试场景的文件保护方案

## 概述

本文档描述了针对单作业内 Task 重试导致的文件损坏问题的解决方案。该方案**利用现有的 writeToken 机制**，无需修改 marker 文件命名规范，**无需实现 task 级别心跳**，完全向后兼容。

## 问题场景

```
时间线 ──────────────────────────────────────────────────────────────────────────►
     │
     │  Task A (attempt=0) 开始写 log 文件
     │  ├──────────────────────────────────────────────────────────────────────►
     │  │                                                    (假死但实际还在写)
     │  │
     │  │        Spark/Flink 认为 Task A 超时
     │  │        启动 Task A' (attempt=1) 重试
     │  │              │
     │  │              ├─────────────────────────────────────────────────────────►
     │  │              │                           (开始写同一个 log 文件)
     │  │              │
     │  └──────────────┼──────────────────────────────────────────────────────────►
     │                 │
     │     【危险！】两个 task 同时写同一个物理文件 → 文件损坏
```

### 问题根因

1. Task 执行变慢（GC、网络延迟、资源争用等）
2. Spark/Flink 的 Executor 心跳超时，认为 Task 失败
3. 调度器启动新的 Task Attempt 重试
4. 原 Task 实际还在写入
5. 两个 Task 同时写同一个 log 文件，导致数据交错损坏

## 核心设计：利用现有 writeToken

### writeToken 机制说明

Hudi 的文件命名中已经包含 `writeToken`，格式为：

```
writeToken = {taskPartitionId}-{stageId}-{attemptNumber}
```

**数据文件名格式**：
```
{fileId}_{writeToken}_{commitTime}.parquet
例如: file-abc123_0-5-0_20240115120000000.parquet
                  ^^^^^
                  writeToken: partition=0, stage=5, attempt=0
```

**Marker 文件名格式**（与数据文件对应）：
```
{fileId}_{writeToken}_{commitTime}.parquet.marker.{IOType}
例如: file-abc123_0-5-0_20240115120000000.parquet.marker.CREATE
```

### 关键发现

**不同 task attempt 的 writeToken 不同**，因此 marker 文件名也不同：

```
原始 Task (attempt=0) 的 marker：
file-abc123_0-5-0_20240115120000000.parquet.marker.APPEND
            ^^^^^
            attemptNumber = 0

重试 Task (attempt=1) 的 marker：
file-abc123_0-5-1_20240115120000000.parquet.marker.APPEND
            ^^^^^
            attemptNumber = 1  ← 不同！
```

**这意味着：无需修改 marker 命名规范，只需解析 writeToken 即可检测 attempt 冲突！**

### 为什么不需要心跳机制？

由于 writeToken 机制，不同 task attempt 写的是**不同的物理文件**：

| 场景 | 原始 task 状态 | 处理方式 | 结果 |
|-----|--------------|---------|------|
| 原始 task 还活着 | 假死但仍在写 | 两者都各写各的文件 | 两份独立文件，commit 时去重 |
| 原始 task 已死 | 真正失败 | 重试 task 正常写入 | 正常 |
| 原始 task 刚完成 | 已完成 | 重试 task rollover 后写入 | 两份文件，commit 时去重 |

**关键点**：由于 writeToken 机制保证文件名唯一，无论原始 task 是否还活着，都不会出现两个 task 写同一个物理文件的情况。

## 详细流程图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Task 重试场景的文件保护方案                                    │
│               (基于 writeToken 解析，无需心跳，完全向后兼容)                       │
└─────────────────────────────────────────────────────────────────────────────────┘

                              ┌───────────────────────┐
                              │  Task 启动（写入前）   │
                              │  可能是原始 task      │
                              │  也可能是重试 task    │
                              └───────────┬───────────┘
                                          │
                                          ▼
                       ┌────────────────────────────────────┐
                       │  获取当前 Task 的 writeToken        │
                       │  writeToken = partitionId + "-" +  │
                       │    stageId + "-" + attemptNumber   │
                       │                                    │
                       │  例如: "0-5-0" (attempt=0)         │
                       │        "0-5-1" (attempt=1, 重试)   │
                       └────────────────┬───────────────────┘
                                        │
                                        ▼
               ╔════════════════════════════════════════════════════╗
               ║  扫描当前 instant 下同一 fileId 的所有 marker       ║
               ║  路径: .temp/{instant}/{partition}/                ║
               ║  匹配: {fileId}_*_{commitTime}.*.marker.*          ║
               ╚════════════════════════╤═══════════════════════════╝
                                        │
                                        ▼
               ┌────────────────────────────────────────────────────┐
               │  从 marker 文件名中解析 writeToken                  │
               │                                                    │
               │  marker 文件名:                                    │
               │  file-abc123_0-5-0_xxx.parquet.marker.APPEND       │
               │              ^^^^^                                 │
               │              解析出 writeToken = "0-5-0"           │
               │              提取 attemptNumber = 0                │
               └────────────────────────┬───────────────────────────┘
                                        │
                                        ▼
               ╔════════════════════════════════════════════════════╗
               ║  检查是否存在同一 fileId 但不同 attemptNumber       ║
               ║  的 marker 文件？                                  ║
               ╚════════════════════════╤═══════════════════════════╝
                                        │
                  ┌─────────────────────┴─────────────────────┐
                  │                                           │
                  ▼                                           ▼
       ┌─────────────────────┐                    ┌─────────────────────────┐
       │  没有其他 attempt    │                    │   发现其他 attempt 的    │
       │  的 marker           │                    │   marker 存在！          │
       │  (正常情况)          │                    │   (task 重试场景)        │
       └──────────┬──────────┘                    └────────────┬────────────┘
                  │                                            │
                  ▼                                            ▼
       ┌─────────────────────┐               ┌─────────────────────────────────┐
       │ 创建 marker 文件     │               │   【强制 Rollover】              │
       │ (文件名包含当前      │               │   创建新的 log 文件版本          │
       │  writeToken)         │               │                                 │
       └──────────┬──────────┘               │   新文件名自动包含当前           │
                  │                          │   writeToken，保证文件名唯一     │
                  │                          │                                 │
                  │                          │   例如:                         │
                  │                          │   原 task: .log.1_0-5-0        │
                  │                          │   重试 task: .log.2_0-5-1      │
                  │                          └──────────────┬──────────────────┘
                  │                                         │
                  └────────────────────┬───────────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │    创建 marker 文件      │
                          │    写入数据文件          │
                          │    （各写各的，互不干扰） │
                          └─────────────────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │   Commit 阶段处理去重    │
                          │   只保留成功的 attempt   │
                          └─────────────────────────┘
```

## Marker 文件目录结构示例

```
/data/hudi/my_table/.hoodie/.temp/20240115120000000/
└── year=2024/month=01/
    │
    │  ┌─────────────────────────────────────────────────────────────────┐
    │  │ 同一 fileId，不同 attempt 的 marker 文件（文件名自动不同）        │
    │  └─────────────────────────────────────────────────────────────────┘
    │
    ├── file-abc123_0-5-0_20240115120000000.parquet.marker.APPEND
    │               ^^^^^
    │               原始 task (attempt=0) 的 writeToken
    │
    └── file-abc123_0-5-1_20240115120000000.parquet.marker.APPEND
                    ^^^^^
                    重试 task (attempt=1) 的 writeToken
    
    对应的数据文件（也是不同物理文件）：
    ├── .file-abc123_20240115100000000.log.1_0-5-0   ← 原始 task 写的
    └── .file-abc123_20240115100000000.log.2_0-5-1   ← 重试 task 写的 (rollover)
```

## 关键实现代码

### 1. 从 marker 文件名解析 writeToken

```java
/**
 * 从 marker 文件名中提取 writeToken 和 attemptNumber
 */
public class WriteTokenParser {
    
    /**
     * 解析 marker 文件名中的 writeToken
     * 
     * @param markerFileName 例如: "file-abc123_0-5-0_20240115.parquet.marker.APPEND"
     * @return writeToken 例如: "0-5-0"
     */
    public static String extractWriteToken(String markerFileName) {
        // 文件名格式: {fileId}_{writeToken}_{commitTime}.{ext}.marker.{IOType}
        String[] parts = markerFileName.split("_");
        if (parts.length >= 3) {
            return parts[parts.length - 2];  // writeToken 在倒数第二段
        }
        return null;
    }
    
    /**
     * 从 writeToken 中提取 attemptNumber
     * 
     * @param writeToken 例如: "0-5-1"
     * @return attemptNumber 例如: 1
     */
    public static int extractAttemptNumber(String writeToken) {
        // writeToken 格式: {taskPartitionId}-{stageId}-{attemptNumber}
        String[] parts = writeToken.split("-");
        if (parts.length >= 3) {
            return Integer.parseInt(parts[2]);
        }
        return 0;
    }
}
```

### 2. 检测其他 attempt 的 marker

```java
/**
 * 检测是否有其他 task attempt 正在写同一个 fileId
 */
public boolean hasOtherAttemptWriting(String partitionPath, String fileId, 
                                       String currentWriteToken) {
    // 获取当前 attempt number
    int currentAttempt = WriteTokenParser.extractAttemptNumber(currentWriteToken);
    
    // 列出当前 instant 下该 fileId 的所有 marker
    String markerDir = basePath + "/.hoodie/.temp/" + instantTime + "/" + partitionPath;
    List<String> markers = storage.listFilesWithPrefix(markerDir, fileId + "_");
    
    for (String markerPath : markers) {
        String markerFileName = new Path(markerPath).getName();
        String writeToken = WriteTokenParser.extractWriteToken(markerFileName);
        int attemptNumber = WriteTokenParser.extractAttemptNumber(writeToken);
        
        // 发现不同 attempt 的 marker
        if (attemptNumber != currentAttempt) {
            LOG.warn("Detected other task attempt writing same fileId: " + fileId + 
                     ", current attempt: " + currentAttempt + 
                     ", other attempt: " + attemptNumber);
            return true;
        }
    }
    return false;
}
```

### 3. 处理冲突：强制 Rollover

```java
/**
 * 处理 task attempt 冲突 - 简化版，无需心跳
 * 
 * 核心思路：由于 writeToken 机制保证不同 attempt 写不同物理文件，
 * 检测到冲突时直接 rollover 即可，无需关心其他 task 的状态。
 */
public void handleAttemptConflict(HoodieLogFormatWriter writer, 
                                   String fileId, 
                                   String currentWriteToken) {
    if (hasOtherAttemptWriting(partitionPath, fileId, currentWriteToken)) {
        // 检测到其他 attempt 存在，强制 rollover 到新 log 文件
        // 新文件自动使用当前 writeToken，保证文件名唯一
        int currentAttempt = WriteTokenParser.extractAttemptNumber(currentWriteToken);
        LOG.info("Task (attempt={}) detected other attempt writing same fileId: {}, " +
                 "forcing rollover to ensure file isolation", currentAttempt, fileId);
        writer.rollOver();
    }
    // 继续正常写入，各 task 写各自的文件，互不干扰
}
```

## 方案优势

| 优势 | 说明 |
|-----|------|
| **完全向后兼容** | 不修改 marker 文件命名规范 |
| **零额外存储** | 不需要创建额外的标记文件 |
| **利用现有机制** | writeToken 已包含 attemptNumber |
| **新老客户端兼容** | 老客户端正常工作，新客户端增加检测 |
| **无迁移成本** | 无需升级现有表格式 |
| **无需心跳机制** | 简化实现，降低复杂度 |
| **实现简单** | 只需解析文件名 + 强制 rollover |

## 与分区级并发检测的对比

| 维度 | 分区级并发检测 | Task 重试文件保护 |
|-----|---------------|------------------|
| 解决的问题 | 不同 instant (作业) 写同一分区 | 同一 instant 内 task 重试 |
| 冲突检测粒度 | 分区级 | 文件级 (fileId + writeToken) |
| 检测依据 | 分区目录是否存在 | 解析 marker 文件名中的 writeToken |
| 需要锁 | 是 (ZK 分区锁) | 否 |
| 需要心跳 | 否 | 否 |
| 修改 marker 格式 | 否 | 否 |
| 向后兼容 | 是 | 是 |
| 冲突处理方式 | 抛出异常，任务失败 | 强制 rollover 到新文件 |

## 配置参数

```properties
# 启用 task attempt 冲突检测
hoodie.write.task.attempt.conflict.detection.enable=true
```

## Spark 使用方法

### Spark DataSource API

```scala
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

val hudiOptions = Map(
  "hoodie.table.name" -> "my_table",
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "partition",
  "hoodie.datasource.write.precombine.field" -> "ts",
  
  // 启用 task attempt 冲突检测
  "hoodie.write.task.attempt.conflict.detection.enable" -> "true"
)

df.write
  .format("hudi")
  .options(hudiOptions)
  .mode("append")
  .save("/path/to/hudi/table")
```

### Spark SQL

```sql
-- 创建表时配置
CREATE TABLE hudi_table (
  id INT,
  name STRING,
  ts TIMESTAMP,
  partition STRING
) USING hudi
PARTITIONED BY (partition)
TBLPROPERTIES (
  'hoodie.write.task.attempt.conflict.detection.enable' = 'true'
);

-- 或者通过 SET 命令配置
SET hoodie.write.task.attempt.conflict.detection.enable=true;

INSERT INTO hudi_table VALUES (1, 'Alice', current_timestamp(), '2024-01');
```

### Spark Structured Streaming

```scala
import org.apache.hudi.DataSourceWriteOptions._

val hudiOptions = Map(
  "hoodie.table.name" -> "streaming_table",
  "hoodie.datasource.write.recordkey.field" -> "id",
  "hoodie.datasource.write.partitionpath.field" -> "partition",
  "hoodie.datasource.write.precombine.field" -> "ts",
  "hoodie.datasource.write.operation" -> "upsert",
  
  // 启用 task attempt 冲突检测（流式场景推荐开启）
  "hoodie.write.task.attempt.conflict.detection.enable" -> "true"
)

streamingDF.writeStream
  .format("hudi")
  .options(hudiOptions)
  .option("checkpointLocation", "/path/to/checkpoint")
  .outputMode("append")
  .start("/path/to/hudi/table")
```

### HoodieWriteClient API

```java
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.config.HoodieWriteConfig;

HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
    .withPath("/path/to/hudi/table")
    .withSchema(schema)
    // 启用 task attempt 冲突检测
    .withTaskAttemptConflictDetectionEnable(true)
    .build();

SparkRDDWriteClient<HoodieRecordPayload> client = 
    new SparkRDDWriteClient<>(engineContext, writeConfig);

// 执行写入操作
client.upsert(records, instantTime);
```

## Flink 使用方法

### Flink SQL

```sql
-- 创建 Hudi 表
CREATE TABLE hudi_table (
  id INT,
  name STRING,
  ts TIMESTAMP(3),
  partition STRING
) PARTITIONED BY (partition)
WITH (
  'connector' = 'hudi',
  'path' = '/path/to/hudi/table',
  'table.type' = 'MERGE_ON_READ',
  
  -- 启用 task attempt 冲突检测
  'hoodie.write.task.attempt.conflict.detection.enable' = 'true'
);

-- 插入数据
INSERT INTO hudi_table VALUES (1, 'Alice', TIMESTAMP '2024-01-15 12:00:00', '2024-01');
```

### Flink DataStream API

```java
import org.apache.hudi.configuration.FlinkOptions;

Configuration conf = new Configuration();
conf.setString(FlinkOptions.PATH, "/path/to/hudi/table");
conf.setString(FlinkOptions.TABLE_TYPE, "MERGE_ON_READ");
conf.setString(FlinkOptions.RECORD_KEY_FIELD, "id");
conf.setString(FlinkOptions.PARTITION_PATH_FIELD, "partition");
conf.setString(FlinkOptions.PRECOMBINE_FIELD, "ts");

// 启用 task attempt 冲突检测
conf.setBoolean("hoodie.write.task.attempt.conflict.detection.enable", true);

DataStream<RowData> dataStream = ...;

HoodiePipeline.Builder builder = HoodiePipeline.builder("hudi_table")
    .column("id INT")
    .column("name STRING")
    .column("ts TIMESTAMP(3)")
    .column("partition STRING")
    .pk("id")
    .partition("partition")
    .options(conf);

builder.sink(dataStream, false);
env.execute("Flink Hudi Write Job");
```

### Flink Table API

```java
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

EnvironmentSettings settings = EnvironmentSettings.newInstance()
    .inStreamingMode()
    .build();
TableEnvironment tableEnv = TableEnvironment.create(settings);

// 设置全局配置
tableEnv.getConfig().getConfiguration()
    .setString("hoodie.write.task.attempt.conflict.detection.enable", "true");

// 创建表
tableEnv.executeSql("""
    CREATE TABLE hudi_table (
      id INT,
      name STRING,
      ts TIMESTAMP(3),
      partition STRING,
      PRIMARY KEY (id) NOT ENFORCED
    ) PARTITIONED BY (partition)
    WITH (
      'connector' = 'hudi',
      'path' = '/path/to/hudi/table',
      'table.type' = 'MERGE_ON_READ',
      'hoodie.write.task.attempt.conflict.detection.enable' = 'true'
    )
    """);
```

## 使用建议

### 何时启用

| 场景 | 建议 | 原因 |
|-----|------|------|
| **MOR 表 + 流式写入** | ✅ 强烈推荐 | 流式场景 task 重试频繁，log 文件追加模式风险高 |
| **MOR 表 + 批量写入** | ✅ 推荐 | 批量写入时间长，也可能触发 task 重试 |
| **COW 表** | ⚠️ 可选 | COW 表每次创建新文件，风险相对较低 |
| **对象存储 (S3/OSS/GCS)** | ✅ 强烈推荐 | 对象存储 append 无租约保护，冲突风险更高 |
| **HDFS** | ⚠️ 可选 | HDFS 有租约机制，但假死场景仍可能出问题 |

### 性能影响

- **开销极小**：仅在 log 文件打开时扫描 marker 目录
- **无分布式锁**：不涉及 ZK 等外部依赖
- **无网络调用**：只是本地/对象存储的目录 list 操作

### 与其他配置的配合

```properties
# 1. 基础配置
hoodie.write.task.attempt.conflict.detection.enable=true

# 2. 搭配推测执行（Spark）
# 注意：开启推测执行后更需要此功能
spark.speculation=true
spark.speculation.interval=100ms
spark.speculation.multiplier=1.5
spark.speculation.quantile=0.75

# 3. 搭配 Flink checkpoint
# Flink 的 checkpoint 失败可能导致 task 重试
execution.checkpointing.interval=60000
execution.checkpointing.timeout=600000
```

## Commit 阶段的去重处理

当同一个 fileId 存在多个 task attempt 的输出时，Commit 阶段需要处理去重：

```java
/**
 * Commit 阶段处理多 attempt 输出的去重
 */
public List<HoodieWriteStat> deduplicateAttemptOutputs(List<HoodieWriteStat> writeStats) {
    // 按 fileId 分组
    Map<String, List<HoodieWriteStat>> statsByFileId = writeStats.stream()
        .collect(Collectors.groupingBy(HoodieWriteStat::getFileId));
    
    List<HoodieWriteStat> result = new ArrayList<>();
    for (Map.Entry<String, List<HoodieWriteStat>> entry : statsByFileId.entrySet()) {
        List<HoodieWriteStat> statsForFile = entry.getValue();
        
        if (statsForFile.size() == 1) {
            // 只有一个 attempt，直接使用
            result.add(statsForFile.get(0));
        } else {
            // 多个 attempt，选择 attemptNumber 最大的（最新的重试）
            HoodieWriteStat latestStat = statsForFile.stream()
                .max(Comparator.comparingInt(stat -> 
                    WriteTokenParser.extractAttemptNumber(stat.getPath())))
                .orElse(statsForFile.get(0));
            result.add(latestStat);
            
            // 其他 attempt 的文件将在后续 cleaning 中被清理
            LOG.info("FileId {} has {} attempts, keeping attempt with highest attemptNumber",
                     entry.getKey(), statsForFile.size());
        }
    }
    return result;
}
```

## 最终效果

通过该方案，即使发生 task 重试：

```
原 task (attempt=0) 写的文件:
  .file-abc123_20240115100000000.log.1_0-5-0
                                      ^^^^^
                                      writeToken 包含 attempt=0

重试 task (attempt=1) 写的文件（rollover 后）:
  .file-abc123_20240115100000000.log.2_0-5-1
                                      ^^^^^
                                      writeToken 包含 attempt=1
```

**两个文件物理上完全独立，不会相互干扰。** Commit 时选择最新 attempt 的输出，其他文件由 cleaning 清理。

## 总结

该方案的核心思想是：**利用 writeToken 天然的唯一性，将"防止冲突写入"转化为"各写各的文件 + Commit 时去重"**，从而：

1. 避免了复杂的心跳机制
2. 避免了分布式锁
3. 完全向后兼容
4. 实现简单可靠
