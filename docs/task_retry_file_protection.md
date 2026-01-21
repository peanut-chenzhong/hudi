# Task 重试场景的文件保护方案

## 概述

本文档描述了针对单作业内 Task 重试导致的文件损坏问题的解决方案。该方案**利用现有的 writeToken 机制**，无需修改 marker 文件命名规范，完全向后兼容。

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

## 详细流程图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Task 重试场景的文件保护方案                                    │
│                   (基于 writeToken 解析，完全向后兼容)                            │
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
       ┌─────────────────────┐               ╔═════════════════════════════════╗
       │ 创建 marker 文件     │               ║   判断：我是重试 task 吗？       ║
       │ (文件名包含当前      │               ║   (attemptNumber > 0)            ║
       │  writeToken)         │               ╚═══════════════╤═════════════════╝
       └──────────┬──────────┘                               │
                  │                             ┌─────────────┴─────────────┐
                  │                             │                           │
                  │                             ▼                           ▼
                  │                  ┌─────────────────────┐    ┌─────────────────────┐
                  │                  │  是重试 task         │    │  是原始 task         │
                  │                  │  (attemptNumber > 0) │    │  (attemptNumber = 0) │
                  │                  └──────────┬──────────┘    └──────────┬──────────┘
                  │                             │                          │
                  │                             ▼                          ▼
                  │              ┌─────────────────────────┐   ┌─────────────────────┐
                  │              │  【强制 Rollover】       │   │  检查原 task marker  │
                  │              │  创建新的 log 文件版本   │   │  的心跳是否过期      │
                  │              │                         │   └──────────┬──────────┘
                  │              │  新文件名自动包含新的    │              │
                  │              │  writeToken，保证唯一   │    ┌────────┴────────┐
                  │              │                         │    │                 │
                  │              │  例如:                   │    ▼                 ▼
                  │              │  原: .log.1_0-5-0       │  ┌──────────────┐  ┌──────────────┐
                  │              │  新: .log.2_0-5-1       │  │ 心跳过期      │  │ 心跳正常     │
                  │              └──────────┬──────────────┘  │ 原task可能死  │  │ 原task还活   │
                  │                         │                 └───────┬──────┘  └───────┬──────┘
                  │                         │                         │                 │
                  │                         │                         ▼                 ▼
                  │                         │                 ┌──────────────┐  ┌──────────────┐
                  │                         │                 │ 继续写入     │  │ 等待/放弃    │
                  │                         │                 │ 原task的marker│  │ 让重试task   │
                  │                         │                 │ 会被清理     │  │ 接管         │
                  │                         │                 └───────┬──────┘  └──────────────┘
                  │                         │                         │
                  └─────────────────────────┼─────────────────────────┘
                                            │
                                            ▼
                             ┌──────────────────────────────┐
                             │    安全地写入数据文件         │
                             │    不会与其他 attempt 冲突    │
                             └──────────────────────────────┘
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
 * 处理 task attempt 冲突
 */
public void handleAttemptConflict(HoodieLogFormatWriter writer, 
                                   String fileId, 
                                   String currentWriteToken) {
    int currentAttempt = WriteTokenParser.extractAttemptNumber(currentWriteToken);
    
    if (currentAttempt > 0) {
        // 重试 task：强制 rollover 到新 log 文件
        // 新文件自动使用当前 writeToken，保证文件名唯一
        LOG.info("Retry task (attempt=" + currentAttempt + ") detected conflict, " +
                 "forcing rollover to new log file");
        writer.rollOver();
    } else {
        // 原始 task：检查重试 task 是否还活着
        // 如果重试 task 活跃，考虑放弃当前写入
        LOG.warn("Original task detected retry task, checking heartbeat...");
        // ... 心跳检测逻辑
    }
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

## 与分区级并发检测的对比

| 维度 | 分区级并发检测 | Task 重试文件保护 |
|-----|---------------|------------------|
| 解决的问题 | 不同 instant (作业) 写同一分区 | 同一 instant 内 task 重试 |
| 冲突检测粒度 | 分区级 | 文件级 (fileId + writeToken) |
| 检测依据 | 分区目录是否存在 | 解析 marker 文件名中的 writeToken |
| 需要锁 | 是 (ZK 分区锁) | 否 |
| 修改 marker 格式 | 否 | 否 |
| 向后兼容 | 是 | 是 |
| 冲突处理方式 | 抛出异常，任务失败 | 强制 rollover 到新文件 |

## 配置参数（建议）

```properties
# 启用 task attempt 冲突检测
hoodie.write.task.attempt.conflict.detection.enable=true

# 检测到其他 attempt 时的处理策略
# ROLLOVER: 强制创建新文件（推荐）
# FAIL: 抛出异常
# WAIT: 等待原 task 完成
hoodie.write.task.attempt.conflict.strategy=ROLLOVER

# 心跳超时时间（用于判断原 task 是否还活着）
hoodie.write.task.heartbeat.timeout.ms=60000
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

**两个文件物理上完全独立，不会相互干扰。** Commit 时的 compaction/cleaning 会处理可能的重复数据。
