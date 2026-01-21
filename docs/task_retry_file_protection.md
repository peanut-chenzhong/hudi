# Task 重试场景的文件保护方案

## 概述

本文档描述了针对单作业内 Task 重试导致的文件损坏问题的解决方案。该方案结合了 **扩展 Marker 机制**（方案 1）和 **强制 Rollover**（方案 3）来确保文件安全。

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

## 解决方案

### 方案设计

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Task 重试场景的文件保护方案                                    │
│                   (方案 1: 扩展 Marker + 方案 3: 强制 Rollover)                   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 详细流程图

```
                              ┌───────────────────────┐
                              │  Task 启动（写入前）   │
                              │  可能是原始 task      │
                              │  也可能是重试 task    │
                              └───────────┬───────────┘
                                          │
                                          ▼
                       ┌────────────────────────────────────┐
                       │  获取当前 Task 的唯一标识           │
                       │  taskAttemptId = partitionId +     │
                       │    stageId + stageAttemptNumber    │
                       └────────────────┬───────────────────┘
                                        │
                                        ▼
               ╔════════════════════════════════════════════════════╗
               ║     检查当前 instant 下是否有同一 fileId 的        ║
               ║     其他 taskAttemptId 的 marker 文件              ║
               ║     路径: .temp/{instant}/{partition}/             ║
               ║           {fileId}_{attemptId}.marker.APPEND       ║
               ╚════════════════════════╤═══════════════════════════╝
                                        │
                  ┌─────────────────────┴─────────────────────┐
                  │                                           │
                  ▼                                           ▼
       ┌─────────────────────┐                    ┌─────────────────────────┐
       │  没有其他 attempt    │                    │   发现其他 attempt 的    │
       │  的 marker           │                    │   marker 存在！          │
       │  (正常情况)          │                    │   (重试场景)             │
       └──────────┬──────────┘                    └────────────┬────────────┘
                  │                                            │
                  ▼                                            ▼
       ┌─────────────────────┐               ╔═════════════════════════════════╗
       │ 创建带 attemptId    │               ║   判断：我是重试 task 吗？       ║
       │ 的 marker 文件      │               ║   (stageAttemptNumber > 0)       ║
       │ {fileId}_{attemptId}│               ╚═══════════════╤═════════════════╝
       │ .marker.APPEND      │                               │
       └──────────┬──────────┘                 ┌─────────────┴─────────────┐
                  │                            │                           │
                  ▼                            ▼                           ▼
       ┌─────────────────────┐     ┌─────────────────────┐    ┌─────────────────────┐
       │  正常 append 到     │     │  我是重试 task       │    │  我是原始 task       │
       │  log 文件           │     │  原 task 可能还在写   │    │  但有重试 task 启动   │
       └──────────┬──────────┘     └──────────┬──────────┘    └──────────┬──────────┘
                  │                           │                          │
                  │                           ▼                          ▼
                  │              ┌─────────────────────────┐   ┌─────────────────────┐
                  │              │  【方案 3】强制 Rollover │   │  检查原 task 心跳    │
                  │              │  创建新的 log 文件版本   │   │  是否还活着          │
                  │              │  不 append 到原文件     │   └──────────┬──────────┘
                  │              │                         │              │
                  │              │  新文件名包含新 attempt  │    ┌────────┴────────┐
                  │              │  的 writeToken，保证唯一 │    │                 │
                  │              └──────────┬──────────────┘    ▼                 ▼
                  │                         │          ┌──────────────┐  ┌──────────────┐
                  │                         │          │ 心跳过期      │  │ 心跳正常     │
                  │                         │          │ 原task已死    │  │ 原task还活   │
                  │                         │          └───────┬──────┘  └───────┬──────┘
                  │                         │                  │                 │
                  │                         │                  ▼                 ▼
                  │                         │          ┌──────────────┐  ┌──────────────┐
                  │                         │          │ 清理原marker │  │ 等待/自杀    │
                  │                         │          │ 继续写入     │  │ 让重试task接管│
                  │                         │          └───────┬──────┘  └──────────────┘
                  │                         │                  │
                  │                         │                  │
                  └─────────────────────────┼──────────────────┘
                                            │
                                            ▼
                             ┌──────────────────────────────┐
                             │    安全地写入 log 文件        │
                             │    不会与其他 attempt 冲突    │
                             └──────────────────────────────┘
```

## Marker 文件命名规范

### 现有命名（不含 attemptId）

```
{fileId}.marker.{IOType}
例如: file-abc123.marker.APPEND
```

### 扩展命名（包含 attemptId）

```
{fileId}_{taskAttemptId}.marker.{IOType}
例如: file-abc123_0-5-0.marker.APPEND     # attempt 0
      file-abc123_0-5-1.marker.APPEND     # attempt 1 (重试)
```

## 文件目录结构示例

```
/data/hudi/my_table/.hoodie/.temp/20240115120000000/
└── year=2024/month=01/
    ├── file-abc123_0-5-0.marker.APPEND     # 原始 task (attempt=0) 的 marker
    ├── file-abc123_0-5-1.marker.APPEND     # 重试 task (attempt=1) 的 marker
    │
    ├── .file-abc123_xxx.log.1_0-5-0        # 原始 task 写的 log 文件
    └── .file-abc123_xxx.log.2_0-5-1        # 重试 task 写的 log 文件 (rollover 后的新文件)
```

## 关键实现点

### 1. taskAttemptId 获取

```java
// Spark
String taskAttemptId = TaskContext.get().taskAttemptId() + "-" + 
                       TaskContext.get().stageId() + "-" + 
                       TaskContext.get().attemptNumber();

// Flink
String taskAttemptId = getRuntimeContext().getTaskInfo().getTaskNameWithSubtasks() + "-" +
                       getRuntimeContext().getAttemptNumber();
```

### 2. 冲突检测逻辑

```java
public boolean hasOtherAttemptWriting(String partitionPath, String fileId, String currentAttemptId) {
    // 扫描 .temp/{instant}/{partition}/ 目录
    // 查找包含 {fileId}_ 前缀的 marker 文件
    // 检查是否有不同 attemptId 的 marker 存在
    
    String markerPrefix = fileId + "_";
    List<String> markers = listMarkersWithPrefix(partitionPath, markerPrefix);
    
    for (String marker : markers) {
        String attemptId = extractAttemptId(marker);
        if (!attemptId.equals(currentAttemptId)) {
            return true;  // 发现其他 attempt 的 marker
        }
    }
    return false;
}
```

### 3. 强制 Rollover 逻辑

```java
public void handleAttemptConflict(String fileId, String currentAttemptId, boolean isRetryAttempt) {
    if (isRetryAttempt) {
        // 重试 task：强制创建新的 log 文件版本
        // 新文件名包含当前 attempt 的 writeToken
        LOG.info("Retry task detected, forcing rollover to new log file");
        forceRolloverToNewLogFile();
    } else {
        // 原始 task：检查重试 task 的状态
        // 如果重试 task 已经开始写入，考虑放弃当前写入
        LOG.warn("Original task detected retry task marker, may need to abort");
        checkAndHandleRetryTask();
    }
}
```

## 两种场景对比

| 维度 | 分区级并发检测 | Task 重试文件保护 |
|-----|---------------|------------------|
| 解决的问题 | 不同 instant (作业) 写同一分区 | 同一 instant 内 task 重试 |
| 冲突检测粒度 | 分区级 | 文件级 (fileId + attemptId) |
| 锁机制 | ZK 分区锁 | 无需锁，靠 marker 检测 |
| Marker 文件名 | `{fileId}.marker.{IOType}` | `{fileId}_{attemptId}.marker.{IOType}` |
| 冲突处理方式 | 抛出异常，任务失败 | 强制 rollover 到新文件 |

## 最终结果

通过该方案，即使发生 task 重试：

1. **原 task 写的文件**: `.fileId_xxx.log.1_0-5-0`
2. **重试 task 写的文件**: `.fileId_xxx.log.2_0-5-1` (不同物理文件！)

两个文件独立，不会相互干扰。Commit 时的 compaction/cleaning 会处理可能的重复数据。

## 配置参数（建议）

```properties
# 启用 task attempt 级别的冲突检测
hoodie.write.marker.include.task.attempt.id=true

# 检测到其他 attempt 时的处理策略
# ROLLOVER: 强制创建新文件
# FAIL: 抛出异常
# WAIT: 等待原 task 完成
hoodie.write.task.attempt.conflict.strategy=ROLLOVER

# Task 心跳超时时间（用于判断原 task 是否还活着）
hoodie.write.task.heartbeat.timeout.ms=60000
```
