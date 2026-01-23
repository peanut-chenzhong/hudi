# Task 重试场景的文件保护方案

## 概述

本文档描述了针对单作业内 Task 重试导致的文件损坏问题的解决方案。该方案**利用现有的 Marker 文件机制**，无需额外配置，无需修改文件命名规范，完全向后兼容。

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

## 核心设计：利用 Marker 文件的原子性

### Marker 文件机制

Hudi 在写入数据文件之前会创建对应的 Marker 文件：

```
Marker 文件路径: .hoodie/.temp/{instantTime}/{partitionPath}/{dataFileName}.marker.{IOType}

例如:
.hoodie/.temp/20240115120000000/year=2024/month=01/.file-abc123_xxx.log.1_0-5-0.marker.APPEND
```

### 关键发现：Marker 文件的 "已存在则失败" 机制

`DirectWriteMarkers.create()` 方法中的实现：

```java
private Option<StoragePath> create(StoragePath markerPath, boolean checkIfExists) {
    // ...
    if (checkIfExists && storage.exists(markerPath)) {
        LOG.warn("Marker Path=" + markerPath + " already exists, cancel creation");
        return Option.empty();  // Marker 已存在，返回 empty
    }
    // 创建 marker 文件
    storage.create(markerPath, false).close();
    return Option.of(markerPath);
}
```

**这意味着**：如果 Marker 文件已经存在（由另一个 task/attempt 创建），新的创建请求会返回 `Option.empty()`，从而触发 rollover！

### 工作流程

```
Task A (attempt=0):                     Task A' (attempt=1):
     │                                       │
     ├─ preLogFileOpen()                     ├─ preLogFileOpen()
     │    └─ createAppendMarker()            │    └─ createAppendMarker()
     │         │                             │         │
     │         ├─ Marker 不存在              │         ├─ Marker 已存在!
     │         ├─ 创建成功 ✓                 │         │  (Task A 已创建)
     │         └─ return true                │         ├─ 返回 Option.empty()
     │                                       │         └─ return false
     │                                       │
     ├─ fs.append() 打开文件                 ├─ rollover() 创建新文件
     │                                       │
     └─ 写入数据                             └─ 写入数据到新文件
```

## 详细流程图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Task 重试场景的文件保护方案                                    │
│              (利用 Marker 文件原子性，无需额外配置，完全向后兼容)                  │
└─────────────────────────────────────────────────────────────────────────────────┘

                              ┌───────────────────────┐
                              │  Task 启动（写入前）   │
                              │  可能是原始 task      │
                              │  也可能是重试 task    │
                              └───────────┬───────────┘
                                          │
                                          ▼
               ╔════════════════════════════════════════════════════╗
               ║  准备 append 到已有 log 文件                        ║
               ║  调用 HoodieLogFormatWriter.getOutputStream()      ║
               ╚════════════════════════╤═══════════════════════════╝
                                        │
                                        ▼
               ┌────────────────────────────────────────────────────┐
               │  HoodieLogFileWriteCallback.preLogFileOpen()       │
               │  调用 createAppendMarker(logFile)                  │
               └────────────────────────┬───────────────────────────┘
                                        │
                                        ▼
               ┌────────────────────────────────────────────────────┐
               │  DirectWriteMarkers.createIfNotExists()            │
               │                                                    │
               │  Marker 路径:                                      │
               │  .hoodie/.temp/{instant}/{partition}/{log}.APPEND  │
               └────────────────────────┬───────────────────────────┘
                                        │
                                        ▼
               ╔════════════════════════════════════════════════════╗
               ║  检查 Marker 文件是否已存在？                       ║
               ║  storage.exists(markerPath)                        ║
               ╚════════════════════════╤═══════════════════════════╝
                                        │
                  ┌─────────────────────┴─────────────────────┐
                  │                                           │
                  ▼                                           ▼
       ┌─────────────────────┐                    ┌─────────────────────────┐
       │  不存在              │                    │   已存在                 │
       │  (当前 task 是第一   │                    │   (其他 task/attempt     │
       │   个写这个文件的)    │                    │    已经在写了)           │
       └──────────┬──────────┘                    └────────────┬────────────┘
                  │                                            │
                  ▼                                            ▼
       ┌─────────────────────┐               ┌─────────────────────────────────┐
       │ 创建 Marker 成功    │               │   返回 Option.empty()            │
       │ return true         │               │   preLogFileOpen 返回 false      │
       │                     │               │                                 │
       │ 正常 append         │               │   【触发 Rollover】              │
       │ 写入数据            │               │   创建新的 log 文件版本          │
       └──────────┬──────────┘               │   新文件使用当前 task 的         │
                  │                          │   writeToken                    │
                  │                          └──────────────┬──────────────────┘
                  │                                         │
                  └────────────────────┬───────────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │    各 task 写各自的文件   │
                          │    （物理上完全隔离）     │
                          └─────────────────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │   Commit 阶段处理去重    │
                          │   只保留成功的 attempt   │
                          └─────────────────────────┘
```

## 为什么这个方案有效？

### 1. Marker 文件的原子性保证

不同 instant 的 Marker 文件在不同目录下，自然隔离：

```
Instant 1: .hoodie/.temp/instant1/partition/file.APPEND
Instant 2: .hoodie/.temp/instant2/partition/file.APPEND
                         ^^^^^^^^          ^^^^^^^^
                         不同目录，不冲突
```

同一 instant 内，Marker 文件的原子创建保证：

```
Task A (attempt=0): 创建 .temp/instant1/partition/file.APPEND → 成功
Task A'(attempt=1): 创建 .temp/instant1/partition/file.APPEND → 失败（已存在）
                                                                     ↓
                                                              触发 rollover
```

### 2. 不需要复杂的 writeToken 比较

之前考虑过比较 log 文件名中的 `writeToken` 与当前 task 的 `writeToken`，但这个方案有问题：

- Log 文件名中的 `writeToken` 是**首次创建该文件**的 task 的 token
- 跨 instant 的正常 append 也会导致 token 不匹配，触发不必要的 rollover

**Marker 文件方案不存在这个问题**，因为：
- 不同 instant 的 Marker 在不同目录
- 同一 instant 内，第一个创建 Marker 的 task 成功，其他 attempt 失败

### 3. 零配置，完全向后兼容

这个机制是 Hudi 已有的行为，不需要：
- 新增配置项
- 修改文件命名规范
- 实现心跳机制

## Log 文件目录结构示例

```
/data/hudi/my_table/year=2024/month=01/
│
│  ┌─────────────────────────────────────────────────────────────────┐
│  │ 同一 fileId，不同 attempt 创建的 log 文件（文件名唯一）           │
│  └─────────────────────────────────────────────────────────────────┘
│
├── .file-abc123_20240115100000000.log.1_0-5-0
│                                       ^^^^^
│                                       writeToken = 0-5-0
│                                       (原始 task attempt=0 创建)
│
└── .file-abc123_20240115100000000.log.2_0-5-1
                                        ^^^^^
                                        writeToken = 0-5-1
                                        (重试 task attempt=1 创建，rollover 后)

对应的 Marker 文件：
/data/hudi/my_table/.hoodie/.temp/20240115120000000/year=2024/month=01/
├── .file-abc123_20240115100000000.log.1_0-5-0.marker.APPEND  ← 原始 task 创建
└── .file-abc123_20240115100000000.log.2_0-5-1.marker.APPEND  ← 重试 task 创建（rollover 后的新文件）
```

## 关键实现代码

### 1. AppendLogWriteCallback

```java
/**
 * HoodieWriteHandle.AppendLogWriteCallback
 * 
 * Task 重试保护：如果 Marker 文件已存在（由其他 task/attempt 创建），
 * Marker 创建会失败，导致 preLogFileOpen/preLogFileCreate 返回 false，
 * 从而触发 rollover 到新的 log 文件。这保证了不同的 task attempt 
 * 写入不同的物理文件，避免并发写入导致的数据损坏。
 */
protected class AppendLogWriteCallback implements HoodieLogFileWriteCallback {

    @Override
    public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
        // 尝试创建 Marker 文件
        // 如果 Marker 已存在（其他 task/attempt 在写），创建失败，触发 rollover
        return createAppendMarker(logFileToAppend);
    }

    @Override
    public boolean preLogFileCreate(HoodieLogFile logFileToCreate) {
        return createAppendMarker(logFileToCreate);
    }

    private boolean createAppendMarker(HoodieLogFile logFile) {
        WriteMarkers writeMarkers = WriteMarkersFactory.get(...);
        // createIfNotExists: 已存在返回 empty，触发 rollover
        return writeMarkers.createIfNotExists(...).isPresent();
    }
}
```

### 2. DirectWriteMarkers.create()

```java
private Option<StoragePath> create(StoragePath markerPath, boolean checkIfExists) {
    // 确保目录存在
    if (!storage.exists(dirPath)) {
        storage.createDirectory(dirPath);
    }
    
    // 关键：如果 Marker 已存在，返回 empty
    if (checkIfExists && storage.exists(markerPath)) {
        LOG.warn("Marker Path=" + markerPath + " already exists, cancel creation");
        return Option.empty();  // ← 触发 rollover
    }
    
    // 创建 Marker 文件
    storage.create(markerPath, false).close();
    return Option.of(markerPath);
}
```

### 3. HoodieLogFormatWriter.getOutputStream()

```java
private FSDataOutputStream getOutputStream() throws IOException, InterruptedException {
    if (this.output == null) {
        if (fs.exists(path)) {
            // 调用 callback 检查是否可以 append
            boolean canAppend = isAppendSupported 
                ? logFileWriteCallback.preLogFileOpen(logFile)  // ← Marker 检查
                : false;
            
            if (canAppend) {
                this.output = fs.append(path, bufferSize);
            }
            
            if (!canAppend) {
                // Marker 创建失败，rollover 到新文件
                rollOver();
                createNewFile();
            }
        } else {
            createNewFile();
        }
    }
    return output;
}
```

## 场景分析

| 场景 | Task A 状态 | Task A' 行为 | 结果 |
|-----|------------|-------------|------|
| 正常重试 | A 已死亡 | Marker 不存在，正常写入 | A' 接替 A 继续写 |
| 假死重试 | A 假死但还在写 | Marker 已存在，rollover | A 和 A' 各写各的文件 |
| 竞争创建 | A 正在创建 Marker | A' 等待后发现已存在，rollover | A 和 A' 各写各的文件 |

## 与分区并发冲突检测的关系

Task 重试保护和分区级并发冲突检测是两个独立的机制：

| 机制 | 目的 | 场景 |
|-----|------|------|
| Task 重试保护 | 防止同一 instant 内不同 task attempt 写同一文件 | 单作业内 |
| 分区并发冲突检测 | 防止不同 instant 写同一分区 | 多作业并发 |

两个机制可以同时启用，互不冲突。

## 总结

Task 重试场景的文件保护方案利用 Hudi 现有的 Marker 文件机制：

1. **原子性**：Marker 文件的 "已存在则失败" 保证同一 instant 内只有一个 task 能成功创建 Marker
2. **隔离性**：不同 instant 的 Marker 在不同目录，自然隔离
3. **零配置**：不需要新增配置项或修改现有行为
4. **向后兼容**：完全兼容现有的文件命名规范和 Marker 机制

当 Task 重试发生时，重试的 task 会发现 Marker 已存在，自动 rollover 到新的 log 文件，从而避免数据损坏。
