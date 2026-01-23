# 分区级并发早期冲突检测流程

## 概述

本文档描述了 `PartitionTransactionDirectMarkerBasedDetectionStrategy` 的详细工作流程，该策略用于在多作业（如 Flink + Spark）同时写入同一张 Hudi 表时，通过分区级锁实现早期冲突检测。

## 核心设计目标

1. **分区级冲突检测**：不同 instant（作业）写同一分区时检测冲突
2. **减少 ZK 压力**：只在首次创建分区目录时获取锁，后续 task 跳过锁
3. **高效并发**：不同分区的写入完全并行，无锁竞争

## 详细流程图

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

## Marker 文件目录结构

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
```

## ZK 锁优化效果

| 场景 | 锁获取次数 |
|-----|----------|
| 100 个 task 写同一分区 | **1 次** |
| 100 个 task 写 10 个分区 | **10 次** |
| 1000 个 task 写 100 个分区 | **100 次** |

## 配置参数

```properties
# 启用 OCC 模式
hoodie.write.concurrency.mode=optimistic_concurrency_control

# 使用 ZooKeeper 锁
hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
hoodie.write.lock.zookeeper.url=zk1:2181,zk2:2181,zk3:2181
hoodie.write.lock.zookeeper.base_path=/hudi/locks

# 使用分区级早期检测策略
hoodie.write.lock.early.conflict.detection.strategy=org.apache.hudi.table.marker.PartitionTransactionDirectMarkerBasedDetectionStrategy
hoodie.write.early.conflict.detection.enable=true
```

## 关键代码路径

1. `DirectWriteMarkers.createWithEarlyConflictDetection()` - 入口
2. `PartitionTransactionDirectMarkerBasedDetectionStrategy.detectAndResolveConflictIfNecessary()` - 冲突检测
3. `PartitionBasedDirectMarkerDetectionStrategy.checkPartitionMarkerConflict()` - 扫描其他 instant

## 冲突处理

当检测到冲突时，抛出 `HoodieEarlyConflictDetectionException`，导致当前 task 失败。
Spark/Flink 的重试机制会在稍后重试该 task，届时：
- 如果另一个 instant 已完成：重试成功
- 如果另一个 instant 仍在进行：继续失败，直到超时或另一方完成
