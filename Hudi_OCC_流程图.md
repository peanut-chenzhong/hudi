# Hudi OCC 并发控制流程图

## 一、整体写入流程（包含早期冲突检测）

```mermaid
flowchart TD
    Start([开始写入]) --> Init[初始化写入客户端]
    Init --> CheckOCC{是否启用OCC?}
    CheckOCC -->|否| SingleWriter[单写入者模式]
    CheckOCC -->|是| OCCMode[OCC模式]
    
    OCCMode --> CreateInstant[创建 Instant]
    CreateInstant --> StartHeartbeat[启动心跳机制]
    StartHeartbeat --> WriteData[开始写入数据]
    
    WriteData --> CreateMarker{创建 Marker 前}
    CreateMarker --> CheckEarlyConflict{启用早期冲突检测?}
    
    CheckEarlyConflict -->|否| CreateMarkerFile[直接创建 Marker]
    CheckEarlyConflict -->|是| EarlyConflictDetect[执行早期冲突检测]
    
    EarlyConflictDetect --> EarlyConflictResult{检测到冲突?}
    EarlyConflictResult -->|是| EarlyAbort[抛出异常,中止写入]
    EarlyConflictResult -->|否| CreateMarkerFile
    
    CreateMarkerFile --> WriteFile[写入数据文件]
    WriteFile --> MoreFiles{还有文件?}
    MoreFiles -->|是| CreateMarker
    MoreFiles -->|否| FinishWrite[完成写入]
    
    FinishWrite --> Commit[开始提交]
    Commit --> CommitConflictDetect[执行提交时冲突检测]
    CommitConflictDetect --> CommitConflictResult{检测到冲突?}
    
    CommitConflictResult -->|是| ResolveConflict[尝试解决冲突]
    ResolveConflict --> ResolveResult{解决成功?}
    ResolveResult -->|否| CommitAbort[抛出异常,中止提交]
    ResolveResult -->|是| WriteMetadata[写入提交元数据]
    
    CommitConflictResult -->|否| WriteMetadata
    WriteMetadata --> CompleteCommit[完成提交]
    CompleteCommit --> StopHeartbeat[停止心跳]
    StopHeartbeat --> End([写入成功])
    
    EarlyAbort --> End
    CommitAbort --> End
    SingleWriter --> End
    
    style Start fill:#e1f5ff
    style End fill:#c8e6c9
    style EarlyAbort fill:#ffcdd2
    style CommitAbort fill:#ffcdd2
    style EarlyConflictDetect fill:#fff9c4
    style CommitConflictDetect fill:#fff9c4
```

## 二、早期冲突检测详细流程

```mermaid
flowchart TD
    Start([开始早期冲突检测]) --> GetStrategy[获取早期冲突检测策略]
    GetStrategy --> CheckMarkerType{Marker 类型?}
    
    CheckMarkerType -->|Direct| DirectStrategy[Direct Marker 策略]
    CheckMarkerType -->|Timeline Server| TimelineStrategy[Timeline Server 策略]
    
    DirectStrategy --> ListTempDir[列出 .temp 目录]
    ListTempDir --> GetCandidateInstants[获取候选 Instant<br/>活跃且未过期]
    GetCandidateInstants --> FilterByHeartbeat[根据心跳过滤]
    
    FilterByHeartbeat --> CheckPartition[检查候选 Instant 的 Marker 目录]
    CheckPartition --> PartitionExists{分区路径存在?}
    
    PartitionExists -->|否| NoConflict1[无冲突]
    PartitionExists -->|是| ListMarkers[列出分区下的 Marker 文件]
    ListMarkers --> CheckFileId{找到相同 FileId?}
    
    CheckFileId -->|是| HasConflict1[检测到冲突]
    CheckFileId -->|否| NoConflict1
    
    TimelineStrategy --> AsyncCheck[异步检查冲突]
    AsyncCheck --> GetResult[获取检查结果]
    GetResult --> HasConflict2{检测到冲突?}
    
    HasConflict2 -->|是| HasConflict1
    HasConflict2 -->|否| NoConflict1
    
    HasConflict1 --> ThrowException[抛出 HoodieEarlyConflictDetectionException]
    ThrowException --> Abort[中止写入]
    
    NoConflict1 --> AllowWrite[允许创建 Marker]
    AllowWrite --> End([继续写入])
    Abort --> End
    
    style Start fill:#e1f5ff
    style End fill:#c8e6c9
    style HasConflict1 fill:#ffcdd2
    style ThrowException fill:#ffcdd2
    style Abort fill:#ffcdd2
    style CheckPartition fill:#fff9c4
    style AsyncCheck fill:#fff9c4
```

## 三、提交时冲突检测详细流程

```mermaid
flowchart TD
    Start([开始提交时冲突检测]) --> ReloadTimeline[重新加载活跃时间线]
    ReloadTimeline --> GetStrategy[获取冲突解决策略]
    GetStrategy --> GetCandidateInstants[获取候选 Instant]
    
    GetCandidateInstants --> GetCompletedInstants[获取自上次成功写入以来的已完成 Instant]
    GetCompletedInstants --> GetPendingInstants[获取待处理的压缩/聚类 Instant]
    GetPendingInstants --> MergeInstants[合并候选 Instant 流]
    
    MergeInstants --> CreateThisOp[创建当前操作对象<br/>ConcurrentOperation]
    CreateThisOp --> ExtractThisPartitions[提取当前操作的分区路径和文件ID]
    
    ExtractThisPartitions --> LoopInstants[遍历候选 Instant]
    LoopInstants --> CreateOtherOp[创建其他操作对象]
    CreateOtherOp --> ExtractOtherPartitions[提取其他操作的分区路径和文件ID]
    
    ExtractOtherPartitions --> CheckConflict[检查冲突]
    CheckConflict --> ConflictType{冲突检测策略?}
    
    ConflictType -->|文件组级| FileGroupCheck[检查 分区路径,文件ID 交集]
    ConflictType -->|分区级| PartitionCheck[检查分区路径交集]
    ConflictType -->|Bucket级| BucketCheck[检查 Bucket ID 交集]
    
    FileGroupCheck --> HasIntersection1{交集非空?}
    PartitionCheck --> HasIntersection2{交集非空?}
    BucketCheck --> HasIntersection3{交集非空?}
    
    HasIntersection1 -->|是| HasConflict[检测到冲突]
    HasIntersection1 -->|否| NoConflict
    HasIntersection2 -->|是| HasConflict
    HasIntersection2 -->|否| NoConflict
    HasIntersection3 -->|是| HasConflict
    HasIntersection3 -->|否| NoConflict
    
    HasConflict --> ResolveConflict[尝试解决冲突]
    ResolveConflict --> CheckOpType{操作类型?}
    
    CheckOpType -->|压缩操作| CheckTimestamp[检查时间戳]
    CheckOpType -->|日志压缩| AllowLogCompact[允许继续]
    CheckOpType -->|其他| AbortCommit[中止提交]
    
    CheckTimestamp --> TimestampCompare{压缩时间戳更早?}
    TimestampCompare -->|是| AllowContinue[允许继续]
    TimestampCompare -->|否| AbortCommit
    
    AllowLogCompact --> AllowContinue
    AllowContinue --> UpdateMetadata[更新元数据]
    UpdateMetadata --> NoConflict
    
    NoConflict --> MoreInstants{还有 Instant?}
    MoreInstants -->|是| LoopInstants
    MoreInstants -->|否| Success[冲突检测通过]
    
    AbortCommit --> ThrowException[抛出 HoodieWriteConflictException]
    ThrowException --> End([提交失败])
    Success --> End
    
    style Start fill:#e1f5ff
    style End fill:#c8e6c9
    style HasConflict fill:#ffcdd2
    style AbortCommit fill:#ffcdd2
    style ThrowException fill:#ffcdd2
    style CheckConflict fill:#fff9c4
    style ResolveConflict fill:#fff9c4
```

## 四、冲突解决策略对比

```mermaid
flowchart LR
    Start([冲突检测]) --> Strategy{冲突解决策略}
    
    Strategy -->|文件组级| FileGroup[SimpleConcurrentFileWritesConflictResolutionStrategy]
    Strategy -->|分区级| Partition[PartitionBasedConcurrentWritesConflictResolutionStrategy]
    Strategy -->|Bucket级| Bucket[BucketIndexConcurrentFileWritesConflictResolutionStrategy]
    
    FileGroup --> Check1[检查 分区路径,文件ID 交集]
    Partition --> Check2[检查分区路径交集]
    Bucket --> Check3[检查 Bucket ID 交集]
    
    Check1 --> Result1[文件组级别冲突]
    Check2 --> Result2[分区级别冲突]
    Check3 --> Result3[Bucket级别冲突]
    
    Result1 --> Resolve1[解决冲突]
    Result2 --> Resolve2[解决冲突]
    Result3 --> Resolve3[解决冲突]
    
    Resolve1 --> End([完成])
    Resolve2 --> End
    Resolve3 --> End
    
    style Start fill:#e1f5ff
    style End fill:#c8e6c9
    style FileGroup fill:#fff9c4
    style Partition fill:#fff9c4
    style Bucket fill:#fff9c4
```

## 五、心跳机制流程

```mermaid
flowchart TD
    Start([写入开始]) --> InitHeartbeat[初始化心跳客户端]
    InitHeartbeat --> StartHeartbeat[启动心跳]
    StartHeartbeat --> FirstHeartbeat[立即发送第一次心跳]
    FirstHeartbeat --> ScheduleTimer[调度定时任务]
    
    ScheduleTimer --> WaitInterval[等待心跳间隔]
    WaitInterval --> UpdateHeartbeat[更新心跳文件]
    UpdateHeartbeat --> CheckExpired{心跳是否过期?}
    
    CheckExpired -->|是| InterruptThread[中断线程]
    CheckExpired -->|否| UpdateTimestamp[更新最后心跳时间]
    
    UpdateTimestamp --> IncrementCount[增加心跳计数]
    IncrementCount --> WaitInterval
    
    InterruptThread --> Abort[中止写入]
    
    UpdateTimestamp --> CheckStop{写入是否完成?}
    CheckStop -->|否| WaitInterval
    CheckStop -->|是| StopHeartbeat[停止心跳]
    StopHeartbeat --> DeleteHeartbeat[删除心跳文件]
    DeleteHeartbeat --> End([完成])
    
    Abort --> End
    
    style Start fill:#e1f5ff
    style End fill:#c8e6c9
    style Abort fill:#ffcdd2DD
    style UpdateHeartbeat fill:#fff9c4
    style CheckExpired fill:#fff9c4
```

## 六、分区级并发控制流程

```mermaid
flowchart TD
    Start([分区级并发写入]) --> FlinkWrite[Flink 写入分区 A]
    Start --> SparkWrite[Spark 写入分区 B]
    
    FlinkWrite --> FlinkCheck[Flink: 检查分区 A]
    SparkWrite --> SparkCheck[Spark: 检查分区 B]
    
    FlinkCheck --> FlinkConflict{分区 A 冲突?}
    SparkCheck --> SparkConflict{分区 B 冲突?}
    
    FlinkConflict -->|是| FlinkAbort[Flink 中止]
    FlinkConflict -->|否| FlinkContinue[Flink 继续]
    
    SparkConflict -->|是| SparkAbort[Spark 中止]
    SparkConflict -->|否| SparkContinue[Spark 继续]
    
    FlinkContinue --> FlinkWriteData[Flink 写入数据]
    SparkContinue --> SparkWriteData[Spark 写入数据]
    
    FlinkWriteData --> FlinkCommit[Flink 提交]
    SparkWriteData --> SparkCommit[Spark 提交]
    
    FlinkCommit --> FlinkCommitCheck[Flink: 提交时冲突检测]
    SparkCommit --> SparkCommitCheck[Spark: 提交时冲突检测]
    
    FlinkCommitCheck --> FlinkCommitConflict{分区 A 冲突?}
    SparkCommitCheck --> SparkCommitConflict{分区 B 冲突?}
    
    FlinkCommitConflict -->|是| FlinkCommitAbort[Flink 提交失败]
    FlinkCommitConflict -->|否| FlinkSuccess[Flink 提交成功]
    
    SparkCommitConflict -->|是| SparkCommitAbort[Spark 提交失败]
    SparkCommitConflict -->|否| SparkSuccess[Spark 提交成功]
    
    FlinkSuccess --> End([并发写入成功])
    SparkSuccess --> End
    
    FlinkAbort --> End
    SparkAbort --> End
    FlinkCommitAbort --> End
    SparkCommitAbort --> End
    
    style Start fill:#e1f5ff
    style End fill:#c8e6c9
    style FlinkAbort fill:#ffcdd2
    style SparkAbort fill:#ffcdd2
    style FlinkCommitAbort fill:#ffcdd2
    style SparkCommitAbort fill:#ffcdd2
    style FlinkSuccess fill:#c8e6c9
    style SparkSuccess fill:#c8e6c9
```

## 七、冲突检测算法对比

```mermaid
flowchart TD
    Start([冲突检测]) --> GetOps[获取两个操作]
    GetOps --> Extract1[提取操作1的分区和文件ID]
    GetOps --> Extract2[提取操作2的分区和文件ID]
    
    Extract1 --> Strategy{冲突检测策略}
    Extract2 --> Strategy
    
    Strategy -->|文件组级| FileGroupAlgo[算法: 检查 分区,文件ID 对交集]
    Strategy -->|分区级| PartitionAlgo[算法: 检查分区路径交集]
    Strategy -->|Bucket级| BucketAlgo[算法: 检查 Bucket ID 交集]
    
    FileGroupAlgo --> Intersection1{交集非空?}
    PartitionAlgo --> Intersection2{交集非空?}
    BucketAlgo --> Intersection3{交集非空?}
    
    Intersection1 -->|是| Conflict1[文件组级冲突]
    Intersection1 -->|否| NoConflict1[无冲突]
    
    Intersection2 -->|是| Conflict2[分区级冲突]
    Intersection2 -->|否| NoConflict2[无冲突]
    
    Intersection3 -->|是| Conflict3[Bucket级冲突]
    Intersection3 -->|否| NoConflict3[无冲突]
    
    Conflict1 --> Resolve[解决冲突]
    Conflict2 --> Resolve
    Conflict3 --> Resolve
    
    NoConflict1 --> Success[允许继续]
    NoConflict2 --> Success
    NoConflict3 --> Success
    
    Resolve --> End([完成])
    Success --> End
    
    style Start fill:#e1f5ff
    style End fill:#c8e6c9
    style Conflict1 fill:#ffcdd2
    style Conflict2 fill:#ffcdd2
    style Conflict3 fill:#ffcdd2
    style FileGroupAlgo fill:#fff9c4
    style PartitionAlgo fill:#fff9c4
    style BucketAlgo fill:#fff9c4
```

## 图例说明

- 🟦 **蓝色**：流程开始
- 🟩 **绿色**：成功/完成
- 🟥 **红色**：失败/中止
- 🟨 **黄色**：关键检测点

## 关键概念

1. **早期冲突检测**：在写入阶段通过 Marker 机制提前检测冲突
2. **提交时冲突检测**：在提交元数据前进行最终冲突检测
3. **心跳机制**：用于判断写入操作是否仍在进行
4. **冲突解决策略**：可插拔的策略，支持文件组级、分区级、Bucket级

## 使用建议

1. **启用早期冲突检测**：可以提前发现冲突，节省计算资源
2. **合理配置心跳**：根据作业执行时间设置心跳间隔
3. **选择合适策略**：根据业务场景选择文件组级或分区级冲突检测
4. **监控冲突率**：监控写入冲突率，评估并发策略的有效性
