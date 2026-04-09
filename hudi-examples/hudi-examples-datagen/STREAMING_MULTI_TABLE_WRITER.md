<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
-->
# 多表 Hudi Streaming 写入工具（详细说明）

`hudi-examples-datagen` 是一个可配置的 Spark Structured Streaming 写入工具，可以通过一次 `spark-submit` 同时写入多张 Hudi 表。  
它主要用于压测、并发验证与参数调优场景。

该工程是**独立 Maven 工程**（无 parent），依赖版本在 `pom.xml` 内自管理。  
当前默认版本组合为：**Spark 3.5.0 + Hudi 0.15.0 + Scala 2.12**。

## 1. 目标与能力

本工具支持：

- 单作业写 N 张表（`tables.count`）
- 按“前缀 + 序号”自动生成表名
- 支持 COW / MOR
- 通过固定线程池控制多表并发写入
- 生成覆盖基础类型与复杂类型的测试数据
- 按批次控制 insert/update 比例（默认 20/80）
- 通过配置透传自定义 Hudi 参数

主类：

- `org.apache.hudi.examples.datagen.streaming.HudiStreamingMultiTableWriter`

核心代码：

- `src/main/java/org/apache/hudi/examples/datagen/streaming/HudiStreamingMultiTableWriter.java`
- `src/main/java/org/apache/hudi/examples/datagen/streaming/StreamingJobConfig.java`
- `src/main/java/org/apache/hudi/examples/datagen/streaming/MultiTableDataGenerator.java`

---

## 2. 构建与产物

构建命令：

```bash
mvn -f hudi-examples/hudi-examples-datagen/pom.xml -DskipTests package
```

产物：

- 业务 jar：`hudi-examples/hudi-examples-datagen/target/hudi-examples-datagen-1.0.0-SNAPSHOT.jar`
- 依赖目录：`hudi-examples/hudi-examples-datagen/target/lib/*`

---

## 3. 运行流程

1. 通过 `--config <properties-file>` 读取配置。
2. 按数量/前缀/起始序号生成目标表列表。
3. 启动 Structured Streaming（使用 `rate` 作为触发源）。
4. 每个 micro-batch（`foreachBatch`）执行：
   - 把每张表的写任务提交到固定线程池；
   - 同时运行的任务数不超过 `tables.max.concurrent.writes`；
   - 其余表任务在队列中等待；
   - 当前批次全部表任务结束后，才进入下一个批次。

关键点：

- 表与表之间：并发（线程池控制）
- 单表内部：仍由 Spark 任务并行执行
- 批次边界：强一致等待（当前批次写完才进入下一批）

---

## 4. 配置说明

配置文件格式：Java `.properties`

`--config` 支持：

- HDFS 路径，例如：`hdfs:///path/to/hudi-streaming-multi-table-writer-mor.properties`
- 本地路径，例如：`/opt/conf/...` 或 `D:/conf/...`

必填项：

- `table.base.path`
- `checkpoint.base.path`

### 4.1 核心任务参数

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `spark.app.name` | string | `hudi-streaming-multi-table-writer` | Spark 应用名 |
| `stream.trigger.interval.sec` | int | `10` | 批次触发间隔（秒） |
| `stream.records.per.batch` | int | `5000` | 每张表每批写入记录数 |
| `ratio.insert` | double | `0.2` | insert 比例；update 比例为 `1-ratio.insert` |

### 4.2 多表生成参数

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `tables.count` | int | `1` | 作业写入表数量 |
| `tables.max.concurrent.writes` | int | `1` | 同时写入的最大表数 |
| `tables.database` | string | `default` | 逻辑数据库名（用于路径/同步） |
| `tables.prefix` | string | `hudi_stream_tbl_` | 表名前缀 |
| `tables.start.index` | int | `1` | 起始序号 |

自动生成：

- 表名：`<tables.prefix><tables.start.index + i>`
- 表路径：`<table.base.path>/<tables.database>/<tableName>`
- checkpoint 路径：`<checkpoint.base.path>/<tables.database>/<tableName>`

### 4.3 表模板参数

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `table.type` | string | `MERGE_ON_READ` | 表类型（`COPY_ON_WRITE` / `MERGE_ON_READ`） |
| `table.index.type` | string | `BLOOM` | 索引类型 |
| `table.partitioned` | boolean | `true` | 是否分区表 |
| `table.partition.field` | string | `dt` | 分区字段 |
| `table.recordkey.field` | string | `id` | 主键字段 |
| `table.precombine.field` | string | `ts` | precombine 字段 |

### 4.4 自定义 Hudi 参数透传

所有 `hudi.option.` 前缀的配置都会透传为写入参数（去掉前缀后生效）。

示例：

```properties
hudi.option.hoodie.keep.max.commits=60
hudi.option.hoodie.metadata.enable=true
```

等价为：

- `hoodie.keep.max.commits=60`
- `hoodie.metadata.enable=true`

注意：以下字段会按“当前目标表”自动派生并强制覆盖，用户配置会被忽略：

- `hoodie.table.name`
- `hoodie.database.name`
- `hoodie.datasource.hive_sync.database`
- `hoodie.datasource.hive_sync.table`
- `hoodie.datasource.hive_sync.partition_fields`（分区表时）

---

## 5. 数据模型与生成逻辑

每条记录包含：

- 基础类型：byte、short、int、long、float、double、decimal
- 标量类型：boolean、string、binary、date、timestamp
- 复杂类型：array、map、struct
- Hudi 关键字段：`id`、`ts`、`dt`
- 标记字段：`op_type`（`insert` / `update`）

每张表每批的 insert/update 逻辑：

- 首批 key 池为空时，可能先全量 insert 预热；
- 预热后：
  - `insert = round(recordsPerBatch * ratio.insert)`
  - `update = recordsPerBatch - insert`
- update 从该表已有 key 池随机采样，形成稳定更新负载。

---

## 6. 多表并发模型

并发由固定线程池控制：

- 线程池大小：`tables.max.concurrent.writes`
- 每批任务数：`tables.count`
- 超过并发上限的任务会排队等待
- 批次在所有表任务完成后结束

例子：

- `tables.count=1000`
- `tables.max.concurrent.writes=20`

表现：

- 最多 20 张表同时写；
- 其余 980 张在队列等待；
- 某张完成后，下一张补位执行；
- 全部 1000 张完成后才进入下一批。

---

## 7. 模板配置文件

已提供模板：

- `src/main/resources/hudi-streaming-multi-table-writer.properties`
- `src/main/resources/hudi-streaming-multi-table-writer-cow.properties`
- `src/main/resources/hudi-streaming-multi-table-writer-mor.properties`

建议：

- 吞吐基线验证先用 COW 模板；
- 更新压测场景先用 MOR 模板并按需调 compaction 参数。

---

## 8. spark-submit 示例

### 8.1 YARN 集群模式

```bash
spark-submit \
  --class org.apache.hudi.examples.datagen.streaming.HudiStreamingMultiTableWriter \
  --master yarn \
  --deploy-mode cluster \
  --jars hudi-examples/hudi-examples-datagen/target/lib/* \
  hudi-examples/hudi-examples-datagen/target/hudi-examples-datagen-1.0.0-SNAPSHOT.jar \
  --config /path/to/hudi-streaming-multi-table-writer-mor.properties
```

### 8.2 本地快速验证

```bash
spark-submit \
  --class org.apache.hudi.examples.datagen.streaming.HudiStreamingMultiTableWriter \
  --master local[4] \
  --jars hudi-examples/hudi-examples-datagen/target/lib/* \
  hudi-examples/hudi-examples-datagen/target/hudi-examples-datagen-1.0.0-SNAPSHOT.jar \
  --config hudi-examples/hudi-examples-datagen/src/main/resources/hudi-streaming-multi-table-writer.properties
```

---

## 9. 运行建议

1. 先小规模验证：
   - 低 `tables.count`
   - 低 `tables.max.concurrent.writes`
   - 确认存储、Hive Sync、checkpoint 正常
2. 再渐进扩容：
   - 并发逐步提升（例如 5 -> 10 -> 20）
   - 观察 executor 内存、shuffle、写入延迟
3. MOR 场景：
   - 根据更新压力调整 compaction 策略
4. 大规模多表：
   - `tables.max.concurrent.writes` 不要超过集群饱和点
   - checkpoint 与数据路径建议使用稳定分布式存储

---

## 10. 启动校验

启动前会做参数校验：

- `tables.count > 0`
- `stream.records.per.batch > 0`
- `stream.trigger.interval.sec > 0`
- `0 <= ratio.insert <= 1`
- `tables.max.concurrent.writes > 0`
- `tables.max.concurrent.writes <= tables.count`

不满足会快速失败并报明确错误。

---

## 11. 当前限制

- 目标表集合在启动后固定，不支持运行时动态增减。
- key 池存在进程内存中，不做外部持久化。
- 一个批次会等待所有表完成，慢表会拖长批次时长。

---

## 12. 常见问题排查

- 启动即失败：
  - 检查必填项（`table.base.path`、`checkpoint.base.path`）
  - 检查数值范围（`tables.max.concurrent.writes`、`ratio.insert`）
  - 若使用 HDFS 配置路径，确认运行环境可访问 NameNode 且客户端配置完整
- 吞吐低：
  - 谨慎增大 `tables.max.concurrent.writes`
  - 调整 Spark 资源与 Hudi 写入参数
- 小文件过多：
  - 调整 `hoodie.parquet.small.file.limit`、并行度、提交节奏
- Hive 同步异常：
  - 不要手工配置 `hudi.option.hoodie.datasource.hive_sync.database/table/partition_fields`
  - 检查 HMS 连通性与通用开关（如 `enable`、`mode`）

---

## 13. 脱离 Hudi 仓库独立构建

可以把本目录完整拷出，作为独立工程构建。

### 13.1 最小文件集合

建议至少保留：

- `pom.xml`
- `src/main/java/**`
- `src/main/resources/**`
- `STREAMING_MULTI_TABLE_WRITER.md`

### 13.2 可调版本参数

在 `pom.xml` 中按需修改：

- `hudi.version`
- `spark.version`
- `scala.binary.version`
- `log4j.version`

兼容建议：

- Spark 3.5.x + Hudi 0.15.0：`hudi-spark3.5-bundle_${scala.binary.version}`
- 若切换 Spark 次版本，需要同时调整：
  - `spark.version`
  - Hudi Spark Bundle artifact 名称

### 13.3 独立构建命令

```bash
mvn -DskipTests compile
mvn -DskipTests package
```

### 13.4 本地 Maven 仓库无权限时

可指定可写本地仓库：

```bash
mvn -DskipTests package -Dmaven.repo.local=/path/to/writable-m2
```

### 13.5 独立工程运行示例

```bash
spark-submit \
  --class org.apache.hudi.examples.datagen.streaming.HudiStreamingMultiTableWriter \
  --master yarn \
  --deploy-mode cluster \
  --jars target/lib/* \
  target/hudi-examples-datagen-1.0.0-SNAPSHOT.jar \
  --config /path/to/hudi-streaming-multi-table-writer-mor.properties
```
