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
# Streaming Multi-Table Hudi Writer (Detailed Guide)

`hudi-examples-datagen` provides a configurable Spark Structured Streaming job that writes to many Hudi tables from one `spark-submit`.  
It is designed for pressure testing, concurrency testing, and parameter benchmarking.
The project is standalone (no Maven parent) and manages its own dependency versions in `pom.xml`.
Current default dependency set in this project is **Spark 3.5.0 + Hudi 0.15.0 + Scala 2.12**.

## 1. Scope and Goals

This tool focuses on:

- Writing N tables in one job (`tables.count`)
- Dynamically generating table names by prefix + index
- Supporting COW and MOR templates
- Supporting table-level write concurrency with a bounded thread pool
- Generating rich synthetic data covering primitive and complex types
- Controlling insert/update ratio per micro-batch
- Passing arbitrary Hudi options via config file

Main class:

- `org.apache.hudi.examples.datagen.streaming.HudiStreamingMultiTableWriter`

Core implementation files:

- `src/main/java/org/apache/hudi/examples/datagen/streaming/HudiStreamingMultiTableWriter.java`
- `src/main/java/org/apache/hudi/examples/datagen/streaming/StreamingJobConfig.java`
- `src/main/java/org/apache/hudi/examples/datagen/streaming/MultiTableDataGenerator.java`

---

## 2. Build and Artifacts

Build command:

```bash
mvn -f hudi-examples/hudi-examples-datagen/pom.xml -DskipTests package
```

Generated artifacts:

- Job jar: `hudi-examples/hudi-examples-datagen/target/hudi-examples-datagen-1.0.0-SNAPSHOT.jar`
- Runtime dependencies: `hudi-examples/hudi-examples-datagen/target/lib/*`

---

## 3. Runtime Flow

1. Read config from `--config <properties-file>`.
2. Build table list from count/prefix/index.
3. Start Spark Structured Streaming (`rate` source as trigger).
4. For each micro-batch (`foreachBatch`):
   - Submit each table write task to a fixed-size thread pool.
   - At most `tables.max.concurrent.writes` table tasks run at the same time.
   - Remaining tables wait in the executor queue.
   - Wait for all table tasks in this batch to finish before moving to next batch.

Important behavior:

- Table-level scheduling: concurrent
- Same-table write path: still Spark job execution semantics
- Batch barrier: next micro-batch starts after all tables in current batch complete

---

## 4. Configuration Reference

Config format is Java `.properties`.
`--config` supports:

- HDFS path, for example: `hdfs:///path/to/hudi-streaming-multi-table-writer-mor.properties`
- Local path, for example: `/opt/conf/hudi-streaming-multi-table-writer-mor.properties` or `D:/conf/...`

Required keys:

- `table.base.path`
- `checkpoint.base.path`

### 4.1 Core job parameters

| Key | Type | Default | Description |
|---|---|---|---|
| `spark.app.name` | string | `hudi-streaming-multi-table-writer` | Spark app name |
| `stream.trigger.interval.sec` | int | `10` | Micro-batch interval (seconds) |
| `stream.records.per.batch` | int | `5000` | Records generated per table per batch |
| `ratio.insert` | double | `0.2` | Insert ratio; update ratio is `1 - ratio.insert` |

### 4.2 Multi-table generation

| Key | Type | Default | Description |
|---|---|---|---|
| `tables.count` | int | `1` | Number of tables written by one job |
| `tables.max.concurrent.writes` | int | `1` | Max tables writing concurrently |
| `tables.database` | string | `default` | Logical database name for path/options |
| `tables.prefix` | string | `hudi_stream_tbl_` | Table name prefix |
| `tables.start.index` | int | `1` | Start index for generated names |

Generated table name:

- `<tables.prefix><tables.start.index + i>`

Generated table path:

- `<table.base.path>/<tables.database>/<tableName>`

Generated table checkpoint path:

- `<checkpoint.base.path>/<tables.database>/<tableName>`

### 4.3 Table template

| Key | Type | Default | Description |
|---|---|---|---|
| `table.type` | string | `MERGE_ON_READ` | Hudi table type (`COPY_ON_WRITE` or `MERGE_ON_READ`) |
| `table.index.type` | string | `BLOOM` | Hudi index type |
| `table.partitioned` | boolean | `true` | Partitioned table switch |
| `table.partition.field` | string | `dt` | Partition path field if partitioned |
| `table.recordkey.field` | string | `id` | Hudi record key field |
| `table.precombine.field` | string | `ts` | Hudi precombine field |

### 4.4 Custom Hudi options passthrough

Any property with prefix `hudi.option.` is forwarded to writer options after prefix removal.
However, the following fields are always auto-derived per target table and user values are ignored:

- `hoodie.table.name`
- `hoodie.database.name`
- `hoodie.datasource.hive_sync.database`
- `hoodie.datasource.hive_sync.table`
- `hoodie.datasource.hive_sync.partition_fields` (when partitioned table is enabled)

Example:

```properties
hudi.option.hoodie.keep.max.commits=60
hudi.option.hoodie.metadata.enable=true
```

Equivalent writer options:

- `hoodie.keep.max.commits=60`
- `hoodie.metadata.enable=true`

---

## 5. Data Model and Generation Logic

Each generated row includes:

- Primitive: byte, short, int, long, float, double, decimal
- Scalar: boolean, string, binary, date, timestamp
- Complex: array, map, struct
- Hudi-related: `id`, `ts`, `dt`
- Marker field: `op_type` (`insert` or `update`)

Insert/update behavior per table:

- If key pool is empty (first batch), all records may be inserts to warm key pool.
- After warm-up:
  - inserts = `round(recordsPerBatch * ratio.insert)`
  - updates = `recordsPerBatch - inserts`
- Updates sample existing keys from that table key pool.

This gives stable update-heavy workloads for MOR/COW benchmarking.

---

## 6. Multi-table Concurrency Model

Concurrency is controlled by a fixed thread pool:

- Pool size = `tables.max.concurrent.writes`
- Number of submitted tasks per batch = `tables.count`
- Queue behavior: if task count > pool size, extra tasks wait
- Synchronization point: batch completes after all tasks complete

Example:

- `tables.count=1000`
- `tables.max.concurrent.writes=20`

Behavior:

- 20 tables write in parallel
- 980 table tasks wait in queue
- As tasks finish, queued tables are scheduled
- Next micro-batch starts only after all 1000 table tasks finish

---

## 7. Example Config Templates

Provided templates:

- `src/main/resources/hudi-streaming-multi-table-writer.properties`
- `src/main/resources/hudi-streaming-multi-table-writer-cow.properties`
- `src/main/resources/hudi-streaming-multi-table-writer-mor.properties`

Typical recommendation:

- COW throughput baseline: start with COW template
- MOR update-heavy: start with MOR template + compaction tuning

---

## 8. spark-submit Examples

### 8.1 YARN cluster mode

```bash
spark-submit \
  --class org.apache.hudi.examples.datagen.streaming.HudiStreamingMultiTableWriter \
  --master yarn \
  --deploy-mode cluster \
  --jars hudi-examples/hudi-examples-datagen/target/lib/* \
  hudi-examples/hudi-examples-datagen/target/hudi-examples-datagen-1.0.0-SNAPSHOT.jar \
  --config /path/to/hudi-streaming-multi-table-writer-mor.properties
```

### 8.2 Local quick verification

```bash
spark-submit \
  --class org.apache.hudi.examples.datagen.streaming.HudiStreamingMultiTableWriter \
  --master local[4] \
  --jars hudi-examples/hudi-examples-datagen/target/lib/* \
  hudi-examples/hudi-examples-datagen/target/hudi-examples-datagen-1.0.0-SNAPSHOT.jar \
  --config hudi-examples/hudi-examples-datagen/src/main/resources/hudi-streaming-multi-table-writer.properties
```

---

## 9. Operational Suggestions

1. Start small:
   - small `tables.count`
   - low `tables.max.concurrent.writes`
   - verify filesystem, HMS sync, and checkpoint correctness
2. Scale gradually:
   - increase concurrent writes in steps (5 -> 10 -> 20)
   - monitor executor memory, shuffle, and table service pressure
3. For MOR:
   - align compaction policy with update pressure
4. For many tables:
   - keep `tables.max.concurrent.writes` below cluster saturation point
   - ensure checkpoint and table paths are on stable distributed storage

---

## 10. Validation and Safety Checks

Validation currently enforced:

- `tables.count > 0`
- `stream.records.per.batch > 0`
- `stream.trigger.interval.sec > 0`
- `0 <= ratio.insert <= 1`
- `tables.max.concurrent.writes > 0`
- `tables.max.concurrent.writes <= tables.count`

If invalid, job fails fast at startup with clear error.

---

## 11. Known Constraints

- Table list is static after startup (not dynamically reconfigured during runtime).
- Current key pool is in-process memory (no persistent external key state).
- Batch completion waits all tables in that batch; one slow table can extend batch duration.

---

## 12. Troubleshooting Quick List

- Job exits on startup:
  - check required keys (`table.base.path`, `checkpoint.base.path`)
  - check numeric ranges (`tables.max.concurrent.writes`, `ratio.insert`)
  - if using HDFS config path, ensure NameNode URI and client configs are available in runtime environment
- Throughput low:
  - increase `tables.max.concurrent.writes` carefully
  - tune Spark resources and Hudi write options
- Too many small files:
  - tune `hoodie.parquet.small.file.limit`, parallelism, and commit frequency
- Hive sync issues:
  - do not configure `hudi.option.hoodie.datasource.hive_sync.database/table/partition_fields` manually
  - verify HMS endpoint/network and generic hive sync switches (such as `enable`, `mode`)

---

## 13. Build as a Fully Standalone Project

You can copy this module out of the Hudi monorepo and build it as an independent project.

### 13.1 Minimal files to copy

Copy the whole directory:

- `hudi-examples/hudi-examples-datagen`

Keep at least:

- `pom.xml`
- `src/main/java/**`
- `src/main/resources/**`
- `STREAMING_MULTI_TABLE_WRITER.md`

### 13.2 Version knobs in standalone `pom.xml`

The project is already parent-free. You only need to adjust these properties in `pom.xml`:

- `hudi.version`
- `spark.version`
- `scala.binary.version`
- `log4j.version`

Recommended compatibility:

- Spark 3.5.x + Hudi 0.15.0 -> use `hudi-spark3.5-bundle_${scala.binary.version}`
- If you switch Spark minor version, align both:
  - Spark dependency version (`spark.version`)
  - Hudi spark bundle artifact name (`hudi-spark<spark-minor>-bundle_${scala.binary.version}`)

### 13.3 Build commands (outside monorepo)

```bash
mvn -DskipTests compile
mvn -DskipTests package
```

### 13.4 If local Maven repository is not writable

If your default Maven repo path has permission issues, use an explicit writable local repo:

```bash
mvn -DskipTests package -Dmaven.repo.local=/path/to/writable-m2
```

### 13.5 spark-submit after standalone build

```bash
spark-submit \
  --class org.apache.hudi.examples.datagen.streaming.HudiStreamingMultiTableWriter \
  --master yarn \
  --deploy-mode cluster \
  --jars target/lib/* \
  target/hudi-examples-datagen-1.0.0-SNAPSHOT.jar \
  --config /path/to/hudi-streaming-multi-table-writer-mor.properties
```
