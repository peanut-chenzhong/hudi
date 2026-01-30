# Hudi Repair Tool

独立的 Hudi 表修复工具，无需依赖 Hudi 源码编译，可单独打包部署。

## 特性

- **独立编译** - 不依赖 Hudi 源码，通过 Maven Central 获取依赖
- **Fat Jar** - 打包成包含所有依赖的可执行 jar
- **批量处理** - 支持同时修复多张表
- **灵活配置** - 支持多种 Spark/Hudi 版本组合
- **安全模式** - 默认 dry-run 模式，仅移动不删除

## 功能列表

### CleanupOrphanReplacedFilesTool

清理 `replacecommit` 遗留的孤儿文件。

**适用场景：**
- Clustering 或 Insert Overwrite 操作后，被替换的旧文件未被正常清理
- Archive 归档后 Cleaner 无法清理的历史 replaced 文件
- 存储空间占用异常，需要清理无效文件

**工作原理：**
```
┌─────────────────────────────────────────────────────────────┐
│  1. 读取 Archived Timeline + Active Timeline                │
│                          ↓                                   │
│  2. 过滤所有 COMPLETED 状态的 replacecommit                  │
│                          ↓                                   │
│  3. 解析 HoodieReplaceCommitMetadata                         │
│     获取 partitionToReplaceFileIds (被替换的 FileGroup)      │
│                          ↓                                   │
│  4. 检查这些文件是否仍存在于分区目录                          │
│                          ↓                                   │
│  5. 移动孤儿文件到 .delete 目录                              │
└─────────────────────────────────────────────────────────────┘
```

## 编译打包

### 前置条件

- JDK 1.8+
- Maven 3.6+

### 快速编译（默认 Spark 3.3.1 + Hudi 0.11.0）

```bash
cd hudi-repair-tool
mvn clean package -DskipTests
```

### 指定版本编译

```bash
# Spark 3.2
mvn clean package -DskipTests -Pspark3.2

# Spark 3.1
mvn clean package -DskipTests -Pspark3.1

# Spark 2.4 (Scala 2.11)
mvn clean package -DskipTests -Pspark2.4
```

### 自定义版本

直接修改 pom.xml 中的 properties，或通过命令行指定：

```bash
mvn clean package -DskipTests \
  -Dhudi.version=0.11.0 \
  -Dspark.version=3.3.1 \
  -Dhadoop.version=3.2.4
```

### 输出文件

```
target/hudi-repair-tool-1.0.0.jar
```

## 使用方法

### 基本用法

```bash
spark-submit \
  --class org.apache.hudi.repair.CleanupOrphanReplacedFilesTool \
  --master yarn \
  --deploy-mode client \
  hudi-repair-tool-1.0.0.jar \
  --base-paths <table_paths> \
  [options]
```

### 参数说明

| 参数 | 简写 | 必填 | 默认值 | 说明 |
|------|------|:----:|--------|------|
| `--base-paths` | `-p` | 否* | - | 逗号分隔的 Hudi 表路径列表 |
| `--path-file` | `-f` | 否* | - | 包含表路径的文件（每行一个，# 开头为注释） |
| `--dry-run` | `-d` | 否 | `true` | 预览模式，仅报告不执行 |
| `--start-instant-time` | `-s` | 否 | - | 起始 instant 时间（包含） |
| `--end-instant-time` | `-e` | 否 | - | 结束 instant 时间（包含） |
| `--parallelism` | - | 否 | `1` | 并行处理表的数量 |
| `--delete-dir-name` | - | 否 | `.delete` | 孤儿文件移动目标目录 |
| `--output-path` | `-o` | 否 | - | 结果报告输出路径 |
| `--spark-master` | - | 否 | `local[*]` | Spark Master URL |
| `--help` | `-h` | 否 | - | 显示帮助 |

> *注：`--base-paths` 和 `--path-file` 必须至少指定一个

## 使用示例

### 示例 1：预览单表孤儿文件

```bash
spark-submit \
  --class org.apache.hudi.repair.CleanupOrphanReplacedFilesTool \
  --master local[4] \
  hudi-repair-tool-1.0.0.jar \
  --base-paths hdfs:///data/hudi/my_table \
  --dry-run true
```

### 示例 2：清理多张表

```bash
spark-submit \
  --class org.apache.hudi.repair.CleanupOrphanReplacedFilesTool \
  --master yarn \
  --deploy-mode client \
  --num-executors 5 \
  hudi-repair-tool-1.0.0.jar \
  --base-paths hdfs:///data/hudi/table1,hdfs:///data/hudi/table2,hdfs:///data/hudi/table3 \
  --dry-run false \
  --parallelism 3
```

### 示例 3：通过文件批量处理

创建表路径文件 `tables.txt`：

```text
# 生产环境表
hdfs:///data/hudi/prod/orders
hdfs:///data/hudi/prod/users
hdfs:///data/hudi/prod/products

# 测试环境表
hdfs:///data/hudi/test/orders
```

执行：

```bash
spark-submit \
  --class org.apache.hudi.repair.CleanupOrphanReplacedFilesTool \
  --master yarn \
  --deploy-mode client \
  --num-executors 10 \
  --executor-memory 4g \
  --driver-memory 2g \
  hudi-repair-tool-1.0.0.jar \
  --path-file hdfs:///config/tables.txt \
  --parallelism 10 \
  --dry-run false \
  --output-path hdfs:///output/repair_result_$(date +%Y%m%d)
```

### 示例 4：指定时间范围

```bash
spark-submit \
  --class org.apache.hudi.repair.CleanupOrphanReplacedFilesTool \
  --master yarn \
  hudi-repair-tool-1.0.0.jar \
  --base-paths hdfs:///data/hudi/my_table \
  --start-instant-time 20240101000000000 \
  --end-instant-time 20240115235959999 \
  --dry-run false
```

### 示例 5：配合 hudi-spark-bundle 使用

如果环境中已有 hudi-spark-bundle，可以排除重复依赖：

```bash
spark-submit \
  --class org.apache.hudi.repair.CleanupOrphanReplacedFilesTool \
  --master yarn \
  --jars /path/to/hudi-spark3.3-bundle_2.12-0.14.1.jar \
  --conf spark.driver.userClassPathFirst=true \
  hudi-repair-tool-1.0.0.jar \
  --base-paths hdfs:///data/hudi/my_table \
  --dry-run false
```

## 输出说明

### 控制台输出

```
========================================
Cleanup Summary
========================================
Total tables processed: 3
Successful: 3
Failed: 0
Total orphan files found: 15
Total orphan size: 1073741824 bytes (1024 MB)
Dry run mode: false
========================================
Table: hdfs:///data/hudi/table1 - Success: true, Orphan files: 5
  - hdfs:///data/hudi/table1/dt=2024-01-01/abc_0-1-0_20240101.parquet (104857600 bytes) -> MOVED
  - hdfs:///data/hudi/table1/dt=2024-01-01/def_0-1-0_20240101.parquet (104857600 bytes) -> MOVED
  ... and 3 more files
```

### CSV 报告

```csv
table_path,instant_time,partition_path,file_id,file_path,file_size,action,status
hdfs:///data/hudi/table1,20240101120000,dt=2024-01-01,abc-123,hdfs:///.../abc_0-1-0.parquet,104857600,MOVED,Moved to ...
```

### Action 状态

| Action | 说明 |
|--------|------|
| `WOULD_MOVE` | dry-run 模式，该文件将被移动 |
| `MOVED` | 文件已成功移动 |
| `FAILED` | 文件移动失败 |
| `ERROR` | 处理发生错误 |

## 目录结构变化

```
/table/partition/
├── current_file.parquet          ← 有效文件（不变）
├── .hoodie_partition_metadata    ← 元数据（不变）
└── .delete/                      ← 新建目录
    └── orphan_file.parquet       ← 被移入的孤儿文件
```

## 最佳实践

### 1. 先预览后执行

```bash
# Step 1: 预览
spark-submit ... --dry-run true --output-path hdfs:///output/preview

# Step 2: 检查
hdfs dfs -cat hdfs:///output/preview/part-*

# Step 3: 执行
spark-submit ... --dry-run false
```

### 2. 定期维护

```bash
# crontab: 每周日凌晨 2 点
0 2 * * 0 /path/to/run_cleanup.sh >> /var/log/hudi_cleanup.log 2>&1
```

### 3. 后续清理

```bash
# 查看 .delete 目录
hdfs dfs -du -s -h /data/hudi/*/\*/.delete

# 确认后删除
hdfs dfs -rm -r /data/hudi/*/\*/.delete
```

## 版本兼容性

| 编译 Profile | Hudi | Spark | Scala | Hadoop |
|-------------|------|-------|-------|--------|
| (默认) | 0.11.0 | 3.3.1 | 2.12 | 3.x |
| spark3.2 | 0.11.0 | 3.2.3 | 2.12 | 3.x |
| spark3.1 | 0.11.0 | 3.1.3 | 2.12 | 3.x |
| spark2.4 | 0.11.0 | 2.4.8 | 2.11 | 2.x |

## 故障排查

### 找不到表元数据

```
Error: Could not find .hoodie directory
```
→ 检查路径是否正确，确保是有效的 Hudi 表

### 权限不足

```
Error: Permission denied
```
→ 使用有权限的用户执行

### 内存不足

```
java.lang.OutOfMemoryError
```
→ 增加 Driver 内存或使用 `--start-instant-time` 限制范围

## 项目结构

```
hudi-repair-tool/
├── pom.xml                    # Maven 配置（独立项目）
├── README.md                  # 使用文档
└── src/
    ├── main/
    │   ├── java/org/apache/hudi/repair/
    │   │   ├── CleanupOrphanReplacedFilesTool.java    # 主程序
    │   │   └── CleanupOrphanReplacedFilesConfig.java  # 配置类
    │   └── resources/
    │       └── log4j.properties
    └── test/
        ├── java/org/apache/hudi/repair/
        │   └── TestCleanupOrphanReplacedFilesConfig.java
        └── resources/
            └── log4j-surefire.properties
```

## License

Apache License 2.0
