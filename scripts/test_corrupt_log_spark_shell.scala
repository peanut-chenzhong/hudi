// ===========================================================================
// MOR Log 文件损坏对后续 Log 文件影响验证脚本 (spark-shell 执行)
// ===========================================================================
//
// 验证目标：
//   当一个 file group 有两个 log 文件(log.1 和 log.2)，如果 log.1 的尾部
//   被写入了非 MAGIC 头的坏数据，后续 log.2 的数据是否会丢失。
//
// 原理：
//   Hudi 读取 log 文件时，每个 block 以 MAGIC 字节(#HUDI#) 开头。
//   如果在文件尾部遇到 >= 6 字节的非 MAGIC 数据，会抛出
//   CorruptedLogFileException (RuntimeException)，导致整个 scan 操作失败，
//   后续 log 文件完全不会被读取。
//
// 使用方式：
//   spark-shell --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.x.x
//
//   然后分阶段粘贴执行，或者 :load /path/to/this/script.scala
//
// ===========================================================================

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream}
import scala.collection.mutable.ArrayBuffer

// ====================================================================
// 配置区域 (请根据实际环境修改)
// ====================================================================
val tableName = "test_corrupt_log_verify"

// HDFS:
val basePath = "/tmp/hudi/test_corrupt_log_verify"
// OBS:
// val basePath = "obs://your-bucket/hudi/test_corrupt_log_verify"
// Local (测试):
// val basePath = "file:///tmp/hudi/test_corrupt_log_verify"

// ====================================================================

val partitionValue = "2024-01-01"
val partitionPath = s"dt=$partitionValue"
val fs = FileSystem.get(new java.net.URI(basePath), spark.sparkContext.hadoopConfiguration)
val partitionDir = new Path(basePath + "/" + partitionPath)

// ====== Helper 函数 ======
def listLogFiles(): Array[org.apache.hadoop.fs.FileStatus] = {
  if (!fs.exists(partitionDir)) return Array.empty
  fs.listStatus(partitionDir)
    .filter(f => f.getPath.getName.contains(".log."))
    .sortBy(_.getPath.getName)
}

def printLogFiles(label: String): Unit = {
  val lf = listLogFiles()
  println(s"\n  [$label] 共 ${lf.length} 个 log 文件:")
  lf.foreach(f => println(s"    ${f.getPath.getName}  (${f.getLen} bytes)"))
}

def printSep(title: String): Unit = {
  println("\n" + "=" * 70)
  println(s"  $title")
  println("=" * 70)
}

// ====================================================================
//  STEP 1: 创建 MOR 表, Insert 100 条记录 → parquet base 文件
// ====================================================================
printSep("STEP 1: Insert 100 条记录 (创建 parquet base)")

val df1 = spark.range(1, 101).toDF("id")
  .withColumn("name", concat(lit("batch1_user_"), col("id").cast("string")))
  .withColumn("ts", lit(1000L))
  .withColumn("dt", lit(partitionValue))

df1.write.format("hudi")
  .option("hoodie.table.name", tableName)
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.partitionpath.field", "dt")
  .option("hoodie.datasource.write.operation", "insert")
  .option("hoodie.insert.shuffle.parallelism", "1")
  .option("hoodie.upsert.shuffle.parallelism", "1")
  .mode(SaveMode.Overwrite)
  .save(basePath)

println("  Insert 完成!")
printLogFiles("Insert 之后")

// ====================================================================
//  STEP 2: 第一次 Upsert (更新 ID 1-50) → 生成 log.1
// ====================================================================
printSep("STEP 2: 第一次 Upsert (更新 ID 1-50) → log.1")

val df2 = spark.range(1, 51).toDF("id")
  .withColumn("name", concat(lit("batch2_updated_"), col("id").cast("string")))
  .withColumn("ts", lit(2000L))
  .withColumn("dt", lit(partitionValue))

df2.write.format("hudi")
  .option("hoodie.table.name", tableName)
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.partitionpath.field", "dt")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.upsert.shuffle.parallelism", "1")
  .mode(SaveMode.Append)
  .save(basePath)

println("  第一次 Upsert 完成!")
printLogFiles("第一次 Upsert 之后")

// ====================================================================
//  STEP 3: 隐藏 log.1 → 为构造双 log 文件环境做准备
// ====================================================================
printSep("STEP 3: 临时隐藏 log.1")

val logFilesStep2 = listLogFiles()
require(logFilesStep2.nonEmpty, "ERROR: 未找到 log 文件，请检查 upsert 是否成功!")

val originalLog1 = logFilesStep2.head.getPath
val backupLog1 = new Path(originalLog1.getParent, originalLog1.getName + ".bak")

fs.rename(originalLog1, backupLog1)
println(s"  已隐藏: ${originalLog1.getName} → ${backupLog1.getName}")
printLogFiles("隐藏之后")

// ====================================================================
//  STEP 4: 第二次 Upsert (更新 ID 51-100) → 生成新的 log.1
//          因为原始 log.1 被隐藏，writer 会重新创建 log.1
// ====================================================================
printSep("STEP 4: 第二次 Upsert (更新 ID 51-100) → 新 log.1")

val df3 = spark.range(51, 101).toDF("id")
  .withColumn("name", concat(lit("batch3_updated_"), col("id").cast("string")))
  .withColumn("ts", lit(3000L))
  .withColumn("dt", lit(partitionValue))

df3.write.format("hudi")
  .option("hoodie.table.name", tableName)
  .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.partitionpath.field", "dt")
  .option("hoodie.datasource.write.operation", "upsert")
  .option("hoodie.upsert.shuffle.parallelism", "1")
  .mode(SaveMode.Append)
  .save(basePath)

println("  第二次 Upsert 完成!")
printLogFiles("第二次 Upsert 之后")

// ====================================================================
//  STEP 5: 重命名新 log.1 → log.2, 恢复原始 log.1
//          这样就构造出了 log.1 (batch2 数据) + log.2 (batch3 数据)
// ====================================================================
printSep("STEP 5: 重命名 → 构造 log.1 + log.2")

val newLogFiles = listLogFiles()
require(newLogFiles.nonEmpty, "ERROR: 未找到新的 log 文件!")

val newLog1 = newLogFiles.head.getPath
// 把 .log.1_ 替换为 .log.2_ (修改版本号)
val log2Name = newLog1.getName.replaceFirst("\\.log\\.1_", ".log.2_")
val log2Path = new Path(newLog1.getParent, log2Name)

// 重命名: 新log.1 → log.2
fs.rename(newLog1, log2Path)
println(s"  新 log.1 → log.2: {$log2Name}")

// 恢复原始 log.1
fs.rename(backupLog1, originalLog1)
println(s"  恢复原始 log.1: ${originalLog1.getName}")

printLogFiles("构造完成")

// ====================================================================
//  STEP 6: 验证双 log 文件环境下正常读取
// ====================================================================
printSep("STEP 6: 验证正常读取 (未损坏)")

val normalDf = spark.read.format("hudi").load(basePath)
val normalCount = normalDf.count()
val batch2Count = normalDf.filter(col("name").startsWith("batch2")).count()
val batch3Count = normalDf.filter(col("name").startsWith("batch3")).count()
val batch1Count = normalDf.filter(col("name").startsWith("batch1")).count()

println(s"  总记录数: $normalCount  (预期 100)")
println(s"  batch1 (未更新): $batch1Count 条")
println(s"  batch2 (log.1 更新 ID 1-50): $batch2Count 条")
println(s"  batch3 (log.2 更新 ID 51-100): $batch3Count 条")
println()
println("  前 5 条记录:")
normalDf.select("id", "name", "ts").orderBy("id").show(5, truncate = false)

// ====================================================================
//  STEP 7: 向 log.1 尾部追加坏数据 (模拟并发写入导致的损坏)
// ====================================================================
printSep("STEP 7: 向 log.1 尾部追加坏数据")

val log1Path = originalLog1
val log1SizeBefore = fs.getFileStatus(log1Path).getLen

// 追加 >= 6 字节的非 MAGIC 数据
// (Hudi MAGIC = "#HUDI#" = 6 字节, 如果非 MAGIC 数据 >= 6 字节, 触发 CorruptedLogFileException)
val corruptData = "CORRUPT_DATA_BY_CONCURRENT_WRITER_NO_MAGIC_HEADER!!!".getBytes("UTF-8")
val appendStream = fs.append(log1Path)
appendStream.write(corruptData)
appendStream.hflush()
appendStream.close()

val log1SizeAfter = fs.getFileStatus(log1Path).getLen
println(s"  文件: ${log1Path.getName}")
println(s"  追加前大小: $log1SizeBefore bytes")
println(s"  追加后大小: $log1SizeAfter bytes")
println(s"  追加坏数据: ${corruptData.length} bytes")
println(s"  坏数据内容: ${new String(corruptData)}")

// ====================================================================
//  STEP 8: 查询验证 — 预期读取失败
// ====================================================================
printSep("STEP 8: 查询验证 (log.1 已损坏)")

println("  正在尝试读取数据...")
println()

try {
  val corruptDf = spark.read.format("hudi").load(basePath)
  val count = corruptDf.count()
  println(s"  [结果] 查询成功, 读到 $count 条记录")
  println()

  // 检查数据完整性
  val b2 = corruptDf.filter(col("name").startsWith("batch2")).count()
  val b3 = corruptDf.filter(col("name").startsWith("batch3")).count()
  println(s"  batch2 记录 (来自 log.1): $b2 条")
  println(s"  batch3 记录 (来自 log.2): $b3 条")

  if (b3 == 0) {
    println()
    println("  *** 验证结论: log.2 的数据全部丢失! ***")
    println("  *** log.1 尾部的坏数据导致 log.2 不可读! ***")
  } else if (b3 < 50) {
    println()
    println(s"  *** 验证结论: log.2 的数据部分丢失! ($b3/50) ***")
  } else {
    println()
    println("  log.2 数据完整, 未出现预期的数据丢失 (可能是 Hudi 版本已修复)")
  }
} catch {
  case e: Exception =>
    println("  [结果] 查询失败, 抛出异常!")
    println()
    println(s"  异常类型: ${e.getClass.getName}")
    println(s"  异常消息: ${e.getMessage}")

    // 提取根因
    var cause = e.getCause
    val causeChain = new ArrayBuffer[String]()
    while (cause != null) {
      causeChain += s"    → ${cause.getClass.getSimpleName}: ${cause.getMessage}"
      cause = cause.getCause
    }
    if (causeChain.nonEmpty) {
      println("  异常链:")
      causeChain.foreach(println)
    }

    println()
    println("  *** 验证结论: log.1 尾部坏数据导致整个 scan 操作崩溃! ***")
    println("  *** log.2 的数据完全没有机会被读取, 数据全部丢失! ***")
}

// ====================================================================
//  STEP 9: 对比 — Snapshot 读 vs Read Optimized 读
// ====================================================================
printSep("STEP 9: Read Optimized 查询 (仅读 parquet, 不读 log)")

try {
  val roDF = spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "read_optimized")
    .load(basePath)
  val roCount = roDF.count()
  println(s"  Read Optimized 查询成功, 读到 $roCount 条记录")
  println("  (Read Optimized 只读 parquet 文件, 不受 log 文件损坏影响)")
  println()
  println("  前 5 条记录 (原始 batch1 数据):")
  roDF.select("id", "name", "ts").orderBy("id").show(5, truncate = false)
} catch {
  case e: Exception =>
    println(s"  Read Optimized 查询也失败: ${e.getMessage}")
}

// ====================================================================
//  清理指引
// ====================================================================
printSep("测试完成")
println(s"""
  |  测试路径: $basePath
  |
  |  清理命令:
  |    hdfs dfs -rm -r $basePath
  |    或
  |    hadoop fs -rm -r $basePath
  |
  |  如果使用 OBS:
  |    hadoop fs -rm -r $basePath
""".stripMargin)
