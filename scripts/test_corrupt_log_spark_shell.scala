// ===========================================================================
// MOR Log 文件坏块影响验证脚本 (spark-shell 执行)
// ===========================================================================
//
// 包含两个测试场景:
//
//   场景 A - 跨文件: log.1 尾部追加坏数据 → log.2 的数据是否丢失
//   场景 B - 文件内: log.1 中间插入坏数据(block1 → 坏数据 → block2) → block2 是否丢失
//
// 原理:
//   Hudi 读取 log 文件时，每个 block 以 MAGIC 字节(#HUDI#) 开头。
//   如果在读取下一个 block 时遇到 >= 6 字节的非 MAGIC 数据，会抛出
//   CorruptedLogFileException (RuntimeException)，导致整个 scan 操作失败。
//
// 使用方式:
//   spark-shell --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.x.x
//   然后分阶段粘贴执行，或者 :load /path/to/this/script.scala
//
// ===========================================================================

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream}
import scala.collection.mutable.ArrayBuffer

// ====================================================================
// 公共配置
// ====================================================================
// HDFS:
val baseDir = "/tmp/hudi"
// OBS:
// val baseDir = "obs://your-bucket/hudi"
// Local:
// val baseDir = "file:///tmp/hudi"

val partitionValue = "2024-01-01"
val partitionPathStr = s"dt=$partitionValue"

// ====== 公共 Helper 函数 ======
def getFs(path: String) = FileSystem.get(new java.net.URI(path), spark.sparkContext.hadoopConfiguration)

def listLogFiles(fs: FileSystem, partDir: Path): Array[org.apache.hadoop.fs.FileStatus] = {
  if (!fs.exists(partDir)) return Array.empty
  fs.listStatus(partDir)
    .filter(f => f.getPath.getName.contains(".log.") && !f.getPath.getName.endsWith(".bak"))
    .sortBy(_.getPath.getName)
}

def printLogFiles(fs: FileSystem, partDir: Path, label: String): Unit = {
  val lf = listLogFiles(fs, partDir)
  println(s"\n  [$label] 共 ${lf.length} 个 log 文件:")
  lf.foreach(f => println(s"    ${f.getPath.getName}  (${f.getLen} bytes)"))
}

def printSep(title: String): Unit = {
  println("\n" + "=" * 70)
  println(s"  $title")
  println("=" * 70)
}

def printBigSep(title: String): Unit = {
  println("\n")
  println("#" * 70)
  println(s"#  $title")
  println("#" * 70)
}

def writeHudiData(basePath: String, tableName: String, df: org.apache.spark.sql.DataFrame,
                  operation: String, mode: SaveMode): Unit = {
  df.write.format("hudi")
    .option("hoodie.table.name", tableName)
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
    .option("hoodie.datasource.write.recordkey.field", "id")
    .option("hoodie.datasource.write.precombine.field", "ts")
    .option("hoodie.datasource.write.partitionpath.field", "dt")
    .option("hoodie.datasource.write.operation", operation)
    .option("hoodie.insert.shuffle.parallelism", "1")
    .option("hoodie.upsert.shuffle.parallelism", "1")
    .mode(mode)
    .save(basePath)
}

def queryAndReport(basePath: String, label: String): Unit = {
  println(s"  正在尝试 $label ...")
  println()
  try {
    val df = spark.read.format("hudi").load(basePath)
    val count = df.count()
    println(s"  [结果] 查询成功, 读到 $count 条记录")

    val b1 = df.filter(col("name").startsWith("batch1")).count()
    val b2 = df.filter(col("name").startsWith("batch2")).count()
    val b3 = df.filter(col("name").startsWith("batch3")).count()
    println(s"  batch1 (未更新): $b1 条")
    println(s"  batch2 (更新 ID 1-50): $b2 条")
    println(s"  batch3 (更新 ID 51-100): $b3 条")

    if (b2 == 0 && b3 == 0) {
      println("\n  *** 所有 log 数据全部丢失, 只剩 parquet base 数据! ***")
    } else if (b3 == 0) {
      println("\n  *** batch3 数据全部丢失! ***")
    } else {
      println("\n  数据完整, 未出现预期的数据丢失 (可能是 Hudi 版本已修复)")
    }
    println()
    println("  前 5 条记录:")
    df.select("id", "name", "ts").orderBy("id").show(5, truncate = false)
  } catch {
    case e: Exception =>
      println("  [结果] 查询失败, 抛出异常!")
      println()
      println(s"  异常类型: ${e.getClass.getName}")
      println(s"  异常消息: ${e.getMessage}")

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
      println("  *** 坏数据导致整个 scan 操作崩溃, 后续数据完全丢失! ***")
  }
}

def queryReadOptimized(basePath: String): Unit = {
  printSep("Read Optimized 查询 (仅读 parquet, 不读 log)")
  try {
    val roDF = spark.read.format("hudi")
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(basePath)
    val roCount = roDF.count()
    println(s"  Read Optimized 查询成功, 读到 $roCount 条记录")
    println("  (Read Optimized 只读 parquet 文件, 不受 log 文件损坏影响)")
    println()
    println("  前 5 条记录:")
    roDF.select("id", "name", "ts").orderBy("id").show(5, truncate = false)
  } catch {
    case e: Exception =>
      println(s"  Read Optimized 查询也失败: ${e.getMessage}")
  }
}

// ####################################################################
// ####################################################################
//
//  场景 A: 跨文件 — log.1 尾部坏数据导致 log.2 数据丢失
//
// ####################################################################
// ####################################################################

printBigSep("场景 A: 跨文件 — log.1 尾部坏数据 → log.2 不可读")

val tableNameA = "test_corrupt_log_cross_file"
val basePathA = s"$baseDir/$tableNameA"
val fsA = getFs(basePathA)
val partDirA = new Path(basePathA + "/" + partitionPathStr)

// ------ A.1: Insert 100 条记录 ------
printSep("A.1: Insert 100 条记录 (创建 parquet base)")

val dfA1 = spark.range(1, 101).toDF("id")
  .withColumn("name", concat(lit("batch1_user_"), col("id").cast("string")))
  .withColumn("ts", lit(1000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathA, tableNameA, dfA1, "insert", SaveMode.Overwrite)
println("  Insert 完成!")
printLogFiles(fsA, partDirA, "Insert 之后")

// ------ A.2: 第一次 Upsert → log.1 (batch2, ID 1-50) ------
printSep("A.2: 第一次 Upsert (更新 ID 1-50) → log.1")

val dfA2 = spark.range(1, 51).toDF("id")
  .withColumn("name", concat(lit("batch2_updated_"), col("id").cast("string")))
  .withColumn("ts", lit(2000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathA, tableNameA, dfA2, "upsert", SaveMode.Append)
println("  第一次 Upsert 完成!")
printLogFiles(fsA, partDirA, "第一次 Upsert")

// ------ A.3: 隐藏 log.1 ------
printSep("A.3: 临时隐藏 log.1")

val logFilesA2 = listLogFiles(fsA, partDirA)
require(logFilesA2.nonEmpty, "ERROR: 未找到 log 文件!")

val origLog1A = logFilesA2.head.getPath
val bakLog1A = new Path(origLog1A.getParent, origLog1A.getName + ".bak")
fsA.rename(origLog1A, bakLog1A)
println(s"  已隐藏: ${origLog1A.getName} → ${bakLog1A.getName}")

// ------ A.4: 第二次 Upsert → 新 log.1 (batch3, ID 51-100) ------
printSep("A.4: 第二次 Upsert (更新 ID 51-100) → 新 log.1")

val dfA3 = spark.range(51, 101).toDF("id")
  .withColumn("name", concat(lit("batch3_updated_"), col("id").cast("string")))
  .withColumn("ts", lit(3000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathA, tableNameA, dfA3, "upsert", SaveMode.Append)
println("  第二次 Upsert 完成!")

// ------ A.5: 重命名新 log.1 → log.2, 恢复原始 log.1 ------
printSep("A.5: 重命名 → 构造 log.1 + log.2")

val newLogA = listLogFiles(fsA, partDirA).head.getPath
val log2NameA = newLogA.getName.replaceFirst("\\.log\\.1_", ".log.2_")
val log2PathA = new Path(newLogA.getParent, log2NameA)

fsA.rename(newLogA, log2PathA)
println(s"  新 log.1 → log.2: $log2NameA")

fsA.rename(bakLog1A, origLog1A)
println(s"  恢复原始 log.1: ${origLog1A.getName}")
printLogFiles(fsA, partDirA, "构造完成: log.1 + log.2")

// ------ A.6: 验证正常读取 ------
printSep("A.6: 验证正常读取 (未损坏)")
queryAndReport(basePathA, "正常 Snapshot 查询")

// ------ A.7: 向 log.1 尾部追加坏数据 ------
printSep("A.7: 向 log.1 尾部追加坏数据")

val sizeBeforeA = fsA.getFileStatus(origLog1A).getLen
val corruptDataA = "CORRUPT_DATA_BY_CONCURRENT_WRITER_NO_MAGIC!!!".getBytes("UTF-8")
val streamA = fsA.append(origLog1A)
streamA.write(corruptDataA)
streamA.hflush()
streamA.close()

println(s"  文件: ${origLog1A.getName}")
println(s"  追加前大小: $sizeBeforeA bytes")
println(s"  追加后大小: ${fsA.getFileStatus(origLog1A).getLen} bytes")
println(s"  追加坏数据: ${corruptDataA.length} bytes")

// ------ A.8: 查询验证 (预期失败) ------
printSep("A.8: 查询验证 — log.1 尾部损坏")
println()
println("  文件结构:")
println("    log.1: [block1_batch2] [CORRUPT_TAIL]")
println("    log.2: [block2_batch3]")
println("  预期: reader 在 log.1 尾部遇到坏数据 → 异常 → log.2 不可读")
println()
queryAndReport(basePathA, "损坏后 Snapshot 查询")

// ------ A.9: Read Optimized 对比 ------
queryReadOptimized(basePathA)


// ####################################################################
// ####################################################################
//
//  场景 B: 文件内 — log 文件中间有坏数据, 后续 block 丢失
//
// ####################################################################
// ####################################################################

printBigSep("场景 B: 文件内 — log 文件中间坏数据 → 后续 block 丢失")

val tableNameB = "test_corrupt_log_middle_block"
val basePathB = s"$baseDir/$tableNameB"
val fsB = getFs(basePathB)
val partDirB = new Path(basePathB + "/" + partitionPathStr)

// ------ B.1: Insert 100 条记录 ------
printSep("B.1: Insert 100 条记录 (创建 parquet base)")

val dfB1 = spark.range(1, 101).toDF("id")
  .withColumn("name", concat(lit("batch1_user_"), col("id").cast("string")))
  .withColumn("ts", lit(1000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathB, tableNameB, dfB1, "insert", SaveMode.Overwrite)
println("  Insert 完成!")
printLogFiles(fsB, partDirB, "Insert 之后")

// ------ B.2: 第一次 Upsert → log.1 包含 block1 (batch2, ID 1-50) ------
printSep("B.2: 第一次 Upsert (更新 ID 1-50) → log.1 block1")

val dfB2 = spark.range(1, 51).toDF("id")
  .withColumn("name", concat(lit("batch2_updated_"), col("id").cast("string")))
  .withColumn("ts", lit(2000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathB, tableNameB, dfB2, "upsert", SaveMode.Append)
println("  第一次 Upsert 完成!")
printLogFiles(fsB, partDirB, "第一次 Upsert")

// ------ B.3: 验证正常读取 (单 block) ------
printSep("B.3: 验证正常读取 (单 block, 无损坏)")
queryAndReport(basePathB, "正常 Snapshot 查询")

// ------ B.4: 向 log.1 尾部追加坏数据 (模拟并发写入的损坏) ------
printSep("B.4: 向 log.1 当前尾部追加坏数据 (在 block1 之后)")

val logFilesB2 = listLogFiles(fsB, partDirB)
require(logFilesB2.nonEmpty, "ERROR: 未找到 log 文件!")

val log1B = logFilesB2.head.getPath
val sizeBeforeCorruptB = fsB.getFileStatus(log1B).getLen

val corruptDataB = "JUNK_FROM_CONCURRENT_WRITER_NOT_MAGIC_HEADER_DATA!!!".getBytes("UTF-8")
val streamB = fsB.append(log1B)
streamB.write(corruptDataB)
streamB.hflush()
streamB.close()

val sizeAfterCorruptB = fsB.getFileStatus(log1B).getLen
println(s"  文件: ${log1B.getName}")
println(s"  追加前大小: $sizeBeforeCorruptB bytes (包含 block1)")
println(s"  追加后大小: $sizeAfterCorruptB bytes (block1 + 坏数据)")
println(s"  追加坏数据: ${corruptDataB.length} bytes")
println()
println("  当前 log.1 结构: [block1_batch2] [CORRUPT_BYTES]")

// ------ B.5: 第二次 Upsert → writer append block2 到 log.1 (坏数据之后) ------
printSep("B.5: 第二次 Upsert (更新 ID 51-100) → block2 追加到 log.1 坏数据之后")
println("  注意: Hudi writer 不读取文件内容, 只是在文件末尾 append,")
println("  所以 block2 会被成功写入到坏数据之后")
println()

val dfB3 = spark.range(51, 101).toDF("id")
  .withColumn("name", concat(lit("batch3_updated_"), col("id").cast("string")))
  .withColumn("ts", lit(3000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathB, tableNameB, dfB3, "upsert", SaveMode.Append)

val sizeAfterB3 = fsB.getFileStatus(log1B).getLen
println(s"  第二次 Upsert 完成!")
println(s"  log.1 最终大小: $sizeAfterB3 bytes")
println()
println("  最终 log.1 结构: [block1_batch2] [CORRUPT_BYTES] [block2_batch3]")
println("                      ↑ 有效           ↑ >= 6字节       ↑ 有效但不可达")
println("                                        非 MAGIC 数据")
printLogFiles(fsB, partDirB, "第二次 Upsert (注意仍然是同一个 log 文件)")

// ------ B.6: 查询验证 (预期失败) ------
printSep("B.6: 查询验证 — log.1 中间有坏数据")
println()
println("  预期: reader 读完 block1 → 遇到坏数据 → CorruptedLogFileException")
println("  block2 (batch3 数据) 虽然有效, 但永远不会被读到")
println()
queryAndReport(basePathB, "损坏后 Snapshot 查询")

// ------ B.7: Read Optimized 对比 ------
queryReadOptimized(basePathB)

// ####################################################################
// ####################################################################
//
//  场景 C: 文件内 — 有 MAGIC 头但内容损坏的坏块 → 可被跳过
//
//  与场景 B 的区别:
//    场景 B: 坏数据没有 MAGIC 头 → readMagic() 抛异常 → 致命
//    场景 C: 坏数据有 MAGIC 头(#HUDI#) → readBlock() 检测到 corrupt
//           → 调用 createCorruptBlock() → scanForNextAvailableBlockOffset()
//           → 返回 HoodieCorruptBlock 对象 → reader 继续读后续 block
//
// ####################################################################
// ####################################################################

printBigSep("场景 C: 文件内 — 有 MAGIC 头但内容损坏 → 坏块被跳过")

val tableNameC = "test_corrupt_log_magic_corrupt"
val basePathC = s"$baseDir/$tableNameC"
val fsC = getFs(basePathC)
val partDirC = new Path(basePathC + "/" + partitionPathStr)

// ------ C.1: Insert 100 条记录 ------
printSep("C.1: Insert 100 条记录 (创建 parquet base)")

val dfC1 = spark.range(1, 101).toDF("id")
  .withColumn("name", concat(lit("batch1_user_"), col("id").cast("string")))
  .withColumn("ts", lit(1000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathC, tableNameC, dfC1, "insert", SaveMode.Overwrite)
println("  Insert 完成!")
printLogFiles(fsC, partDirC, "Insert 之后")

// ------ C.2: 第一次 Upsert → log.1 包含 block1 (batch2, ID 1-50) ------
printSep("C.2: 第一次 Upsert (更新 ID 1-50) → log.1 block1")

val dfC2 = spark.range(1, 51).toDF("id")
  .withColumn("name", concat(lit("batch2_updated_"), col("id").cast("string")))
  .withColumn("ts", lit(2000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathC, tableNameC, dfC2, "upsert", SaveMode.Append)
println("  第一次 Upsert 完成!")
printLogFiles(fsC, partDirC, "第一次 Upsert")

// ------ C.3: 向 log.1 追加"有 MAGIC 头但内容损坏"的数据 ------
printSep("C.3: 向 log.1 追加有 MAGIC 头但内容损坏的数据")

val logFilesC2 = listLogFiles(fsC, partDirC)
require(logFilesC2.nonEmpty, "ERROR: 未找到 log 文件!")

val log1C = logFilesC2.head.getPath
val sizeBeforeCorruptC = fsC.getFileStatus(log1C).getLen

// 构造: #HUDI# + 垃圾内容 (模拟写了 MAGIC 后 writer 崩溃或数据错乱)
// MAGIC: 6 bytes = [#, H, U, D, I, #]
// 后面跟随垃圾数据, 让 blockSize (readLong) 得到一个随机值
val magicBytes = Array[Byte]('#', 'H', 'U', 'D', 'I', '#')
val garbageContent = "THIS_IS_GARBAGE_AFTER_MAGIC_SIMULATING_PARTIAL_WRITE_OR_INTERLEAVED_CONCURRENT_APPEND!!!".getBytes("UTF-8")
val corruptBlockWithMagic = magicBytes ++ garbageContent

val streamC = fsC.append(log1C)
streamC.write(corruptBlockWithMagic)
streamC.hflush()
streamC.close()

val sizeAfterCorruptC = fsC.getFileStatus(log1C).getLen
println(s"  文件: ${log1C.getName}")
println(s"  追加前大小: $sizeBeforeCorruptC bytes (包含 block1)")
println(s"  追加后大小: $sizeAfterCorruptC bytes (block1 + 有 MAGIC 的坏数据)")
println(s"  追加坏数据: ${corruptBlockWithMagic.length} bytes")
println(s"  坏数据结构: [#HUDI# (6 bytes)] [garbage (${garbageContent.length} bytes)]")
println()
println("  当前 log.1 结构: [#HUDI# block1] [#HUDI# <garbage>]")

// ------ C.4: 第二次 Upsert → writer append block2 到 log.1 ------
printSep("C.4: 第二次 Upsert (更新 ID 51-100) → block2 追加到 log.1")
println("  注意: writer 在坏数据之后继续 append, block2 有完整的 MAGIC 头")
println()

val dfC3 = spark.range(51, 101).toDF("id")
  .withColumn("name", concat(lit("batch3_updated_"), col("id").cast("string")))
  .withColumn("ts", lit(3000L))
  .withColumn("dt", lit(partitionValue))

writeHudiData(basePathC, tableNameC, dfC3, "upsert", SaveMode.Append)

val sizeAfterC3 = fsC.getFileStatus(log1C).getLen
println(s"  第二次 Upsert 完成!")
println(s"  log.1 最终大小: $sizeAfterC3 bytes")
println()
println("  最终 log.1 结构: [#HUDI# block1] [#HUDI# <garbage>] [#HUDI# block2]")
println("                      ↑ 有效           ↑ 有 MAGIC 头      ↑ 有效")
println("                                        内容损坏")
println()
println("  处理流程预期:")
println("    1. reader 读完 block1 → OK")
println("    2. hasNext() → readMagic() 找到 #HUDI# → 返回 true")
println("    3. next() → readBlock() → 读 blockSize (垃圾) → isBlockCorrupted() → true")
println("    4. createCorruptBlock() → scanForNextAvailableBlockOffset() → 找到 block2 的 #HUDI#")
println("    5. 返回 HoodieCorruptBlock (不是异常!)")
println("    6. 继续读 block2 → OK!")
printLogFiles(fsC, partDirC, "第二次 Upsert")

// ------ C.5: 查询验证 (预期: 坏块被跳过, block2 仍然可读) ------
printSep("C.5: 查询验证 — log.1 中间有 MAGIC 头的坏块")
println()
println("  预期: 坏块被 createCorruptBlock() 跳过, block2 (batch3) 仍然可读!")
println()
queryAndReport(basePathC, "损坏后 Snapshot 查询")

// ------ C.6: Read Optimized 对比 ------
queryReadOptimized(basePathC)


// ####################################################################
// 总结
// ####################################################################
printBigSep("测试总结")
println("""
  |  场景 A (跨文件, 无 MAGIC):
  |    log.1: [#HUDI# block1] [CORRUPT_TAIL (无 MAGIC)]
  |    log.2: [#HUDI# block2]
  |    → readMagic() 遇到非 MAGIC 数据 → CorruptedLogFileException → 整个 scan 崩溃
  |    → log.2 完全不可读 ❌
  |
  |  场景 B (文件内, 无 MAGIC):
  |    log.1: [#HUDI# block1] [CORRUPT (无 MAGIC)] [#HUDI# block2]
  |    → readMagic() 遇到非 MAGIC 数据 → CorruptedLogFileException → 整个 scan 崩溃
  |    → block2 完全不可读 ❌
  |
  |  场景 C (文件内, 有 MAGIC):
  |    log.1: [#HUDI# block1] [#HUDI# <garbage>] [#HUDI# block2]
  |    → readMagic() 找到 #HUDI# → readBlock() → isBlockCorrupted() → true
  |    → createCorruptBlock() → scanForNextAvailableBlockOffset() → 找到 block2
  |    → 返回 HoodieCorruptBlock 对象 → reader 继续读 block2 ✓
  |
  |  关键结论:
  |    1. 坏块是否有 MAGIC 头, 决定了处理路径完全不同:
  |       - 无 MAGIC: 在 hasNext() 就抛异常, 致命
  |       - 有 MAGIC: 在 readBlock() 中被 isBlockCorrupted() 检测, 可跳过
  |    2. 并发写入场景中, 两种损坏都可能发生:
  |       - 字节级交错 → 连 MAGIC 都被破坏 → 致命 (场景 A/B)
  |       - 一方写完 MAGIC 后崩溃/数据错乱 → 有 MAGIC 无有效内容 → 可恢复 (场景 C)
  |    3. 即使场景 C 可以跳过坏块, 坏块中包含的数据仍然丢失
  |
  |  OCC 风险:
  |    rollback 和 normal writer 并发 append 同一个 log 文件时:
  |    - 最坏情况: 字节交错, 连 MAGIC 都被破坏 → scan 崩溃, 全部数据丢失
  |    - 较好情况: 一方完整写入后另一方再写 → 有 MAGIC 坏块 → 部分数据丢失
  |    无论哪种情况, 都会造成数据丢失, 必须做 rollback log 文件隔离
""".stripMargin)

println(s"""
  |  清理命令:
  |    hadoop fs -rm -r $basePathA
  |    hadoop fs -rm -r $basePathB
  |    hadoop fs -rm -r $basePathC
""".stripMargin)
