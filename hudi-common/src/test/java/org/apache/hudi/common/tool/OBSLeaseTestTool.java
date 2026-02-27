/*
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
 */

package org.apache.hudi.common.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OBS/HDFS 并发 Append 租约测试工具。
 *
 * <p>用于验证对象存储（如华为 OBS、S3A 等）或 HDFS 在以下场景下是否存在租约（lease）限制：
 * <ol>
 *   <li>Test 1: 两个线程同时尝试 append 同一个文件（并发 append）</li>
 *   <li>Test 2: 第一个流未关闭时，第二个流尝试 append 同一个文件（未释放流）</li>
 *   <li>Test 3: 多线程（N 个）同时 append 同一个文件（高并发压测）</li>
 *   <li>Test 4: 一个线程 create，另一个线程同时 append（create + append 并发）</li>
 *   <li>Test 5: 验证并发 append 后文件内容是否完整（数据正确性）</li>
 * </ol>
 *
 * <h3>使用方式</h3>
 * <pre>
 * # 编译
 * mvn package -pl hudi-common -DskipTests
 *
 * # 运行（需将 OBS/HDFS 相关 jar 和 core-site.xml 放入 classpath）
 * java -cp "hudi-common/target/*:hudi-common/target/dependency/*:/path/to/hadoop-conf" \
 *   org.apache.hudi.common.tool.OBSLeaseTestTool \
 *   obs://your-bucket/test-dir/lease-test
 *
 * # HDFS 测试
 * java -cp "..." org.apache.hudi.common.tool.OBSLeaseTestTool hdfs://namenode:8020/tmp/lease-test
 *
 * # 本地文件系统测试
 * java -cp "..." org.apache.hudi.common.tool.OBSLeaseTestTool file:///tmp/lease-test
 * </pre>
 *
 * <h3>用途</h3>
 * 用于验证 Hudi MOR 表在 OCC 场景下 rollback append ROLLBACK_BLOCK 时，
 * 如果两个 writer 并发 append 同一个 log 文件，存储层是否能提供类似 HDFS 租约的互斥保护。
 */
public class OBSLeaseTestTool {

  private static final String SEPARATOR = "=".repeat(80);
  private static final String THIN_SEPARATOR = "-".repeat(60);

  private final FileSystem fs;
  private final Path basePath;
  private final AtomicInteger testFileCounter = new AtomicInteger(0);

  public OBSLeaseTestTool(FileSystem fs, Path basePath) {
    this.fs = fs;
    this.basePath = basePath;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: OBSLeaseTestTool <test-base-path>");
      System.err.println("  Examples:");
      System.err.println("    obs://bucket/test-dir/lease-test");
      System.err.println("    hdfs://namenode:8020/tmp/lease-test");
      System.err.println("    file:///tmp/lease-test");
      System.exit(1);
    }

    String testBasePath = args[0];
    Configuration conf = new Configuration();
    // 如果需要自定义配置（如 OBS 的 AK/SK），可以在此添加或通过 core-site.xml 注入
    // conf.set("fs.obs.access.key", "xxx");
    // conf.set("fs.obs.secret.key", "xxx");
    // conf.set("fs.obs.endpoint", "xxx");

    Path path = new Path(testBasePath);
    FileSystem fs = path.getFileSystem(conf);

    System.out.println(SEPARATOR);
    System.out.println("OBS/HDFS Lease Test Tool");
    System.out.println(SEPARATOR);
    System.out.println("FileSystem impl : " + fs.getClass().getName());
    System.out.println("Test base path  : " + testBasePath);
    System.out.println("Supports append : " + isSupportAppend(fs, path));
    System.out.println(SEPARATOR);
    System.out.println();

    OBSLeaseTestTool tool = new OBSLeaseTestTool(fs, path);

    try {
      // 确保测试目录存在
      fs.mkdirs(path);

      tool.runTest1_ConcurrentAppend();
      tool.runTest2_AppendWithoutClosing();
      tool.runTest3_HighConcurrencyAppend(8);
      tool.runTest4_CreateAndAppendConcurrently();
      tool.runTest5_DataIntegrityCheck();
    } finally {
      // 清理测试目录
      System.out.println();
      System.out.println(SEPARATOR);
      System.out.print("Cleaning up test directory... ");
      try {
        fs.delete(path, true);
        System.out.println("Done.");
      } catch (Exception e) {
        System.out.println("Failed: " + e.getMessage());
      }
      fs.close();
    }

    System.out.println(SEPARATOR);
    System.out.println("All tests completed.");
    System.out.println(SEPARATOR);
  }

  // ============================================================
  // Test 1: 两个线程同时 append 同一个文件
  // ============================================================
  private void runTest1_ConcurrentAppend() throws Exception {
    printTestHeader("Test 1: Concurrent Append (2 threads)");
    System.out.println("Description: Two threads simultaneously try to append to the same file.");
    System.out.println("Expected on HDFS: One thread gets AlreadyBeingCreatedException.");
    System.out.println("Testing on: " + fs.getClass().getSimpleName());
    System.out.println(THIN_SEPARATOR);

    Path testFile = createTestFile("test1_concurrent_append");

    CyclicBarrier barrier = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);

    Future<TestResult> future1 = executor.submit(() -> {
      try {
        barrier.await(10, TimeUnit.SECONDS);
        FSDataOutputStream out = fs.append(testFile);
        byte[] data = "Thread-1-data\n".getBytes(StandardCharsets.UTF_8);
        out.write(data);
        out.hflush();
        Thread.sleep(2000); // 持有流 2 秒
        out.close();
        return new TestResult(true, "Thread-1: append succeeded, wrote " + data.length + " bytes");
      } catch (Exception e) {
        return new TestResult(false, "Thread-1: " + e.getClass().getSimpleName() + " - " + e.getMessage());
      }
    });

    Future<TestResult> future2 = executor.submit(() -> {
      try {
        barrier.await(10, TimeUnit.SECONDS);
        FSDataOutputStream out = fs.append(testFile);
        byte[] data = "Thread-2-data\n".getBytes(StandardCharsets.UTF_8);
        out.write(data);
        out.hflush();
        Thread.sleep(2000);
        out.close();
        return new TestResult(true, "Thread-2: append succeeded, wrote " + data.length + " bytes");
      } catch (Exception e) {
        return new TestResult(false, "Thread-2: " + e.getClass().getSimpleName() + " - " + e.getMessage());
      }
    });

    TestResult result1 = future1.get(30, TimeUnit.SECONDS);
    TestResult result2 = future2.get(30, TimeUnit.SECONDS);

    System.out.println(result1.message);
    System.out.println(result2.message);

    if (result1.success && result2.success) {
      System.out.println();
      printConclusion("⚠ BOTH threads succeeded! NO lease protection. "
          + "Concurrent append is allowed — potential data corruption risk!");
    } else if (!result1.success && !result2.success) {
      printConclusion("✗ Both threads failed. Append may not be supported.");
    } else {
      printConclusion("✓ Only ONE thread succeeded. Lease-like protection exists.");
    }

    executor.shutdownNow();
    printTestFooter();
  }

  // ============================================================
  // Test 2: 未关闭流时另一个 writer 尝试 append
  // ============================================================
  private void runTest2_AppendWithoutClosing() throws Exception {
    printTestHeader("Test 2: Append Without Closing Previous Stream");
    System.out.println("Description: Open a stream, keep it open, then try to open another append stream.");
    System.out.println("Expected on HDFS: Second open gets AlreadyBeingCreatedException.");
    System.out.println(THIN_SEPARATOR);

    Path testFile = createTestFile("test2_without_closing");

    FSDataOutputStream stream1 = null;
    FSDataOutputStream stream2 = null;
    try {
      System.out.println("Opening first append stream...");
      stream1 = fs.append(testFile);
      stream1.write("stream1-data\n".getBytes(StandardCharsets.UTF_8));
      stream1.hflush();
      System.out.println("First stream opened and data written (NOT closed).");

      System.out.println("Trying to open second append stream...");
      try {
        stream2 = fs.append(testFile);
        stream2.write("stream2-data\n".getBytes(StandardCharsets.UTF_8));
        stream2.hflush();
        System.out.println("Second stream opened and data written.");
        printConclusion("⚠ Second stream succeeded! NO lease protection on open streams.");
      } catch (Exception e) {
        System.out.println("Second stream FAILED: " + e.getClass().getSimpleName() + " - " + e.getMessage());
        printConclusion("✓ Lease protection detected: cannot open second stream while first is active.");
      }
    } catch (Exception e) {
      System.out.println("First stream FAILED: " + e.getClass().getName() + " - " + e.getMessage());
      printConclusion("✗ Append itself is not supported: " + e.getMessage());
    } finally {
      closeQuietly(stream1, "stream1");
      closeQuietly(stream2, "stream2");
    }

    printTestFooter();
  }

  // ============================================================
  // Test 3: 高并发 append（N 个线程）
  // ============================================================
  private void runTest3_HighConcurrencyAppend(int numThreads) throws Exception {
    printTestHeader("Test 3: High Concurrency Append (" + numThreads + " threads)");
    System.out.println("Description: " + numThreads + " threads simultaneously try to append to the same file.");
    System.out.println(THIN_SEPARATOR);

    Path testFile = createTestFile("test3_high_concurrency");

    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger failCount = new AtomicInteger(0);
    CountDownLatch latch = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      final int threadId = i;
      executor.submit(() -> {
        try {
          barrier.await(10, TimeUnit.SECONDS);
          FSDataOutputStream out = fs.append(testFile);
          String data = "Thread-" + threadId + "-data-" + System.nanoTime() + "\n";
          out.write(data.getBytes(StandardCharsets.UTF_8));
          out.hflush();
          Thread.sleep(1000);
          out.close();
          successCount.incrementAndGet();
          System.out.println("  Thread-" + threadId + ": ✓ SUCCESS");
        } catch (Exception e) {
          failCount.incrementAndGet();
          System.out.println("  Thread-" + threadId + ": ✗ FAILED - " + e.getClass().getSimpleName()
              + ": " + truncate(e.getMessage(), 100));
        } finally {
          latch.countDown();
        }
      });
    }

    latch.await(60, TimeUnit.SECONDS);

    System.out.println();
    System.out.println("Results: " + successCount.get() + " succeeded, " + failCount.get() + " failed");

    if (successCount.get() == numThreads) {
      printConclusion("⚠ ALL " + numThreads + " threads succeeded! NO lease protection at all.");
    } else if (successCount.get() == 1) {
      printConclusion("✓ Only 1 thread succeeded. Strong lease protection (like HDFS).");
    } else if (successCount.get() > 1) {
      printConclusion("⚠ " + successCount.get() + " threads succeeded (>1). Weak/partial lease protection.");
    } else {
      printConclusion("✗ All threads failed. Append may not be supported.");
    }

    executor.shutdownNow();
    printTestFooter();
  }

  // ============================================================
  // Test 4: 一个线程 create，另一个线程 append
  // ============================================================
  private void runTest4_CreateAndAppendConcurrently() throws Exception {
    printTestHeader("Test 4: Create + Append Concurrently");
    System.out.println("Description: One thread creates (overwrites) a file while another appends.");
    System.out.println(THIN_SEPARATOR);

    Path testFile = createTestFile("test4_create_append");

    CyclicBarrier barrier = new CyclicBarrier(2);
    ExecutorService executor = Executors.newFixedThreadPool(2);

    Future<TestResult> createFuture = executor.submit(() -> {
      try {
        barrier.await(10, TimeUnit.SECONDS);
        FSDataOutputStream out = fs.create(testFile, true); // overwrite=true
        byte[] data = "CREATE-overwrite-data\n".getBytes(StandardCharsets.UTF_8);
        out.write(data);
        Thread.sleep(2000);
        out.hflush();
        out.close();
        return new TestResult(true, "Creator: create(overwrite) succeeded");
      } catch (Exception e) {
        return new TestResult(false, "Creator: " + e.getClass().getSimpleName() + " - " + e.getMessage());
      }
    });

    Future<TestResult> appendFuture = executor.submit(() -> {
      try {
        barrier.await(10, TimeUnit.SECONDS);
        FSDataOutputStream out = fs.append(testFile);
        byte[] data = "APPEND-data\n".getBytes(StandardCharsets.UTF_8);
        out.write(data);
        Thread.sleep(2000);
        out.hflush();
        out.close();
        return new TestResult(true, "Appender: append succeeded");
      } catch (Exception e) {
        return new TestResult(false, "Appender: " + e.getClass().getSimpleName() + " - " + e.getMessage());
      }
    });

    TestResult createResult = createFuture.get(30, TimeUnit.SECONDS);
    TestResult appendResult = appendFuture.get(30, TimeUnit.SECONDS);

    System.out.println(createResult.message);
    System.out.println(appendResult.message);

    if (createResult.success && appendResult.success) {
      printConclusion("⚠ Both create and append succeeded! NO mutual exclusion.");
    } else if (createResult.success && !appendResult.success) {
      printConclusion("✓ Create succeeded, append blocked. Lease protection for create vs append.");
    } else if (!createResult.success && appendResult.success) {
      printConclusion("Append succeeded, create blocked. Interesting — append has priority.");
    } else {
      printConclusion("Both failed. Something else is wrong.");
    }

    executor.shutdownNow();
    printTestFooter();
  }

  // ============================================================
  // Test 5: 并发 append 后数据完整性检查
  // ============================================================
  private void runTest5_DataIntegrityCheck() throws Exception {
    printTestHeader("Test 5: Data Integrity After Sequential Appends");
    System.out.println("Description: Sequentially append known data blocks, then verify file content.");
    System.out.println(THIN_SEPARATOR);

    Path testFile = createTestFile("test5_integrity");

    int numAppends = 5;
    int totalExpectedBytes = 0;
    StringBuilder expectedContent = new StringBuilder();

    // 初始文件内容
    String initContent = new String(
        fs.open(testFile).readAllBytes(), StandardCharsets.UTF_8);
    expectedContent.append(initContent);
    totalExpectedBytes += initContent.length();

    for (int i = 0; i < numAppends; i++) {
      String data = "BLOCK-" + i + "-" + "X".repeat(100) + "\n";
      FSDataOutputStream out = fs.append(testFile);
      out.write(data.getBytes(StandardCharsets.UTF_8));
      out.hflush();
      out.close();
      expectedContent.append(data);
      totalExpectedBytes += data.getBytes(StandardCharsets.UTF_8).length;
      System.out.println("  Append " + (i + 1) + "/" + numAppends + ": wrote " + data.length() + " chars");
    }

    // 读取并验证
    long actualSize = fs.getFileStatus(testFile).getLen();
    String actualContent = new String(
        fs.open(testFile).readAllBytes(), StandardCharsets.UTF_8);

    System.out.println();
    System.out.println("Expected size: " + totalExpectedBytes + " bytes");
    System.out.println("Actual size  : " + actualSize + " bytes");
    System.out.println("Content match: " + expectedContent.toString().equals(actualContent));

    if (actualSize == totalExpectedBytes && expectedContent.toString().equals(actualContent)) {
      printConclusion("✓ Data integrity OK. Sequential append works correctly.");
    } else {
      printConclusion("✗ Data integrity FAILED! Expected " + totalExpectedBytes
          + " bytes but got " + actualSize + " bytes.");
      if (!expectedContent.toString().equals(actualContent)) {
        System.out.println("  Content mismatch details:");
        System.out.println("  Expected (first 200 chars): " + truncate(expectedContent.toString(), 200));
        System.out.println("  Actual   (first 200 chars): " + truncate(actualContent, 200));
      }
    }

    printTestFooter();
  }

  // ============================================================
  // Helper methods
  // ============================================================

  private Path createTestFile(String name) throws IOException {
    Path testFile = new Path(basePath, name + "_" + testFileCounter.incrementAndGet() + ".log");
    FSDataOutputStream out = fs.create(testFile, true);
    out.write(("init-" + name + "\n").getBytes(StandardCharsets.UTF_8));
    out.close();
    System.out.println("Created test file: " + testFile);
    return testFile;
  }

  private static boolean isSupportAppend(FileSystem fs, Path path) {
    try {
      Path testFile = new Path(path, ".append_support_check");
      FSDataOutputStream out = fs.create(testFile, true);
      out.write("test".getBytes(StandardCharsets.UTF_8));
      out.close();
      try {
        FSDataOutputStream appendOut = fs.append(testFile);
        appendOut.write("append".getBytes(StandardCharsets.UTF_8));
        appendOut.close();
        return true;
      } catch (UnsupportedOperationException | IOException e) {
        return false;
      } finally {
        fs.delete(testFile, false);
      }
    } catch (Exception e) {
      return false;
    }
  }

  private static void closeQuietly(FSDataOutputStream stream, String name) {
    if (stream != null) {
      try {
        stream.close();
        System.out.println("  Closed " + name + " successfully.");
      } catch (Exception e) {
        System.out.println("  Failed to close " + name + ": " + e.getMessage());
      }
    }
  }

  private static String truncate(String s, int maxLen) {
    if (s == null) {
      return "null";
    }
    return s.length() <= maxLen ? s : s.substring(0, maxLen) + "...";
  }

  private static void printTestHeader(String title) {
    System.out.println();
    System.out.println(SEPARATOR);
    System.out.println("  " + title);
    System.out.println(SEPARATOR);
  }

  private static void printTestFooter() {
    System.out.println(THIN_SEPARATOR);
  }

  private static void printConclusion(String msg) {
    System.out.println();
    System.out.println("  >>> CONCLUSION: " + msg);
  }

  private static class TestResult {
    final boolean success;
    final String message;

    TestResult(boolean success, String message) {
      this.success = success;
      this.message = message;
    }
  }
}
