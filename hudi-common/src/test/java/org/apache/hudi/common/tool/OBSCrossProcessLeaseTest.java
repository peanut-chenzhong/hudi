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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * OBS/HDFS 跨进程并发 Append 租约测试工具。
 *
 * <p>设计为两阶段运行：在两个独立的进程（不同 JVM / 不同机器）中分别启动，
 * 通过共享存储上的信号文件协调，验证不同进程并发 append 同一文件时是否存在租约保护。
 *
 * <h3>使用方式（需要两个终端/进程）</h3>
 *
 * <b>步骤 1：先启动 Process A（持有者）</b>
 * <pre>
 * spark-submit --class org.apache.hudi.common.tool.OBSCrossProcessLeaseTest \
 *   --master local[*] java-test-1.0-SNAPSHOT.jar \
 *   obs://bucket/test-dir  hold  60
 *
 * # 参数说明：
 * #   obs://bucket/test-dir  — 测试目录（两个进程必须相同）
 * #   hold                   — 角色：创建文件并持有 append 流
 * #   60                     — 持有时间（秒），在此期间流保持打开
 * </pre>
 *
 * <b>步骤 2：等 Process A 输出 "READY" 后，启动 Process B（挑战者）</b>
 * <pre>
 * spark-submit --class org.apache.hudi.common.tool.OBSCrossProcessLeaseTest \
 *   --master local[*] java-test-1.0-SNAPSHOT.jar \
 *   obs://bucket/test-dir  challenge
 *
 * # 参数说明：
 * #   obs://bucket/test-dir  — 测试目录（与 Process A 相同）
 * #   challenge              — 角色：等待信号文件出现后尝试 append
 * </pre>
 *
 * <b>步骤 3（可选）：清理</b>
 * <pre>
 * spark-submit --class org.apache.hudi.common.tool.OBSCrossProcessLeaseTest \
 *   --master local[*] java-test-1.0-SNAPSHOT.jar \
 *   obs://bucket/test-dir  cleanup
 * </pre>
 *
 * <h3>文件布局</h3>
 * <pre>
 *   test-dir/
 *     target.log        — 被测试的目标文件（两个进程都尝试 append）
 *     signal_ready      — 信号文件：Process A 创建，表示 append 流已打开
 *     signal_challenge  — 信号文件：Process B 创建，表示挑战完成
 * </pre>
 */
public class OBSCrossProcessLeaseTest {

  private static final String SEPARATOR = repeatChar('=', 80);
  private static final String TARGET_FILE = "target.log";
  private static final String SIGNAL_READY = "signal_ready";
  private static final String SIGNAL_CHALLENGE_DONE = "signal_challenge_done";

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      printUsage();
      System.exit(1);
    }

    String testDir = args[0];
    String role = args[1].toLowerCase();

    Configuration conf = new Configuration();
    Path basePath = new Path(testDir);
    FileSystem fs = basePath.getFileSystem(conf);

    String hostInfo = InetAddress.getLocalHost().getHostName() + " (PID: " + getProcessId() + ")";

    System.out.println(SEPARATOR);
    System.out.println("OBS Cross-Process Lease Test");
    System.out.println(SEPARATOR);
    System.out.println("FileSystem  : " + fs.getClass().getName());
    System.out.println("Test dir    : " + testDir);
    System.out.println("Role        : " + role);
    System.out.println("Host/PID    : " + hostInfo);
    System.out.println(SEPARATOR);
    System.out.println();

    switch (role) {
      case "hold":
        int holdSeconds = args.length >= 3 ? Integer.parseInt(args[2]) : 60;
        runHolder(fs, basePath, holdSeconds, hostInfo);
        break;
      case "challenge":
        int waitTimeout = args.length >= 3 ? Integer.parseInt(args[2]) : 120;
        runChallenger(fs, basePath, waitTimeout, hostInfo);
        break;
      case "cleanup":
        runCleanup(fs, basePath);
        break;
      case "read":
        runRead(fs, basePath);
        break;
      default:
        System.err.println("Unknown role: " + role);
        printUsage();
        System.exit(1);
    }
  }

  /**
   * 角色: Holder（持有者）
   * 1. 创建目标文件
   * 2. 打开 append 流并写入数据
   * 3. 创建 signal_ready 信号文件
   * 4. 保持流打开 N 秒
   * 5. 关闭流
   */
  private static void runHolder(FileSystem fs, Path basePath, int holdSeconds, String hostInfo)
      throws Exception {
    Path targetFile = new Path(basePath, TARGET_FILE);
    Path signalReady = new Path(basePath, SIGNAL_READY);
    Path signalChallengeDone = new Path(basePath, SIGNAL_CHALLENGE_DONE);

    // 清理旧的信号文件
    deleteQuietly(fs, signalReady);
    deleteQuietly(fs, signalChallengeDone);

    // 确保目录存在
    fs.mkdirs(basePath);

    // 步骤 1：创建目标文件
    System.out.println("[Holder] Creating target file: " + targetFile);
    FSDataOutputStream createOut = fs.create(targetFile, true);
    createOut.write(("init-data-by-holder\n").getBytes(StandardCharsets.UTF_8));
    createOut.close();
    System.out.println("[Holder] Target file created.");

    // 步骤 2：打开 append 流
    System.out.println("[Holder] Opening append stream...");
    FSDataOutputStream appendOut = fs.append(targetFile);
    String holderData = "[HOLDER-DATA][" + hostInfo + "][" + System.currentTimeMillis() + "]\n";
    appendOut.write(holderData.getBytes(StandardCharsets.UTF_8));
    appendOut.hflush();
    System.out.println("[Holder] Append stream opened, data written and flushed.");
    System.out.println("[Holder] Written: " + holderData.trim());

    // 步骤 3：创建信号文件，通知 Challenger 可以开始
    System.out.println("[Holder] Creating READY signal...");
    FSDataOutputStream signalOut = fs.create(signalReady, true);
    signalOut.write(("ready:" + hostInfo + ":" + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
    signalOut.close();

    System.out.println();
    System.out.println(SEPARATOR);
    System.out.println("  [READY] Append stream is OPEN. Holding for " + holdSeconds + " seconds.");
    System.out.println("  Now start the CHALLENGER process in another terminal!");
    System.out.println(SEPARATOR);
    System.out.println();

    // 步骤 4：保持流打开，每 5 秒打印状态
    long startTime = System.currentTimeMillis();
    long endTime = startTime + holdSeconds * 1000L;
    int tick = 0;
    while (System.currentTimeMillis() < endTime) {
      tick++;
      long remaining = (endTime - System.currentTimeMillis()) / 1000;
      System.out.println("[Holder] Still holding... (" + remaining + "s remaining)"
          + (fs.exists(signalChallengeDone) ? " [Challenger has finished]" : ""));

      // 每 10 秒额外写一次数据，保持流活跃
      if (tick % 2 == 0) {
        try {
          String keepAlive = "[HOLDER-KEEPALIVE-" + tick + "]\n";
          appendOut.write(keepAlive.getBytes(StandardCharsets.UTF_8));
          appendOut.hflush();
          System.out.println("[Holder] Keepalive data written.");
        } catch (Exception e) {
          System.out.println("[Holder] WARNING: Keepalive write failed: " + e.getMessage());
        }
      }

      Thread.sleep(5000);
    }

    // 步骤 5：关闭流
    System.out.println("[Holder] Hold time expired. Closing append stream...");
    try {
      appendOut.close();
      System.out.println("[Holder] Stream closed successfully.");
    } catch (Exception e) {
      System.out.println("[Holder] Stream close failed: " + e.getClass().getSimpleName() + " - " + e.getMessage());
    }

    // 读取最终文件内容
    System.out.println();
    System.out.println("[Holder] Final file content:");
    printFileContent(fs, targetFile);
  }

  /**
   * 角色: Challenger（挑战者）
   * 1. 等待 signal_ready 信号文件出现
   * 2. 尝试 append 目标文件
   * 3. 报告结果
   */
  private static void runChallenger(FileSystem fs, Path basePath, int waitTimeoutSeconds, String hostInfo)
      throws Exception {
    Path targetFile = new Path(basePath, TARGET_FILE);
    Path signalReady = new Path(basePath, SIGNAL_READY);
    Path signalChallengeDone = new Path(basePath, SIGNAL_CHALLENGE_DONE);

    // 步骤 1：等待 Holder 的 READY 信号
    System.out.println("[Challenger] Waiting for READY signal from Holder...");
    long waitStart = System.currentTimeMillis();
    long waitEnd = waitStart + waitTimeoutSeconds * 1000L;
    while (!fs.exists(signalReady)) {
      if (System.currentTimeMillis() > waitEnd) {
        System.out.println("[Challenger] TIMEOUT: Holder did not signal READY within " + waitTimeoutSeconds + "s");
        System.exit(1);
      }
      System.out.println("[Challenger] Waiting... (signal not found yet)");
      Thread.sleep(3000);
    }
    System.out.println("[Challenger] READY signal detected! Holder's append stream should be open.");

    // 步骤 2：尝试 append（这是核心测试）
    System.out.println();
    System.out.println("[Challenger] Attempting to append to: " + targetFile);
    System.out.println(repeatChar('-', 60));

    boolean appendSucceeded = false;
    try {
      FSDataOutputStream appendOut = fs.append(targetFile);
      String challengerData = "[CHALLENGER-DATA][" + hostInfo + "][" + System.currentTimeMillis() + "]\n";
      appendOut.write(challengerData.getBytes(StandardCharsets.UTF_8));
      appendOut.hflush();
      appendOut.close();
      appendSucceeded = true;

      System.out.println("[Challenger] APPEND SUCCEEDED!");
      System.out.println("[Challenger] Written: " + challengerData.trim());
    } catch (Exception e) {
      System.out.println("[Challenger] APPEND FAILED!");
      System.out.println("[Challenger] Exception: " + e.getClass().getName());
      System.out.println("[Challenger] Message : " + e.getMessage());
    }

    // 步骤 3：创建完成信号
    try {
      FSDataOutputStream signalOut = fs.create(signalChallengeDone, true);
      signalOut.write(("done:" + appendSucceeded + ":" + hostInfo).getBytes(StandardCharsets.UTF_8));
      signalOut.close();
    } catch (Exception e) {
      // ignore
    }

    // 步骤 4：结论
    System.out.println();
    System.out.println(SEPARATOR);
    if (appendSucceeded) {
      System.out.println("  >>> CONCLUSION: ⚠ CROSS-PROCESS APPEND SUCCEEDED!");
      System.out.println("  >>> OBS does NOT have cross-process lease protection.");
      System.out.println("  >>> Two different processes CAN append to the same file simultaneously.");
      System.out.println("  >>> This confirms the ROLLBACK_WRITE_TOKEN isolation approach is NECESSARY.");
    } else {
      System.out.println("  >>> CONCLUSION: ✓ CROSS-PROCESS APPEND BLOCKED!");
      System.out.println("  >>> OBS has cross-process lease protection (like HDFS).");
      System.out.println("  >>> Two different processes CANNOT append to the same file simultaneously.");
      System.out.println("  >>> Storage-level protection exists for the concurrent rollback scenario.");
    }
    System.out.println(SEPARATOR);

    // 读取最终文件内容
    System.out.println();
    System.out.println("[Challenger] Current file content:");
    printFileContent(fs, targetFile);
  }

  /**
   * 清理测试目录
   */
  private static void runCleanup(FileSystem fs, Path basePath) throws Exception {
    System.out.println("Cleaning up: " + basePath);
    if (fs.exists(basePath)) {
      fs.delete(basePath, true);
      System.out.println("Deleted.");
    } else {
      System.out.println("Directory does not exist.");
    }
  }

  /**
   * 读取并打印目标文件内容
   */
  private static void runRead(FileSystem fs, Path basePath) throws Exception {
    Path targetFile = new Path(basePath, TARGET_FILE);
    System.out.println("Reading: " + targetFile);
    printFileContent(fs, targetFile);
  }

  // ============================================================
  // Helper methods
  // ============================================================

  private static void printFileContent(FileSystem fs, Path file) throws IOException {
    if (!fs.exists(file)) {
      System.out.println("  (file does not exist)");
      return;
    }
    long size = fs.getFileStatus(file).getLen();
    String content = new String(readAllBytes(fs.open(file)), StandardCharsets.UTF_8);
    System.out.println("  File size: " + size + " bytes");
    System.out.println("  Content:");
    String[] lines = content.split("\n");
    for (int i = 0; i < lines.length; i++) {
      System.out.println("    [" + (i + 1) + "] " + lines[i]);
    }
  }

  private static void deleteQuietly(FileSystem fs, Path path) {
    try {
      fs.delete(path, false);
    } catch (Exception e) {
      // ignore
    }
  }

  private static String getProcessId() {
    try {
      String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
      return name.split("@")[0];
    } catch (Exception e) {
      return "unknown";
    }
  }

  private static String repeatChar(char c, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, c);
    return new String(chars);
  }

  private static byte[] readAllBytes(InputStream in) throws IOException {
    try {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] tmp = new byte[4096];
      int bytesRead;
      while ((bytesRead = in.read(tmp)) != -1) {
        buffer.write(tmp, 0, bytesRead);
      }
      return buffer.toByteArray();
    } finally {
      in.close();
    }
  }

  private static void printUsage() {
    System.err.println("Usage: OBSCrossProcessLeaseTest <test-dir> <role> [options]");
    System.err.println();
    System.err.println("Roles:");
    System.err.println("  hold [seconds]       - Create file, open append stream, hold for N seconds (default: 60)");
    System.err.println("  challenge [timeout]  - Wait for holder's signal, then try to append (timeout default: 120s)");
    System.err.println("  cleanup              - Delete test directory");
    System.err.println("  read                 - Read and print target file content");
    System.err.println();
    System.err.println("Steps:");
    System.err.println("  1. Terminal A: spark-submit ... <jar> obs://bucket/test-dir hold 60");
    System.err.println("  2. Wait for 'READY' message in Terminal A");
    System.err.println("  3. Terminal B: spark-submit ... <jar> obs://bucket/test-dir challenge");
    System.err.println("  4. Check conclusions in both terminals");
    System.err.println("  5. (Optional) spark-submit ... <jar> obs://bucket/test-dir cleanup");
  }
}
