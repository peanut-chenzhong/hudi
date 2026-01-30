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

package org.apache.hudi.repair;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Tool to cleanup orphan replaced files from archived replacecommit.
 *
 * <p>This tool scans the archived timeline for replacecommit actions, identifies
 * file groups that were replaced (e.g., by clustering or insert_overwrite),
 * checks if the replaced data files still exist on storage, and moves them
 * to a .delete directory within the same partition for safe cleanup.</p>
 *
 * <h2>Usage:</h2>
 * <pre>
 * spark-submit \
 *   --class org.apache.hudi.repair.CleanupOrphanReplacedFilesTool \
 *   --master yarn \
 *   hudi-repair-tool-1.0.0.jar \
 *   --base-paths hdfs:///path/to/table1,hdfs:///path/to/table2 \
 *   --dry-run false
 *
 * # Or with a file containing table paths:
 * spark-submit \
 *   --class org.apache.hudi.repair.CleanupOrphanReplacedFilesTool \
 *   --master yarn \
 *   hudi-repair-tool-1.0.0.jar \
 *   --path-file hdfs:///config/tables_to_repair.txt \
 *   --dry-run false \
 *   --parallelism 10
 * </pre>
 */
public class CleanupOrphanReplacedFilesTool implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CleanupOrphanReplacedFilesTool.class);

  private final CleanupOrphanReplacedFilesConfig config;
  private transient SparkSession spark;
  private transient JavaSparkContext jsc;

  public CleanupOrphanReplacedFilesTool(CleanupOrphanReplacedFilesConfig config) {
    this.config = config;
  }

  /**
   * Main entry point.
   */
  public static void main(String[] args) {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    JCommander commander = JCommander.newBuilder()
        .addObject(config)
        .programName("CleanupOrphanReplacedFilesTool")
        .build();

    try {
      commander.parse(args);

      if (config.help) {
        commander.usage();
        System.exit(0);
      }

      config.validate();

      LOG.info("Starting CleanupOrphanReplacedFilesTool");
      LOG.info("Config: {}", config);

      CleanupOrphanReplacedFilesTool tool = new CleanupOrphanReplacedFilesTool(config);
      int result = tool.run();
      System.exit(result);

    } catch (Exception e) {
      LOG.error("Error running CleanupOrphanReplacedFilesTool", e);
      commander.usage();
      System.exit(1);
    }
  }

  /**
   * Run the cleanup tool.
   *
   * @return 0 on success, non-zero on failure
   */
  public int run() {
    LOG.info("Starting CleanupOrphanReplacedFilesTool with config: {}", config);

    initSparkSession();

    try {
      // Get list of table paths to process
      List<String> tablePaths = getTablePaths();
      LOG.info("Found {} tables to process", tablePaths.size());

      if (tablePaths.isEmpty()) {
        LOG.warn("No tables to process");
        return 0;
      }

      // Process tables in parallel using Spark
      JavaRDD<String> pathsRDD = jsc.parallelize(tablePaths, config.parallelism);

      // Broadcast config
      final CleanupOrphanReplacedFilesConfig broadcastConfig = config;

      // Process each table and collect results
      JavaRDD<TableRepairResult> resultsRDD = pathsRDD.map(basePath -> {
        return processTable(basePath, broadcastConfig);
      });

      // Collect all results
      List<TableRepairResult> allResults = resultsRDD.collect();

      // Print summary
      printSummary(allResults);

      // Optionally write results to output path
      if (config.outputPath != null && !config.outputPath.isEmpty()) {
        writeResults(allResults);
      }

      return 0;

    } catch (Exception e) {
      LOG.error("Error during cleanup", e);
      return 1;
    } finally {
      if (spark != null) {
        spark.stop();
      }
    }
  }

  /**
   * Initialize Spark session.
   */
  private void initSparkSession() {
    SparkConf sparkConf = new SparkConf()
        .setAppName("CleanupOrphanReplacedFilesTool")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

    if (config.sparkMaster != null && !config.sparkMaster.isEmpty()) {
      sparkConf.setMaster(config.sparkMaster);
    }

    this.spark = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate();
    this.jsc = new JavaSparkContext(spark.sparkContext());

    LOG.info("Spark session initialized with master: {}", spark.sparkContext().master());
  }

  /**
   * Get list of table paths to process.
   */
  private List<String> getTablePaths() throws IOException {
    List<String> paths = new ArrayList<>();

    // Add paths from command line
    paths.addAll(config.getBasePathList());

    // Add paths from file
    if (config.pathFile != null && !config.pathFile.isEmpty()) {
      paths.addAll(readPathsFromFile(config.pathFile));
    }

    // Remove duplicates
    return paths.stream().distinct().collect(Collectors.toList());
  }

  /**
   * Read table paths from a file (one path per line).
   */
  private List<String> readPathsFromFile(String filePath) throws IOException {
    List<String> paths = new ArrayList<>();
    Configuration hadoopConf = jsc.hadoopConfiguration();
    FileSystem fs = new Path(filePath).getFileSystem(hadoopConf);

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(fs.open(new Path(filePath)), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String trimmed = line.trim();
        // Skip empty lines and comments
        if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
          paths.add(trimmed);
        }
      }
    }

    LOG.info("Read {} table paths from file: {}", paths.size(), filePath);
    return paths;
  }

  /**
   * Process a single table.
   */
  private static TableRepairResult processTable(String basePath, CleanupOrphanReplacedFilesConfig config) {
    TableRepairResult result = new TableRepairResult(basePath);

    LOG.info("Processing table: {}", basePath);

    try {
      Configuration hadoopConf = new Configuration();
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setConf(hadoopConf)
          .setBasePath(basePath)
          .build();

      FileSystem fs = new Path(basePath).getFileSystem(hadoopConf);

      // Load archived timeline
      HoodieArchivedTimeline archivedTimeline;
      if (config.startInstantTime != null && !config.startInstantTime.isEmpty()) {
        archivedTimeline = new HoodieArchivedTimeline(metaClient, config.startInstantTime);
      } else {
        archivedTimeline = new HoodieArchivedTimeline(metaClient);
      }

      // Also get active timeline
      HoodieTimeline activeTimeline = metaClient.getActiveTimeline();

      // Get all replacecommit instants from archived timeline
      List<HoodieInstant> archivedReplaceCommits = archivedTimeline.getInstants()
          .filter(instant -> HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction()))
          .filter(instant -> instant.isCompleted())
          .filter(instant -> isInTimeRange(instant.getTimestamp(), config.startInstantTime, config.endInstantTime))
          .collect(Collectors.toList());

      // Get all replacecommit instants from active timeline
      List<HoodieInstant> activeReplaceCommits = activeTimeline.getInstants()
          .filter(instant -> HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction()))
          .filter(instant -> instant.isCompleted())
          .filter(instant -> isInTimeRange(instant.getTimestamp(), config.startInstantTime, config.endInstantTime))
          .collect(Collectors.toList());

      LOG.info("Table {} - Found {} replacecommit instants ({} archived, {} active)",
          basePath, archivedReplaceCommits.size() + activeReplaceCommits.size(),
          archivedReplaceCommits.size(), activeReplaceCommits.size());

      // Process archived replacecommits
      for (HoodieInstant instant : archivedReplaceCommits) {
        processReplaceCommit(basePath, instant, archivedTimeline, fs, config, result);
      }

      // Process active replacecommits
      for (HoodieInstant instant : activeReplaceCommits) {
        processReplaceCommit(basePath, instant, activeTimeline, fs, config, result);
      }

      result.setSuccess(true);

    } catch (Exception e) {
      LOG.error("Error processing table: {}", basePath, e);
      result.setSuccess(false);
      result.setErrorMessage(e.getMessage());
    }

    return result;
  }

  /**
   * Check if instant time is in the specified range.
   */
  private static boolean isInTimeRange(String instantTime, String startTime, String endTime) {
    if (startTime != null && !startTime.isEmpty() && instantTime.compareTo(startTime) < 0) {
      return false;
    }
    if (endTime != null && !endTime.isEmpty() && instantTime.compareTo(endTime) > 0) {
      return false;
    }
    return true;
  }

  /**
   * Process a single replacecommit instant.
   */
  private static void processReplaceCommit(String basePath, HoodieInstant instant,
      HoodieTimeline timeline, FileSystem fs, CleanupOrphanReplacedFilesConfig config,
      TableRepairResult result) {

    String instantTime = instant.getTimestamp();

    try {
      byte[] commitMetadataBytes = timeline.getInstantDetails(instant).get();
      HoodieReplaceCommitMetadata replaceMetadata = HoodieReplaceCommitMetadata.fromBytes(
          commitMetadataBytes, HoodieReplaceCommitMetadata.class);

      Map<String, List<String>> partitionToReplaceFileIds = replaceMetadata.getPartitionToReplaceFileIds();

      if (partitionToReplaceFileIds == null || partitionToReplaceFileIds.isEmpty()) {
        return;
      }

      for (Map.Entry<String, List<String>> entry : partitionToReplaceFileIds.entrySet()) {
        String partitionPath = entry.getKey();
        List<String> fileIds = entry.getValue();

        Path partitionDir = new Path(basePath, partitionPath);

        for (String fileId : fileIds) {
          List<FileStatus> orphanFiles = findOrphanFilesForFileId(fs, partitionDir, fileId);

          for (FileStatus fileStatus : orphanFiles) {
            Path filePath = fileStatus.getPath();
            long fileSize = fileStatus.getLen();

            OrphanFileInfo fileInfo = new OrphanFileInfo();
            fileInfo.setInstantTime(instantTime);
            fileInfo.setPartitionPath(partitionPath);
            fileInfo.setFileId(fileId);
            fileInfo.setFilePath(filePath.toString());
            fileInfo.setFileSize(fileSize);

            if (config.dryRun) {
              fileInfo.setAction("WOULD_MOVE");
              fileInfo.setStatus("DRY_RUN");
              result.addOrphanFile(fileInfo);
            } else {
              // Actually move the file
              Path deleteDir = new Path(partitionDir, config.deleteDirName);
              Path targetPath = new Path(deleteDir, filePath.getName());

              try {
                // Create delete directory if not exists
                if (!fs.exists(deleteDir)) {
                  fs.mkdirs(deleteDir);
                }

                // Move file
                boolean success = fs.rename(filePath, targetPath);

                if (success) {
                  fileInfo.setAction("MOVED");
                  fileInfo.setStatus("Moved to " + targetPath);
                } else {
                  fileInfo.setAction("FAILED");
                  fileInfo.setStatus("Failed to move file");
                }

              } catch (Exception e) {
                fileInfo.setAction("ERROR");
                fileInfo.setStatus(e.getMessage());
              }

              result.addOrphanFile(fileInfo);
            }
          }
        }
      }

    } catch (Exception e) {
      LOG.warn("Failed to parse replacecommit metadata for instant {} in table {}: {}",
          instantTime, basePath, e.getMessage());
    }
  }

  /**
   * Find all base files and log files for a given fileId in the partition directory.
   */
  private static List<FileStatus> findOrphanFilesForFileId(FileSystem fs, Path partitionDir, String fileId) {
    List<FileStatus> orphanFiles = new ArrayList<>();

    try {
      if (fs.exists(partitionDir)) {
        FileStatus[] files = fs.listStatus(partitionDir);

        for (FileStatus file : files) {
          String fileName = file.getPath().getName();

          // Skip hidden files and directories
          if (!fileName.startsWith(".") && !file.isDirectory()) {
            if (isFileForFileId(fileName, fileId)) {
              orphanFiles.add(file);
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to list files in partition {}: {}", partitionDir, e.getMessage());
    }

    return orphanFiles;
  }

  /**
   * Check if a file belongs to the given fileId.
   */
  private static boolean isFileForFileId(String fileName, String fileId) {
    // Base file pattern: fileId_writeToken_instantTime.parquet
    if (fileName.startsWith(fileId + "_") || fileName.startsWith(fileId + "-")) {
      return true;
    }

    // Log file pattern: .fileId_instantTime.log.version_writeToken
    if (fileName.startsWith("." + fileId + "_")) {
      return true;
    }

    return false;
  }

  /**
   * Print summary of results.
   */
  private void printSummary(List<TableRepairResult> results) {
    int totalTables = results.size();
    int successTables = (int) results.stream().filter(TableRepairResult::isSuccess).count();
    int failedTables = totalTables - successTables;
    int totalOrphanFiles = results.stream().mapToInt(r -> r.getOrphanFiles().size()).sum();
    long totalOrphanSize = results.stream()
        .flatMap(r -> r.getOrphanFiles().stream())
        .mapToLong(OrphanFileInfo::getFileSize)
        .sum();

    System.out.println();
    System.out.println("========================================");
    System.out.println("Cleanup Summary");
    System.out.println("========================================");
    System.out.println("Total tables processed: " + totalTables);
    System.out.println("Successful: " + successTables);
    System.out.println("Failed: " + failedTables);
    System.out.println("Total orphan files found: " + totalOrphanFiles);
    System.out.println("Total orphan size: " + totalOrphanSize + " bytes (" + (totalOrphanSize / (1024 * 1024)) + " MB)");
    System.out.println("Dry run mode: " + config.dryRun);
    System.out.println("========================================");

    // Print details for each table
    for (TableRepairResult result : results) {
      System.out.println("Table: " + result.getBasePath() + " - Success: " + result.isSuccess()
          + ", Orphan files: " + result.getOrphanFiles().size());

      if (!result.isSuccess()) {
        System.out.println("  Error: " + result.getErrorMessage());
      }

      // Print first 10 orphan files as sample
      int count = 0;
      for (OrphanFileInfo file : result.getOrphanFiles()) {
        if (count++ < 10) {
          System.out.println("  - " + file.getFilePath() + " (" + file.getFileSize() + " bytes) -> " + file.getAction());
        } else {
          System.out.println("  ... and " + (result.getOrphanFiles().size() - 10) + " more files");
          break;
        }
      }
    }
    System.out.println();
  }

  /**
   * Write results to output path.
   */
  private void writeResults(List<TableRepairResult> results) {
    try {
      List<String> lines = new ArrayList<>();
      lines.add("table_path,instant_time,partition_path,file_id,file_path,file_size,action,status");

      for (TableRepairResult tableResult : results) {
        for (OrphanFileInfo file : tableResult.getOrphanFiles()) {
          lines.add(String.format("%s,%s,%s,%s,%s,%d,%s,%s",
              tableResult.getBasePath(),
              file.getInstantTime(),
              file.getPartitionPath(),
              file.getFileId(),
              file.getFilePath(),
              file.getFileSize(),
              file.getAction(),
              file.getStatus().replace(",", ";")));
        }
      }

      JavaRDD<String> outputRDD = jsc.parallelize(lines, 1);
      outputRDD.saveAsTextFile(config.outputPath);

      LOG.info("Results written to: {}", config.outputPath);
      System.out.println("Results written to: " + config.outputPath);

    } catch (Exception e) {
      LOG.error("Failed to write results to {}: {}", config.outputPath, e.getMessage());
    }
  }

  /**
   * Result for a single table repair operation.
   */
  public static class TableRepairResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private String basePath;
    private boolean success;
    private String errorMessage;
    private List<OrphanFileInfo> orphanFiles = new ArrayList<>();

    public TableRepairResult(String basePath) {
      this.basePath = basePath;
    }

    public String getBasePath() {
      return basePath;
    }

    public boolean isSuccess() {
      return success;
    }

    public void setSuccess(boolean success) {
      this.success = success;
    }

    public String getErrorMessage() {
      return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    public List<OrphanFileInfo> getOrphanFiles() {
      return orphanFiles;
    }

    public void addOrphanFile(OrphanFileInfo file) {
      this.orphanFiles.add(file);
    }
  }

  /**
   * Information about an orphan file.
   */
  public static class OrphanFileInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private String instantTime;
    private String partitionPath;
    private String fileId;
    private String filePath;
    private long fileSize;
    private String action;
    private String status;

    public String getInstantTime() {
      return instantTime;
    }

    public void setInstantTime(String instantTime) {
      this.instantTime = instantTime;
    }

    public String getPartitionPath() {
      return partitionPath;
    }

    public void setPartitionPath(String partitionPath) {
      this.partitionPath = partitionPath;
    }

    public String getFileId() {
      return fileId;
    }

    public void setFileId(String fileId) {
      this.fileId = fileId;
    }

    public String getFilePath() {
      return filePath;
    }

    public void setFilePath(String filePath) {
      this.filePath = filePath;
    }

    public long getFileSize() {
      return fileSize;
    }

    public void setFileSize(long fileSize) {
      this.fileSize = fileSize;
    }

    public String getAction() {
      return action;
    }

    public void setAction(String action) {
      this.action = action;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }
}
