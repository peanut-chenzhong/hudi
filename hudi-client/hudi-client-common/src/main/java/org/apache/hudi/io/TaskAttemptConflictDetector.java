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

package org.apache.hudi.io;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects conflicts between different task attempts writing to the same file.
 * 
 * <p>In Spark/Flink, when a task is considered "dead" due to timeout but is actually
 * still writing (a "falsely dead" task), the scheduler may start a retry task.
 * This can lead to two tasks writing to the same log file concurrently, causing
 * data corruption.</p>
 * 
 * <p>This class leverages the existing writeToken mechanism in Hudi file naming.
 * Different task attempts have different writeTokens (which include attemptNumber),
 * so their marker files and data files have unique names. By detecting marker files
 * from other attempts, we can trigger a rollover to a new log file, ensuring each
 * task writes to its own physical file.</p>
 * 
 * <p>Key insight: Since writeToken guarantees file name uniqueness across attempts,
 * we don't need heartbeat mechanisms - just force rollover when conflict is detected,
 * and let the commit phase handle deduplication.</p>
 */
public class TaskAttemptConflictDetector {

  private static final Logger LOG = LoggerFactory.getLogger(TaskAttemptConflictDetector.class);

  /**
   * Pattern to extract writeToken from marker file name.
   * Marker file format: {fileId}_{writeToken}_{commitTime}.{ext}.marker.{IOType}
   * Example: file-abc123_0-5-0_20240115120000000.log.1_0-5-0.marker.APPEND
   */
  private static final Pattern MARKER_FILE_PATTERN = Pattern.compile(
      "^(.+?)_(\\d+-\\d+-\\d+)_\\d+.*\\.marker\\." + IOType.APPEND.name() + "$");

  /**
   * Pattern to extract writeToken from log file name.
   * Log file format: .{fileId}_{baseCommitTime}.log.{version}_{writeToken}
   * Example: .file-abc123_20240115100000000.log.1_0-5-0
   */
  private static final Pattern LOG_FILE_PATTERN = Pattern.compile(
      "^\\.(.+?)_\\d+\\.log\\.(\\d+)_(\\d+-\\d+-\\d+)$");

  private final HoodieStorage storage;
  private final String basePath;
  private final String instantTime;

  public TaskAttemptConflictDetector(HoodieStorage storage, String basePath, String instantTime) {
    this.storage = storage;
    this.basePath = basePath;
    this.instantTime = instantTime;
  }

  /**
   * Checks if there are marker files from other task attempts for the same file.
   * 
   * @param partitionPath    the partition path
   * @param fileId           the file ID
   * @param currentWriteToken the current task's write token
   * @return true if other attempt markers exist, false otherwise
   */
  public boolean hasOtherAttemptMarkers(String partitionPath, String fileId, String currentWriteToken) {
    int currentTaskPartitionId = FSUtils.getTaskPartitionIdFromWriteToken(currentWriteToken);
    int currentStageId = FSUtils.getStageIdFromWriteToken(currentWriteToken);
    int currentAttemptNumber = FSUtils.getAttemptNumberFromWriteToken(currentWriteToken);

    if (currentAttemptNumber < 0) {
      LOG.warn("Failed to parse current writeToken: {}, skipping conflict detection", currentWriteToken);
      return false;
    }

    StoragePath markerPartitionDir = new StoragePath(
        basePath + "/" + HoodieTableMetaClient.TEMPFOLDER_NAME + "/" + instantTime + "/" + partitionPath);

    try {
      if (!storage.exists(markerPartitionDir)) {
        return false;
      }

      List<StoragePathInfo> markerFiles = storage.listDirectEntries(markerPartitionDir);
      for (StoragePathInfo pathInfo : markerFiles) {
        if (!pathInfo.isFile()) {
          continue;
        }

        String markerFileName = pathInfo.getPath().getName();
        // Only check APPEND markers for the same fileId
        if (!markerFileName.contains(fileId) || !markerFileName.endsWith(IOType.APPEND.name())) {
          continue;
        }

        // Extract writeToken from marker file name
        String otherWriteToken = extractWriteTokenFromMarkerFileName(markerFileName);
        if (otherWriteToken == null) {
          continue;
        }

        int otherTaskPartitionId = FSUtils.getTaskPartitionIdFromWriteToken(otherWriteToken);
        int otherStageId = FSUtils.getStageIdFromWriteToken(otherWriteToken);
        int otherAttemptNumber = FSUtils.getAttemptNumberFromWriteToken(otherWriteToken);

        // Check if it's the same task (same taskPartitionId and stageId) but different attempt
        if (otherTaskPartitionId == currentTaskPartitionId
            && otherStageId == currentStageId
            && otherAttemptNumber != currentAttemptNumber) {
          LOG.warn("Detected marker from different task attempt for fileId: {}, "
                  + "current attempt: {}, other attempt: {}, current writeToken: {}, other writeToken: {}",
              fileId, currentAttemptNumber, otherAttemptNumber, currentWriteToken, otherWriteToken);
          return true;
        }
      }
    } catch (IOException e) {
      LOG.warn("Error checking for other attempt markers in partition {}: {}", partitionPath, e.getMessage());
    }

    return false;
  }

  /**
   * Extracts the writeToken from a marker file name.
   * 
   * <p>Marker file formats:
   * <ul>
   *   <li>Base file: {fileId}_{writeToken}_{commitTime}.parquet.marker.CREATE</li>
   *   <li>Log file: {fileId}_{writeToken}_{commitTime}.log.{version}_{logWriteToken}.marker.APPEND</li>
   * </ul>
   * 
   * @param markerFileName the marker file name
   * @return the writeToken, or null if extraction fails
   */
  public static String extractWriteTokenFromMarkerFileName(String markerFileName) {
    if (markerFileName == null || markerFileName.isEmpty()) {
      return null;
    }

    // For log file markers, the writeToken is embedded in the log file name part
    // Example: .fileId_commitTime.log.1_0-5-0.marker.APPEND
    // The log writeToken is after the version number
    
    // First, try to extract from log file marker format
    // Log marker: {fileId}_{writeToken}_{commitTime}.log.{version}_{logWriteToken}.marker.APPEND
    int logMarkerIdx = markerFileName.indexOf(".log.");
    if (logMarkerIdx > 0 && markerFileName.endsWith(".marker." + IOType.APPEND.name())) {
      // Extract the portion between .log. and .marker.
      int markerExtIdx = markerFileName.indexOf(".marker.");
      if (markerExtIdx > logMarkerIdx) {
        String logPart = markerFileName.substring(logMarkerIdx + 5, markerExtIdx); // after ".log."
        // logPart format: {version}_{writeToken}
        int underscoreIdx = logPart.indexOf('_');
        if (underscoreIdx > 0 && underscoreIdx < logPart.length() - 1) {
          return logPart.substring(underscoreIdx + 1);
        }
      }
    }

    // For base file markers: {fileId}_{writeToken}_{commitTime}.parquet.marker.CREATE
    // Extract writeToken which is between first underscore and second underscore
    String[] parts = markerFileName.split("_");
    if (parts.length >= 3) {
      // writeToken is typically the second part
      return parts[1];
    }

    return null;
  }

  /**
   * Determines if the current task should force a rollover based on conflict detection.
   * 
   * @param partitionPath    the partition path
   * @param fileId           the file ID
   * @param currentWriteToken the current task's write token
   * @return true if rollover should be forced, false otherwise
   */
  public boolean shouldForceRollover(String partitionPath, String fileId, String currentWriteToken) {
    if (hasOtherAttemptMarkers(partitionPath, fileId, currentWriteToken)) {
      LOG.info("Task attempt conflict detected for fileId: {}, forcing rollover to new log file. "
              + "Current writeToken: {}", fileId, currentWriteToken);
      return true;
    }
    return false;
  }

  /**
   * Extracts the writeToken from a log file path.
   * 
   * @param logFilePath the log file path
   * @return the writeToken, or null if extraction fails
   */
  public static String extractWriteTokenFromLogFile(StoragePath logFilePath) {
    return FSUtils.getWriteTokenFromLogPath(logFilePath);
  }
}
