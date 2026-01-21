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

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TaskAttemptConflictDetector}.
 */
public class TestTaskAttemptConflictDetector extends HoodieCommonTestHarness {

  private HoodieStorage storage;

  @BeforeEach
  public void setup() throws IOException {
    initMetaClient();
    storage = metaClient.getStorage();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testExtractWriteTokenFromMarkerFileName() {
    // Test log file marker format: {fileId}_{writeToken}_{commitTime}.log.{version}_{logWriteToken}.marker.APPEND
    String logMarker1 = "file-abc123_0-5-0_20240115120000000.log.1_0-5-0.marker.APPEND";
    assertEquals("0-5-0", TaskAttemptConflictDetector.extractWriteTokenFromMarkerFileName(logMarker1));

    String logMarker2 = "file-abc123_0-5-1_20240115120000000.log.2_0-5-1.marker.APPEND";
    assertEquals("0-5-1", TaskAttemptConflictDetector.extractWriteTokenFromMarkerFileName(logMarker2));

    // Test base file marker format: {fileId}_{writeToken}_{commitTime}.parquet.marker.CREATE
    String baseMarker = "file-abc123_0-5-0_20240115120000000.parquet.marker.CREATE";
    assertEquals("0-5-0", TaskAttemptConflictDetector.extractWriteTokenFromMarkerFileName(baseMarker));

    // Test edge cases
    assertNull(TaskAttemptConflictDetector.extractWriteTokenFromMarkerFileName(null));
    assertNull(TaskAttemptConflictDetector.extractWriteTokenFromMarkerFileName(""));
  }

  @Test
  public void testHasOtherAttemptMarkers_NoConflict() throws IOException {
    String instantTime = "20240115120000000";
    String partitionPath = "year=2024/month=01";
    String fileId = "file-abc123";
    String currentWriteToken = "0-5-0";

    TaskAttemptConflictDetector detector = new TaskAttemptConflictDetector(
        storage, basePath, instantTime);

    // Create marker directory structure
    StoragePath markerDir = new StoragePath(basePath, ".hoodie/.temp/" + instantTime + "/" + partitionPath);
    storage.createDirectory(markerDir);

    // Create marker file for current task (same writeToken)
    String markerFileName = fileId + "_" + currentWriteToken + "_" + instantTime + ".log.1_" + currentWriteToken + ".marker.APPEND";
    storage.create(new StoragePath(markerDir, markerFileName), true).close();

    // Should not detect conflict (same task, same attempt)
    assertFalse(detector.hasOtherAttemptMarkers(partitionPath, fileId, currentWriteToken));
  }

  @Test
  public void testHasOtherAttemptMarkers_WithConflict() throws IOException {
    String instantTime = "20240115120000000";
    String partitionPath = "year=2024/month=01";
    String fileId = "file-abc123";
    String currentWriteToken = "0-5-1";  // retry task (attempt=1)
    String originalWriteToken = "0-5-0";  // original task (attempt=0)

    TaskAttemptConflictDetector detector = new TaskAttemptConflictDetector(
        storage, basePath, instantTime);

    // Create marker directory structure
    StoragePath markerDir = new StoragePath(basePath, ".hoodie/.temp/" + instantTime + "/" + partitionPath);
    storage.createDirectory(markerDir);

    // Create marker file for original task (attempt=0)
    String originalMarkerFileName = fileId + "_" + originalWriteToken + "_" + instantTime + ".log.1_" + originalWriteToken + ".marker.APPEND";
    storage.create(new StoragePath(markerDir, originalMarkerFileName), true).close();

    // Retry task (attempt=1) should detect conflict
    assertTrue(detector.hasOtherAttemptMarkers(partitionPath, fileId, currentWriteToken));
  }

  @Test
  public void testHasOtherAttemptMarkers_DifferentFileId() throws IOException {
    String instantTime = "20240115120000000";
    String partitionPath = "year=2024/month=01";
    String fileId = "file-abc123";
    String otherFileId = "file-xyz789";
    String currentWriteToken = "0-5-0";
    String otherWriteToken = "0-5-1";

    TaskAttemptConflictDetector detector = new TaskAttemptConflictDetector(
        storage, basePath, instantTime);

    // Create marker directory structure
    StoragePath markerDir = new StoragePath(basePath, ".hoodie/.temp/" + instantTime + "/" + partitionPath);
    storage.createDirectory(markerDir);

    // Create marker file for a different fileId
    String otherMarkerFileName = otherFileId + "_" + otherWriteToken + "_" + instantTime + ".log.1_" + otherWriteToken + ".marker.APPEND";
    storage.create(new StoragePath(markerDir, otherMarkerFileName), true).close();

    // Should not detect conflict (different fileId)
    assertFalse(detector.hasOtherAttemptMarkers(partitionPath, fileId, currentWriteToken));
  }

  @Test
  public void testHasOtherAttemptMarkers_DifferentTaskPartition() throws IOException {
    String instantTime = "20240115120000000";
    String partitionPath = "year=2024/month=01";
    String fileId = "file-abc123";
    String currentWriteToken = "0-5-0";  // taskPartitionId=0
    String otherWriteToken = "1-5-0";    // taskPartitionId=1 (different task, not a retry)

    TaskAttemptConflictDetector detector = new TaskAttemptConflictDetector(
        storage, basePath, instantTime);

    // Create marker directory structure
    StoragePath markerDir = new StoragePath(basePath, ".hoodie/.temp/" + instantTime + "/" + partitionPath);
    storage.createDirectory(markerDir);

    // Create marker file for a different taskPartitionId (different task, not a retry)
    String otherMarkerFileName = fileId + "_" + otherWriteToken + "_" + instantTime + ".log.1_" + otherWriteToken + ".marker.APPEND";
    storage.create(new StoragePath(markerDir, otherMarkerFileName), true).close();

    // Should not detect conflict (different task, not a retry)
    assertFalse(detector.hasOtherAttemptMarkers(partitionPath, fileId, currentWriteToken));
  }

  @Test
  public void testHasOtherAttemptMarkers_EmptyDirectory() throws IOException {
    String instantTime = "20240115120000000";
    String partitionPath = "year=2024/month=01";
    String fileId = "file-abc123";
    String currentWriteToken = "0-5-0";

    TaskAttemptConflictDetector detector = new TaskAttemptConflictDetector(
        storage, basePath, instantTime);

    // Don't create any marker files
    // Should not detect conflict (directory doesn't exist)
    assertFalse(detector.hasOtherAttemptMarkers(partitionPath, fileId, currentWriteToken));
  }

  @Test
  public void testShouldForceRollover() throws IOException {
    String instantTime = "20240115120000000";
    String partitionPath = "year=2024/month=01";
    String fileId = "file-abc123";
    String currentWriteToken = "0-5-1";  // retry task
    String originalWriteToken = "0-5-0";  // original task

    TaskAttemptConflictDetector detector = new TaskAttemptConflictDetector(
        storage, basePath, instantTime);

    // Create marker directory structure
    StoragePath markerDir = new StoragePath(basePath, ".hoodie/.temp/" + instantTime + "/" + partitionPath);
    storage.createDirectory(markerDir);

    // Create marker file for original task
    String originalMarkerFileName = fileId + "_" + originalWriteToken + "_" + instantTime + ".log.1_" + originalWriteToken + ".marker.APPEND";
    storage.create(new StoragePath(markerDir, originalMarkerFileName), true).close();

    // Retry task should force rollover
    assertTrue(detector.shouldForceRollover(partitionPath, fileId, currentWriteToken));

    // Original task should not force rollover (no conflict for it)
    assertFalse(detector.shouldForceRollover(partitionPath, fileId, originalWriteToken));
  }

  @ParameterizedTest
  @CsvSource({
      "0-5-0,0-5-1,true",   // same task, different attempt -> conflict
      "0-5-1,0-5-0,true",   // same task, different attempt -> conflict
      "0-5-0,0-5-0,false",  // same task, same attempt -> no conflict
      "0-5-0,1-5-0,false",  // different taskPartitionId -> no conflict
      "0-5-0,0-6-0,false",  // different stageId -> no conflict
  })
  public void testConflictDetectionScenarios(
      String existingWriteToken, String currentWriteToken, boolean expectedConflict) throws IOException {
    String instantTime = "20240115120000000";
    String partitionPath = "year=2024/month=01";
    String fileId = "file-abc123";

    TaskAttemptConflictDetector detector = new TaskAttemptConflictDetector(
        storage, basePath, instantTime);

    // Create marker directory structure
    StoragePath markerDir = new StoragePath(basePath, ".hoodie/.temp/" + instantTime + "/" + partitionPath);
    storage.createDirectory(markerDir);

    // Create marker file for existing task
    String existingMarkerFileName = fileId + "_" + existingWriteToken + "_" + instantTime + ".log.1_" + existingWriteToken + ".marker.APPEND";
    storage.create(new StoragePath(markerDir, existingMarkerFileName), true).close();

    assertEquals(expectedConflict, detector.hasOtherAttemptMarkers(partitionPath, fileId, currentWriteToken),
        "Conflict detection failed for existing=" + existingWriteToken + ", current=" + currentWriteToken);
  }
}
