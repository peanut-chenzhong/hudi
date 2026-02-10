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

package org.apache.hudi.common.util;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MarkerUtils#hasExpiredHeartbeatPartitionConflict}.
 */
class TestMarkerUtilsExpiredHeartbeat extends HoodieCommonTestHarness {

  private HoodieStorage storage;

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    storage = HoodieStorageUtils.getStorage(basePath, HoodieTestUtils.getDefaultStorageConfWithDefaults());
    // Create the temp folder and heartbeat folder
    storage.createDirectory(new StoragePath(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME));
    storage.createDirectory(new StoragePath(
        HoodieTableMetaClient.getHeartbeatFolderPath(basePath)));
  }

  /**
   * Helper to create a marker directory for a given instant and partition.
   */
  private void createMarkerDir(String instantTime, String partitionPath) throws IOException {
    StoragePath markerDir = new StoragePath(
        basePath + StoragePath.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME
            + StoragePath.SEPARATOR + instantTime);
    storage.createDirectory(markerDir);

    if (!StringUtils.isNullOrEmpty(partitionPath)) {
      StoragePath markerPartitionDir = new StoragePath(markerDir, partitionPath);
      storage.createDirectory(markerPartitionDir);
      // Create a dummy marker file to make it realistic
      StoragePath dummyMarker = new StoragePath(markerPartitionDir, "dummy.marker.APPEND");
      try (OutputStream os = storage.create(dummyMarker, false)) {
        os.write(0);
      }
    }
  }

  /**
   * Helper to create a heartbeat file for a given instant.
   * The heartbeat file's modification time determines whether it's expired.
   */
  private void createHeartbeat(String instantTime) throws IOException {
    StoragePath heartbeatPath = new StoragePath(
        HoodieTableMetaClient.getHeartbeatFolderPath(basePath), instantTime);
    try (OutputStream os = storage.create(heartbeatPath, true)) {
      os.write(0);
    }
  }

  @Test
  public void testNoTempFolder() {
    // Remove the temp folder to simulate a fresh table
    String nonExistentBase = basePath + "/non_existent";
    assertFalse(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, nonExistentBase, "002", 60000L, "2023/01/01"));
  }

  @Test
  public void testNoConflict_NoOtherInstants() {
    // No other instant marker dirs exist
    assertFalse(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", 60000L, "2023/01/01"));
  }

  @Test
  public void testNoConflict_ActiveHeartbeat() throws IOException {
    // Another instant with active heartbeat in the same partition
    createMarkerDir("001", "2023/01/01");
    createHeartbeat("001"); // Heartbeat is fresh (just created)

    // Use a very large timeout so heartbeat is NOT expired
    assertFalse(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", Long.MAX_VALUE, "2023/01/01"));
  }

  @Test
  public void testNoConflict_ExpiredHeartbeatDifferentPartition() throws IOException {
    // Another instant with expired heartbeat but in a DIFFERENT partition
    createMarkerDir("001", "2023/01/02");
    // No heartbeat file → getLastHeartbeatTime returns 0 → always expired

    assertTrue(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", 1L, "2023/01/02"),
        "Should detect conflict in same partition");

    // Check different partition - should not conflict
    assertFalse(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", 1L, "2023/01/03"),
        "Should not detect conflict in different partition");
  }

  @Test
  public void testConflict_ExpiredHeartbeatSamePartition() throws IOException {
    // Another instant with expired heartbeat in the SAME partition
    createMarkerDir("001", "2023/01/01");
    // No heartbeat file created → heartbeat time is 0 → expired with any timeout

    assertTrue(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", 1L, "2023/01/01"));
  }

  @Test
  public void testNoConflict_InstantAfterCurrent() throws IOException {
    // Instant AFTER current should be skipped
    createMarkerDir("003", "2023/01/01");
    // No heartbeat → expired

    assertFalse(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", 1L, "2023/01/01"),
        "Should skip instants after current instant time");
  }

  @Test
  public void testNoConflict_SameInstant() throws IOException {
    // Same instant should be skipped
    createMarkerDir("002", "2023/01/01");

    assertFalse(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", 1L, "2023/01/01"),
        "Should skip current writer's own instant");
  }

  @Test
  public void testConflict_MultipleInstants() throws IOException {
    // Multiple instants:
    // - 001: active heartbeat, same partition (should be skipped)
    // - 0001: expired heartbeat, different partition (should be skipped)
    // - 00001: expired heartbeat, same partition (should trigger conflict)
    createMarkerDir("001", "2023/01/01");
    createHeartbeat("001");

    createMarkerDir("0001", "2023/01/02");
    // No heartbeat for 0001 → expired

    createMarkerDir("00001", "2023/01/01");
    // No heartbeat for 00001 → expired

    // Active heartbeat with large timeout → only check expired
    assertTrue(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", Long.MAX_VALUE, "2023/01/01"),
        "Should detect conflict from expired heartbeat instant 00001 in same partition");
  }

  @Test
  public void testNoConflict_EmptyPartitionPath() throws IOException {
    // Test with empty partition path (non-partitioned table)
    createMarkerDir("001", "");
    // No heartbeat → expired

    // For empty partition path, we check the instant marker dir itself
    assertTrue(MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", 1L, ""));
  }

  // ====================================================================
  // Tests for hasExpiredHeartbeatPartitionConflictFromPaths
  // (the overload that accepts pre-listed instant paths)
  // ====================================================================

  /**
   * Helper to get pre-listed instant paths from .temp directory.
   */
  private List<StoragePath> listInstantPaths() throws IOException {
    StoragePath tempPath = new StoragePath(basePath, HoodieTableMetaClient.TEMPFOLDER_NAME);
    if (!storage.exists(tempPath)) {
      return Collections.emptyList();
    }
    return storage.listDirectEntries(tempPath).stream()
        .map(StoragePathInfo::getPath)
        .collect(Collectors.toList());
  }

  @Test
  public void testFromPaths_EmptyList() {
    // Empty pre-listed paths → no conflict
    assertFalse(MarkerUtils.hasExpiredHeartbeatPartitionConflictFromPaths(
        storage, Collections.emptyList(), basePath, "002", 60000L, "2023/01/01"));
  }

  @Test
  public void testFromPaths_ConflictDetected() throws IOException {
    // Setup: expired heartbeat instant in same partition
    createMarkerDir("001", "2023/01/01");
    // No heartbeat → expired

    List<StoragePath> paths = listInstantPaths();
    assertTrue(MarkerUtils.hasExpiredHeartbeatPartitionConflictFromPaths(
        storage, paths, basePath, "002", 1L, "2023/01/01"),
        "FromPaths should detect conflict same as the original method");
  }

  @Test
  public void testFromPaths_NoConflict_ActiveHeartbeat() throws IOException {
    createMarkerDir("001", "2023/01/01");
    createHeartbeat("001"); // Fresh heartbeat

    List<StoragePath> paths = listInstantPaths();
    assertFalse(MarkerUtils.hasExpiredHeartbeatPartitionConflictFromPaths(
        storage, paths, basePath, "002", Long.MAX_VALUE, "2023/01/01"),
        "FromPaths should skip active heartbeat instants");
  }

  @Test
  public void testFromPaths_ConsistentWithOriginal() throws IOException {
    // Setup complex scenario: multiple instants with mixed states
    createMarkerDir("001", "2023/01/01");
    createHeartbeat("001");
    createMarkerDir("0001", "2023/01/02");
    createMarkerDir("00001", "2023/01/01");

    List<StoragePath> paths = listInstantPaths();

    // Both methods should return the same result
    boolean resultOriginal = MarkerUtils.hasExpiredHeartbeatPartitionConflict(
        storage, basePath, "002", Long.MAX_VALUE, "2023/01/01");
    boolean resultFromPaths = MarkerUtils.hasExpiredHeartbeatPartitionConflictFromPaths(
        storage, paths, basePath, "002", Long.MAX_VALUE, "2023/01/01");

    assertEquals(resultOriginal, resultFromPaths,
        "Both methods should produce consistent results");
    assertTrue(resultOriginal, "Should detect conflict from expired instant 00001");
  }
}
