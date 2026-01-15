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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PartitionBasedDirectMarkerDetectionStrategy}.
 */
public class TestPartitionBasedDirectMarkerDetectionStrategy extends HoodieCommonTestHarness {

  private HoodieStorage storage;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    storage = HoodieStorageUtils.getStorage(basePath, getDefaultStorageConf());
    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, HoodieTableType.COPY_ON_WRITE);
  }

  @AfterEach
  public void tearDown() throws Exception {
    Path path = new Path(basePath);
    FileSystem fs = path.getFileSystem(new Configuration());
    fs.delete(path, true);
  }

  /**
   * Test that no conflict is detected when markers are in different partitions.
   * 
   * Scenario:
   * - Instant 001 has markers in partition "2024/01"
   * - Instant 002 (current) wants to write to partition "2024/02"
   * - Expected: No conflict (different partitions)
   */
  @Test
  public void testNoConflictWhenMarkersInDifferentPartitions() throws IOException {
    String rootBaseMarkerDir = basePath + "/.hoodie/.temp";
    
    // Old instant 001 writes to partition 2024/01
    String oldInstant = "001";
    String oldPartition = "2024/01";
    Set<String> oldMarkers = Stream.of(
        oldPartition + "/file-a-001.parquet.marker.CREATE"
    ).collect(Collectors.toSet());
    prepareMarkerFiles(rootBaseMarkerDir, oldInstant, oldPartition, oldMarkers);
    createCommitFile(oldInstant);
    
    // Current instant 002 wants to write to partition 2024/02 (different partition)
    String currentInstant = "002";
    String currentPartition = "2024/02";
    String currentFileId = "file-b";
    
    HoodieWriteConfig config = createMockConfig();
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    
    PartitionBasedDirectMarkerDetectionStrategy strategy = new PartitionBasedDirectMarkerDetectionStrategy(
        storage, currentPartition, currentFileId, currentInstant, activeTimeline, config);
    
    // Should NOT detect conflict since partitions are different
    assertFalse(strategy.hasMarkerConflict(), 
        "Should NOT detect conflict when markers are in different partitions");
  }

  /**
   * Test that conflict is detected when markers are in the same partition.
   * 
   * Scenario:
   * - Instant 001 has markers in partition "2024/01"
   * - Instant 002 (current) wants to write to partition "2024/01"
   * - Expected: Conflict detected (same partition)
   */
  @Test
  public void testConflictWhenMarkersInSamePartition() throws IOException {
    String rootBaseMarkerDir = basePath + "/.hoodie/.temp";
    
    // Old instant 001 writes to partition 2024/01
    String oldInstant = "001";
    String partition = "2024/01";
    Set<String> oldMarkers = Stream.of(
        partition + "/file-a-001.parquet.marker.CREATE"
    ).collect(Collectors.toSet());
    prepareMarkerFiles(rootBaseMarkerDir, oldInstant, partition, oldMarkers);
    createHeartbeatFile(oldInstant);
    
    // Current instant 002 wants to write to the SAME partition
    String currentInstant = "002";
    String currentFileId = "file-b";  // Different file ID but same partition
    
    HoodieWriteConfig config = createMockConfig();
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    
    PartitionBasedDirectMarkerDetectionStrategy strategy = new PartitionBasedDirectMarkerDetectionStrategy(
        storage, partition, currentFileId, currentInstant, activeTimeline, config);
    
    // Should detect conflict since partitions are the same
    assertTrue(strategy.hasMarkerConflict(), 
        "Should detect conflict when markers are in the same partition");
  }

  /**
   * Test that resolveMarkerConflict throws exception.
   */
  @Test
  public void testResolveMarkerConflictThrowsException() throws IOException {
    String currentInstant = "002";
    String partition = "2024/01";
    String fileId = "file-a";
    
    HoodieWriteConfig config = createMockConfig();
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    
    PartitionBasedDirectMarkerDetectionStrategy strategy = new PartitionBasedDirectMarkerDetectionStrategy(
        storage, partition, fileId, currentInstant, activeTimeline, config);
    
    assertThrows(HoodieEarlyConflictDetectionException.class, () -> {
      strategy.resolveMarkerConflict(basePath, partition, "file.parquet");
    });
  }

  /**
   * Test scenario: Flink and Spark write to different partitions - no conflict.
   */
  @Test
  public void testFlinkAndSparkWriteToDifferentPartitions() throws IOException {
    String rootBaseMarkerDir = basePath + "/.hoodie/.temp";
    
    // Flink instant 001 writes to partition_2024_01
    String flinkInstant = "001";
    String flinkPartition = "partition_2024_01";
    Set<String> flinkMarkers = Stream.of(
        flinkPartition + "/flink-file-001.parquet.marker.CREATE"
    ).collect(Collectors.toSet());
    prepareMarkerFiles(rootBaseMarkerDir, flinkInstant, flinkPartition, flinkMarkers);
    createCommitFile(flinkInstant);
    
    // Spark instant 002 wants to write to partition_2024_02
    String sparkInstant = "002";
    String sparkPartition = "partition_2024_02";
    String sparkFileId = "spark-file";
    
    HoodieWriteConfig config = createMockConfig();
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    
    PartitionBasedDirectMarkerDetectionStrategy strategy = new PartitionBasedDirectMarkerDetectionStrategy(
        storage, sparkPartition, sparkFileId, sparkInstant, activeTimeline, config);
    
    // Should NOT detect conflict - Flink and Spark write to different partitions
    assertFalse(strategy.hasMarkerConflict(), 
        "Flink and Spark should NOT conflict when writing to different partitions");
  }

  /**
   * Test scenario: Multiple instants with markers in different partitions.
   */
  @Test
  public void testMultipleInstantsWithDifferentPartitions() throws IOException {
    String rootBaseMarkerDir = basePath + "/.hoodie/.temp";
    
    // Instant 001 writes to partition A
    String instant1 = "001";
    String partition1 = "partitionA";
    Set<String> markers1 = Stream.of(
        partition1 + "/file-a-001.parquet.marker.CREATE"
    ).collect(Collectors.toSet());
    prepareMarkerFiles(rootBaseMarkerDir, instant1, partition1, markers1);
    createCommitFile(instant1);
    
    // Instant 002 writes to partition B
    String instant2 = "002";
    String partition2 = "partitionB";
    Set<String> markers2 = Stream.of(
        partition2 + "/file-b-002.parquet.marker.CREATE"
    ).collect(Collectors.toSet());
    prepareMarkerFiles(rootBaseMarkerDir, instant2, partition2, markers2);
    createCommitFile(instant2);
    
    // Current instant 003 wants to write to partition C (different from all)
    String currentInstant = "003";
    String currentPartition = "partitionC";
    String currentFileId = "file-c";
    
    HoodieWriteConfig config = createMockConfig();
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    
    PartitionBasedDirectMarkerDetectionStrategy strategy = new PartitionBasedDirectMarkerDetectionStrategy(
        storage, currentPartition, currentFileId, currentInstant, activeTimeline, config);
    
    // Should NOT detect conflict - all partitions are different
    assertFalse(strategy.hasMarkerConflict(), 
        "Should NOT detect conflict when all partitions are different");
  }

  /**
   * Test detectAndResolveConflictIfNecessary when no conflict.
   */
  @Test
  public void testDetectAndResolveConflictIfNecessaryNoConflict() throws IOException {
    String currentInstant = "001";
    String partition = "2024/01";
    String fileId = "file-a";
    
    HoodieWriteConfig config = createMockConfig();
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    
    PartitionBasedDirectMarkerDetectionStrategy strategy = new PartitionBasedDirectMarkerDetectionStrategy(
        storage, partition, fileId, currentInstant, activeTimeline, config);
    
    // Should not throw exception when no conflict
    strategy.detectAndResolveConflictIfNecessary();
  }

  /**
   * Test detectAndResolveConflictIfNecessary when conflict exists.
   */
  @Test
  public void testDetectAndResolveConflictIfNecessaryWithConflict() throws IOException {
    String rootBaseMarkerDir = basePath + "/.hoodie/.temp";
    
    // Old instant 001 writes to partition 2024/01
    String oldInstant = "001";
    String partition = "2024/01";
    Set<String> oldMarkers = Stream.of(
        partition + "/file-a-001.parquet.marker.CREATE"
    ).collect(Collectors.toSet());
    prepareMarkerFiles(rootBaseMarkerDir, oldInstant, partition, oldMarkers);
    createHeartbeatFile(oldInstant);
    
    // Current instant 002 wants to write to the SAME partition
    String currentInstant = "002";
    String fileId = "file-b";
    
    HoodieWriteConfig config = createMockConfig();
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    
    PartitionBasedDirectMarkerDetectionStrategy strategy = new PartitionBasedDirectMarkerDetectionStrategy(
        storage, partition, fileId, currentInstant, activeTimeline, config);
    
    // Should throw exception when conflict exists
    assertThrows(HoodieEarlyConflictDetectionException.class, () -> {
      strategy.detectAndResolveConflictIfNecessary();
    });
  }

  // Helper methods

  private void prepareMarkerFiles(String baseMarkerDir, String instant, String partition, Set<String> markers) throws IOException {
    String markerDir = baseMarkerDir + "/" + instant;
    storage.createDirectory(new StoragePath(markerDir));
    
    // Create partition directory in marker dir
    String partitionMarkerDir = markerDir + "/" + partition;
    storage.createDirectory(new StoragePath(partitionMarkerDir));
    
    // Create MARKERS0 file with marker entries
    File markersFile = new File(markerDir + "/MARKERS0");
    markersFile.getParentFile().mkdirs();
    try (BufferedWriter out = new BufferedWriter(new FileWriter(markersFile))) {
      for (String marker : markers) {
        out.write(marker);
        out.write("\n");
      }
    }
    
    // Also create marker files in the partition directory
    for (String marker : markers) {
      String markerFileName = marker.substring(marker.lastIndexOf("/") + 1);
      storage.create(new StoragePath(partitionMarkerDir + "/" + markerFileName), true).close();
    }
  }

  private void createCommitFile(String instant) throws IOException {
    storage.create(new StoragePath(basePath + "/.hoodie/" + instant + ".commit"), true).close();
  }

  private void createHeartbeatFile(String instant) throws IOException {
    String heartbeatDir = basePath + "/.hoodie/.heartbeat";
    storage.createDirectory(new StoragePath(heartbeatDir));
    storage.create(new StoragePath(heartbeatDir + "/" + instant), true).close();
  }

  private HoodieWriteConfig createMockConfig() {
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getBasePath()).thenReturn(basePath);
    when(config.earlyConflictDetectionCheckCommitConflict()).thenReturn(false);
    when(config.getHoodieClientHeartbeatIntervalInMs()).thenReturn(60000L);
    when(config.getHoodieClientHeartbeatTolerableMisses()).thenReturn(2);
    return config;
  }
}
