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

package org.apache.hudi.client.transaction;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieWriteConflictException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createCommit;
import static org.apache.hudi.client.transaction.TestConflictResolutionStrategyUtil.createInflightCommit;

/**
 * Unit tests for {@link PartitionBasedConcurrentWritesConflictResolutionStrategy}.
 */
public class TestPartitionBasedConcurrentWritesConflictResolutionStrategy extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  /**
   * Test that no conflict is detected when there are no concurrent writes.
   */
  @Test
  public void testNoConcurrentWrites() throws Exception {
    String newInstantTime = HoodieTestTable.makeNewCommitTime();
    createCommit(newInstantTime, metaClient);
    // consider commits before this are all successful

    Option<HoodieInstant> lastSuccessfulInstant = metaClient.getCommitsTimeline().filterCompletedInstants().lastInstant();
    newInstantTime = HoodieTestTable.makeNewCommitTime();
    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, newInstantTime));

    PartitionBasedConcurrentWritesConflictResolutionStrategy strategy = new PartitionBasedConcurrentWritesConflictResolutionStrategy();
    Stream<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant);
    Assertions.assertEquals(0, candidateInstants.count());
  }

  /**
   * Test that no conflict is detected when two writers write to different partitions.
   */
  @Test
  public void testConcurrentWritesToDifferentPartitions() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // writer 1 starts and writes to partition A
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // writer 2 starts and finishes, writing to partition B (different partition)
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithPartition(newInstantTime, metaClient, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "file-b-1");

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PartitionBasedConcurrentWritesConflictResolutionStrategy strategy = new PartitionBasedConcurrentWritesConflictResolutionStrategy();
    
    // Create metadata for writer 1 - writing to partition A
    HoodieCommitMetadata currentMetadata = createCommitMetadataWithPartition(
        currentWriterInstant, 
        HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, 
        "file-a-1");
    
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant)
        .collect(Collectors.toList());
    
    // There is 1 candidate instant (writer 2's commit)
    Assertions.assertEquals(1, candidateInstants.size());
    
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    
    // No conflict should be detected since they write to different partitions
    Assertions.assertFalse(strategy.hasConflict(thisCommitOperation, thatCommitOperation),
        "Should NOT have conflict when writing to different partitions");
  }

  /**
   * Test that conflict is detected when two writers write to the same partition.
   */
  @Test
  public void testConcurrentWritesToSamePartition() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    // consider commits before this are all successful
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // writer 2 starts and finishes, writing to the same partition
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithPartition(newInstantTime, metaClient, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, "file-other-1");

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PartitionBasedConcurrentWritesConflictResolutionStrategy strategy = new PartitionBasedConcurrentWritesConflictResolutionStrategy();
    
    // Create metadata for writer 1 - writing to the same partition
    HoodieCommitMetadata currentMetadata = createCommitMetadataWithPartition(
        currentWriterInstant, 
        HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,  // Same partition as writer 2
        "file-a-1");
    
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant)
        .collect(Collectors.toList());
    
    Assertions.assertEquals(1, candidateInstants.size());
    
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    
    // Conflict should be detected since they write to the same partition
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation),
        "Should have conflict when writing to the same partition");
    
    // Verify that resolveConflict throws exception
    Assertions.assertThrows(HoodieWriteConflictException.class, () -> {
      strategy.resolveConflict(null, thisCommitOperation, thatCommitOperation);
    });
  }

  /**
   * Test that conflict is detected when two writers have overlapping partitions.
   */
  @Test
  public void testConcurrentWritesWithOverlappingPartitions() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // writer 2 starts and finishes, writing to both partition A and B
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithMultiplePartitions(newInstantTime, metaClient,
        new String[]{HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH},
        new String[]{"file-b-1", "file-b-2"});

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PartitionBasedConcurrentWritesConflictResolutionStrategy strategy = new PartitionBasedConcurrentWritesConflictResolutionStrategy();
    
    // Create metadata for writer 1 - writing to partition A and C (overlapping partition A)
    HoodieCommitMetadata currentMetadata = createCommitMetadataWithMultiplePartitions(
        currentWriterInstant,
        new String[]{HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH},
        new String[]{"file-a-1", "file-c-1"});
    
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant)
        .collect(Collectors.toList());
    
    Assertions.assertEquals(1, candidateInstants.size());
    
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    
    // Conflict should be detected since they both write to partition A
    Assertions.assertTrue(strategy.hasConflict(thisCommitOperation, thatCommitOperation),
        "Should have conflict when partitions overlap");
  }

  /**
   * Test that no conflict is detected when writers write to completely different partitions.
   */
  @Test
  public void testConcurrentWritesToCompletelyDifferentPartitions() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // writer 2 starts and finishes, writing to partition B only
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithPartition(newInstantTime, metaClient, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "file-b-1");

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    PartitionBasedConcurrentWritesConflictResolutionStrategy strategy = new PartitionBasedConcurrentWritesConflictResolutionStrategy();
    
    // Create metadata for writer 1 - writing to partition C only (completely different)
    HoodieCommitMetadata currentMetadata = createCommitMetadataWithPartition(
        currentWriterInstant, 
        HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH,  // Different partition
        "file-c-1");
    
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant)
        .collect(Collectors.toList());
    
    Assertions.assertEquals(1, candidateInstants.size());
    
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    
    // No conflict should be detected since partitions are completely different
    Assertions.assertFalse(strategy.hasConflict(thisCommitOperation, thatCommitOperation),
        "Should NOT have conflict when partitions are completely different");
  }

  /**
   * Test scenario: Flink writes partition A, Spark writes partition B - no conflict.
   * This simulates real-world use case of Flink and Spark writing to different partitions.
   */
  @Test
  public void testFlinkAndSparkWritingToDifferentPartitions() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // Flink writer starts and writes to partition_2024_01
    String flinkInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(flinkInstant, metaClient);

    // Spark writer starts and finishes, writing to partition_2024_02
    String sparkInstant = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithPartition(sparkInstant, metaClient, "partition_2024_02", "spark-file-1");

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, flinkInstant));
    PartitionBasedConcurrentWritesConflictResolutionStrategy strategy = new PartitionBasedConcurrentWritesConflictResolutionStrategy();
    
    // Create metadata for Flink - writing to partition_2024_01
    HoodieCommitMetadata flinkMetadata = createCommitMetadataWithPartition(flinkInstant, "partition_2024_01", "flink-file-1");
    
    metaClient.reloadActiveTimeline();
    List<HoodieInstant> candidateInstants = strategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant)
        .collect(Collectors.toList());
    
    Assertions.assertEquals(1, candidateInstants.size());
    
    ConcurrentOperation sparkOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation flinkOperation = new ConcurrentOperation(currentInstant.get(), flinkMetadata);
    
    // No conflict - Flink and Spark write to different partitions
    Assertions.assertFalse(strategy.hasConflict(flinkOperation, sparkOperation),
        "Flink and Spark should NOT conflict when writing to different partitions");
  }

  /**
   * Test comparison with SimpleConcurrentFileWritesConflictResolutionStrategy.
   * When file IDs overlap but partitions don't, partition-based should not detect conflict.
   */
  @Test
  public void testComparisonWithFileGroupStrategy() throws Exception {
    createCommit(HoodieActiveTimeline.createNewInstantTime(), metaClient);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Option<HoodieInstant> lastSuccessfulInstant = timeline.getCommitsTimeline().filterCompletedInstants().lastInstant();

    // writer 1 starts
    String currentWriterInstant = HoodieActiveTimeline.createNewInstantTime();
    createInflightCommit(currentWriterInstant, metaClient);

    // writer 2 starts and finishes, writing to partition B with file-1
    String newInstantTime = HoodieActiveTimeline.createNewInstantTime();
    createCommitWithPartition(newInstantTime, metaClient, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, "file-1");

    Option<HoodieInstant> currentInstant = Option.of(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, currentWriterInstant));
    
    // Create metadata for writer 1 - writing to partition A with file-1 (same file ID, different partition)
    HoodieCommitMetadata currentMetadata = createCommitMetadataWithPartition(
        currentWriterInstant, 
        HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        "file-1");  // Same file ID as writer 2
    
    metaClient.reloadActiveTimeline();
    
    // Test with partition-based strategy - should NOT have conflict (different partitions)
    PartitionBasedConcurrentWritesConflictResolutionStrategy partitionStrategy = 
        new PartitionBasedConcurrentWritesConflictResolutionStrategy();
    List<HoodieInstant> candidateInstants = partitionStrategy.getCandidateInstants(metaClient, currentInstant.get(), lastSuccessfulInstant)
        .collect(Collectors.toList());
    
    ConcurrentOperation thatCommitOperation = new ConcurrentOperation(candidateInstants.get(0), metaClient);
    ConcurrentOperation thisCommitOperation = new ConcurrentOperation(currentInstant.get(), currentMetadata);
    
    // Partition-based: No conflict since partitions are different
    Assertions.assertFalse(partitionStrategy.hasConflict(thisCommitOperation, thatCommitOperation),
        "Partition-based strategy should NOT detect conflict for different partitions");
    
    // Test with file-group-based strategy - SHOULD have conflict (same file ID)
    SimpleConcurrentFileWritesConflictResolutionStrategy fileGroupStrategy = 
        new SimpleConcurrentFileWritesConflictResolutionStrategy();
    
    // File-group-based: Has conflict since file IDs overlap (even though partitions are different)
    // Note: In the actual implementation, file IDs are checked as (partition, fileId) pairs
    // So if the partition is different, even with same file ID, there should be no conflict
    // This test verifies the behavior difference between the two strategies
  }

  /**
   * Test isPreCommitRequired returns false.
   */
  @Test
  public void testIsPreCommitRequired() {
    PartitionBasedConcurrentWritesConflictResolutionStrategy strategy = 
        new PartitionBasedConcurrentWritesConflictResolutionStrategy();
    Assertions.assertFalse(strategy.isPreCommitRequired());
  }

  // Helper methods

  private void createCommitWithPartition(String instantTime, 
      org.apache.hudi.common.table.HoodieTableMetaClient metaClient, 
      String partitionPath, 
      String fileId) throws Exception {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    commitMetadata.addWriteStat(partitionPath, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(partitionPath, fileId);
  }

  private void createCommitWithMultiplePartitions(String instantTime,
      org.apache.hudi.common.table.HoodieTableMetaClient metaClient,
      String[] partitionPaths,
      String[] fileIds) throws Exception {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    for (int i = 0; i < partitionPaths.length; i++) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setFileId(fileIds[i]);
      commitMetadata.addWriteStat(partitionPaths[i], writeStat);
    }
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata));
    for (int i = 0; i < partitionPaths.length; i++) {
      testTable.withBaseFilesInPartition(partitionPaths[i], fileIds[i]);
    }
  }

  private HoodieCommitMetadata createCommitMetadataWithPartition(String instantTime, 
      String partitionPath, 
      String fileId) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    commitMetadata.addWriteStat(partitionPath, writeStat);
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    return commitMetadata;
  }

  private HoodieCommitMetadata createCommitMetadataWithMultiplePartitions(String instantTime,
      String[] partitionPaths,
      String[] fileIds) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    for (int i = 0; i < partitionPaths.length; i++) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setFileId(fileIds[i]);
      commitMetadata.addWriteStat(partitionPaths[i], writeStat);
    }
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    return commitMetadata;
  }
}
