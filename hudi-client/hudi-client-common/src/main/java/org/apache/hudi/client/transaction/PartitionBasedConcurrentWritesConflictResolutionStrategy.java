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
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * Partition-based conflict resolution strategy for concurrent writes.
 * 
 * This strategy allows concurrent writes to different partitions without conflicts.
 * It only checks for partition-level conflicts, not file-level conflicts.
 * This means that Flink and Spark can write to different partitions concurrently
 * without any conflicts being detected.
 * 
 * <p>
 * Key differences from {@link SimpleConcurrentFileWritesConflictResolutionStrategy}:
 * <ul>
 *   <li>Only checks partition paths, not file IDs</li>
 *   <li>Allows concurrent writes to different partitions</li>
 *   <li>Only reports conflicts when two operations write to the same partition</li>
 * </ul>
 * </p>
 * 
 * <p>
 * Usage:
 * Set the following configuration to use this strategy:
 * <pre>
 * hoodie.write.concurrency.mode=optimistic_concurrency_control
 * hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
 * hoodie.write.lock.conflict.resolution.strategy=org.apache.hudi.client.transaction.PartitionBasedConcurrentWritesConflictResolutionStrategy
 * </pre>
 * </p>
 */
public class PartitionBasedConcurrentWritesConflictResolutionStrategy
    implements ConflictResolutionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionBasedConcurrentWritesConflictResolutionStrategy.class);

  @Override
  public Stream<HoodieInstant> getCandidateInstants(HoodieTableMetaClient metaClient, HoodieInstant currentInstant,
                                                    Option<HoodieInstant> lastSuccessfulInstant) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    // To find which instants are conflicting, we apply the following logic
    // 1. Get completed instants timeline only for commits that have happened since the last successful write.
    // 2. Get any scheduled or completed compaction or clustering operations that have started and/or finished
    // after the current instant. We need to check for write conflicts since they may have mutated the same partitions
    // that are being newly created by the current write.
    Stream<HoodieInstant> completedCommitsInstantStream = activeTimeline
        .getCommitsTimeline()
        .filterCompletedInstants()
        .findInstantsAfter(lastSuccessfulInstant.isPresent() ? lastSuccessfulInstant.get().getTimestamp() : HoodieTimeline.INIT_INSTANT_TS)
        .getInstantsAsStream();

    Stream<HoodieInstant> compactionAndClusteringPendingTimeline = activeTimeline
        .getTimelineOfActions(CollectionUtils.createSet(REPLACE_COMMIT_ACTION, COMPACTION_ACTION))
        .findInstantsAfter(currentInstant.getTimestamp())
        .filterInflightsAndRequested()
        .getInstantsAsStream();
    return Stream.concat(completedCommitsInstantStream, compactionAndClusteringPendingTimeline);
  }

  @Override
  public boolean hasConflict(ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // Extract partition paths from both operations
    Set<String> partitionsForThisOperation = thisOperation.getMutatedPartitionAndFileIds()
        .stream()
        .map(Pair::getLeft)  // Extract partition path
        .collect(Collectors.toSet());
    
    Set<String> partitionsForOtherOperation = otherOperation.getMutatedPartitionAndFileIds()
        .stream()
        .map(Pair::getLeft)  // Extract partition path
        .collect(Collectors.toSet());
    
    // Check for partition-level intersection
    // Only report conflict if both operations write to the same partition(s)
    Set<String> partitionIntersection = new HashSet<>(partitionsForThisOperation);
    partitionIntersection.retainAll(partitionsForOtherOperation);
    
    if (!partitionIntersection.isEmpty()) {
      LOG.info("Found conflicting writes at partition level between first operation = " + thisOperation
          + ", second operation = " + otherOperation + " , intersecting partitions " + partitionIntersection);
      return true;
    }
    
    // No partition-level conflict - different partitions can be written concurrently
    LOG.debug("No partition-level conflict detected. Operations write to different partitions. "
        + "This operation partitions: " + partitionsForThisOperation
        + ", Other operation partitions: " + partitionsForOtherOperation);
    return false;
  }

  @Override
  public Option<HoodieCommitMetadata> resolveConflict(HoodieTable table,
      ConcurrentOperation thisOperation, ConcurrentOperation otherOperation) {
    // A completed COMPACTION action eventually shows up as a COMMIT action on the timeline.
    // We need to ensure we handle this during conflict resolution and not treat the commit from a
    // compaction operation as a regular commit. Regular commits & deltacommits are candidates for conflict.
    // Since the REPLACE action with CLUSTER operation does not support concurrent updates, we have
    // to consider it as conflict if we see overlapping partitions. Once concurrent updates are
    // supported for CLUSTER (https://issues.apache.org/jira/browse/HUDI-1042),
    // add that to the below check so that concurrent updates do not conflict.
    if (otherOperation.getOperationType() == WriteOperationType.COMPACT) {
      if (HoodieTimeline.compareTimestamps(otherOperation.getInstantTimestamp(), HoodieTimeline.LESSER_THAN, thisOperation.getInstantTimestamp())) {
        return thisOperation.getCommitMetadataOption();
      }
    } else if (HoodieTimeline.LOG_COMPACTION_ACTION.equals(thisOperation.getInstantActionType())) {
      // Since log compaction is a rewrite operation, it can be committed along with other delta commits.
      // The ordering of the commits is taken care by AbstractHoodieLogRecordReader scan method.
      // Conflict arises only if the log compaction commit has a lesser timestamp compared to compaction commit.
      return thisOperation.getCommitMetadataOption();
    }
    // just abort the current write if conflicts are found at partition level
    throw new HoodieWriteConflictException(new ConcurrentModificationException(
        "Cannot resolve conflicts for overlapping writes at partition level. "
        + "Partition-based concurrency control only allows one writer per partition."));
  }

  @Override
  public boolean isPreCommitRequired() {
    return false;
  }
}
