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

import org.apache.hudi.common.conflict.detection.DirectMarkerBasedDetectionStrategy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Partition-based early conflict detection strategy for direct marker writers.
 * 
 * This strategy performs partition-level conflict detection during the write phase,
 * allowing concurrent writes to different partitions without conflicts.
 * 
 * <p>
 * Key differences from {@link SimpleDirectMarkerBasedDetectionStrategy}:
 * <ul>
 *   <li>Only checks partition paths, not file IDs</li>
 *   <li>Allows concurrent writes to different partitions</li>
 *   <li>Only reports conflicts when two operations write to the same partition</li>
 * </ul>
 * </p>
 */
public class PartitionBasedDirectMarkerDetectionStrategy extends DirectMarkerBasedDetectionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionBasedDirectMarkerDetectionStrategy.class);
  protected final String basePath;
  private final boolean checkCommitConflict;
  private final Set<String> completedCommitInstants;
  protected final long maxAllowableHeartbeatIntervalInMs;

  public PartitionBasedDirectMarkerDetectionStrategy(HoodieStorage storage, String partitionPath, String fileId, String instantTime,
                                                  HoodieActiveTimeline activeTimeline, HoodieWriteConfig config) {
    super(storage, partitionPath, fileId, instantTime, activeTimeline, config);
    this.basePath = config.getBasePath();
    this.checkCommitConflict = config.earlyConflictDetectionCheckCommitConflict();
    this.completedCommitInstants = new HashSet<>(activeTimeline.getCommitsTimeline().filterCompletedInstants().getInstants());
    this.maxAllowableHeartbeatIntervalInMs = config.getHoodieClientHeartbeatIntervalInMs() * config.getHoodieClientHeartbeatTolerableMisses();
  }

  @Override
  public boolean hasMarkerConflict() {
    try {
      // Check for partition-level marker conflicts
      boolean markerConflict = checkPartitionMarkerConflict(basePath, maxAllowableHeartbeatIntervalInMs);
      
      // Optionally check commit conflicts at partition level
      boolean commitConflict = checkCommitConflict && 
          MarkerUtils.hasCommitConflict(activeTimeline, Stream.of(fileId).collect(Collectors.toSet()), completedCommitInstants);
      
      return markerConflict || commitConflict;
    } catch (IOException e) {
      LOG.warn("Exception occurs during create marker file in partition-based early conflict detection mode.");
      throw new HoodieIOException("Exception occurs during create marker file in partition-based early conflict detection mode.", e);
    }
  }

  /**
   * Check for partition-level marker conflicts.
   * Only reports conflicts if another operation is writing to the same partition.
   *
   * <p>This method also computes the expired heartbeat partition conflict as a side-effect,
   * reusing the same {@code .temp} directory listing. The result can be read via
   * {@link #isExpiredHeartbeatPartitionConflictDetected()}.
   * 
   * @param basePath Base path of the table
   * @param maxAllowableHeartbeatIntervalInMs Maximum allowable heartbeat interval
   * @return true if there's a partition-level conflict
   * @throws IOException upon errors
   */
  private boolean checkPartitionMarkerConflict(String basePath, long maxAllowableHeartbeatIntervalInMs) throws IOException {
    String tempFolderPath = basePath + StoragePath.SEPARATOR + HoodieTableMetaClient.TEMPFOLDER_NAME;

    // List .temp directory ONCE
    List<StoragePath> allInstantPaths = storage.listDirectEntries(new StoragePath(tempFolderPath)).stream()
        .map(StoragePathInfo::getPath)
        .collect(Collectors.toList());

    // Single-pass classification: each instant's heartbeat is checked ONCE
    MarkerUtils.InstantClassification classification = MarkerUtils.classifyInstantsByHeartbeat(
        activeTimeline, allInstantPaths, instantTime,
        maxAllowableHeartbeatIntervalInMs, storage, basePath);

    // Active heartbeat instants → partition-level conflict detection
    boolean hasPartitionConflict = classification.activeHeartbeatInstants.stream().anyMatch(currentMarkerDirPath -> {
      try {
        StoragePath markerPartitionPath = new StoragePath(currentMarkerDirPath, partitionPath);
        // If the partition path exists in another instant's marker directory, there's a conflict
        if (storage.exists(markerPartitionPath)) {
          // Check if there are any marker files in this partition
          List<StoragePathInfo> markers = storage.listDirectEntries(markerPartitionPath);
          if (!markers.isEmpty()) {
            LOG.warn("Detected partition-level conflict: partition " + partitionPath 
                + " is being written by another operation in instant " + currentMarkerDirPath);
            return true;
          }
        }
        return false;
      } catch (IOException e) {
        throw new HoodieIOException("IOException occurs during checking partition-level marker conflict", e);
      }
    });

    // Expired heartbeat instants → partition-level conflict detection (same classification, zero extra heartbeat IO)
    this.expiredHeartbeatPartitionConflictDetected =
        MarkerUtils.hasExpiredHeartbeatInPartition(storage, classification.expiredHeartbeatInstants, partitionPath);

    if (hasPartitionConflict) {
      LOG.warn("Detected partition-level marker conflict for partition: " + partitionPath + " at instant " + instantTime);
      return true;
    }
    
    LOG.debug("No partition-level marker conflict detected for partition: " + partitionPath);
    return false;
  }

  @Override
  public void resolveMarkerConflict(String basePath, String partitionPath, String dataFileName) {
    throw new HoodieEarlyConflictDetectionException(new ConcurrentModificationException(
        "Early partition-level conflict detected. Another writer is writing to the same partition: " + partitionPath));
  }

  @Override
  public void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException {
    if (hasMarkerConflict()) {
      resolveMarkerConflict(basePath, partitionPath, fileId);
    }
  }
}
