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

import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Partition-based early conflict detection strategy with optimized transaction lock support.
 * 
 * <p>This strategy extends {@link PartitionBasedDirectMarkerDetectionStrategy} by adding
 * distributed locking to ensure atomicity during partition directory creation.
 * 
 * <h3>Optimization: Lock Only on First Partition Directory Creation</h3>
 * 
 * <p>To reduce ZooKeeper pressure, the lock is only acquired when the partition directory
 * under the current instant's marker folder does NOT exist yet. The flow is:
 * 
 * <pre>
 * Check: Does .temp/{instantTime}/{partitionPath}/ exist?
 *   |
 *   +-- NO (first task writing to this partition in this instant)
 *   |     1. Acquire partition-level lock
 *   |     2. Double-check directory doesn't exist (in case of race)
 *   |     3. Perform conflict detection against OTHER instants
 *   |     4. Create partition directory
 *   |     5. Release lock
 *   |
 *   +-- YES (partition directory already created by another task in same instant)
 *         1. Skip lock acquisition (no ZK pressure!)
 *         2. Return directly
 * </pre>
 * 
 * <p>This optimization significantly reduces ZK lock acquisitions because:
 * <ul>
 *   <li>Only the FIRST task writing to a partition needs the lock</li>
 *   <li>Subsequent tasks in the same instant skip the lock entirely</li>
 *   <li>Different partitions use different locks, so no contention</li>
 * </ul>
 * 
 * <p>
 * Usage:
 * <pre>
 * hoodie.write.concurrency.mode=optimistic_concurrency_control
 * hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
 * hoodie.write.lock.early.conflict.detection.strategy=org.apache.hudi.table.marker.PartitionTransactionDirectMarkerBasedDetectionStrategy
 * hoodie.write.early.conflict.detection.enable=true
 * </pre>
 */
public class PartitionTransactionDirectMarkerBasedDetectionStrategy
    extends PartitionBasedDirectMarkerDetectionStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(
      PartitionTransactionDirectMarkerBasedDetectionStrategy.class);

  private final HoodieWriteConfig writeConfig;
  private final FileSystem fs;
  private final StoragePath markerPartitionDirPath;

  public PartitionTransactionDirectMarkerBasedDetectionStrategy(
      HoodieStorage storage, String partitionPath, String fileId, String instantTime,
      HoodieActiveTimeline activeTimeline, HoodieWriteConfig config) {
    super(storage, partitionPath, fileId, instantTime, activeTimeline, config);
    this.writeConfig = config;
    this.fs = (FileSystem) storage.getFileSystem();
    // Build the marker partition directory path: .temp/{instantTime}/{partitionPath}/
    String markerDirPath = config.getBasePath() + StoragePath.SEPARATOR 
        + HoodieTableMetaClient.TEMPFOLDER_NAME + StoragePath.SEPARATOR + instantTime;
    this.markerPartitionDirPath = (partitionPath == null || partitionPath.isEmpty())
        ? new StoragePath(markerDirPath)
        : new StoragePath(markerDirPath, partitionPath);
  }

  /**
   * Detects conflicts with optimized partition-level locking.
   * 
   * <p>Lock is only acquired when partition directory doesn't exist yet.
   * If partition directory already exists (created by another task in same instant),
   * we skip lock acquisition entirely.
   *
   * <p>In both fast and slow paths, expired heartbeat partition conflicts are checked.
   * In the fast path, a standalone check is performed (since the .temp listing from the
   * slow path is not available). In the slow path, the check is performed inside
   * {@code super.detectAndResolveConflictIfNecessary()} as part of the same scan.
   */
  @Override
  public void detectAndResolveConflictIfNecessary() throws HoodieEarlyConflictDetectionException {
    try {
      // Fast path: if partition directory already exists, skip lock entirely
      if (storage.exists(markerPartitionDirPath)) {
        LOG.debug("Partition directory {} already exists for instant {}, skipping lock acquisition",
            markerPartitionDirPath, instantTime);
        // Directory exists, meaning another task in the same instant already created it
        // and passed conflict detection. Safe to proceed without lock.

        // Still need to check expired heartbeat partition conflict in the fast path,
        // because the condition could have changed since the first task's check.
        if (writeConfig.isExpiredHeartbeatPartitionConflictCheckEnabled()) {
          this.expiredHeartbeatPartitionConflictDetected =
              MarkerUtils.hasExpiredHeartbeatPartitionConflict(
                  storage, basePath, instantTime, maxAllowableHeartbeatIntervalInMs, partitionPath);
        }
        return;
      }
    } catch (IOException e) {
      LOG.warn("Failed to check partition directory existence, falling back to lock path", e);
      // Fall through to lock path
    }

    // Slow path: partition directory doesn't exist, need to acquire lock
    // The expired heartbeat check is performed inside super.detectAndResolveConflictIfNecessary()
    // as part of the same .temp directory scan (no extra IO).
    detectConflictAndCreateDirectoryWithLock();
  }

  /**
   * Acquires lock, performs conflict detection, and creates partition directory.
   */
  private void detectConflictAndCreateDirectoryWithLock() {
    LockManager lockManager = new LockManager(writeConfig, fs,
        createPartitionLockProps(writeConfig, partitionPath));
    try {
      // Acquire partition-level lock
      LOG.info("Acquiring partition lock for partition: {} at instant: {}", partitionPath, instantTime);
      lockManager.lock();
      LOG.info("Partition lock acquired for partition: {} at instant: {}", partitionPath, instantTime);

      // Double-check: another task might have created the directory while we were waiting for lock
      if (storage.exists(markerPartitionDirPath)) {
        LOG.info("Partition directory {} was created by another task while waiting for lock, "
            + "skipping conflict detection", markerPartitionDirPath);
        return;
      }

      // Perform conflict detection against OTHER instants
      // This checks if any other instant is writing to the same partition
      LOG.info("Performing partition-level conflict detection for partition: {} at instant: {}", 
          partitionPath, instantTime);
      super.detectAndResolveConflictIfNecessary();

      // Create partition directory to mark this partition as "occupied" by current instant
      // Other tasks in the same instant will see this and skip the lock
      LOG.info("Creating marker partition directory: {}", markerPartitionDirPath);
      storage.createDirectory(markerPartitionDirPath);

    } catch (HoodieEarlyConflictDetectionException e) {
      LOG.warn("Partition-level conflict detected for partition: {} at instant: {}", partitionPath, instantTime);
      throw e;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to create marker partition directory: " + markerPartitionDirPath, e);
    } catch (Exception e) {
      LOG.warn("Exception occurs during partition-based early conflict detection with transaction lock.", e);
      throw e;
    } finally {
      // Always release lock
      try {
        lockManager.unlock();
        LOG.info("Partition lock released for partition: {} at instant: {}", partitionPath, instantTime);
      } catch (Exception ignored) {
        // Lock might already be released
      }
      lockManager.close();
    }
  }

  /**
   * Creates lock properties for partition-level locking.
   * Uses only the partition path as the lock key, so all operations on the same 
   * partition will compete for the same lock.
   *
   * @param writeConfig Hudi write configs.
   * @param partitionPath Relative partition path.
   * @return Updated lock related configs with partition path as lock key.
   */
  private static TypedProperties createPartitionLockProps(
      HoodieWriteConfig writeConfig, String partitionPath) {
    if (!ZookeeperBasedLockProvider.class.getName().equals(writeConfig.getLockProviderClass())) {
      throw new HoodieNotSupportedException(
          "Only ZK-based lock is supported for PartitionTransactionDirectMarkerBasedDetectionStrategy. "
          + "Current lock provider: " + writeConfig.getLockProviderClass());
    }
    TypedProperties props = new TypedProperties(writeConfig.getProps());
    // Use partition path as lock key (not partitionPath/fileId)
    // This ensures all operations on the same partition compete for the same lock
    String lockKey = (partitionPath != null && !partitionPath.isEmpty()) 
        ? "partition_lock_" + partitionPath.replace("/", "_")
        : "partition_lock_default";
    props.setProperty(LockConfiguration.ZK_LOCK_KEY_PROP_KEY, lockKey);
    LOG.debug("Created partition lock with key: {}", lockKey);
    return props;
  }
}
