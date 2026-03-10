/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.transaction;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;

import org.apache.hadoop.fs.FileSystem;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;

/**
 * Partition-level transaction manager for direct marker conflict detection.
 *
 * <p>Unlike {@link DirectMarkerTransactionManager} which uses file-level locks
 * (lock key = {@code partitionPath/fileId}), this manager uses partition-level locks
 * (lock key = {@code partition_lock_{partitionPath}}). This ensures that all tasks
 * writing to the same partition compete for the same lock, regardless of their fileId.
 *
 * <p>This is used by {@code PartitionTransactionDirectMarkerBasedDetectionStrategy}
 * to ensure atomicity during partition directory creation. The partition-level lock
 * guarantees that only one task per partition performs conflict detection and creates
 * the partition marker directory.
 *
 * <p>Lock key design:
 * <ul>
 *   <li>{@link DirectMarkerTransactionManager}: {@code partitionPath/fileId} → file-level lock</li>
 *   <li>{@link PartitionDirectMarkerTransactionManager}: {@code partition_lock_{partitionPath}} → partition-level lock</li>
 * </ul>
 */
public class PartitionDirectMarkerTransactionManager extends TransactionManager {
  private final String partitionPath;

  public PartitionDirectMarkerTransactionManager(HoodieWriteConfig config, FileSystem fs, String partitionPath) {
    super(new LockManager(config, fs, createPartitionLockProps(config, partitionPath)), config.isLockRequired());
    this.partitionPath = partitionPath;
  }

  public void beginTransaction(String newTxnOwnerInstantTime) {
    if (isLockRequired) {
      LOG.info("Partition transaction starting for {} on partition {}", newTxnOwnerInstantTime, partitionPath);
      lockManager.lock();
      reset(currentTxnOwnerInstant, Option.of(getInstant(newTxnOwnerInstantTime)), Option.empty());
      LOG.info("Partition transaction started for {} on partition {}", newTxnOwnerInstantTime, partitionPath);
    }
  }

  public void endTransaction(String currentTxnOwnerInstantTime) {
    if (isLockRequired) {
      LOG.info("Partition transaction ending for {} on partition {}", currentTxnOwnerInstantTime, partitionPath);
      if (reset(Option.of(getInstant(currentTxnOwnerInstantTime)), Option.empty(), Option.empty())) {
        lockManager.unlock();
        LOG.info("Partition transaction ended for {} on partition {}", currentTxnOwnerInstantTime, partitionPath);
      }
    }
  }

  /**
   * Creates lock properties with partition-level lock key.
   *
   * <p>Uses the absolute partition path (basePath + partitionPath) as the lock key
   * to ensure cross-table isolation. Different tables with the same partition path
   * will have different lock keys because their basePaths differ.
   *
   * <p>Example:
   * <ul>
   *   <li>basePath=/data/hudi/table_a, partition=dt=2024-01-15
   *       → lockKey=partition_lock__data_hudi_table_a_dt=2024-01-15</li>
   *   <li>basePath=/data/hudi/table_b, partition=dt=2024-01-15
   *       → lockKey=partition_lock__data_hudi_table_b_dt=2024-01-15</li>
   * </ul>
   *
   * @param writeConfig   Hudi write configs.
   * @param partitionPath Relative partition path.
   * @return Lock properties with partition-level lock key.
   */
  private static TypedProperties createPartitionLockProps(
      HoodieWriteConfig writeConfig, String partitionPath) {
    if (!ZookeeperBasedLockProvider.class.getName().equals(writeConfig.getLockProviderClass())) {
      throw new HoodieNotSupportedException(
          "Only ZK-based lock is supported for PartitionDirectMarkerTransactionManager. "
          + "Current lock provider: " + writeConfig.getLockProviderClass());
    }
    TypedProperties props = new TypedProperties(writeConfig.getProps());
    // Use absolute partition path as lock key for cross-table isolation
    String absolutePartitionPath = (partitionPath != null && !partitionPath.isEmpty())
        ? writeConfig.getBasePath() + Path.SEPARATOR + partitionPath
        : writeConfig.getBasePath();
    String lockKey = "partition_lock_" + absolutePartitionPath.replaceAll("/|:", "_");
    props.setProperty(LockConfiguration.ZK_LOCK_KEY_PROP_KEY, lockKey);
    return props;
  }

  private HoodieInstant getInstant(String instantTime) {
    return new HoodieInstant(HoodieInstant.State.INFLIGHT, EMPTY_STRING, instantTime);
  }
}
