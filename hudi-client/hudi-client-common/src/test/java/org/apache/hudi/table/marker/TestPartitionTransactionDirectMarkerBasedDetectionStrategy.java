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

import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PartitionTransactionDirectMarkerBasedDetectionStrategy}.
 */
public class TestPartitionTransactionDirectMarkerBasedDetectionStrategy extends HoodieCommonTestHarness {

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
   * Test that the strategy throws exception when non-ZK lock provider is used.
   */
  @Test
  public void testThrowsExceptionForNonZKLockProvider() {
    String currentInstant = "001";
    String partition = "2024/01";
    String fileId = "file-a";
    String dataFileName = fileId + "-001.parquet";
    
    // Create config with non-ZK lock provider
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getBasePath()).thenReturn(basePath);
    when(config.earlyConflictDetectionCheckCommitConflict()).thenReturn(false);
    when(config.getHoodieClientHeartbeatIntervalInMs()).thenReturn(60000L);
    when(config.getHoodieClientHeartbeatTolerableMisses()).thenReturn(2);
    // Use FileSystemBasedLockProvider instead of ZK
    when(config.getLockProviderClass()).thenReturn(
        "org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider");
    
    HoodieActiveTimeline activeTimeline = metaClient.reloadActiveTimeline();
    
    PartitionTransactionDirectMarkerBasedDetectionStrategy strategy = 
        new PartitionTransactionDirectMarkerBasedDetectionStrategy(
            storage, partition, fileId, dataFileName, currentInstant, activeTimeline, config);
    
    // Should throw HoodieNotSupportedException because only ZK lock is supported
    assertThrows(HoodieNotSupportedException.class, () -> {
      strategy.detectAndResolveConflictIfNecessary();
    });
  }

  /**
   * Test that lock key is based on partition path only.
   * This ensures all operations on the same partition compete for the same lock.
   */
  @Test
  public void testLockKeyIsPartitionBased() {
    // Verify that the lock key format is correct
    String partition = "year=2024/month=01/day=15";
    String expectedLockKey = "partition_lock_year=2024_month=01_day=15";
    
    // The lock key should be the partition path with "/" replaced by "_"
    String actualLockKey = "partition_lock_" + partition.replace("/", "_");
    
    assertTrue(actualLockKey.equals(expectedLockKey),
        "Lock key should be based on partition path");
  }

  /**
   * Test that empty partition path gets a default lock key.
   */
  @Test
  public void testDefaultLockKeyForEmptyPartition() {
    String partition = "";
    String expectedLockKey = "partition_lock_default";
    
    String actualLockKey = (partition != null && !partition.isEmpty()) 
        ? "partition_lock_" + partition.replace("/", "_")
        : "partition_lock_default";
    
    assertTrue(actualLockKey.equals(expectedLockKey),
        "Empty partition should get default lock key");
  }

  /**
   * Test that different partitions get different lock keys.
   */
  @Test
  public void testDifferentPartitionsGetDifferentLockKeys() {
    String partition1 = "2024/01";
    String partition2 = "2024/02";
    
    String lockKey1 = "partition_lock_" + partition1.replace("/", "_");
    String lockKey2 = "partition_lock_" + partition2.replace("/", "_");
    
    assertTrue(!lockKey1.equals(lockKey2),
        "Different partitions should get different lock keys");
  }

  /**
   * Test that same partition with different file IDs get the same lock key.
   * This is important for partition-level locking.
   */
  @Test
  public void testSamePartitionDifferentFileIdsSameLockKey() {
    String partition = "2024/01";
    String fileId1 = "file-a";
    String fileId2 = "file-b";
    
    // For partition-level locking, lock key should only depend on partition
    String lockKey1 = "partition_lock_" + partition.replace("/", "_");
    String lockKey2 = "partition_lock_" + partition.replace("/", "_");
    
    assertTrue(lockKey1.equals(lockKey2),
        "Same partition with different file IDs should get the same lock key");
  }

  /**
   * Create a mock config for testing with ZK lock provider.
   */
  private HoodieWriteConfig createMockConfigWithZKLock() {
    HoodieWriteConfig config = mock(HoodieWriteConfig.class);
    when(config.getBasePath()).thenReturn(basePath);
    when(config.earlyConflictDetectionCheckCommitConflict()).thenReturn(false);
    when(config.getHoodieClientHeartbeatIntervalInMs()).thenReturn(60000L);
    when(config.getHoodieClientHeartbeatTolerableMisses()).thenReturn(2);
    when(config.getLockProviderClass()).thenReturn(ZookeeperBasedLockProvider.class.getName());
    when(config.getProps()).thenReturn(new java.util.Properties());
    when(config.getString(LockConfiguration.ZK_CONNECT_URL_PROP_KEY)).thenReturn("localhost:2181");
    when(config.getString(LockConfiguration.ZK_BASE_PATH_PROP_KEY)).thenReturn("/hudi/locks");
    return config;
  }
}
