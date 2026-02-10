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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.log.HoodieLogFileWriteCallback;
import org.apache.hudi.common.util.MarkerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A {@link HoodieAppendHandle} that supports APPEND write incrementally(mini-batches).
 *
 * <p>For the first mini-batch, it initializes and sets up the next file path to write,
 * then closes the file writer. The subsequent mini-batches are appended to the same file
 * through a different append handle with same write file name.
 *
 * <p>The back-up writer may rollover on condition(for e.g, the filesystem does not support append
 * or the file size hits the configured threshold).
 */
public class FlinkAppendHandle<T, I, K, O>
    extends HoodieAppendHandle<T, I, K, O> implements MiniBatchHandle {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkAppendHandle.class);

  private boolean isClosed = false;

  /**
   * Set of markers created by this task instance within the current instant.
   * Used to distinguish between markers created by current task vs other task attempts.
   * This is shared across multiple FlinkAppendHandle instances within the same task.
   */
  private final Set<String> createdMarkers;

  /**
   * Creates a FlinkAppendHandle without task retry protection.
   * This constructor is kept for backward compatibility.
   */
  public FlinkAppendHandle(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath,
      String fileId,
      Iterator<HoodieRecord<T>> recordItr,
      TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier, null);
  }

  /**
   * Creates a FlinkAppendHandle with task retry protection.
   *
   * @param config             Write config
   * @param instantTime        Instant time
   * @param hoodieTable        Hoodie table
   * @param partitionPath      Partition path
   * @param fileId             File ID
   * @param recordItr          Record iterator
   * @param taskContextSupplier Task context supplier
   * @param createdMarkers     Set of markers already created by this task instance.
   *                           If null, task retry protection is disabled.
   *                           This set should be shared across all FlinkAppendHandle instances
   *                           within the same Flink task to track markers created in previous batches.
   */
  public FlinkAppendHandle(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath,
      String fileId,
      Iterator<HoodieRecord<T>> recordItr,
      TaskContextSupplier taskContextSupplier,
      Set<String> createdMarkers) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, recordItr, taskContextSupplier);
    this.createdMarkers = createdMarkers;
  }

  protected HoodieLogFileWriteCallback getLogWriteCallback() {
    return new AppendLogWriteCallback() {
      @Override
      public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
        String markerKey = partitionPath + "/" + logFileToAppend.getFileName();

        // If task retry protection is enabled (createdMarkers is not null)
        if (createdMarkers != null) {
          // Check if marker was created by this task instance in a previous batch
          if (createdMarkers.contains(markerKey)) {
            LOG.debug("Marker already created by this task instance, continue appending: {}", markerKey);
            return !hasExpiredHeartbeatPartitionConflict();
          }

          // Try to create the marker
          WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
          Option<StoragePath> result = writeMarkers.createIfNotExists(partitionPath, logFileToAppend.getFileName(), IOType.APPEND);

          if (result.isPresent()) {
            // Marker created successfully, record it
            createdMarkers.add(markerKey);
            LOG.info("Marker created successfully for task retry protection: {}", markerKey);
            return !hasExpiredHeartbeatPartitionConflict();
          } else {
            // Marker already exists but not created by this task instance
            // This means another task attempt (e.g., the "zombie" original task) created it
            LOG.warn("Detected task retry conflict: marker {} exists but was not created by this task instance. "
                + "Triggering rollover to avoid file corruption.", markerKey);
            return false;
          }
        }

        // Task retry protection disabled, use original behavior
        // Just skip the marker creation if it already exists, the new data would append to
        // the file directly.
        WriteMarkers writeMarkers = WriteMarkersFactory.get(config.getMarkersType(), hoodieTable, instantTime);
        writeMarkers.createIfNotExists(partitionPath, logFileToAppend.getFileName(), IOType.APPEND);
        return !hasExpiredHeartbeatPartitionConflict();
      }

      /**
       * Checks for expired heartbeat partition conflict.
       * If a writer with expired heartbeat has markers in the same partition,
       * it might be "falsely dead" (heartbeat expired but task still running).
       * Returns true if conflict detected and rollover is needed.
       */
      private boolean hasExpiredHeartbeatPartitionConflict() {
        if (!config.isExpiredHeartbeatPartitionConflictCheckEnabled()) {
          return false;
        }
        long maxAllowableHeartbeatIntervalInMs = config.getHoodieClientHeartbeatIntervalInMs()
            * config.getHoodieClientHeartbeatTolerableMisses();
        boolean conflict = MarkerUtils.hasExpiredHeartbeatPartitionConflict(
            hoodieTable.getStorage(),
            config.getBasePath(),
            instantTime,
            maxAllowableHeartbeatIntervalInMs,
            partitionPath);
        if (conflict) {
          LOG.warn("Detected expired heartbeat partition conflict for partition: {}. "
              + "Rolling over to a new log file to prevent potential data corruption "
              + "from a 'falsely dead' writer.", partitionPath);
        }
        return conflict;
      }
    };
  }

  @Override
  public boolean canWrite(HoodieRecord record) {
    return true;
  }

  @Override
  protected boolean needsUpdateLocation() {
    return false;
  }

  @Override
  protected boolean isUpdateRecord(HoodieRecord<T> hoodieRecord) {
    // do not use the HoodieRecord operation because hoodie writer has its own
    // INSERT/MERGE bucket for 'UPSERT' semantics. For e.g, a hoodie record with fresh new key
    // and operation HoodieCdcOperation.DELETE would be put into either an INSERT bucket or UPDATE bucket.
    return hoodieRecord.getCurrentLocation() != null
        && hoodieRecord.getCurrentLocation().getInstantTime().equals("U");
  }

  @Override
  public List<WriteStatus> close() {
    try {
      return super.close();
    } finally {
      this.isClosed = true;
    }
  }

  @Override
  public void closeGracefully() {
    if (isClosed) {
      return;
    }
    try {
      close();
    } catch (Throwable throwable) {
      // The intermediate log file can still append based on the incremental MERGE semantics,
      // there is no need to delete the file.
      LOG.warn("Error while trying to dispose the APPEND handle", throwable);
    }
  }

  @Override
  public StoragePath getWritePath() {
    return writer.getLogFile().getPath();
  }
}
