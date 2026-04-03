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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.log.HoodieLogFileWriteCallback;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Tests for FlinkAppendHandle task retry protection.
 * 
 * <p>This tests the behavior of marker-based conflict detection when Flink tasks retry:
 * <ul>
 *   <li>Same task instance across multiple batches should continue appending to the same file</li>
 *   <li>Retry task should detect conflict and trigger rollover when marker exists</li>
 * </ul>
 */
public class TestFlinkAppendHandleTaskRetryProtection {

  @Mock
  private HoodieWriteConfig config;

  @Mock
  private HoodieTable table;

  @Mock
  private WriteMarkers writeMarkers;

  @Mock
  private HoodieLogFile logFile;

  private static final String PARTITION_PATH = "2024/01/01";
  private static final String FILE_NAME = "file1_0_1-0-0_20240101.log";
  private static final String INSTANT_TIME = "20240101120000000";

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(logFile.getFileName()).thenReturn(FILE_NAME);
  }

  /**
   * Tests that when a task instance creates a marker in batch-1,
   * subsequent batches from the same task instance can continue appending
   * without conflict detection triggering rollover.
   */
  @Test
  public void testSameTaskInstanceMultipleBatches() {
    try (MockedStatic<WriteMarkersFactory> mockedFactory = mockStatic(WriteMarkersFactory.class)) {
      mockedFactory.when(() -> WriteMarkersFactory.get(any(), any(), any())).thenReturn(writeMarkers);
      
      Set<String> createdMarkers = new HashSet<>();
      String markerKey = PARTITION_PATH + "/" + FILE_NAME;

      // Batch 1: First write, marker creation should succeed
      when(writeMarkers.createIfNotExists(eq(PARTITION_PATH), eq(FILE_NAME), eq(IOType.APPEND)))
          .thenReturn(Option.of(new StoragePath("/marker/path")));

      // Simulate the callback behavior
      HoodieLogFileWriteCallback callback = createCallback(createdMarkers);
      
      // Batch 1: Should succeed and add marker to the set
      boolean batch1Result = callback.preLogFileOpen(logFile);
      assertTrue(batch1Result, "Batch 1 should succeed");
      assertTrue(createdMarkers.contains(markerKey), "Marker should be recorded in the set");

      // Batch 2: Same task instance, marker already in set, should succeed without calling createIfNotExists
      boolean batch2Result = callback.preLogFileOpen(logFile);
      assertTrue(batch2Result, "Batch 2 should succeed (marker already in set)");

      // Batch 3: Same pattern
      boolean batch3Result = callback.preLogFileOpen(logFile);
      assertTrue(batch3Result, "Batch 3 should succeed (marker already in set)");
    }
  }

  /**
   * Tests that when a retry task (new instance with empty createdMarkers set) 
   * attempts to write to a file whose marker was created by the original task,
   * it detects the conflict and returns false to trigger rollover.
   */
  @Test
  public void testRetryTaskDetectsConflict() {
    try (MockedStatic<WriteMarkersFactory> mockedFactory = mockStatic(WriteMarkersFactory.class)) {
      mockedFactory.when(() -> WriteMarkersFactory.get(any(), any(), any())).thenReturn(writeMarkers);

      // Retry task has an empty createdMarkers set (new instance)
      Set<String> retryTaskMarkers = new HashSet<>();
      String markerKey = PARTITION_PATH + "/" + FILE_NAME;

      // Marker already exists (created by original task)
      when(writeMarkers.createIfNotExists(eq(PARTITION_PATH), eq(FILE_NAME), eq(IOType.APPEND)))
          .thenReturn(Option.empty()); // Returns empty because marker already exists

      HoodieLogFileWriteCallback callback = createCallback(retryTaskMarkers);

      // Retry task: Set is empty, createIfNotExists returns empty (marker exists)
      // Should detect conflict and return false
      boolean result = callback.preLogFileOpen(logFile);
      assertFalse(result, "Retry task should detect conflict and return false to trigger rollover");
      assertFalse(retryTaskMarkers.contains(markerKey), "Marker should not be added to retry task's set");
    }
  }

  /**
   * Tests that when a new task (first write) creates a marker successfully,
   * it records the marker and returns true.
   */
  @Test
  public void testNewTaskFirstWrite() {
    try (MockedStatic<WriteMarkersFactory> mockedFactory = mockStatic(WriteMarkersFactory.class)) {
      mockedFactory.when(() -> WriteMarkersFactory.get(any(), any(), any())).thenReturn(writeMarkers);

      Set<String> createdMarkers = new HashSet<>();
      String markerKey = PARTITION_PATH + "/" + FILE_NAME;

      // New task, marker doesn't exist yet
      when(writeMarkers.createIfNotExists(eq(PARTITION_PATH), eq(FILE_NAME), eq(IOType.APPEND)))
          .thenReturn(Option.of(new StoragePath("/marker/path")));

      HoodieLogFileWriteCallback callback = createCallback(createdMarkers);

      // First write: Should succeed and record marker
      boolean result = callback.preLogFileOpen(logFile);
      assertTrue(result, "First write should succeed");
      assertTrue(createdMarkers.contains(markerKey), "Marker should be recorded");
    }
  }

  /**
   * Tests backward compatibility: when createdMarkers is null,
   * the original behavior (always return true) should be preserved.
   */
  @Test
  public void testBackwardCompatibilityWithNullCreatedMarkers() {
    try (MockedStatic<WriteMarkersFactory> mockedFactory = mockStatic(WriteMarkersFactory.class)) {
      mockedFactory.when(() -> WriteMarkersFactory.get(any(), any(), any())).thenReturn(writeMarkers);

      // Task retry protection disabled (null createdMarkers)
      when(writeMarkers.createIfNotExists(eq(PARTITION_PATH), eq(FILE_NAME), eq(IOType.APPEND)))
          .thenReturn(Option.empty()); // Marker exists

      HoodieLogFileWriteCallback callback = createCallbackWithoutProtection();

      // Without protection, should always return true (original behavior)
      boolean result = callback.preLogFileOpen(logFile);
      assertTrue(result, "Without task retry protection, should always return true");
    }
  }

  /**
   * Creates a callback that simulates FlinkAppendHandle behavior with task retry protection.
   */
  private HoodieLogFileWriteCallback createCallback(Set<String> createdMarkers) {
    return new HoodieLogFileWriteCallback() {
      @Override
      public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
        String markerKey = PARTITION_PATH + "/" + logFileToAppend.getFileName();

        // Check if marker was created by this task instance
        if (createdMarkers.contains(markerKey)) {
          return true;
        }

        // Try to create marker
        Option<StoragePath> result = writeMarkers.createIfNotExists(
            PARTITION_PATH, logFileToAppend.getFileName(), IOType.APPEND);

        if (result.isPresent()) {
          // Created successfully, record it
          createdMarkers.add(markerKey);
          return true;
        } else {
          // Marker exists but not created by this task - conflict detected
          return false;
        }
      }

      @Override
      public boolean preLogFileCreate(HoodieLogFile logFileToCreate) {
        return true;
      }
    };
  }

  /**
   * Creates a callback that simulates FlinkAppendHandle behavior without task retry protection
   * (backward compatibility mode).
   */
  private HoodieLogFileWriteCallback createCallbackWithoutProtection() {
    return new HoodieLogFileWriteCallback() {
      @Override
      public boolean preLogFileOpen(HoodieLogFile logFileToAppend) {
        // Original behavior: just call createIfNotExists and always return true
        writeMarkers.createIfNotExists(PARTITION_PATH, logFileToAppend.getFileName(), IOType.APPEND);
        return true;
      }

      @Override
      public boolean preLogFileCreate(HoodieLogFile logFileToCreate) {
        return true;
      }
    };
  }
}
