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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Different concurrency modes for write operations.
 */
@EnumDescription("Concurrency modes for write operations.")
public enum WriteConcurrencyMode {
  // Only a single writer can perform write ops
  @EnumFieldDescription("Only one active writer to the table. Maximizes throughput.")
  SINGLE_WRITER,

  // Multiple writer can perform write ops with lazy conflict resolution using locks
  @EnumFieldDescription("Multiple writers can operate on the table with lazy conflict resolution "
      + "using locks. This means that only one writer succeeds if multiple writers write to the "
      + "same file group.")
  OPTIMISTIC_CONCURRENCY_CONTROL,

  // Partition-level optimistic concurrency control: multiple writers can write to different
  // partitions concurrently. Automatically enables partition-based early conflict detection,
  // partition-level conflict resolution, and expired-heartbeat partition conflict checking.
  @EnumFieldDescription("Partition-level optimistic concurrency control. Multiple writers can write "
      + "to different partitions concurrently without conflict. This mode automatically configures: "
      + "(1) partition-based early conflict detection strategy, "
      + "(2) partition-level conflict resolution strategy, "
      + "(3) expired heartbeat partition conflict checking, "
      + "and (4) LAZY failed writes cleaner policy. "
      + "Writers targeting the same partition will be detected early and fail fast.")
  OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT;

  public boolean supportsOptimisticConcurrencyControl() {
    return this == OPTIMISTIC_CONCURRENCY_CONTROL
        || this == OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT;
  }

  public boolean isPartitionLevelConcurrency() {
    return this == OPTIMISTIC_CONCURRENCY_CONTROL_PARTITION_LIMIT;
  }
}
