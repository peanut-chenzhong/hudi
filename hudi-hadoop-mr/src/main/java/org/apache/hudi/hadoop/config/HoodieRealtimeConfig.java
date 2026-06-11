/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hadoop.config;

import org.apache.hudi.common.config.HoodieMetadataConfig;

/**
 * Class to hold props related to Hoodie RealtimeInputFormat and RealtimeRecordReader.
 */
public final class HoodieRealtimeConfig {

  // Fraction of mapper/reducer task memory used for compaction of log files
  public static final String COMPACTION_MEMORY_FRACTION_PROP = "compaction.memory.fraction";
  public static final String DEFAULT_COMPACTION_MEMORY_FRACTION = "0.75";
  // used to choose a trade off between IO vs Memory when performing compaction process
  // Depending on outputfile size and memory provided, choose true to avoid OOM for large file
  // size + small memory
  public static final String COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP = "compaction.lazy.block.read.enabled";
  public static final String DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED = "true";

  // Property to set the max memory for dfs inputstream buffer size
  public static final String MAX_DFS_STREAM_BUFFER_SIZE_PROP = "hoodie.memory.dfs.buffer.max.size";
  // Setting this to lower value of 1 MB since no control over how many RecordReaders will be started in a mapper
  public static final int DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE = 1024 * 1024; // 1 MB
  // Property to set file path prefix for spillable file
  public static final String SPILLABLE_MAP_BASE_PATH_PROP = "hoodie.memory.spillable.map.path";
  // Default file path prefix for spillable file
  public static final String DEFAULT_SPILLABLE_MAP_BASE_PATH = "/tmp/";
  public static final String ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN =
      "hoodie" + HoodieMetadataConfig.OPTIMIZED_LOG_BLOCKS_SCAN;

  // Precompute timeline state on driver side and serialize to realtime splits.
  public static final String PRELOAD_SPLIT_TIMELINE_STATE = "hoodie.realtime.split.timeline.preload.enabled";
  public static final boolean DEFAULT_PRELOAD_SPLIT_TIMELINE_STATE = false;

  // Property to enable segmented merge for realtime MOR reads.
  public static final String SEGMENTED_MERGE_READ_ENABLED_PROP = "hoodie.realtime.merge.segmented.enabled";
  public static final boolean DEFAULT_SEGMENTED_MERGE_READ_ENABLED = false;

  // Maximum number of base records to process in one segment.
  public static final String SEGMENTED_MERGE_MAX_KEYS_PROP = "hoodie.realtime.merge.segment.max.keys";
  public static final int DEFAULT_SEGMENTED_MERGE_MAX_KEYS = 200_000;

  // Maximum estimated buffered bytes to process in one segment.
  public static final String SEGMENTED_MERGE_MAX_BYTES_PROP = "hoodie.realtime.merge.segment.max.bytes";
  public static final long DEFAULT_SEGMENTED_MERGE_MAX_BYTES = 128L * 1024L * 1024L;

  // Segment log scan mode: FULLKEYS or PREFIX.
  public static final String SEGMENTED_MERGE_LOG_SCAN_MODE_PROP = "hoodie.realtime.merge.segment.log.scan.mode";
  public static final String DEFAULT_SEGMENTED_MERGE_LOG_SCAN_MODE = "FULLKEYS";
}
