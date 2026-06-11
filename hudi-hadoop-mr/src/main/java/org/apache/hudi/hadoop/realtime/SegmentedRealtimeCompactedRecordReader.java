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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.utils.HiveAvroSerializer;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Segmented implementation of merged realtime reader for MOR tables.
 *
 * <p>This reader scans and merges base records in bounded segments to avoid building
 * a full key-to-record map for all base keys upfront.</p>
 */
public class SegmentedRealtimeCompactedRecordReader extends AbstractRealtimeRecordReader
    implements RecordReader<NullWritable, ArrayWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentedRealtimeCompactedRecordReader.class);

  private final RecordReader<NullWritable, ArrayWritable> parquetReader;
  private final HoodieMergedLogRecordScanner mergedLogRecordScanner;
  private final int recordKeyIndex;
  private final int segmentMaxKeys;
  private final long segmentMaxBytes;
  private final SegmentLogScanMode segmentLogScanMode;
  private final ExternalSpillableMap<String, Boolean> consumedBaseKeys;

  private final List<BaseSegmentRecord> segmentRecords = new ArrayList<>();
  private int segmentCursor = 0;
  private boolean parquetExhausted = false;
  private boolean logOnlyScanInitialized = false;
  private Iterator<String> logOnlyIterator;

  private long segmentCount = 0;
  private long segmentPeakKeys = 0;
  private long segmentPeakBytes = 0;
  private long logRescanRounds = 0;
  private long totalSegmentMergeTimeMs = 0;

  public SegmentedRealtimeCompactedRecordReader(RealtimeSplit split, JobConf job,
                                                RecordReader<NullWritable, ArrayWritable> realReader) throws IOException {
    super(split, job);
    this.parquetReader = realReader;
    this.recordKeyIndex = split.getVirtualKeyInfo()
        .map(HoodieVirtualKeyInfo::getRecordKeyFieldIndex)
        .orElse(HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS);
    this.segmentMaxKeys = Math.max(1, job.getInt(HoodieRealtimeConfig.SEGMENTED_MERGE_MAX_KEYS_PROP,
        HoodieRealtimeConfig.DEFAULT_SEGMENTED_MERGE_MAX_KEYS));
    this.segmentMaxBytes = Math.max(1024L, job.getLong(HoodieRealtimeConfig.SEGMENTED_MERGE_MAX_BYTES_PROP,
        HoodieRealtimeConfig.DEFAULT_SEGMENTED_MERGE_MAX_BYTES));
    this.segmentLogScanMode = SegmentLogScanMode.fromConfig(
        job.get(HoodieRealtimeConfig.SEGMENTED_MERGE_LOG_SCAN_MODE_PROP,
            HoodieRealtimeConfig.DEFAULT_SEGMENTED_MERGE_LOG_SCAN_MODE));
    this.mergedLogRecordScanner = getMergedLogRecordScanner(false);
    this.consumedBaseKeys = createConsumedBaseKeysTracker();
  }

  private ExternalSpillableMap<String, Boolean> createConsumedBaseKeysTracker() {
    long maxCompactionMemoryInBytes = HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes(jobConf);
    long trackerMemory = Math.max(16L * 1024L * 1024L, maxCompactionMemoryInBytes / 8);
    String spillableMapBasePath = jobConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP,
        HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH);
    ExternalSpillableMap.DiskMapType diskMapType = jobConf.getEnum(
        HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(),
        HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue());
    boolean bitCaskCompressionEnabled = jobConf.getBoolean(
        HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue());
    try {
      return new ExternalSpillableMap<>(trackerMemory, spillableMapBasePath,
          new DefaultSizeEstimator<>(), new DefaultSizeEstimator<>(), diskMapType, bitCaskCompressionEnabled);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create consumed base key tracker", e);
    }
  }

  private HoodieMergedLogRecordScanner getMergedLogRecordScanner(boolean forceFullScan) throws IOException {
    return HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(HoodieStorageUtils.getStorage(
            split.getPath().toString(), HadoopFSUtils.getStorageConf(jobConf)))
        .withBasePath(split.getBasePath())
        .withLogFilePaths(split.getDeltaLogPaths())
        .withReaderSchema(getLogScannerReaderSchema())
        .withLatestInstantTime(split.getMaxCommitTime())
        .withMaxMemorySizeInBytes(HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes(jobConf))
        .withReverseReader(false)
        .withBufferSize(jobConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP,
            HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withSpillableMapBasePath(jobConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP,
            HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))
        .withDiskMapType(jobConf.getEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(),
            HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue()))
        .withBitCaskDiskMapCompressionEnabled(jobConf.getBoolean(
            HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
            HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))
        .withOptimizedLogBlocksScan(jobConf.getBoolean(HoodieRealtimeConfig.ENABLE_OPTIMIZED_LOG_BLOCKS_SCAN, false))
        .withPrecomputedTimelineState(getPrecomputedTimelineState())
        .withInternalSchema(schemaEvolutionContext.internalSchemaOption.orElse(InternalSchema.getEmptyInternalSchema()))
        .withForceFullScan(forceFullScan)
        .build();
  }

  @Override
  public boolean next(NullWritable nullWritable, ArrayWritable arrayWritable) throws IOException {
    while (true) {
      if (segmentCursor < segmentRecords.size()) {
        BaseSegmentRecord baseRecord = segmentRecords.get(segmentCursor++);
        String key = baseRecord.key;
        HoodieRecord<?> deltaRecord = mergedLogRecordScanner.getRecords().get(key);
        if (deltaRecord != null) {
          mergedLogRecordScanner.getRecords().remove(key);
          Option<HoodieAvroIndexedRecord> rec = supportPayload
              ? mergeRecord(deltaRecord, baseRecord.arrayWritable)
              : buildGenericRecordwithCustomPayload(deltaRecord);
          if (!rec.isPresent()) {
            continue;
          }
          setUpWritable(rec, arrayWritable, key);
          return true;
        }
        copyArrayWritable(baseRecord.arrayWritable, arrayWritable);
        return true;
      }

      if (!parquetExhausted && loadNextSegment()) {
        continue;
      }

      if (!logOnlyScanInitialized) {
        initializeLogOnlyScan();
      }

      while (logOnlyIterator != null && logOnlyIterator.hasNext()) {
        String key = logOnlyIterator.next();
        if (consumedBaseKeys.containsKey(key)) {
          continue;
        }
        HoodieRecord<?> record = mergedLogRecordScanner.getRecords().get(key);
        if (record == null) {
          continue;
        }
        Option<HoodieAvroIndexedRecord> rec = buildGenericRecordwithCustomPayload(record);
        if (!rec.isPresent()) {
          continue;
        }
        setUpWritable(rec, arrayWritable, key);
        return true;
      }
      return false;
    }
  }

  private void initializeLogOnlyScan() {
    if (logOnlyScanInitialized) {
      return;
    }
    logOnlyScanInitialized = true;
    long startMs = System.currentTimeMillis();
    mergedLogRecordScanner.scan();
    logRescanRounds++;
    logOnlyIterator = mergedLogRecordScanner.getRecords().keySet().iterator();
    LOG.info("Segmented realtime log-only scan initialized; segment_count={}, segment_peak_keys={}, segment_peak_bytes={}, "
            + "log_rescan_rounds={}, segment_merge_time_ms={}, initialization_time_ms={}",
        segmentCount, segmentPeakKeys, segmentPeakBytes, logRescanRounds, totalSegmentMergeTimeMs,
        System.currentTimeMillis() - startMs);
  }

  private boolean loadNextSegment() throws IOException {
    if (parquetExhausted) {
      return false;
    }

    segmentRecords.clear();
    segmentCursor = 0;
    List<String> segmentKeys = new ArrayList<>();
    Set<String> dedupedKeys = new HashSet<>();
    long bufferedBytes = 0;

    NullWritable key = parquetReader.createKey();
    while (segmentRecords.size() < segmentMaxKeys && bufferedBytes < segmentMaxBytes) {
      ArrayWritable value = parquetReader.createValue();
      if (!parquetReader.next(key, value)) {
        parquetExhausted = true;
        break;
      }

      String recordKey = value.get()[recordKeyIndex].toString();
      segmentRecords.add(new BaseSegmentRecord(recordKey, value));
      consumedBaseKeys.put(recordKey, Boolean.TRUE);
      if (dedupedKeys.add(recordKey)) {
        segmentKeys.add(recordKey);
      }
      bufferedBytes += estimateRecordSize(recordKey, value);
      key = parquetReader.createKey();
    }

    if (segmentRecords.isEmpty()) {
      return false;
    }

    segmentCount++;
    segmentPeakKeys = Math.max(segmentPeakKeys, segmentKeys.size());
    segmentPeakBytes = Math.max(segmentPeakBytes, bufferedBytes);

    long segmentStartMs = System.currentTimeMillis();
    if (segmentLogScanMode == SegmentLogScanMode.PREFIX) {
      mergedLogRecordScanner.scanByKeyPrefixes(segmentKeys);
    } else {
      mergedLogRecordScanner.scanByFullKeys(segmentKeys);
    }
    logRescanRounds++;
    totalSegmentMergeTimeMs += (System.currentTimeMillis() - segmentStartMs);
    return true;
  }

  private long estimateRecordSize(String key, ArrayWritable value) {
    long size = key == null ? 0 : (long) key.length() * 2;
    Writable[] writables = value.get();
    if (writables != null) {
      size += writables.length * 16L;
    }
    return Math.max(64L, size);
  }

  private void copyArrayWritable(ArrayWritable source, ArrayWritable target) {
    Writable[] src = source.get();
    target.set(Arrays.copyOf(src, src.length));
  }

  private Option<HoodieAvroIndexedRecord> buildGenericRecordwithCustomPayload(HoodieRecord<?> record) throws IOException {
    if (usesCustomPayload) {
      return record.toIndexedRecord(getWriterSchema(), payloadProps);
    } else {
      return record.toIndexedRecord(getReaderSchema(), payloadProps);
    }
  }

  private Option<HoodieAvroIndexedRecord> mergeRecord(HoodieRecord<?> newRecord, ArrayWritable writableFromParquet)
      throws IOException {
    GenericRecord oldRecord = convertArrayWritableToHoodieRecord(writableFromParquet);
    GenericRecord genericRecord = HiveAvroSerializer.rewriteRecordIgnoreResultCheck(oldRecord, getLogScannerReaderSchema());
    HoodieRecord<?> record = new HoodieAvroIndexedRecord(genericRecord);
    Option<Pair<HoodieRecord, Schema>> mergeResult = HoodieAvroRecordMerger.INSTANCE.merge(record,
        genericRecord.getSchema(), newRecord, getLogScannerReaderSchema(), payloadProps);
    return mergeResult.map(p -> (HoodieAvroIndexedRecord) p.getLeft());
  }

  private GenericRecord convertArrayWritableToHoodieRecord(ArrayWritable arrayWritable) {
    return serializer.serialize(arrayWritable, getHiveSchema());
  }

  private void setUpWritable(Option<HoodieAvroIndexedRecord> rec, ArrayWritable arrayWritable, String key) {
    GenericRecord recordToReturn = (GenericRecord) rec.get().getData();
    if (usesCustomPayload) {
      recordToReturn = HoodieAvroUtils.rewriteRecord((GenericRecord) rec.get().getData(), getReaderSchema());
    }
    ArrayWritable aWritable = (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(
        recordToReturn, getHiveSchema(), isSupportTimestamp());
    Writable[] replaceValue = aWritable.get();
    if (LOG.isDebugEnabled()) {
      LOG.debug("key {}, base values: {}, log values: {}", key,
          HoodieRealtimeRecordReaderUtils.arrayWritableToString(arrayWritable),
          HoodieRealtimeRecordReaderUtils.arrayWritableToString(aWritable));
    }
    Writable[] originalValue = arrayWritable.get();
    try {
      System.arraycopy(replaceValue, 0, originalValue, 0, Math.min(originalValue.length, replaceValue.length));
      arrayWritable.set(originalValue);
    } catch (RuntimeException re) {
      LOG.error("Got exception when doing array copy", re);
      LOG.error("Base record :{}", HoodieRealtimeRecordReaderUtils.arrayWritableToString(arrayWritable));
      LOG.error("Log record :{}", HoodieRealtimeRecordReaderUtils.arrayWritableToString(aWritable));
      String errMsg = "Base-record :" + HoodieRealtimeRecordReaderUtils.arrayWritableToString(arrayWritable)
          + " ,Log-record :" + HoodieRealtimeRecordReaderUtils.arrayWritableToString(aWritable) + " ,Error :" + re.getMessage();
      throw new RuntimeException(errMsg, re);
    }
  }

  @Override
  public NullWritable createKey() {
    return parquetReader.createKey();
  }

  @Override
  public ArrayWritable createValue() {
    return parquetReader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return parquetReader.getPos();
  }

  @Override
  public void close() throws IOException {
    parquetReader.close();
    mergedLogRecordScanner.close();
    consumedBaseKeys.close();
    LOG.info("Segmented realtime reader closed; segment_count={}, segment_peak_keys={}, segment_peak_bytes={}, "
            + "log_rescan_rounds={}, segment_merge_time_ms={}",
        segmentCount, segmentPeakKeys, segmentPeakBytes, logRescanRounds, totalSegmentMergeTimeMs);
  }

  @Override
  public float getProgress() throws IOException {
    return parquetReader.getProgress();
  }

  private enum SegmentLogScanMode {
    FULLKEYS,
    PREFIX;

    private static SegmentLogScanMode fromConfig(String mode) {
      if (mode == null) {
        return FULLKEYS;
      }
      try {
        return SegmentLogScanMode.valueOf(mode.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        LOG.warn("Unsupported segment log scan mode {}, defaulting to FULLKEYS", mode);
        return FULLKEYS;
      }
    }
  }

  private static class BaseSegmentRecord {
    private final String key;
    private final ArrayWritable arrayWritable;

    private BaseSegmentRecord(String key, ArrayWritable arrayWritable) {
      this.key = key;
      this.arrayWritable = arrayWritable;
    }
  }
}
