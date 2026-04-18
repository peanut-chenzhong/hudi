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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.InputSplitUtils;
import org.apache.hudi.storage.StoragePath;

import java.io.EOFException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Realtime Input Split Interface.
 */
public interface RealtimeSplit extends InputSplitWithLocationInfo {

  /**
   * Return Log File Paths.
   * @return
   */
  default List<String> getDeltaLogPaths() {
    return getDeltaLogFiles().stream().map(entry -> entry.getPath().toString()).collect(Collectors.toList());
  }

  List<HoodieLogFile> getDeltaLogFiles();

  void setDeltaLogFiles(List<HoodieLogFile> deltaLogFiles);

  /**
   * Return Max Instant Time.
   * @return
   */
  String getMaxCommitTime();

  /**
   * Return Base Path of the dataset.
   * @return
   */
  String getBasePath();

  /**
   * Returns Virtual key info if meta fields are disabled.
   * @return
   */
  Option<HoodieVirtualKeyInfo> getVirtualKeyInfo();

  Option<RealtimeSplitTimelineState> getRealtimeSplitTimelineState();

  /**
   * Returns the flag whether this split belongs to an Incremental Query
   */
  boolean getBelongsToIncrementalQuery();

  /**
   * Update Maximum valid instant time.
   * @param maxCommitTime
   */
  void setMaxCommitTime(String maxCommitTime);

  /**
   * Set Base Path.
   * @param basePath
   */
  void setBasePath(String basePath);

  /**
   * Sets the flag whether this split belongs to an Incremental Query
   */
  void setBelongsToIncrementalQuery(boolean belongsToIncrementalQuery);

  void setVirtualKeyInfo(Option<HoodieVirtualKeyInfo> virtualKeyInfo);

  void setRealtimeSplitTimelineState(Option<RealtimeSplitTimelineState> realtimeSplitTimelineStateOpt);

  default void writeToOutput(DataOutput out) throws IOException {
    InputSplitUtils.writeString(getBasePath(), out);
    InputSplitUtils.writeString(getMaxCommitTime(), out);
    InputSplitUtils.writeBoolean(getBelongsToIncrementalQuery(), out);

    out.writeInt(getDeltaLogFiles().size());
    for (HoodieLogFile logFile : getDeltaLogFiles()) {
      InputSplitUtils.writeString(logFile.getPath().toString(), out);
      out.writeLong(logFile.getFileSize());
    }

    Option<HoodieVirtualKeyInfo> virtualKeyInfoOpt = getVirtualKeyInfo();
    if (!virtualKeyInfoOpt.isPresent()) {
      InputSplitUtils.writeBoolean(false, out);
    } else {
      InputSplitUtils.writeBoolean(true, out);
      InputSplitUtils.writeString(virtualKeyInfoOpt.get().getRecordKeyField(), out);
      InputSplitUtils.writeString(String.valueOf(virtualKeyInfoOpt.get().getRecordKeyFieldIndex()), out);
      InputSplitUtils.writeBoolean(virtualKeyInfoOpt.get().getPartitionPathField().isPresent(), out);
      if (virtualKeyInfoOpt.get().getPartitionPathField().isPresent()) {
        InputSplitUtils.writeString(virtualKeyInfoOpt.get().getPartitionPathField().get(), out);
        InputSplitUtils.writeString(String.valueOf(virtualKeyInfoOpt.get().getPartitionPathFieldIndex()), out);
      }
    }

    Option<RealtimeSplitTimelineState> timelineStateOpt = getRealtimeSplitTimelineState();
    if (!timelineStateOpt.isPresent()) {
      InputSplitUtils.writeBoolean(false, out);
      return;
    }

    InputSplitUtils.writeBoolean(true, out);
    RealtimeSplitTimelineState timelineState = timelineStateOpt.get();
    InputSplitUtils.writeBoolean(timelineState.getCompletedTimelineStartInstant() != null, out);
    if (timelineState.getCompletedTimelineStartInstant() != null) {
      InputSplitUtils.writeString(timelineState.getCompletedTimelineStartInstant(), out);
    }
    out.writeInt(timelineState.getCompletedInstants().size());
    for (String instant : timelineState.getCompletedInstants()) {
      InputSplitUtils.writeString(instant, out);
    }
    out.writeInt(timelineState.getInflightInstants().size());
    for (String instant : timelineState.getInflightInstants()) {
      InputSplitUtils.writeString(instant, out);
    }
  }

  default void readFromInput(DataInput in) throws IOException {
    setBasePath(InputSplitUtils.readString(in));
    setMaxCommitTime(InputSplitUtils.readString(in));
    setBelongsToIncrementalQuery(InputSplitUtils.readBoolean(in));

    int totalLogFiles = in.readInt();
    List<HoodieLogFile> deltaLogPaths = new ArrayList<>(totalLogFiles);
    for (int i = 0; i < totalLogFiles; i++) {
      String logFilePath = InputSplitUtils.readString(in);
      long logFileSize = in.readLong();
      deltaLogPaths.add(new HoodieLogFile(new StoragePath(logFilePath), logFileSize));
    }
    setDeltaLogFiles(deltaLogPaths);

    boolean hoodieVirtualKeyPresent = InputSplitUtils.readBoolean(in);
    if (hoodieVirtualKeyPresent) {
      String recordKeyField = InputSplitUtils.readString(in);
      int recordFieldIndex = Integer.parseInt(InputSplitUtils.readString(in));
      boolean isPartitionPathFieldPresent = InputSplitUtils.readBoolean(in);
      Option<String> partitionPathField = isPartitionPathFieldPresent ? Option.of(InputSplitUtils.readString(in)) : Option.empty();
      Option<Integer> partitionPathIndex = isPartitionPathFieldPresent ? Option.of(Integer.parseInt(InputSplitUtils.readString(in))) : Option.empty();
      setVirtualKeyInfo(Option.of(new HoodieVirtualKeyInfo(recordKeyField, partitionPathField, recordFieldIndex, partitionPathIndex)));
    }

    try {
      boolean timelineStatePresent = InputSplitUtils.readBoolean(in);
      if (!timelineStatePresent) {
        setRealtimeSplitTimelineState(Option.empty());
        return;
      }

      boolean timelineStartPresent = InputSplitUtils.readBoolean(in);
      String timelineStart = timelineStartPresent ? InputSplitUtils.readString(in) : null;
      int completedSize = in.readInt();
      Set<String> completedInstants = new HashSet<>(completedSize);
      for (int i = 0; i < completedSize; i++) {
        completedInstants.add(InputSplitUtils.readString(in));
      }
      int inflightSize = in.readInt();
      Set<String> inflightInstants = new HashSet<>(inflightSize);
      for (int i = 0; i < inflightSize; i++) {
        inflightInstants.add(InputSplitUtils.readString(in));
      }
      setRealtimeSplitTimelineState(Option.of(new RealtimeSplitTimelineState(timelineStart, completedInstants, inflightInstants)));
    } catch (EOFException eofException) {
      setRealtimeSplitTimelineState(Option.empty());
    }
  }

  /**
   * The file containing this split's data.
   */
  Path getPath();

  /**
   * The position of the first byte in the file to process.
   */
  long getStart();

  /**
   * The number of bytes in the file to process.
   */
  long getLength();
}
