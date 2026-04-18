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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodiePreCombineAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestAbstractHoodieLogRecordReaderTimelineCache {

  private static final Schema TEST_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}");

  @BeforeEach
  void setUp() {
    AbstractHoodieLogRecordReader.resetTimelineCacheForTest();
  }

  @Test
  void testTimelineLoadedOnceWhenCacheEnabledV1() {
    HoodieTableMetaClient metaClient = createMetaClient();
    TestLogRecordReader first = createReader(metaClient, false, true);
    TestLogRecordReader second = createReader(metaClient, false, true);

    first.scanOnce();
    second.scanOnce();

    assertEquals(1L, AbstractHoodieLogRecordReader.getTimelineCacheLoadCountForTest());
  }

  @Test
  void testTimelineLoadedTwiceWhenCacheDisabled() {
    HoodieTableMetaClient metaClient = createMetaClient();
    TestLogRecordReader first = createReader(metaClient, false, false);
    TestLogRecordReader second = createReader(metaClient, false, false);

    first.scanOnce();
    second.scanOnce();

    assertEquals(2L, AbstractHoodieLogRecordReader.getTimelineCacheLoadCountForTest());
  }

  @Test
  void testTimelineLoadedOnceWhenCacheEnabledV2() {
    HoodieTableMetaClient metaClient = createMetaClient();
    TestLogRecordReader first = createReader(metaClient, true, true);
    TestLogRecordReader second = createReader(metaClient, true, true);

    first.scanOnce();
    second.scanOnce();

    assertEquals(1L, AbstractHoodieLogRecordReader.getTimelineCacheLoadCountForTest());
  }

  private static TestLogRecordReader createReader(HoodieTableMetaClient metaClient, boolean optimizedScan, boolean timelineCacheEnabled) {
    return new TestLogRecordReader(metaClient, optimizedScan, timelineCacheEnabled);
  }

  private static HoodieTableMetaClient createMetaClient() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    HoodieTimeline inflightTimeline = mock(HoodieTimeline.class);

    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getTimelineLayoutVersion()).thenReturn(new TimelineLayoutVersion(1));
    when(metaClient.getCommitsTimeline()).thenReturn(commitsTimeline);
    when(commitsTimeline.filterCompletedInstants()).thenReturn(completedTimeline);
    when(commitsTimeline.filterInflights()).thenReturn(inflightTimeline);
    when(tableConfig.getPayloadClass()).thenReturn("org.apache.hudi.common.model.OverwriteWithLatestAvroPayload");
    when(tableConfig.getPreCombineField()).thenReturn(null);
    when(tableConfig.populateMetaFields()).thenReturn(true);
    return metaClient;
  }

  private static class TestLogRecordReader extends AbstractHoodieLogRecordReader {

    private TestLogRecordReader(HoodieTableMetaClient metaClient, boolean optimizedScan, boolean timelineCacheEnabled) {
      super(
          null,
          "file:///tmp/hudi-test",
          Collections.emptyList(),
          TEST_SCHEMA,
          "20240101010101",
          false,
          1024,
          Option.empty(),
          false,
          true,
          Option.empty(),
          InternalSchema.getEmptyInternalSchema(),
          Option.empty(),
          optimizedScan,
          timelineCacheEnabled,
          0L,
          HoodiePreCombineAvroRecordMerger.INSTANCE,
          Option.of(metaClient));
    }

    private void scanOnce() {
      scanInternal(Option.empty(), false);
    }

    @Override
    public <T> void processNextRecord(HoodieRecord<T> hoodieRecord) {
      // no-op
    }

    @Override
    protected void processNextDeletedRecord(DeleteRecord deleteRecord) {
      // no-op
    }
  }
}
