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

package org.apache.hudi.examples.datagen.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data generator that keeps a key pool per table and emits 20% inserts + 80% updates by default.
 */
public class MultiTableDataGenerator {

  private static final DecimalType DECIMAL_TYPE = DataTypes.createDecimalType(20, 6);
  private static final StructType NESTED_STRUCT = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("nested_str", DataTypes.StringType, false),
      DataTypes.createStructField("nested_int", DataTypes.IntegerType, false)
  });
  private static final StructType ROW_SCHEMA = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField("id", DataTypes.StringType, false),
      DataTypes.createStructField("ts", DataTypes.LongType, false),
      DataTypes.createStructField("dt", DataTypes.StringType, false),
      DataTypes.createStructField("tiny_col", DataTypes.ByteType, false),
      DataTypes.createStructField("short_col", DataTypes.ShortType, false),
      DataTypes.createStructField("int_col", DataTypes.IntegerType, false),
      DataTypes.createStructField("long_col", DataTypes.LongType, false),
      DataTypes.createStructField("float_col", DataTypes.FloatType, false),
      DataTypes.createStructField("double_col", DataTypes.DoubleType, false),
      DataTypes.createStructField("decimal_col", DECIMAL_TYPE, false),
      DataTypes.createStructField("bool_col", DataTypes.BooleanType, false),
      DataTypes.createStructField("str_col", DataTypes.StringType, false),
      DataTypes.createStructField("binary_col", DataTypes.BinaryType, false),
      DataTypes.createStructField("date_col", DataTypes.DateType, false),
      DataTypes.createStructField("timestamp_col", DataTypes.TimestampType, false),
      DataTypes.createStructField("array_col", DataTypes.createArrayType(DataTypes.StringType, false), false),
      DataTypes.createStructField("map_col", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false), false),
      DataTypes.createStructField("struct_col", NESTED_STRUCT, false),
      DataTypes.createStructField("op_type", DataTypes.StringType, false)
  });

  private final StreamingJobConfig config;
  private final Random random = new Random();
  private final Map<String, List<String>> tableKeyPool = new ConcurrentHashMap<>();

  public MultiTableDataGenerator(StreamingJobConfig config) {
    this.config = config;
  }

  public Dataset<Row> generateBatch(SparkSession spark,
                                    StreamingJobConfig.TableTarget tableTarget,
                                    long batchId) {
    int total = config.getRecordsPerBatch();
    List<String> keyPool = tableKeyPool.computeIfAbsent(tableTarget.getTableName(), k -> new ArrayList<>());
    int insertCount = keyPool.isEmpty() ? total : (int) Math.round(total * config.getInsertRatio());
    insertCount = Math.min(insertCount, total);
    int updateCount = total - insertCount;

    List<Row> rows = new ArrayList<>(total);
    synchronized (keyPool) {
      for (int i = 0; i < insertCount; i++) {
        String newKey = UUID.randomUUID().toString();
        keyPool.add(newKey);
        rows.add(buildRow(newKey, true, batchId, i));
      }

      for (int i = 0; i < updateCount; i++) {
        if (keyPool.isEmpty()) {
          String fallbackKey = UUID.randomUUID().toString();
          keyPool.add(fallbackKey);
          rows.add(buildRow(fallbackKey, true, batchId, insertCount + i));
        } else {
          String existingKey = keyPool.get(random.nextInt(keyPool.size()));
          rows.add(buildRow(existingKey, false, batchId, insertCount + i));
        }
      }
    }

    return spark.createDataFrame(rows, ROW_SCHEMA);
  }

  private Row buildRow(String key, boolean isInsert, long batchId, int sequence) {
    long eventTs = System.currentTimeMillis() + sequence;
    LocalDate eventDate = LocalDate.now().minusDays(random.nextInt(30));
    String partitionValue = eventDate.toString();
    Map<String, Integer> mapCol = new HashMap<>();
    mapCol.put("k1", sequence);
    mapCol.put("k2", sequence + 1);
    Row nested = RowFactory.create("nested_" + key.substring(0, 8), sequence);
    BigDecimal decimal = BigDecimal.valueOf(sequence).movePointLeft(3).setScale(6);

    return RowFactory.create(
        key,
        eventTs,
        partitionValue,
        (byte) (sequence % 100),
        (short) (sequence % 1000),
        sequence,
        eventTs + 1000,
        random.nextFloat(),
        random.nextDouble(),
        decimal,
        sequence % 2 == 0,
        "payload_" + batchId + "_" + sequence,
        ("bin_" + key).getBytes(StandardCharsets.UTF_8),
        Date.valueOf(eventDate),
        Timestamp.from(Instant.ofEpochMilli(eventTs)),
        Arrays.asList("a_" + sequence, "b_" + sequence),
        mapCol,
        nested,
        isInsert ? "insert" : "update");
  }
}
