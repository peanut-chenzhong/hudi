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

import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Configuration holder for the configurable Spark Structured Streaming Hudi writer.
 */
public class StreamingJobConfig {

  private static final String HUDI_OPTION_PREFIX = "hudi.option.";

  private final String appName;
  private final String basePath;
  private final String checkpointBasePath;
  private final int triggerIntervalSec;
  private final int recordsPerBatch;
  private final int numTables;
  private final int maxConcurrentTableWrites;
  private final String database;
  private final String tablePrefix;
  private final int startIndex;
  private final String tableType;
  private final String indexType;
  private final boolean partitioned;
  private final String partitionField;
  private final String recordKeyField;
  private final String precombineField;
  private final double insertRatio;
  private final Map<String, String> customHudiOptions;

  private StreamingJobConfig(String appName,
                             String basePath,
                             String checkpointBasePath,
                             int triggerIntervalSec,
                             int recordsPerBatch,
                             int numTables,
                             int maxConcurrentTableWrites,
                             String database,
                             String tablePrefix,
                             int startIndex,
                             String tableType,
                             String indexType,
                             boolean partitioned,
                             String partitionField,
                             String recordKeyField,
                             String precombineField,
                             double insertRatio,
                             Map<String, String> customHudiOptions) {
    this.appName = appName;
    this.basePath = stripTrailingSlash(basePath);
    this.checkpointBasePath = stripTrailingSlash(checkpointBasePath);
    this.triggerIntervalSec = triggerIntervalSec;
    this.recordsPerBatch = recordsPerBatch;
    this.numTables = numTables;
    this.maxConcurrentTableWrites = maxConcurrentTableWrites;
    this.database = database;
    this.tablePrefix = tablePrefix;
    this.startIndex = startIndex;
    this.tableType = tableType;
    this.indexType = indexType;
    this.partitioned = partitioned;
    this.partitionField = partitionField;
    this.recordKeyField = recordKeyField;
    this.precombineField = precombineField;
    this.insertRatio = insertRatio;
    this.customHudiOptions = new LinkedHashMap<>(customHudiOptions);
  }

  public static StreamingJobConfig fromFile(String configPath) throws IOException {
    Properties properties = new Properties();
    try (InputStream inputStream = Files.newInputStream(Paths.get(configPath))) {
      properties.load(inputStream);
    }

    Map<String, String> customHudiOptions = extractByPrefix(properties, HUDI_OPTION_PREFIX);
    StreamingJobConfig config = new StreamingJobConfig(
        getString(properties, "spark.app.name", "hudi-streaming-multi-table-writer"),
        getRequired(properties, "table.base.path"),
        getRequired(properties, "checkpoint.base.path"),
        getInt(properties, "stream.trigger.interval.sec", 10),
        getInt(properties, "stream.records.per.batch", 5000),
        getInt(properties, "tables.count", 1),
        getInt(properties, "tables.max.concurrent.writes", 1),
        getString(properties, "tables.database", "default"),
        getString(properties, "tables.prefix", "hudi_stream_tbl_"),
        getInt(properties, "tables.start.index", 1),
        getString(properties, "table.type", "MERGE_ON_READ"),
        getString(properties, "table.index.type", "BLOOM"),
        getBoolean(properties, "table.partitioned", true),
        getString(properties, "table.partition.field", "dt"),
        getString(properties, "table.recordkey.field", "id"),
        getString(properties, "table.precombine.field", "ts"),
        getDouble(properties, "ratio.insert", 0.2),
        customHudiOptions);
    config.validate();
    return config;
  }

  public List<TableTarget> buildTableTargets() {
    List<TableTarget> targets = new ArrayList<>();
    for (int i = 0; i < numTables; i++) {
      int sequence = startIndex + i;
      String tableName = tablePrefix + sequence;
      String tablePath = basePath + "/" + database + "/" + tableName;
      String checkpointPath = checkpointBasePath + "/" + database + "/" + tableName;
      targets.add(new TableTarget(database, tableName, tablePath, checkpointPath));
    }
    return targets;
  }

  public Map<String, String> buildHudiOptions(TableTarget tableTarget) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("hoodie.table.name", tableTarget.getTableName());
    options.put("hoodie.database.name", tableTarget.getDatabase());
    options.put("hoodie.datasource.write.operation", "upsert");
    options.put("hoodie.datasource.write.recordkey.field", recordKeyField);
    options.put("hoodie.datasource.write.precombine.field", precombineField);
    options.put("hoodie.datasource.write.table.type", tableType);
    options.put("hoodie.index.type", indexType);

    if (partitioned) {
      options.put("hoodie.datasource.write.partitionpath.field", partitionField);
      options.put("hoodie.datasource.write.keygenerator.class", SimpleKeyGenerator.class.getName());
    } else {
      options.put("hoodie.datasource.write.keygenerator.class", NonpartitionedKeyGenerator.class.getName());
    }

    options.putAll(customHudiOptions);
    return options;
  }

  public String getAppName() {
    return appName;
  }

  public int getTriggerIntervalSec() {
    return triggerIntervalSec;
  }

  public int getRecordsPerBatch() {
    return recordsPerBatch;
  }

  public double getInsertRatio() {
    return insertRatio;
  }

  public int getMaxConcurrentTableWrites() {
    return maxConcurrentTableWrites;
  }

  private void validate() {
    if (numTables <= 0) {
      throw new IllegalArgumentException("tables.count must be greater than 0");
    }
    if (recordsPerBatch <= 0) {
      throw new IllegalArgumentException("stream.records.per.batch must be greater than 0");
    }
    if (maxConcurrentTableWrites <= 0) {
      throw new IllegalArgumentException("tables.max.concurrent.writes must be greater than 0");
    }
    if (maxConcurrentTableWrites > numTables) {
      throw new IllegalArgumentException("tables.max.concurrent.writes must be less than or equal to tables.count");
    }
    if (triggerIntervalSec <= 0) {
      throw new IllegalArgumentException("stream.trigger.interval.sec must be greater than 0");
    }
    if (insertRatio < 0 || insertRatio > 1) {
      throw new IllegalArgumentException("ratio.insert must be in [0, 1]");
    }
  }

  private static Map<String, String> extractByPrefix(Properties properties, String prefix) {
    Set<String> propertyNames = properties.stringPropertyNames();
    return propertyNames.stream()
        .filter(name -> name.startsWith(prefix))
        .collect(Collectors.toMap(name -> name.substring(prefix.length()), properties::getProperty,
            (left, right) -> right, LinkedHashMap::new));
  }

  private static String getRequired(Properties properties, String key) {
    String value = properties.getProperty(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing required config key: " + key);
    }
    return value.trim();
  }

  private static String getString(Properties properties, String key, String defaultValue) {
    String value = properties.getProperty(key);
    return value == null ? defaultValue : value.trim();
  }

  private static int getInt(Properties properties, String key, int defaultValue) {
    String value = properties.getProperty(key);
    return value == null ? defaultValue : Integer.parseInt(value.trim());
  }

  private static double getDouble(Properties properties, String key, double defaultValue) {
    String value = properties.getProperty(key);
    return value == null ? defaultValue : Double.parseDouble(value.trim());
  }

  private static boolean getBoolean(Properties properties, String key, boolean defaultValue) {
    String value = properties.getProperty(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value.trim());
  }

  private static String stripTrailingSlash(String value) {
    if (value == null) {
      return null;
    }
    String stripped = value;
    while (stripped.endsWith("/")) {
      stripped = stripped.substring(0, stripped.length() - 1);
    }
    return stripped;
  }

  public static class TableTarget {
    private final String database;
    private final String tableName;
    private final String tablePath;
    private final String checkpointPath;

    public TableTarget(String database, String tableName, String tablePath, String checkpointPath) {
      this.database = database;
      this.tableName = tableName;
      this.tablePath = tablePath;
      this.checkpointPath = checkpointPath;
    }

    public String getDatabase() {
      return database;
    }

    public String getTableName() {
      return tableName;
    }

    public String getTablePath() {
      return tablePath;
    }

    public String getCheckpointPath() {
      return checkpointPath;
    }
  }
}
