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
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * spark-submit entry for streaming writes to multiple Hudi tables using one config file.
 */
public class HudiStreamingMultiTableWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HudiStreamingMultiTableWriter.class);

  public static void main(String[] args) throws Exception {
    String configPath = parseConfigPath(args);
    StreamingJobConfig config = StreamingJobConfig.fromFile(configPath);
    List<StreamingJobConfig.TableTarget> tableTargets = config.buildTableTargets();
    MultiTableDataGenerator dataGenerator = new MultiTableDataGenerator(config);
    int maxConcurrentTableWrites = config.getMaxConcurrentTableWrites();
    ExecutorService tableWriteExecutor = Executors.newFixedThreadPool(maxConcurrentTableWrites);

    SparkSession spark = SparkSession.builder()
        .appName(config.getAppName())
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    Dataset<Row> triggerDataset = spark.readStream()
        .format("rate")
        .option("rowsPerSecond", 1)
        .option("numPartitions", 1)
        .load();

    StreamingQuery query = triggerDataset.writeStream()
        .trigger(Trigger.ProcessingTime(config.getTriggerIntervalSec() * 1000L))
        .option("checkpointLocation", tableTargets.get(0).getCheckpointPath() + "/_driver")
        .foreachBatch((batch, batchId) -> {
          LOG.info("Starting batchId={}, tableCount={}, maxConcurrentTableWrites={}",
              batchId, tableTargets.size(), maxConcurrentTableWrites);

          List<CompletableFuture<Void>> writeTasks = tableTargets.stream()
              .map(target -> CompletableFuture.runAsync(() -> {
                Dataset<Row> tableBatch = dataGenerator.generateBatch(spark, target, batchId);
                long recordCount = tableBatch.count();
                Map<String, String> hudiOptions = config.buildHudiOptions(target);
                tableBatch.write()
                    .format("hudi")
                    .options(hudiOptions)
                    .mode(SaveMode.Append)
                    .save(target.getTablePath());
                LOG.info("Finished table={}, path={}, batchId={}, records={}",
                    target.getTableName(), target.getTablePath(), batchId, recordCount);
              }, tableWriteExecutor))
              .collect(Collectors.toList());

          CompletableFuture.allOf(writeTasks.toArray(new CompletableFuture[0])).join();
        })
        .start();

    LOG.info("Streaming query started with config file {}", configPath);
    query.awaitTermination();
    tableWriteExecutor.shutdown();
  }

  private static String parseConfigPath(String[] args) {
    for (int i = 0; i < args.length; i++) {
      if ("--config".equals(args[i]) && i + 1 < args.length) {
        return args[i + 1];
      }
    }
    throw new IllegalArgumentException("Usage: HudiStreamingMultiTableWriter --config <path-to-properties-file>");
  }
}
