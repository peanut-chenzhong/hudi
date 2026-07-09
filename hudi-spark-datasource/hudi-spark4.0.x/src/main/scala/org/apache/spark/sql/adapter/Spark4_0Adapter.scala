/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.adapter

import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath

import org.apache.avro.Schema
import org.apache.spark.sql._
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSchemaConverters, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.catalyst.util.DateFormatter
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile, SparkParsePartitionUtil}
import org.apache.spark.sql.execution.datasources.HoodieSparkPartitionedFileUtils
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.parser.HoodieExtendedParserInterface
import org.apache.spark.sql.sources.{BaseRelation, Filter}
import org.apache.spark.sql.types.{DataType, Metadata, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.storage.StorageLevel

import java.util.TimeZone

/**
 * Temporary Spark 4.0 adapter skeleton used to wire Spark4 profile end-to-end.
 *
 * The concrete Spark4 behavior will be filled in incrementally while porting
 * parser/catalyst/parquet integration from the upstream Spark4 implementation.
 */
class Spark4_0Adapter extends SparkAdapter {

  override def isColumnarBatchRow(r: InternalRow): Boolean = false

  override def createCatalystMetadataForMetaField: Metadata = Metadata.empty

  override def getCatalogUtils: HoodieCatalogUtils =
    throw new UnsupportedOperationException("Spark4 adapter catalog utils are not implemented yet")

  override def getCatalystExpressionUtils: HoodieCatalystExpressionUtils =
    throw new UnsupportedOperationException("Spark4 adapter catalyst expression utils are not implemented yet")

  override def getCatalystPlanUtils: HoodieCatalystPlansUtils =
    throw new UnsupportedOperationException("Spark4 adapter catalyst plan utils are not implemented yet")

  override def getSchemaUtils: HoodieSchemaUtils =
    throw new UnsupportedOperationException("Spark4 adapter schema utils are not implemented yet")

  override def createAvroSerializer(rootCatalystType: DataType, rootAvroType: Schema, nullable: Boolean): HoodieAvroSerializer =
    throw new UnsupportedOperationException("Spark4 adapter avro serializer is not implemented yet")

  override def createAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType): HoodieAvroDeserializer =
    throw new UnsupportedOperationException("Spark4 adapter avro deserializer is not implemented yet")

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters =
    throw new UnsupportedOperationException("Spark4 adapter avro schema converters are not implemented yet")

  override def createSparkRowSerDe(schema: StructType): SparkRowSerDe =
    throw new UnsupportedOperationException("Spark4 adapter row serde is not implemented yet")

  override def createExtendedSparkParser(spark: SparkSession, delegate: ParserInterface): HoodieExtendedParserInterface =
    throw new UnsupportedOperationException("Spark4 adapter extended parser is not implemented yet")

  override def getSparkParsePartitionUtil: SparkParsePartitionUtil =
    throw new UnsupportedOperationException("Spark4 adapter partition util is not implemented yet")

  override def getSparkPartitionedFileUtils: HoodieSparkPartitionedFileUtils =
    throw new UnsupportedOperationException("Spark4 adapter partitioned file utils are not implemented yet")

  override def getDateFormatter(tz: TimeZone): DateFormatter =
    throw new UnsupportedOperationException("Spark4 adapter date formatter is not implemented yet")

  override def getFilePartitions(sparkSession: SparkSession, partitionedFiles: Seq[PartitionedFile], maxSplitBytes: Long): Seq[FilePartition] =
    throw new UnsupportedOperationException("Spark4 adapter file partitioning is not implemented yet")

  override def resolveHoodieTable(plan: LogicalPlan): Option[CatalogTable] =
    throw new UnsupportedOperationException("Spark4 adapter table resolution is not implemented yet")

  override def isHoodieTable(tableId: TableIdentifier, spark: SparkSession): Boolean =
    throw new UnsupportedOperationException("Spark4 adapter table resolution is not implemented yet")

  override def createLegacyHoodieParquetFileFormat(appendPartitionValues: Boolean): Option[ParquetFileFormat] =
    throw new UnsupportedOperationException("Spark4 adapter parquet format is not implemented yet")

  override def makeColumnarBatch(vectors: Array[ColumnVector], numRows: Int): ColumnarBatch =
    throw new UnsupportedOperationException("Spark4 adapter columnar batch is not implemented yet")

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate =
    throw new UnsupportedOperationException("Spark4 adapter interpreted predicate is not implemented yet")

  override def createRelation(sqlContext: SQLContext,
                              metaClient: HoodieTableMetaClient,
                              schema: Schema,
                              globPaths: Array[StoragePath],
                              parameters: java.util.Map[String, String]): BaseRelation =
    throw new UnsupportedOperationException("Spark4 adapter relation creation is not implemented yet")

  override def createHoodieFileScanRDD(sparkSession: SparkSession,
                                       readFunction: PartitionedFile => Iterator[InternalRow],
                                       filePartitions: Seq[FilePartition],
                                       readDataSchema: StructType,
                                       metadataColumns: Seq[AttributeReference]): FileScanRDD =
    throw new UnsupportedOperationException("Spark4 adapter file scan RDD is not implemented yet")

  override def extractDeleteCondition(deleteFromTable: Command): Expression =
    throw new UnsupportedOperationException("Spark4 adapter delete condition extraction is not implemented yet")

  override def convertStorageLevelToString(level: StorageLevel): String =
    throw new UnsupportedOperationException("Spark4 adapter storage level conversion is not implemented yet")

  override def translateFilter(predicate: Expression, supportNestedPredicatePushdown: Boolean): Option[Filter] =
    throw new UnsupportedOperationException("Spark4 adapter filter translation is not implemented yet")

  override def injectTableFunctions(extensions: SparkSessionExtensions): Unit = {}
}
