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

package org.apache.spark.sql

import org.apache.hudi.SparkAdapterSupport
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

object DataFrameUtil {

  /**
   * Creates a DataFrame out of RDD[InternalRow] that you can get
   * using `df.queryExecution.toRdd`
   */
  def createFromInternalRows(sparkSession: SparkSession, schema:
  StructType, rdd: RDD[InternalRow]): DataFrame = {
    val candidateMethods =
      sparkSession.getClass.getMethods.filter(_.getName == "internalCreateDataFrame") ++
        sparkSession.getClass.getDeclaredMethods.filter(_.getName == "internalCreateDataFrame")

    val internalCreateDataFrame = candidateMethods.find(_.getParameterCount == 2)
      .getOrElse(throw new IllegalStateException("Unable to locate internalCreateDataFrame on SparkSession"))
    internalCreateDataFrame.setAccessible(true)
    internalCreateDataFrame.invoke(sparkSession, rdd, schema).asInstanceOf[DataFrame]
  }

  /**
   * Spark 4 moved Dataset internals under `org.apache.spark.sql.classic`.
   * Use reflection to stay source-compatible with both Spark 3 and Spark 4.
   */
  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    try {
      val datasetObjectClass = Class.forName("org.apache.spark.sql.classic.Dataset$")
      val classicSparkSessionClass = Class.forName("org.apache.spark.sql.classic.SparkSession")
      val module = datasetObjectClass.getField("MODULE$").get(null)
      val method = datasetObjectClass.getMethod("ofRows", classicSparkSessionClass, classOf[LogicalPlan])
      method.invoke(module, sparkSession, logicalPlan).asInstanceOf[DataFrame]
    } catch {
      case _: ClassNotFoundException =>
        val datasetObjectClass = Class.forName("org.apache.spark.sql.Dataset$")
        val module = datasetObjectClass.getField("MODULE$").get(null)
        val method = datasetObjectClass.getMethod("ofRows", classOf[SparkSession], classOf[LogicalPlan])
        method.invoke(module, sparkSession, logicalPlan).asInstanceOf[DataFrame]
    }
  }
}
