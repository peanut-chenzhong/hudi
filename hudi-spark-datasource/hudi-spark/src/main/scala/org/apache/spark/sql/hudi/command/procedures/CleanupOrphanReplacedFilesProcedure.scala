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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieArchivedTimeline, HoodieInstant, HoodieTimeline}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.util.function.Supplier
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Procedure to cleanup orphan replaced files from archived replacecommit.
 *
 * This procedure scans the archived timeline for replacecommit actions, identifies
 * file groups that were replaced (e.g., by clustering or insert_overwrite),
 * checks if the replaced data files still exist on storage, and moves them
 * to a .delete directory within the same partition for safe cleanup.
 *
 * Usage:
 * {{{
 *   CALL cleanup_orphan_replaced_files(table => 'db.table_name')
 *   CALL cleanup_orphan_replaced_files(table => 'db.table_name', dry_run => true)
 *   CALL cleanup_orphan_replaced_files(path => '/path/to/table', dry_run => false)
 * }}}
 */
class CleanupOrphanReplacedFilesProcedure extends BaseProcedure with ProcedureBuilder {

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "path", DataTypes.StringType, None),
    ProcedureParameter.optional(2, "dry_run", DataTypes.BooleanType, true),
    ProcedureParameter.optional(3, "start_instant_time", DataTypes.StringType, None),
    ProcedureParameter.optional(4, "end_instant_time", DataTypes.StringType, None)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("partition_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("action", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("status", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def parameters: Array[ProcedureParameter] = PARAMETERS

  override def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val tablePath = getArgValueOrDefault(args, PARAMETERS(1))
    val dryRun = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Boolean]
    val startInstantTime = getArgValueOrDefault(args, PARAMETERS(3))
    val endInstantTime = getArgValueOrDefault(args, PARAMETERS(4))

    val basePath = getBasePath(tableName, tablePath)
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(jsc.hadoopConfiguration())
      .setBasePath(basePath)
      .build()

    val fs = metaClient.getStorage.getFileSystem
    val results = ArrayBuffer[Row]()

    // Load archived timeline
    val archivedTimeline = if (startInstantTime.isDefined) {
      new HoodieArchivedTimeline(metaClient, startInstantTime.get.asInstanceOf[String])
    } else {
      new HoodieArchivedTimeline(metaClient)
    }

    // Also check active timeline for replacecommit
    val activeTimeline = metaClient.getActiveTimeline

    // Get all replacecommit instants from archived timeline
    val archivedReplaceCommits = archivedTimeline.getInstants.toArray
      .map(_.asInstanceOf[HoodieInstant])
      .filter(instant => HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction))
      .filter(instant => HoodieInstant.State.COMPLETED.equals(instant.getState))
      .filter(instant => {
        val ts = instant.getTimestamp
        val afterStart = startInstantTime.isEmpty || ts >= startInstantTime.get.asInstanceOf[String]
        val beforeEnd = endInstantTime.isEmpty || ts <= endInstantTime.get.asInstanceOf[String]
        afterStart && beforeEnd
      })
      .toList

    // Get all replacecommit instants from active timeline
    val activeReplaceCommits = activeTimeline.getInstants.toArray
      .map(_.asInstanceOf[HoodieInstant])
      .filter(instant => HoodieTimeline.REPLACE_COMMIT_ACTION.equals(instant.getAction))
      .filter(instant => HoodieInstant.State.COMPLETED.equals(instant.getState))
      .filter(instant => {
        val ts = instant.getTimestamp
        val afterStart = startInstantTime.isEmpty || ts >= startInstantTime.get.asInstanceOf[String]
        val beforeEnd = endInstantTime.isEmpty || ts <= endInstantTime.get.asInstanceOf[String]
        afterStart && beforeEnd
      })
      .toList

    val allReplaceCommits = archivedReplaceCommits ++ activeReplaceCommits

    logInfo(s"Found ${allReplaceCommits.size} replacecommit instants to process " +
      s"(${archivedReplaceCommits.size} archived, ${activeReplaceCommits.size} active)")

    // Process each replacecommit
    for (instant <- allReplaceCommits) {
      val instantTime = instant.getTimestamp

      // Get commit metadata
      val commitMetadataBytes = if (archivedReplaceCommits.contains(instant)) {
        archivedTimeline.getInstantDetails(instant)
      } else {
        activeTimeline.getInstantDetails(instant)
      }

      if (commitMetadataBytes.isPresent) {
        try {
          val replaceMetadata = HoodieReplaceCommitMetadata.fromBytes(
            commitMetadataBytes.get(),
            classOf[HoodieReplaceCommitMetadata]
          )

          val partitionToReplaceFileIds = replaceMetadata.getPartitionToReplaceFileIds

          if (partitionToReplaceFileIds != null && !partitionToReplaceFileIds.isEmpty) {
            for ((partitionPath, fileIds) <- partitionToReplaceFileIds.asScala) {
              val partitionDir = new Path(basePath, partitionPath)

              for (fileId <- fileIds.asScala) {
                // Find all files with this fileId in the partition
                val orphanFiles = findOrphanFilesForFileId(fs, partitionDir, fileId)

                for (fileStatus <- orphanFiles) {
                  val filePath = fileStatus.getPath
                  val fileName = filePath.getName
                  val fileSize = fileStatus.getLen

                  if (dryRun) {
                    // Dry run: just report
                    results += Row(
                      instantTime,
                      partitionPath,
                      fileId,
                      filePath.toString,
                      fileSize,
                      "WOULD_MOVE",
                      "DRY_RUN"
                    )
                  } else {
                    // Actually move the file
                    val deleteDir = new Path(partitionDir, ".delete")
                    val targetPath = new Path(deleteDir, fileName)

                    try {
                      // Create .delete directory if not exists
                      if (!fs.exists(deleteDir)) {
                        fs.mkdirs(deleteDir)
                      }

                      // Move file to .delete directory
                      val success = fs.rename(filePath, targetPath)

                      if (success) {
                        results += Row(
                          instantTime,
                          partitionPath,
                          fileId,
                          filePath.toString,
                          fileSize,
                          "MOVED",
                          s"Moved to ${targetPath.toString}"
                        )
                      } else {
                        results += Row(
                          instantTime,
                          partitionPath,
                          fileId,
                          filePath.toString,
                          fileSize,
                          "FAILED",
                          "Failed to move file"
                        )
                      }
                    } catch {
                      case e: Exception =>
                        results += Row(
                          instantTime,
                          partitionPath,
                          fileId,
                          filePath.toString,
                          fileSize,
                          "ERROR",
                          e.getMessage
                        )
                    }
                  }
                }
              }
            }
          }
        } catch {
          case e: Exception =>
            logWarning(s"Failed to parse replacecommit metadata for instant $instantTime: ${e.getMessage}")
        }
      }
    }

    logInfo(s"Processed ${results.size} orphan files " +
      s"(dry_run=$dryRun)")

    results.toSeq
  }

  /**
   * Find all base files and log files for a given fileId in the partition directory.
   * These are files that were replaced but still exist on storage.
   */
  private def findOrphanFilesForFileId(
      fs: org.apache.hadoop.fs.FileSystem,
      partitionDir: Path,
      fileId: String): Seq[FileStatus] = {
    
    val orphanFiles = ArrayBuffer[FileStatus]()

    try {
      if (fs.exists(partitionDir)) {
        val files = fs.listStatus(partitionDir)

        for (file <- files) {
          val fileName = file.getPath.getName

          // Skip hidden files and directories (like .delete, .hoodie_partition_metadata)
          if (!fileName.startsWith(".") && !file.isDirectory) {
            // Check if this file belongs to the fileId
            // Base file pattern: fileId_writeToken_instantTime.parquet
            // Log file pattern: .fileId_instantTime.log.version_writeToken
            if (isFileForFileId(fileName, fileId)) {
              orphanFiles += file
            }
          }
        }
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to list files in partition $partitionDir: ${e.getMessage}")
    }

    orphanFiles.toSeq
  }

  /**
   * Check if a file belongs to the given fileId.
   * Handles both base files and log files naming conventions.
   */
  private def isFileForFileId(fileName: String, fileId: String): Boolean = {
    // Base file pattern: fileId_writeToken_instantTime.parquet
    // e.g., abc-123_0-5-0_20240101120000.parquet
    if (fileName.startsWith(fileId + "_") || fileName.startsWith(fileId + "-")) {
      return true
    }

    // Log file pattern: .fileId_instantTime.log.version_writeToken
    // e.g., .abc-123_20240101120000.log.1_0-5-0
    if (fileName.startsWith("." + fileId + "_")) {
      return true
    }

    false
  }

  private def logInfo(msg: String): Unit = {
    println(s"[INFO] $msg")
  }

  private def logWarning(msg: String): Unit = {
    println(s"[WARN] $msg")
  }

  override def build: Procedure = new CleanupOrphanReplacedFilesProcedure()
}

object CleanupOrphanReplacedFilesProcedure {
  val NAME = "cleanup_orphan_replaced_files"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new CleanupOrphanReplacedFilesProcedure()
  }
}
