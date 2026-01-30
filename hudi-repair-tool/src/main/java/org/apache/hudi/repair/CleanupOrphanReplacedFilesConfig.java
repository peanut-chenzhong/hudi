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

package org.apache.hudi.repair;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for CleanupOrphanReplacedFilesTool.
 */
public class CleanupOrphanReplacedFilesConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  @Parameter(names = {"--base-paths", "-p"}, description = "Comma-separated list of Hudi table base paths to process", 
      required = false)
  public String basePaths;

  @Parameter(names = {"--path-file", "-f"}, description = "File containing list of Hudi table base paths (one per line)", 
      required = false)
  public String pathFile;

  @Parameter(names = {"--dry-run", "-d"}, description = "If true, only report files without moving them (default: true)")
  public boolean dryRun = true;

  @Parameter(names = {"--start-instant-time", "-s"}, description = "Start instant time for filtering replacecommit (inclusive)")
  public String startInstantTime;

  @Parameter(names = {"--end-instant-time", "-e"}, description = "End instant time for filtering replacecommit (inclusive)")
  public String endInstantTime;

  @Parameter(names = {"--parallelism"}, description = "Parallelism for processing multiple tables (default: 1)")
  public int parallelism = 1;

  @Parameter(names = {"--delete-dir-name"}, description = "Name of the directory to move orphan files to (default: .delete)")
  public String deleteDirName = ".delete";

  @Parameter(names = {"--output-path", "-o"}, description = "Path to write the result report (optional)")
  public String outputPath;

  @Parameter(names = {"--spark-master"}, description = "Spark master URL (default: local[*])")
  public String sparkMaster = "local[*]";

  @Parameter(names = {"--help", "-h"}, help = true, description = "Show help message")
  public boolean help = false;

  /**
   * Get the list of base paths to process.
   */
  public List<String> getBasePathList() {
    List<String> paths = new ArrayList<>();
    
    if (basePaths != null && !basePaths.isEmpty()) {
      for (String path : basePaths.split(",")) {
        String trimmed = path.trim();
        if (!trimmed.isEmpty()) {
          paths.add(trimmed);
        }
      }
    }
    
    return paths;
  }

  /**
   * Validate the configuration.
   */
  public void validate() {
    if ((basePaths == null || basePaths.isEmpty()) && (pathFile == null || pathFile.isEmpty())) {
      throw new IllegalArgumentException("Either --base-paths or --path-file must be specified");
    }
  }

  @Override
  public String toString() {
    return "CleanupOrphanReplacedFilesConfig{"
        + "basePaths='" + basePaths + '\''
        + ", pathFile='" + pathFile + '\''
        + ", dryRun=" + dryRun
        + ", startInstantTime='" + startInstantTime + '\''
        + ", endInstantTime='" + endInstantTime + '\''
        + ", parallelism=" + parallelism
        + ", deleteDirName='" + deleteDirName + '\''
        + ", outputPath='" + outputPath + '\''
        + ", sparkMaster='" + sparkMaster + '\''
        + '}';
  }
}
