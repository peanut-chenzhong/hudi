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

import com.beust.jcommander.JCommander;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CleanupOrphanReplacedFilesConfig}.
 */
class TestCleanupOrphanReplacedFilesConfig {

  @Test
  void testParseBasePaths() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    JCommander.newBuilder()
        .addObject(config)
        .build()
        .parse("--base-paths", "/path/to/table1,/path/to/table2,/path/to/table3");

    List<String> paths = config.getBasePathList();
    assertEquals(3, paths.size());
    assertEquals("/path/to/table1", paths.get(0));
    assertEquals("/path/to/table2", paths.get(1));
    assertEquals("/path/to/table3", paths.get(2));
  }

  @Test
  void testParseDryRun() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    JCommander.newBuilder()
        .addObject(config)
        .build()
        .parse("--base-paths", "/path/to/table", "--dry-run", "false");

    assertFalse(config.dryRun);
  }

  @Test
  void testDefaultDryRunIsTrue() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    JCommander.newBuilder()
        .addObject(config)
        .build()
        .parse("--base-paths", "/path/to/table");

    assertTrue(config.dryRun);
  }

  @Test
  void testParseTimeRange() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    JCommander.newBuilder()
        .addObject(config)
        .build()
        .parse(
            "--base-paths", "/path/to/table",
            "--start-instant-time", "20240101000000000",
            "--end-instant-time", "20240115235959999"
        );

    assertEquals("20240101000000000", config.startInstantTime);
    assertEquals("20240115235959999", config.endInstantTime);
  }

  @Test
  void testParseParallelism() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    JCommander.newBuilder()
        .addObject(config)
        .build()
        .parse("--base-paths", "/path/to/table", "--parallelism", "10");

    assertEquals(10, config.parallelism);
  }

  @Test
  void testValidateThrowsWhenNoPathsSpecified() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();

    assertThrows(IllegalArgumentException.class, config::validate);
  }

  @Test
  void testValidatePassesWithBasePaths() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    config.basePaths = "/path/to/table";

    assertDoesNotThrow(config::validate);
  }

  @Test
  void testValidatePassesWithPathFile() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    config.pathFile = "/path/to/tables.txt";

    assertDoesNotThrow(config::validate);
  }

  @Test
  void testBasePathsWithWhitespace() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    config.basePaths = "  /path/to/table1 , /path/to/table2  ,  /path/to/table3  ";

    List<String> paths = config.getBasePathList();
    assertEquals(3, paths.size());
    assertEquals("/path/to/table1", paths.get(0));
    assertEquals("/path/to/table2", paths.get(1));
    assertEquals("/path/to/table3", paths.get(2));
  }

  @Test
  void testCustomDeleteDirName() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    JCommander.newBuilder()
        .addObject(config)
        .build()
        .parse("--base-paths", "/path/to/table", "--delete-dir-name", ".trash");

    assertEquals(".trash", config.deleteDirName);
  }

  @Test
  void testDefaultDeleteDirName() {
    CleanupOrphanReplacedFilesConfig config = new CleanupOrphanReplacedFilesConfig();
    assertEquals(".delete", config.deleteDirName);
  }
}
