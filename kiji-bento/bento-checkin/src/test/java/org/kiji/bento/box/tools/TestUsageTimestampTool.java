/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.bento.box.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.CheckinUtils;

/**
 * Unit tests for the functionality of {@link org.kiji.bento.box.tools.TestUsageTimestampTool}.
 */
public final class TestUsageTimestampTool {
  private static final Logger LOG = LoggerFactory.getLogger(TestUsageTimestampTool.class);
  /** The name of the file to which timestamps should be written. */
  private static final String TIMESTAMP_FILE_NAME = ".kiji-last-used";

  /** The timestamp tool to use in tests. */
  private UsageTimestampTool mTool;

  /** A temporary folder for tests. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  @Before
  public void setup() {
    mTool = new UsageTimestampTool();
  }

  /**
   * @return the contents of the timestamp file in the temporary directory for tests.
   */
  private Long readTimestampFile() throws IOException {
    File timestampFile = new File(mTempDir.getRoot(), TIMESTAMP_FILE_NAME);
    return Long.parseLong(CheckinUtils.readFileAsString(timestampFile).trim());
  }

  /**
   * Tests writing a specified timestamp to a file by writing the timestamp and then reading back
   * its contents.
   */
  @Test
  public void testWriteTimestampFile() throws Exception {
    LOG.info("Writing timestamp file to temporary directory for tests.");
    Long timestamp = 5684L;
    mTool.writeTimestamp(mTempDir.getRoot(), timestamp);

    LOG.info("Reading back contents of timestamp file and comparing.");
    assertEquals("timestamp read does not match timestamp that should have been written.",
        timestamp, readTimestampFile());
  }

  /**
   * Tests writing the current time to the timestamp file and that subsequent calls overwrite
   * existing values.
   */
  @Test
  public void testGenerateAndWriteTimestamp() throws IOException {
    Long startTime = System.currentTimeMillis();
    LOG.info("Writing the first timestamp.");
    assertTrue(mTool.generateAndWriteTimestamp(mTempDir.getRoot()));
    LOG.info("Reading first written timestamp.");
    Long firstTimestampWritten = readTimestampFile();
    assertTrue("Timestamp written was earlier than expected.",
        firstTimestampWritten >= startTime);
    LOG.info("Writing the second timestamp.");
    assertTrue(mTool.generateAndWriteTimestamp(mTempDir.getRoot()));
    LOG.info("Reading second written timestamp.");
    Long secondTimestampWritten = readTimestampFile();
    assertTrue("Timestamp written was earlier than expected.",
        secondTimestampWritten >= firstTimestampWritten);
  }
}
