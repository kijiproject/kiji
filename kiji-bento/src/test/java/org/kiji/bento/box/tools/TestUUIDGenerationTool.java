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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.bento.box.BentoBoxUtils;

/**
 * Unit tests for the functionality of {@link UUIDGenerationTool}.
 */
public final class TestUUIDGenerationTool {
  private static final Logger LOG = LoggerFactory.getLogger(TestUUIDGenerationTool.class);
  /** The name of the file to which UUIDs should be written. */
  private static final String UUID_FILE_NAME = ".kiji-bento-uuid";

  /** The UUID tool to use in tests. */
  private UUIDGenerationTool mTool;

  /** A temporary folder for tests. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  @Before
  public void setup() {
    mTool = new UUIDGenerationTool();
  }

  /**
   * @return the contents of the UUID file in the temporary directory for tests.
   */
  private String readUUIDFile() throws IOException {
    File uuidFile = new File(mTempDir.getRoot(), UUID_FILE_NAME);
    return BentoBoxUtils.readFileAsString(uuidFile).trim();
  }

  /**
   * Checks the temporary directory for this test when there is no UUID file,
   * then writes a file and performs the check again.
   */
  @Test
  public void testIsUUIDFileExists() throws Exception {
    LOG.info("Testing isUUIDFileExists when file does not.");
    assertFalse(mTool.isUUIDFileExists(mTempDir.getRoot()));
    mTempDir.newFile(UUID_FILE_NAME);
    LOG.info("Testing isUUIDFileExists when file does.");
    assertTrue(mTool.isUUIDFileExists(mTempDir.getRoot()));
  }

  /**
   * Tests writing a specified UUID to a file by writing the UUID and then reading back its
   * contents.
   */
  @Test
  public void testWriteUUIDFile() throws Exception {
    LOG.info("Writing UUID file to temporary directory for tests.");
    UUID uuid = UUID.randomUUID();
    mTool.writeUUID(mTempDir.getRoot(), uuid);

    LOG.info("Reading back contents of UUID file and comparing.");
    assertEquals("UUID read from file does not match UUID that should have been written.",
        uuid.toString(), readUUIDFile());
  }

  /**
   * Tests generating and writing a UUID to a file, when that file already exists. In this case,
   * the file should not be overwritten.
   */
  @Test
  public void testGenerateAndWriteUUIDExistingFile() throws IOException {
    LOG.info("Creating a dummy UUID file that should not be overwritten.");
    mTempDir.newFile(UUID_FILE_NAME);
    LOG.info("Trying to write over dummy UUID file (shouldn't happen).");
    assertTrue(mTool.generateAndWriteUUID(mTempDir.getRoot()));
    LOG.info("Reading contents of UUID file and ensuring its empty.");
    assertEquals("UUID file was overwritten when it already existed!", "", readUUIDFile());
  }

  /**
   * Tests generating and writing a UUID to a file, when no UUID file exists. In this case,
   * a new file should be created containing some UUID.
   */
  @Test
  public void testGenerateAndWriteUUID() throws IOException {
    LOG.info("Generating a UUID and writing to a file.");
    assertTrue(mTool.generateAndWriteUUID(mTempDir.getRoot()));
    LOG.info("Reading contents of UUID file and ensuring it's a UUID.");
    UUID.fromString(readUUIDFile());
  }
}
