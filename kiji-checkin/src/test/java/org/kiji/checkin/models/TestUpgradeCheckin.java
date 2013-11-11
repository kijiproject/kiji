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
package org.kiji.checkin.models;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.VersionInfo;

/**
 * Tests the functionality of {@link UpgradeCheckin}.
 */
public class TestUpgradeCheckin {
  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeCheckin.class);

  private static final String EXPECTED_TYPE = "checkversion";
  private static final String EXPECTED_FORMAT = "bento-checkin-1.0.0";
  private static final String EXPECTED_OS = String.format("%s %s %s",
      System.getProperty("os.name"), System.getProperty("os.version"),
      System.getProperty("os.arch"));
  private static final String EXPECTED_JAVA_VERSION = System.getProperty("java.version");
  private static final String EXPECTED_ID = "12345";
  private static final String EXPECTED_PROJECT_NAME = "kiji";
  private static final long EXPECTED_TS = 4951L;
  private static final String JSON_FORMAT =
      "{\"type\":\"%s\","
      + "\"request_format\":\"%s\","
      + "\"os\":\"%s\","
      + "\"bento_version\":\"%s\","
      + "\"java_version\":\"%s\","
      + "\"last_used\":%d,"
      + "\"id\":\"%s\","
      + "\"project_name\":\"%s\"}";

  private String getExpectedJSON() throws IOException {
    return String.format(JSON_FORMAT, EXPECTED_TYPE, EXPECTED_FORMAT, EXPECTED_OS,
        VersionInfo.getSoftwareVersion(this.getClass()), EXPECTED_JAVA_VERSION,
        EXPECTED_TS, EXPECTED_ID, EXPECTED_PROJECT_NAME);
  }

  @Test
  public void testBuild() throws IOException {
    LOG.info("Building a check-in message and verifying its fields.");
    UpgradeCheckin checkin = new UpgradeCheckin.Builder(this.getClass())
        .withId(EXPECTED_ID)
        .withLastUsedMillis(EXPECTED_TS)
        .build();
    assertEquals("Bad message type.", EXPECTED_TYPE, checkin.getType());
    assertEquals("Bad message format.", EXPECTED_FORMAT, checkin.getFormat());
    assertEquals("Bad OS.", EXPECTED_OS, checkin.getOperatingSystem());
    assertEquals("Bad bento version.", VersionInfo.getSoftwareVersion(this.getClass()),
        checkin.getBentoVersion());
    assertEquals("Bad Java version.", EXPECTED_JAVA_VERSION, checkin.getJavaVersion());
    assertEquals("Bad message timestamp.", EXPECTED_TS, checkin.getLastUsedMillis());
    assertEquals("Bad message id.", EXPECTED_ID, checkin.getId());
  }

  @Test(expected = NullPointerException.class)
  public void testBuildMissingId() throws IOException {
    LOG.info("Trying to build a check-in message without an ID.");
    new UpgradeCheckin.Builder(this.getClass())
        .withLastUsedMillis(EXPECTED_TS)
        .build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuildMissingTimestamp() throws IOException {
    LOG.info("Trying to build a check-in message without a timestamp.");
    new UpgradeCheckin.Builder(this.getClass())
        .withId(EXPECTED_ID)
        .build();
  }

  @Test
  public void testToJSON() throws IOException {
    LOG.info("Building a check-in message and verifying its JSON.");
    UpgradeCheckin checkin = new UpgradeCheckin.Builder(this.getClass())
        .withId(EXPECTED_ID)
        .withLastUsedMillis(EXPECTED_TS)
        .build();
    LOG.info("JSON is: " + checkin.toJSON());
    assertEquals("Bad JSON serialization of check-in message.", getExpectedJSON(),
        checkin.toJSON());
  }

  @Test
  public void testEquals() throws IOException {
    LOG.info("Testing equals with two identical messages.");
    UpgradeCheckin checkin1 = new UpgradeCheckin.Builder(this.getClass())
        .withId("hello")
        .withLastUsedMillis(1L)
        .build();
    UpgradeCheckin checkin2 = new UpgradeCheckin.Builder(this.getClass())
        .withId("hello")
        .withLastUsedMillis(1L)
        .build();
    assertTrue("These two check-in messages should have been equal.", checkin1.equals(checkin2));
    LOG.info("Testing equals with unidentical messages.");
    UpgradeCheckin checkin3 = new UpgradeCheckin.Builder(this.getClass())
        .withId("goodbye")
        .withLastUsedMillis(1L)
        .build();
    assertFalse("These two check-in messages should have been unequal.",
        checkin1.equals(checkin3));
  }
}
