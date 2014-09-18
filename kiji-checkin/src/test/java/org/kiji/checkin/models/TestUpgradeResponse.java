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

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.CheckinUtils;

/**
 * Tests the functionality of {@link UpgradeResponse}.
 */
public class TestUpgradeResponse {
  private static final Logger LOG = LoggerFactory.getLogger(TestUpgradeResponse.class);

  private static final String RESPONSE_FORMAT = "bento-checkversion-1.0.0";
  private static final String LATEST_VERSION = "2.0.0";
  private static final String LATEST_VERSION_URL = "http://www.foocorp.com";
  private static final String COMPATIBLE_VERSION = "1.1.0";
  private static final String COMPATIBLE_VERSION_URL = "http://www.foocorp.com";
  private static final String UPGRADE_MESSAGE = "better upgrade!";
  private static final String TEST_JSON = String.format(
      "{\"response_format\":\"%s\","
      + "\"latest_version\":\"%s\","
      + "\"latest_version_url\":\"%s\","
      + "\"compatible_version\":\"%s\","
      + "\"compatible_version_url\":\"%s\","
      + "\"msg\":\"%s\","
      + "\"last_reminder\":0}", RESPONSE_FORMAT, LATEST_VERSION, LATEST_VERSION_URL,
      COMPATIBLE_VERSION, COMPATIBLE_VERSION_URL, UPGRADE_MESSAGE);
  private static final String TEST_JSON_EQUAL_VERSIONS =
      "{\"response_format\":\"bento-checkversion-1.0.0\","
      + "\"latest_version\":\"2.0.0\","
      + "\"latest_version_url\":\"http://www.foocorp.com\","
      + "\"compatible_version\":\"2.0.0\","
      + "\"compatible_version_url\":\"http://www.foocorp.com\","
      + "\"msg\":\"better upgrade!\","
      + "\"last_reminder\":0}";
  private static final String TEST_JSON_NO_COMPATIBLE_VERSION =
      "{\"response_format\":\"bento-checkversion-1.0.0\","
      + "\"latest_version\":\"2.0.0\","
      + "\"latest_version_url\":\"http://www.foocorp.com\","
      + "\"msg\":\"better upgrade!\","
      + "\"last_reminder\":0}";

  //CSOFF: VisibilityModifierCheck
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();
  //CSON: VisibilityModifierCheck

  private void verifyUpgradeResponse(UpgradeResponse response) {
    assertEquals("Response format incorrect.", RESPONSE_FORMAT, response.getResponseFormat());
    assertEquals("Latest version incorrect.", LATEST_VERSION, response.getLatestVersion());
    assertEquals("Latest version URL incorrect.", LATEST_VERSION_URL,
        response.getLatestVersionURL().toString());
    assertEquals("Compatible version incorrect.", COMPATIBLE_VERSION,
        response.getCompatibleVersion());
    assertEquals("Compatible version URL incorrect.", COMPATIBLE_VERSION_URL,
        response.getCompatibleVersionURL().toString());
    assertEquals("Upgrade message incorrect.", UPGRADE_MESSAGE, response.getMessage());
    assertEquals("Last reminder time incorrect.", 0L, response.getLastReminderTimeMillis());
  }

  @Test
  public void testFromJson() {
    LOG.info("Creating upgrade response from test JSON: " + TEST_JSON);
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    LOG.info("Verifying fields of created response.");
    verifyUpgradeResponse(response);
  }

  @Test
  public void testFromFile() throws IOException {
    LOG.info("Writing a file with upgrade response JSON.");
    File jsonFile = new File(mTempFolder.getRoot(), ".kiji-bento-upgrade");
    CheckinUtils.writeObjectToFile(jsonFile, TEST_JSON);

    LOG.info("Getting and verifying an UpgradeResponse from that file content.");
    UpgradeResponse response = UpgradeResponse.fromFile(jsonFile);
    verifyUpgradeResponse(response);
  }

  @Test
  public void testWrite() throws IOException {
    File jsonFile = new File(mTempFolder.getRoot(), ".kiji-bento-upgrade");
    LOG.info("Creating an UpgradeResponse and writing it.");
    UpgradeResponse.fromJSON(TEST_JSON).write(jsonFile);
    LOG.info("Reading back that file into an UpgradeResponse and verifying.");
    verifyUpgradeResponse(UpgradeResponse.fromFile(jsonFile));
  }

  @Test
  public void testJustReminded() {
    LOG.info("Creating an UpgradeResponse to remind the user about.");
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    assertEquals("Last reminder time incorrect.", 0L, response.getLastReminderTimeMillis());
    long currentTime = System.currentTimeMillis();
    LOG.info("Resetting last reminder time and verifying.");
    response.justReminded();
    assertTrue("Last reminder time not updated correctly.",
        response.getLastReminderTimeMillis() >= currentTime);
  }

  @Test
  public void testIsTimeToRemind() {
    LOG.info("Creating an upgrade response that's due for a reminder.");
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    assertTrue("It should be time to remind.", response.isTimeToRemind(1000L));
    LOG.info("Now we shouldn't be due for one.");
    response.justReminded();
    assertFalse("It shouldn't be time to remind.", response.isTimeToRemind(10000L));
  }

  @Test
  public void testParseNonRCVersionString() {
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    LOG.info("Trying to parse a non-rc version string: 1.2.3");
    int[] components = response.parseVersionString("1.2.3");
    LOG.info("Parsed non-rc version, verifying components.");
    assertTrue("Wrong number of components in parsed version string.", 3 == components.length);
    assertEquals("Wrong first component.", 1, components[0]);
    assertEquals("Wrong second component.", 2, components[1]);
    assertEquals("Wrong third component.", 3, components[2]);
  }

  @Test
  public void testParseRCVersionString() {
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    LOG.info("Trying to parse a rc version string: 1.2.3-rc4");
    int[] components = response.parseVersionString("1.2.3-rc4");
    LOG.info("Parsed rc version, verifying components.");
    assertTrue("Wrong number of components in parsed version string.", 4 == components.length);
    assertEquals("Wrong first component.", 1, components[0]);
    assertEquals("Wrong second component.", 2, components[1]);
    assertEquals("Wrong third component.", 3, components[2]);
    assertEquals("Wrong fourth component.", 4, components[3]);
  }

  @Test
  public void testIsVersionNewer() {
    LOG.info("Testing isVersionNewer comparing two non-rc versions.");
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    assertTrue(response.isVersionNewer("1.2.3", "1.2.2"));
    assertTrue(response.isVersionNewer("1.2.3", "1.1.3"));
    assertTrue(response.isVersionNewer("1.2.3", "0.2.3"));
    assertFalse(response.isVersionNewer("1.2.3", "1.2.3"));
    assertFalse(response.isVersionNewer("1.2.2", "1.2.3"));
    assertFalse(response.isVersionNewer("1.1.3", "1.2.3"));
    assertFalse(response.isVersionNewer("0.2.3", "1.2.3"));
    LOG.info("Testing isVersionNewer comparing one rc and one non-rc version, when non-rc "
        + "portions of versions are unequal.");
    assertTrue(response.isVersionNewer("1.2.3", "1.2.2-rc1"));
    assertTrue(response.isVersionNewer("1.2.3", "1.1.3-rc1"));
    assertTrue(response.isVersionNewer("1.2.3", "0.2.3-rc1"));
    assertFalse(response.isVersionNewer("1.2.2-rc1", "1.2.3"));
    assertFalse(response.isVersionNewer("1.1.3-rc1", "1.2.3"));
    assertFalse(response.isVersionNewer("0.2.3-rc1", "1.2.3"));
    LOG.info("Testing isVersionNewer comparing one rc and one non-rc version, "
        + "when non-rc portions are equal.");
    assertTrue(response.isVersionNewer("1.2.3", "1.2.3-rc1"));
    assertFalse(response.isVersionNewer("1.2.3-rc1", "1.2.3"));
    LOG.info("Testing isVersionNewer with two rc versions, with unequal non-rc components.");
    assertTrue(response.isVersionNewer("1.2.3-rc2", "1.2.2-rc1"));
    assertTrue(response.isVersionNewer("1.2.3-rc2", "1.1.3-rc1"));
    assertTrue(response.isVersionNewer("1.2.3-rc2", "0.2.3-rc1"));
    assertFalse(response.isVersionNewer("1.2.2-rc1", "1.2.3-rc2"));
    assertFalse(response.isVersionNewer("1.1.3-rc1", "1.2.3-rc2"));
    assertFalse(response.isVersionNewer("0.2.3-rc1", "1.2.3-rc2"));
    LOG.info("Testing isVersionNewer with two rc versions, with equal non-rc components.");
    assertFalse(response.isVersionNewer("1.2.3-rc1", "1.2.3-rc2"));
    assertTrue(response.isVersionNewer("1.2.3-rc2", "1.2.3-rc1"));
    assertFalse(response.isVersionNewer("1.2.3-rc1", "1.2.3-rc1"));
  }

  @Test
  public void testIsLatestNewer() {
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    LOG.info("Testing isLatestNewer");
    assertTrue(response.isLatestNewer("1.0.0"));
    assertFalse(response.isLatestNewer("2.0.0"));
    assertFalse(response.isLatestNewer("3.0.0"));
    assertTrue(response.isLatestNewer("2.0.0-rc1"));
  }

  @Test
  public void testIsCompatibleNewer() {
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    LOG.info("Testing isCompatibleNewer");
    assertTrue(response.isCompatibleNewer("1.0.0"));
    assertFalse(response.isCompatibleNewer("1.1.0"));
    assertFalse(response.isCompatibleNewer("3.0.0"));
    assertTrue(response.isCompatibleNewer("1.1.0-rc1"));
  }

  @Test
  public void testIsRelevant() {
    LOG.info("Testing isRelevant using response with latest_version > compatible_version");
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON);
    assertFalse("Irrelevant upgrade info is relevant.", response.isRelevant("3.0.0"));
    assertTrue("Relevant upgrade info is irrelevant.", response.isRelevant("1.0.0"));
    assertTrue("Relevant upgrade info is irrelevant.", response.isRelevant("1.2.0"));
    LOG.info("Testing isRelevant using response with latest_version == compatible_version");
    response = UpgradeResponse.fromJSON(TEST_JSON_EQUAL_VERSIONS);
    assertTrue("Relevant upgrade is irrelevant.", response.isRelevant("1.0.0"));
    assertFalse("Irrelevant upgrade is relevant.", response.isRelevant("3.0.0"));
  }

  @Test
  public void testUpgradeReminder() {
    LOG.info("Testing getting an upgrade reminder when there is no compatible version.");
    UpgradeResponse response = UpgradeResponse.fromJSON(TEST_JSON_NO_COMPATIBLE_VERSION);
    String reminder = response.getUpgradeReminder("1.0.0");
    assertTrue("Bad upgrade reminder for incompatible upgrade.",
        reminder.contains("(incompatible with your version"));
    assertFalse("Bad upgrade reminder for incompatible upgrade.",
        reminder.contains("(compatible with your version"));
    reminder = response.getUpgradeReminder("3.0.0");
    assertEquals("Got upgrade reminder for irrelevant upgrade.", "", reminder);

    LOG.info("Testing getting an upgrade reminder when compatible and latest version are equal");
    response = UpgradeResponse.fromJSON(TEST_JSON_EQUAL_VERSIONS);
    reminder = response.getUpgradeReminder("1.0.0");
    assertTrue("Bad upgrade reminder for compatible upgrade.",
        reminder.contains("(compatible with your version"));
    assertFalse("Bad upgrade reminder for compatible upgrade.",
        reminder.contains("(incompatible with your version"));
    reminder = response.getUpgradeReminder("3.0.0");
    assertEquals("Got upgrade reminder for irrelevant upgrade.", "", reminder);

    LOG.info("Testing getting an upgrade reminder when latest version greater than compatible "
        + "version.");
    response = UpgradeResponse.fromJSON(TEST_JSON);
    reminder = response.getUpgradeReminder("1.0.0");
    // Should contain info about both a compatible and incompatible upgrade.
    assertTrue("Bad upgrade reminder for incompatible+compatible upgrade.",
        reminder.contains("(incompatible with your version"));
    assertTrue("Bad upgrade reminder for incompatible+compatible upgrade.",
        reminder.contains("(compatible with your version"));
    reminder = response.getUpgradeReminder("1.1.0");
    // Should contain info only about an incompatible upgrade.
    assertTrue("Bad upgrade reminder for incompatible upgrade.",
        reminder.contains("(incompatible with your version"));
    assertFalse("Bad upgrade reminder for incompatible upgrade.",
        reminder.contains("(compatible with your version"));
    reminder = response.getUpgradeReminder("3.0.0");
    assertEquals("Got upgrade reminder for irrelevant upgrade.", "", reminder);
  }
}
