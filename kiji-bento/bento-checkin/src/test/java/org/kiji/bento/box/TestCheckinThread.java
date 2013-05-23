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

package org.kiji.bento.box;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.CheckinClient;
import org.kiji.checkin.CheckinUtils;
import org.kiji.checkin.models.UpgradeCheckin;
import org.kiji.checkin.models.UpgradeResponse;

/**
 * Tests the functionality of {@link CheckinThread}.
 */
public class TestCheckinThread {
  private static final Logger LOG = LoggerFactory.getLogger(TestCheckinThread.class);

  private static final String EXPECTED_ID = "theuser";

  private static final String RESPONSE_JSON =
      "{\"response_format\":\"bento-checkversion-1.0.0\","
          + "\"latest_version\":\"2.0.0\","
          + "\"latest_version_url\":\"http://www.foocorp.com\","
          + "\"compatible_version\":\"2.0.0\","
          + "\"compatible_version_url\":\"http://www.foocorp.com\","
          + "\"msg\":\"better upgrade!\"}";
  private static final String EXPECTED_WRITTEN_JSON =
      "{\"response_format\":\"bento-checkversion-1.0.0\","
          + "\"latest_version\":\"2.0.0\","
          + "\"latest_version_url\":\"http://www.foocorp.com\","
          + "\"compatible_version\":\"2.0.0\","
          + "\"compatible_version_url\":\"http://www.foocorp.com\","
          + "\"msg\":\"better upgrade!\","
          + "\"last_reminder\":0}";

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private File getUpgradeInfoFile() {
    return new File(mTempFolder.getRoot(), ".kiji-bento-upgrade");
  }

  private File getTimestampFile() {
    return new File(mTempFolder.getRoot(), ".kiji-last-used");
  }

  private void runCheckinThread(long expectedTimestamp) throws IOException, InterruptedException {
    LOG.info("Creating mock upgrade server client to use in check-in thread test.");
    // We'll create the check-in thread with a mocked upgrade server client.
    CheckinClient mockClient = EasyMock.createMock(CheckinClient.class);
    // We expect to do two check-ins with the upgrade server. Both check-ins should be sending
    // the same message and receiving the same response.
    UpgradeCheckin expectedCheckin = new UpgradeCheckin.Builder(this.getClass())
        .withId(EXPECTED_ID)
        .withLastUsedMillis(expectedTimestamp)
        .build();
    UpgradeResponse expectedResponse = UpgradeResponse.fromJSON(RESPONSE_JSON);
    EasyMock.expect(mockClient.checkin(EasyMock.eq(expectedCheckin)))
        .andReturn(expectedResponse)
        .times(2);
    // And we should close on termination.
    mockClient.close();
    EasyMock.expectLastCall();
    EasyMock.replay(mockClient);

    // Create a thread.
    CheckinThread thread = new CheckinThread(EXPECTED_ID, getTimestampFile(),
        getUpgradeInfoFile(), 2000L, mockClient);
    // Now start it running.
    thread.start();
    // Give the thread the chance to do 2 check-ins.
    Thread.sleep(2050L);
    // Now finish the thread and interrupt it in case it's sleeping.
    thread.finish();
    thread.interrupt();
    // Now wait for the thread to actually die.
    thread.join();

    // Now verify the mock and the upgrade info file contents.
    EasyMock.verify(mockClient);
    String upgradeInfoFileJson = CheckinUtils.readFileAsString(getUpgradeInfoFile()).trim();
    LOG.info("JSON written to upgrade file was: " + upgradeInfoFileJson);
    assertEquals("Bad upgrade info file written.", EXPECTED_WRITTEN_JSON, upgradeInfoFileJson);
  }

  @Test
  public void testCheckinThreadNoTSFile() throws IOException, InterruptedException {
    runCheckinThread(0L);
  }

  @Test
  public void testCheckinThreadTSFile() throws IOException, InterruptedException {
    Long timestamp = 4951L;
    CheckinUtils.writeObjectToFile(getTimestampFile(), timestamp);
    runCheckinThread(4951L);
  }
}
