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

package org.kiji.checkin;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.easymock.EasyMock;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.checkin.models.UpgradeCheckin;
import org.kiji.checkin.models.UpgradeResponse;

/**
 * Tests the functionality of {@link UpgradeServerClient}.
 */
public class TestCheckinClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestCheckinClient.class);

  private static final String RESPONSE_JSON =
      "{\"response_format\":\"bento-checkversion-1.0.0\","
      + "\"latest_version\":\"2.0.0\","
      + "\"latest_version_url\":\"http://www.foocorp.com\","
      + "\"compatible_version\":\"2.0.0\","
      + "\"compatible_version_url\":\"http://www.foocorp.com\","
      + "\"msg\":\"better upgrade!\"}";

  @Test
  public void testCheckin() throws IOException, URISyntaxException {
    LOG.info("Creating mocks.");
    // We'll create an UpgradeServerClient using a mock http client. The client will
    // execute one request, which will return an http response, which in turn will return
    // an entity that will provide the content for the response.
    InputStream contentStream = IOUtils.toInputStream(RESPONSE_JSON);
    HttpEntity mockResponseEntity = EasyMock.createMock(HttpEntity.class);
    EasyMock.expect(mockResponseEntity.getContent()).andReturn(contentStream);
    EasyMock.expect(mockResponseEntity.getContentLength()).andReturn(-1L).times(2);
    EasyMock.expect(mockResponseEntity.getContentType())
        .andReturn(new BasicHeader("ContentType", ContentType.APPLICATION_JSON.toString()));

    HttpResponse mockResponse = EasyMock.createMock(HttpResponse.class);
    EasyMock.expect(mockResponse.getEntity()).andReturn(mockResponseEntity);

    HttpClient mockClient = EasyMock.createMock(HttpClient.class);
    EasyMock.expect(mockClient.execute(EasyMock.isA(HttpPost.class))).andReturn(mockResponse);

    LOG.info("Replaying mocks.");
    EasyMock.replay(mockResponseEntity);
    EasyMock.replay(mockResponse);
    EasyMock.replay(mockClient);

    // Create the upgrade server client.
    LOG.info("Creating upgrade server client and making request.");
    CheckinClient upgradeClient = CheckinClient.create(mockClient,
        new URI("http://www.foocorp.com"));
    UpgradeCheckin checkinMessage = new UpgradeCheckin.Builder(this.getClass())
        .withId("12345")
        .withLastUsedMillis(1L)
        .build();
    UpgradeResponse response = upgradeClient.checkin(checkinMessage);

    LOG.info("Verifying mocks.");
    EasyMock.verify(mockResponseEntity, mockResponse, mockClient);

    // Verify the response content.
    LOG.info("Verifying response content.");
    assertEquals("Bad response format.", "bento-checkversion-1.0.0", response.getResponseFormat());
    assertEquals("Bad latest version", "2.0.0", response.getLatestVersion());
    assertEquals("Bad latest version URL.", "http://www.foocorp.com",
        response.getLatestVersionURL().toString());
    assertEquals("Bad compatible version.", "2.0.0", response.getCompatibleVersion());
    assertEquals("Bad compatible version URL.", "http://www.foocorp.com",
        response.getCompatibleVersionURL().toString());
    assertEquals("Bad response msg.", "better upgrade!", response.getMessage());
  }
}
