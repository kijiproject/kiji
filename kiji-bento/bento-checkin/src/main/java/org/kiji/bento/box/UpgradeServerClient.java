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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

/**
 * <p>Client for sending check-in messages to the BentoBox upgrade server. Use the static method
 * {@link #create(org.apache.http.client.HttpClient, java.net.URI)} to obtain an instance that
 * can communicate with a particular upgrade server. Then, use the method
 * {@link #checkin(UpgradeCheckin)} to send a check-in message to the upgrade server and receive
 * a response.</p>
 *
 * <p>Instance of this class are created using an instance of {@link HttpClient}. While the method
 * {@link #checkin(UpgradeCheckin)} will release any resources created for an individual request,
 * clients must close the underlying {@link HttpClient} when it is no longer needed by calling
 * {@link #close()}.</p>
 */
public class UpgradeServerClient implements Closeable {

  /** The http client to use when making requests. */
  private final HttpClient mHttpClient;
  /** The URI to a BentoBox checkin server. */
  private final URI mCheckinServerURI;

  /**
   * Creates a new instance that will make requests to a {@link URI} using an http client.
   *
   * @param httpClient will be used to make http requests.
   * @param checkinServerURI where requests will be sent.
   */
  UpgradeServerClient(HttpClient httpClient, URI checkinServerURI) {
    mHttpClient = httpClient;
    mCheckinServerURI = checkinServerURI;
  }

  /**
   * Creates a new instance that will make requests to a {@link URI} using an http client.
   *
   * @param httpClient will be used to make http requests.
   * @param checkinServerURI where requests will be sent.
   * @return a new upgrade server client.
   */
  public static UpgradeServerClient create(HttpClient httpClient, URI checkinServerURI) {
    return new UpgradeServerClient(httpClient, checkinServerURI);
  }

  /**
   * Sends a check-in message to the upgrade server and receives a response.
   *
   * @param checkinMessage to send to the upgrade server.
   * @return the response sent by the server.
   * @throws IOException if there is a problem making the http request, consuming the response
   *     to the request, or if the upgrade server responded with an error.
   */
  public UpgradeResponse checkin(UpgradeCheckin checkinMessage)
      throws IOException {
    // Create a POST request to send to the update server.
    HttpPost postRequest = new HttpPost(mCheckinServerURI);
    // Create an Entity for the message body with the proper content type and content taken
    // from the provided checkin message.
    HttpEntity postRequestEntity = new StringEntity(checkinMessage.toJSON(),
        ContentType.APPLICATION_JSON);
    postRequest.setEntity(postRequestEntity);

    // Execute the request.
    HttpResponse response = mHttpClient.execute(postRequest);
    try {
      HttpEntity responseEntity = response.getEntity();
      String responseBody = EntityUtils.toString(responseEntity);
      // The response body should contain JSON that can be used to create an upgrade response.
      return UpgradeResponse.fromJSON(responseBody);
    } finally {
      postRequest.releaseConnection();
    }
  }

  /**
   * @return the URI of the upgrade server check-in messages are sent to.
   */
  public URI getServerURI() {
    return mCheckinServerURI;
  }

  /**
   * Closes resources (specifically the http client) used by this instance.
   */
  public void close() {
    mHttpClient.getConnectionManager().shutdown();
  }
}
