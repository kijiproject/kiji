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

package org.kiji.rest.load_test.samplers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

/**
 * Sampler to time POST requests on rows endpoint.
 */
public class PostRequestSampler extends GetRequestSampler {
  private static final String KIJI_REST_ROW_FORMAT = "{\"entityId\":\"%s\",\"cells\":{\"info\":"
      + "{\"email\":[{\"value\":\"%s\",\"timestamp\":%d}],"
      + "\"name\":[{\"value\":\"%s\",\"timestamp\":%d}]}}}";

  /** JSON string of rows to POST. */
  private String mPostRows;

  /** {@inheritDoc} */
  @Override
  public Arguments getDefaultParameters() {
      final Arguments arguments = new Arguments();
      arguments.addArgument("domain", "http://localhost:8080");
      arguments.addArgument("instance", "default");
      arguments.addArgument("table", "users");
      arguments.addArgument("number of rows", "1");
      return arguments;
  }

  /** {@inheritDoc} */
  @Override
  public void setupTest(JavaSamplerContext context) {
    final String rowPath = String.format(ROW_PATH_FORMAT,
        context.getParameter("domain"),
        context.getParameter("instance"),
        context.getParameter("table"));
    final int numberOfRows = context.getIntParameter("number of rows");
    try {
      mURL = new URL(rowPath);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Bad URL: %s", rowPath));
    }
    if (0 > numberOfRows) {
      throw new RuntimeException("Number of rows must be positive.");
    }
    mPostRows = constructRowList(numberOfRows);
  }

  /**
   * Construct a JSON row with random entity id.
   *
   * @return random row.
   */
  private static String constructRandomRow() {
    // Get a positive random long timestamp.
    long timestamp = RANDOM.nextLong();
    timestamp = (timestamp < 0) ? -1 * (timestamp + 1) : timestamp;

    return String.format(KIJI_REST_ROW_FORMAT,
        RANDOM.nextInt(USER_ID_MAX),
        "cristoforo.colombo@genoa.it",
        timestamp, "Cristoforo Colombo",
        timestamp);
  }

  /**
   * Construct a JSON list of KijiREST rows.
   *
   * @param numRows the number of rows to construct.
   * @return JSON list of rows.
   */
  private static String constructRowList(final int numRows) {
    assert(numRows >= 0);
    final StringBuilder rowList = new StringBuilder();
    rowList.append(constructRandomRow());
    for (int i = 1; i < numRows; i++) {
      rowList.append(String.format(",%s", constructRandomRow()));
    }
    return String.format("[%s]", rowList);
  }

  /** {@inheritDoc} */
  @Override
  public SampleResult runTest(JavaSamplerContext context) {
    final SampleResult result = new SampleResult();
    final StringWriter resultMessage = new StringWriter();
    result.sampleStart();
    try {
      final URLConnection connection = (URLConnection) mURL.openConnection();
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setDoOutput(true);
      OutputStreamWriter outsw = new OutputStreamWriter(connection.getOutputStream());
      outsw.write(mPostRows);
      outsw.close();
      final BufferedReader reader = new BufferedReader(
          new InputStreamReader(connection.getInputStream()));
      String resultLine = reader.readLine();
      resultMessage.append(resultLine + "\n");
      result.setLatency(result.currentTimeInMillis() - result.getStartTime());
      while (null != (resultLine = reader.readLine())) {
        resultMessage.append(resultLine + "\n");
      }
      result.sampleEnd();
      result.setSuccessful(true);
      result.setDataType(MediaType.APPLICATION_JSON);
      result.setResponseMessage("OK");
      result.setResponseData(resultMessage.toString(), null);
      result.setResponseCodeOK();
      reader.close();
    } catch (Exception e) {
      result.sampleEnd();
      result.setSuccessful(false);
      result.setResponseMessage(e.getMessage());
      e.printStackTrace(new PrintWriter(resultMessage));
      result.setResponseData(resultMessage.toString(), null);
      result.setDataType(SampleResult.TEXT);
      if (e instanceof WebApplicationException) {
        result.setResponseCode(
            Integer.toString(((WebApplicationException) e).getResponse().getStatus()));
      } else {
        result.setResponseCode("500");
      }
    }
    return result;
  }
}
