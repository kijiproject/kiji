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
import java.net.URL;
import java.net.URLConnection;
import java.util.Random;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

/**
 * Sampler to time GET requests on rows endpoint with 'eid' query parameter set.
 */
public class GetRequestSampler extends AbstractJavaSamplerClient {
  protected static final Random RANDOM = new Random(System.currentTimeMillis());

  protected static final String ROW_PATH_FORMAT = "%s/v1/instances/%s/tables/%s/rows";

  /** Maximum user id value. */
  protected static final int USER_ID_MAX = 1000000;

  /** URL where the users table exists. */
  protected URL mURL;

  /** {@inheritDoc} */
  @Override
  public Arguments getDefaultParameters() {
      final Arguments arguments = new Arguments();
      arguments.addArgument("domain", "http://localhost:8080");
      arguments.addArgument("instance", "default");
      arguments.addArgument("table", "users");
      arguments.addArgument("timerange", "0..");
      arguments.addArgument("versions", "10");
      return arguments;
  }

  /** {@inheritDoc} */
  @Override
  public void setupTest(JavaSamplerContext context) {
    final String rowPath = String.format(ROW_PATH_FORMAT,
        context.getParameter("domain"),
        context.getParameter("instance"),
        context.getParameter("table"));
    final String timerange = context.getParameter("timerange");
    final String versions = context.getParameter("versions");
    final int userId = RANDOM.nextInt(USER_ID_MAX);
    try {
      mURL = new URL(
          String.format(
              "%s?eid=%d&timerange=%s&versions=%s",
              rowPath,
              userId,
              timerange,
              versions));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Bad URL: %s?eid=%d&timerange=%s&versions=%s",
              rowPath,
              userId,
              timerange,
              versions));
    }
  }

  /** {@inheritDoc} */
  @Override
  public SampleResult runTest(JavaSamplerContext context) {
    final SampleResult result = new SampleResult();
    result.sampleStart();
    try {
      final URLConnection connection = (URLConnection) mURL.openConnection();
      final BufferedReader reader = new BufferedReader(
          new InputStreamReader(connection.getInputStream()));
      while (null != reader.readLine());
      result.sampleEnd();
      result.setSuccessful(true);
      reader.close();
    } catch (Exception e) {
      result.sampleEnd();
      result.setSuccessful(false);
    }
    return result;
  }
}
