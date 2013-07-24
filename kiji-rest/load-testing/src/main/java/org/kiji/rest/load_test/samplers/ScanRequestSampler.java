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

import java.net.URL;

import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.soap.encoding.Hex;

/**
 * Sampler to time GET requests on rows endpoint to perform range scans.
 */
public class ScanRequestSampler extends GetRequestSampler {
  /** Maximum value of the limit query parameter. */
  private static final int LIMIT_MAX = 200;

  /** {@inheritDoc} */
  @Override
  protected void setup(JavaSamplerContext context) {
    final String rowPath = String.format(ROW_PATH_FORMAT,
        context.getParameter("domain"),
        context.getParameter("instance"),
        context.getParameter("table"));
    final String timerange = context.getParameter("timerange");
    final String versions = context.getParameter("versions");
    final byte[] startRowKeyBytes = new byte[6];
    RANDOM.nextBytes(startRowKeyBytes);
    final String startRowKey = Hex.encode(startRowKeyBytes);
    final int limit = RANDOM.nextInt(LIMIT_MAX);
    try {
      mURL = new URL(
          String.format(
              "%s?start_rk=%s&limit=%d&timerange=%s&versions=%s",
              rowPath,
              startRowKey,
              limit,
              timerange,
              versions));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Bad URL: %s?start_rk=%s&limit=%d&timerange=%s&versions=%s",
              rowPath,
              startRowKey,
              limit,
              timerange,
              versions));
    }
  }
}
