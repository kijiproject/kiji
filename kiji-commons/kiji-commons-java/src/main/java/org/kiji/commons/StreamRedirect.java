/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.commons;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;

/** Thread which redirects all content from an InputStream to an OutputStream. */
@ApiAudience.Framework
@ApiStability.Experimental
public final class StreamRedirect extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(StreamRedirect.class);

  /**
   * Create a new StreamRedirect from the given InputStream to the given OutputStream.
   *
   * @param in InputStream to redirect to the given OutputStream.
   * @param out OutputStream into which to redirect values from the given InputStream.
   * @return a new StreamRedirect from the given InputStream to the given OutputStream.
   */
  public static StreamRedirect create(
      final InputStream in,
      final OutputStream out
  ) {
    return new StreamRedirect(in, out);
  }

  private final InputStream mIn;
  private final OutputStream mOut;

  /**
   * Private constructor.
   *
   * @param in InputStream to redirect to the given OutputStream.
   * @param out OutputStream into which to redirect values from the given InputStream.
   */
  private StreamRedirect(
      final InputStream in,
      final OutputStream out
  ) {
    mIn = in;
    mOut = out;
  }

  /**
   * {@inheritDoc}
   *
   * Redirects all content from the configured InputStream to the configured OutputStream.
   */
  @Override
  public void run() {
    try (
        // CSOFF: InnerAssignmentCheck
        final BufferedReader reader = new BufferedReader(new InputStreamReader(mIn));
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(mOut))
        // CSON: InnerAssignmentCheck
    ) {
      String line = reader.readLine();
      while (line != null) {
        writer.write(line);
        line = reader.readLine();
        if (line != null) {
          writer.newLine();
        }
      }
    } catch (IOException ioe) {
      LOG.error("Exception while redirecting stream.", ioe);
      throw new RuntimeException(ioe);
    }
  }
}
