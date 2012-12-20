/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce;

import java.io.IOException;

import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;

/** Layouts used for testing KijiMR. */
public final class KijiMRTestLayouts {

  /**
   * Alias for KijiTableLayouts.getLayout(resource).
   *
   * @param resourcePath Path of the resource to load the JSON descriptor from.
   * @return the decoded TableLayoutDesc.
   * @throws IOException on I/O error.
   */
  public static TableLayoutDesc getLayout(String resourcePath) throws IOException {
    return KijiTableLayouts.getLayout(resourcePath);
  }

  /** Generic test layout with user info, all primitive types, a map-type family of strings. */
  public static final String TEST_LAYOUT = "org/kiji/mapreduce/layout/test.json";

  /**
   * @return a generic test layout.
   * @throws IOException on I/O error.
   */
  public static TableLayoutDesc getTestLayout() throws IOException {
    return getLayout(TEST_LAYOUT);
  }

  /** Utility class cannot be instantiated. */
  private KijiMRTestLayouts() {
  }
}
