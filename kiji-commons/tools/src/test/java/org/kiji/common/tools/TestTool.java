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

package org.kiji.common.tools;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * A tool for tests.
 */
public final class TestTool implements KijiTool {
  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "test-tool";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "A test tool for tests.";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "testing";
  }

  /** {@inheritDoc} */
  @Override
  public Configuration generateConfiguration() {
    return new Configuration();
  }

  /** {@inheritDoc} */
  @Override
  public int toolMain(final List<String> args, final Configuration configuration) {
    // Do nothing in this test tool.
    return 0;
  }

  /** {@inheritDoc} */
  @Override
  public String getUsageString() {
    return "";
  }
}
