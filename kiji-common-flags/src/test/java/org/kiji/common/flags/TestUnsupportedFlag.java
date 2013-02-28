/**
 * Licensed to WibiData, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  WibiData, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.kiji.common.flags;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests flag parser for primitive values as object instances (Boolean, Short, Integer, etc).
 */
public class TestUnsupportedFlag {
  private static final Logger LOG = LoggerFactory.getLogger(TestUnsupportedFlag.class);

  private static final class TestObject {
    @Flag public Object unsupported = null;
  }

  /**
   * Ensure unsupported flag types are detected early and reported even if such flags are not used.
   */
  @Test
  public void testUnusedUnsupported() {
    final TestObject test = new TestObject();
    try {
      FlagParser.init(test, new String[]{});
      fail("Should fail");
    } catch (UnsupportedFlagTypeException ufte) {
      LOG.debug("Expected exception: {}", ufte.getMessage());
      assertEquals(
          "Unsupported type 'class java.lang.Object' for flag '--unsupported' "
          + "declared in 'org.kiji.common.flags.TestUnsupportedFlag$TestObject.unsupported'.",
          ufte.getMessage());
    }
  }

  @Test
  public void testEnumUnset() {
    final TestObject test = new TestObject();
    try {
      FlagParser.init(test, new String[]{"--unsupported"});
      fail("Should fail");
    } catch (UnsupportedFlagTypeException ufte) {
      LOG.debug("Expected exception: {}", ufte.getMessage());
      assertEquals(
          "Unsupported type 'class java.lang.Object' for flag '--unsupported' "
          + "declared in 'org.kiji.common.flags.TestUnsupportedFlag$TestObject.unsupported'.",
          ufte.getMessage());
    }
  }
}
