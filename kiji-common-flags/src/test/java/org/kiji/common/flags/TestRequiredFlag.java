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

/** Tests the behavior of required flags. */
public class TestRequiredFlag {
  private static final Logger LOG = LoggerFactory.getLogger(TestRequiredFlag.class);

  private static final class TestObject {
    @Flag(required = true) public String flagRequired = null;
  }

  private TestObject mTest = new TestObject();

  @Test
  public void testValidRequired() {
    FlagParser.init(mTest, new String[]{"--flagRequired=value"});
  }

  @Test
  public void testRequiredValidEmpty() {
    FlagParser.init(mTest, new String[]{"--flagRequired="});
  }

  @Test
  public void testRequiredValidNull() {
    FlagParser.init(mTest, new String[]{"--flagRequired"});
  }

  @Test
  public void testRequiredMissing() {
    try {
      FlagParser.init(mTest, new String[]{});
      fail();
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Flag '--flagRequired' is required but has not been specified.",
          ifve.getMessage());
    }
  }
}
