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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for flags inferred from the environment. */
public class TestEnvFlag {
  private static final Logger LOG = LoggerFactory.getLogger(TestEnvFlag.class);

  private static final class TestObject {
    @Flag(envVar = "HOME")
    public String home = null;

    @Flag(envVar = "___MISSING___")
    public String missing = null;
  }

  private TestObject mTest = new TestObject();

  @Test
  public void testHomeFromEnv() {
    FlagParser.init(mTest, new String[] {});
    assertTrue(mTest.home.startsWith("/"));  // assumes $HOME is defined externally!
  }

  @Test
  public void testHomeFromFlag() {
    FlagParser.init(mTest, new String[] { "--home=meep" });
    assertEquals("meep", mTest.home);
  }

  @Test
  public void testMissingFromEnv() {
    FlagParser.init(mTest, new String[] {});
    assertNull(mTest.missing);
  }

  @Test
  public void testMissingFromFlag() {
    FlagParser.init(mTest, new String[] { "--missing=waouch" });
    assertEquals("waouch", mTest.missing);
  }
}
