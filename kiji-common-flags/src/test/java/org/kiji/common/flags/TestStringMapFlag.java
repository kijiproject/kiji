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
import static org.junit.Assert.fail;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests the parser for a map of strings. */
public class TestStringMapFlag {
  private static final Logger LOG = LoggerFactory.getLogger(TestStringMapFlag.class);

  private static final class TestObject {
    @Flag public Map<String, String> map = null;
  }

  private TestObject mTest = new TestObject();

  @Test
  public void testMapUnspecified() {
    FlagParser.init(mTest, new String[]{});
    assertNull(mTest.map);
  }

  @Test
  public void testMapSingleton() {
    FlagParser.init(mTest, new String[]{"--map=key1=value1"});
    assertEquals(
        ImmutableMap.<String, String>builder().put("key1", "value1").build(),
        mTest.map);
  }

  @Test
  public void testMapMany() {
    FlagParser.init(mTest, new String[]{"--map=key1=value1", "--map=key2=value2"});
    assertEquals(
        ImmutableMap.<String, String>builder()
            .put("key1", "value1")
            .put("key2", "value2")
            .build(),
        mTest.map);
  }

  @Test
  public void testMapDuplicate() {
    FlagParser.init(mTest, new String[]{"--map=key=value1", "--map=key=value2"});
    assertEquals(
        ImmutableMap.<String, String>builder().put("key", "value2").build(),
        mTest.map);
  }

  @Test
  public void testInvalidKeyValue() {
    try {
      FlagParser.init(mTest, new String[]{"--map=key"});
      fail();
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
    }
  }

  @Test
  public void testInvalidKeyValueEmpty() {
    try {
      FlagParser.init(mTest, new String[]{"--map="});
      fail();
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
    }
  }

  @Test
  public void testInvalidKeyValueNull() {
    try {
      FlagParser.init(mTest, new String[]{"--map"});
      fail();
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
    }
  }
}
