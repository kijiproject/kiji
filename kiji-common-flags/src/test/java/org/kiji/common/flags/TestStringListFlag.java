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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Test;

/** Tests the parser for a list of strings. */
public class TestStringListFlag {
  private static final class TestObject {
    @Flag public List<String> list = null;
  }

  private TestObject mTest = new TestObject();

  @Test
  public void testListUnspecified() {
    FlagParser.init(mTest, new String[]{});
    assertNull(mTest.list);
  }

  @Test
  public void testListSingleton() {
    FlagParser.init(mTest, new String[]{"--list=value1"});
    assertArrayEquals(new String[]{"value1"}, mTest.list.toArray());
  }

  @Test
  public void testListSingletonNull() {
    FlagParser.init(mTest, new String[]{"--list"});
    assertArrayEquals(new String[]{null}, mTest.list.toArray());
  }

  @Test
  public void testListSingletonEmpty() {
    FlagParser.init(mTest, new String[]{"--list="});
    assertArrayEquals(new String[]{""}, mTest.list.toArray());
  }

  @Test
  public void testListMany() {
    FlagParser.init(mTest, new String[]{"--list=value1", "--list=value2"});
    assertArrayEquals(new String[]{"value1", "value2"}, mTest.list.toArray());
  }

  @Test
  public void testListDuplicate() {
    FlagParser.init(mTest, new String[]{"--list=value", "--list=value"});
    assertArrayEquals(new String[]{"value", "value"}, mTest.list.toArray());
  }
}
