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

package org.kiji.scoring.params;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Test;

import org.kiji.scoring.Parameters;

/** Tests the parser for a list of strings. */
public class TestStringListParam {
  private static final class TestObject extends Parameters {
    @Param public List<String> list = null;
  }

  @Test
  public void testListUnspecified() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext());
    assertNull(testObj.list);
  }

  @Test
  public void testListSingleton() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("list=value1"));
    assertArrayEquals(new String[]{"value1"}, testObj.list.toArray());
  }

  @Test
  public void testListSingletonNull() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("list"));
    assertNull(testObj.list);
  }

  @Test
  public void testListSingletonEmpty() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("list="));
    assertArrayEquals(new String[]{""}, testObj.list.toArray());
  }

  @Test
  public void testListMany() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("list", "value1|value2"));
    assertArrayEquals(new String[] { "value1", "value2" }, testObj.list.toArray());
  }

  @Test
  public void testListDuplicate() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("list", "value|value"));
    assertArrayEquals(new String[] { "value", "value" }, testObj.list.toArray());
  }
}
