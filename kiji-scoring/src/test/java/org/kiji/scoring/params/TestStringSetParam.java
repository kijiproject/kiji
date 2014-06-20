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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.kiji.scoring.Parameters;

/** Tests the parser for a set of strings. */
public class TestStringSetParam {
  private static final class TestObject extends Parameters {
    @Param public Set<String> set = null;
  }

  @Test
  public void testSetUnspecified() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext());
    assertNull(testObj.set);
  }

  @Test
  public void testSetSingleton() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("set=value1"));
    assertEquals(Sets.newHashSet("value1"), testObj.set);
  }

  @Test
  public void testSetSingletonNull() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("set"));
    assertNull(testObj.set);
  }

  @Test
  public void testSetSingletonEmpty() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("set="));
    assertEquals(Sets.newHashSet(""), testObj.set);
  }

  @Test
  public void testSetSeparated() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("set", "value1|value2|value3"));
    assertEquals(Sets.newHashSet("value1", "value2", "value3"), testObj.set);
  }

  @Test
  public void testSetDuplicate() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("set", "value|value"));
    assertEquals(Sets.newHashSet("value"), testObj.set);
  }
}
