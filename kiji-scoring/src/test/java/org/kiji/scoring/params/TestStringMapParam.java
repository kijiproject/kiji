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
import static org.junit.Assert.fail;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.scoring.Parameters;

/** Tests the parser for a map of strings. */
public class TestStringMapParam {
  private static final Logger LOG = LoggerFactory.getLogger(TestStringMapParam.class);

  private static final class TestObject extends Parameters{
    @Param public Map<String, String> map = null;
  }

  @Test
  public void testMapUnspecified() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext());
    assertNull(testObj.map);
  }

  @Test
  public void testMapSingleton() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("map", "key1=value1"));
    assertEquals(
        ImmutableMap.<String, String>builder().put("key1", "value1").build(),
        testObj.map);
  }

  @Test
  public void testMapMany() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("map", "key1=value1|key2=value2"));
    assertEquals(
        ImmutableMap.<String, String>builder()
            .put("key1", "value1")
            .put("key2", "value2")
            .build(),
        testObj.map);
  }

  @Test
  public void testMapDuplicate() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("map", "key=value1|key=value2"));
    assertEquals(
        ImmutableMap.<String, String>builder().put("key", "value2").build(),
        testObj.map);
  }

  @Test
  public void testInvalidKeyValue() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("map", "key"));
      fail();
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
    }
  }

  @Test
  public void testInvalidKeyValueEmpty() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("map="));
      fail();
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
    }
  }

  @Test
  public void testInvalidKeyValueNull() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("map"));
    assertNull(testObj.map);
  }
}
