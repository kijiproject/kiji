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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.scoring.Parameters;

/**
 * Tests param parser for primitive values as object instances (Boolean, Short, Integer, etc).
 */
public class TestPrimitiveObjectParam {
  private static final Logger LOG = LoggerFactory.getLogger(TestPrimitiveObjectParam.class);

  private static final class TestObject extends Parameters {
    @Param public Boolean paramBoolean = null;
    @Param public Short paramShort= null;
    @Param public Integer paramInteger = null;
    @Param public Long paramLong = null;
    @Param public Float paramFloat = null;
    @Param public Double paramDouble = null;
  }

  @Test
  public void testUnsetBoolean() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext());
    assertNull(testObj.paramBoolean);
  }

  @Test
  public void testFalseBoolean() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramBoolean=false"));
    assertFalse(testObj.paramBoolean);
  }

  @Test
  public void testShort() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramShort=314"));
    assertEquals(314, (short) testObj.paramShort);
  }

  @Test
  public void testShortOverflow() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("paramShort=32768"));
      fail("32768 should not parse as a short");
    } catch (IllegalParamValueException ifve) {
      // Expected exception
    }
  }

  @Test
  public void testInteger() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramInteger=314159"));
    assertEquals((Integer) 314159, testObj.paramInteger);
  }

  @Test
  public void testLong() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramLong=314159"));
    assertEquals((Long) 314159L, testObj.paramLong);
  }

  @Test
  public void testFloat() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramFloat=3.14159"));
    assertEquals(3.14159f, testObj.paramFloat, 0.00001);
  }

  /** Tests float param value with explicit 'f' qualifier, such as 3.14f. */
  @Test
  public void testFloatF() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramFloat=3.14159f"));
    assertEquals(3.14159f, testObj.paramFloat, 0.00001);
  }

  @Test
  public void testDouble() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramDouble=3.14159"));
    assertEquals(3.14159, testObj.paramDouble, 0.00001);
  }

  @Test
  public void testDoubleEmptyString() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("paramDouble="));
      fail("Empty string is not a valid double value.");
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid parameter argument 'paramDouble=' for param type Double: "
          + "'' is not a valid value for Double param declared in "
          + "'org.kiji.scoring.params.TestPrimitiveObjectParam$TestObject.paramDouble'.",
          ifve.getMessage());
    }
  }
}
