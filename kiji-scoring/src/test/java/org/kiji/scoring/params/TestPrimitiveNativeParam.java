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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.scoring.Parameters;

/**
 * Tests Param parser for primitive values using native types (boolean, short, int, etc).
 */
public class TestPrimitiveNativeParam {
  private static final Logger LOG = LoggerFactory.getLogger(TestPrimitiveNativeParam.class);

  private static final class TestObject extends Parameters {
    @Param public boolean ParamBoolean = false;
    @Param public short ParamShort = 0;
    @Param public int ParamInteger = 0;
    @Param public long ParamLong = 123L;
    @Param public float ParamFloat = 0.0f;
    @Param public double ParamDouble = 0.0;
  }

  @Test
  public void testDefaultLong() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext());
    assertEquals(123L, testObj.ParamLong);
  }

  @Test
  public void testBooleanTrue() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamBoolean=True"));
    assertTrue(testObj.ParamBoolean);
  }

  @Test
  public void testBooleanYes() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamBoolean=yes"));
    assertTrue(testObj.ParamBoolean);
  }

  @Test
  public void testBooleanFalse() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamBoolean=false"));
    assertFalse(testObj.ParamBoolean);
  }

  @Test
  public void testBooleanNO() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamBoolean=NO"));
    assertFalse(testObj.ParamBoolean);
  }

  @Test
  public void testBooleanInvalid() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("ParamBoolean=tru"));
      fail("tru should not parse as a boolean");
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid parameter argument 'ParamBoolean=tru' for param type boolean: "
              + "'tru' is not a valid value for boolean param declared in "
              + "'org.kiji.scoring.params.TestPrimitiveNativeParam$TestObject.ParamBoolean'.",
          ifve.getMessage());
    }
  }

  @Test
  public void testShort() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamShort=314"));
    assertEquals(314, testObj.ParamShort);
  }

  @Test
  public void testShortNegative() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamShort=-314"));
    assertEquals(-314, testObj.ParamShort);
  }

  @Test
  public void testShortOverflow() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("ParamShort=32768"));
      fail("32768 should not parse as a short");
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid parameter argument 'ParamShort=32768' for param type short: "
              + "'32768' is not a valid value for short param declared in "
              + "'org.kiji.scoring.params.TestPrimitiveNativeParam$TestObject.ParamShort'.",
          ifve.getMessage());
    }
  }

  @Test
  public void testInteger() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamInteger=314159"));
    assertEquals(314159, testObj.ParamInteger);
  }

  @Test
  public void testLong() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamLong=314159"));
    assertEquals(314159, testObj.ParamLong);
  }

  @Test
  public void testFloat() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamFloat=3.14159"));
    assertEquals(3.14159f, testObj.ParamFloat, 0.00001);
  }

  /** Tests float Param value with explicit 'f' qualifier, such as 3.14f. */
  @Test
  public void testFloatF() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamFloat=3.14159f"));
    assertEquals(3.14159f, testObj.ParamFloat, 0.00001);
  }

  @Test
  public void testDouble() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("ParamDouble=3.14159"));
    assertEquals(3.14159, testObj.ParamDouble, 0.00001);
  }

//  @Test
//  public void testDoubleNull() {
//    try {
//      TestObject testObj = new TestObject();
//      ParamParser.init(testObj, new String[] { "--ParamDouble" });
//    } catch (IllegalParamValueException ifve) {
//      LOG.debug("Expected exception: {}", ifve.getMessage());
//      assertEquals(
//          "Invalid double parameter argument '--ParamDouble' for param 'ParamDouble' declared "
//              + "in 'org.kiji.scoring.params.TestPrimitiveNativeParam$TestObject.ParamDouble'.",
//          ifve.getMessage());
//    }
//  }

  @Test
  public void testDoubleInvalid() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("ParamDouble=3.4.5"));
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid parameter argument 'ParamDouble=3.4.5' for param type double: "
              + "'3.4.5' is not a valid value for double param declared in "
              + "'org.kiji.scoring.params.TestPrimitiveNativeParam$TestObject.ParamDouble'.",
          ifve.getMessage());
    }
  }
}
