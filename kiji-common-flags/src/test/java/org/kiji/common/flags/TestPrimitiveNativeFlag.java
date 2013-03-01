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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests flag parser for primitive values using native types (boolean, short, int, etc).
 */
public class TestPrimitiveNativeFlag {
  private static final Logger LOG = LoggerFactory.getLogger(TestPrimitiveNativeFlag.class);

  private static final class TestObject {
    @Flag public boolean flagBoolean = false;
    @Flag public short flagShort= 0;
    @Flag public int flagInteger = 0;
    @Flag public long flagLong = 123L;
    @Flag public float flagFloat = 0.0f;
    @Flag public double flagDouble = 0.0;
  }

  private TestObject mTest = new TestObject();

  @Test
  public void testDefaultLong() {
    FlagParser.init(mTest, new String[]{});
    assertEquals(123L, mTest.flagLong);
  }

  @Test
  public void testBooleanImplicitTrue() {
    FlagParser.init(mTest, new String[]{"--flagBoolean"});
    assertTrue(mTest.flagBoolean);
  }

  @Test
  public void testBooleanImplicitTrue2() {
    FlagParser.init(mTest, new String[]{"--flagBoolean="});
    assertTrue(mTest.flagBoolean);
  }

  @Test
  public void testBooleanTrue() {
    FlagParser.init(mTest, new String[]{"--flagBoolean=True"});
    assertTrue(mTest.flagBoolean);
  }

  @Test
  public void testBooleanYes() {
    FlagParser.init(mTest, new String[]{"--flagBoolean=yes"});
    assertTrue(mTest.flagBoolean);
  }

  @Test
  public void testBooleanFalse() {
    FlagParser.init(mTest, new String[]{"--flagBoolean=false"});
    assertFalse(mTest.flagBoolean);
  }

  @Test
  public void testBooleanNO() {
    FlagParser.init(mTest, new String[]{"--flagBoolean=NO"});
    assertFalse(mTest.flagBoolean);
  }

  @Test
  public void testBooleanInvalid() {
    try {
      FlagParser.init(mTest, new String[]{"--flagBoolean=tru"});
      fail("tru should not parse as a boolean");
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid command-line argument '--flagBoolean=tru' for flag type boolean: "
          + "'tru' is not a valid value for boolean flag declared in "
          + "'org.kiji.common.flags.TestPrimitiveNativeFlag$TestObject.flagBoolean'.",
          ifve.getMessage());
    }
  }

  @Test
  public void testShort() {
    FlagParser.init(mTest, new String[]{"--flagShort=314"});
    assertEquals(314, (short) mTest.flagShort);
  }

  @Test
  public void testShortNegative() {
    FlagParser.init(mTest, new String[]{"--flagShort=-314"});
    assertEquals(-314, (short) mTest.flagShort);
  }

  @Test
  public void testShortOverflow() {
    try {
      FlagParser.init(mTest, new String[]{"--flagShort=32768"});
      fail("32768 should not parse as a short");
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid command-line argument '--flagShort=32768' for flag type short: "
          + "'32768' is not a valid value for short flag declared in "
          + "'org.kiji.common.flags.TestPrimitiveNativeFlag$TestObject.flagShort'.",
          ifve.getMessage());
    }
  }

  @Test
  public void testInteger() {
    FlagParser.init(mTest, new String[]{"--flagInteger=314159"});
    assertEquals(314159, mTest.flagInteger);
  }

  @Test
  public void testLong() {
    FlagParser.init(mTest, new String[]{"--flagLong=314159"});
    assertEquals(314159, mTest.flagLong);
  }

  @Test
  public void testFloat() {
    FlagParser.init(mTest, new String[]{"--flagFloat=3.14159"});
    assertEquals(3.14159f, mTest.flagFloat, 0.00001);
  }

  /** Tests float flag value with explicit 'f' qualifier, such as 3.14f */
  @Test
  public void testFloatF() {
    FlagParser.init(mTest, new String[]{"--flagFloat=3.14159f"});
    assertEquals(3.14159f, mTest.flagFloat, 0.00001);
  }

  @Test
  public void testDouble() {
    FlagParser.init(mTest, new String[]{"--flagDouble=3.14159"});
    assertEquals(3.14159, mTest.flagDouble, 0.00001);
  }

  @Test
  public void testDoubleNull() {
    try {
      FlagParser.init(mTest, new String[]{"--flagDouble"});
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid double command-line argument '--flagDouble' for flag 'flagDouble' "
          + "declared in 'org.kiji.common.flags.TestPrimitiveNativeFlag$TestObject.flagDouble'.",
          ifve.getMessage());
    }
  }

  @Test
  public void testDoubleInvalid() {
    try {
      FlagParser.init(mTest, new String[]{"--flagDouble=3.4.5"});
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid command-line argument '--flagDouble=3.4.5' for flag type double: "
          + "'3.4.5' is not a valid value for double flag declared in "
          + "'org.kiji.common.flags.TestPrimitiveNativeFlag$TestObject.flagDouble'.",
          ifve.getMessage());
    }
  }
}
