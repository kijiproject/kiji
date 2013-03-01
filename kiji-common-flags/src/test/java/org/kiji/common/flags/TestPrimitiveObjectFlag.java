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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests flag parser for primitive values as object instances (Boolean, Short, Integer, etc).
 */
public class TestPrimitiveObjectFlag {
  private static final Logger LOG = LoggerFactory.getLogger(TestPrimitiveObjectFlag.class);

  private static final class TestObject {
    @Flag public Boolean flagBoolean = null;
    @Flag public Short flagShort= null;
    @Flag public Integer flagInteger = null;
    @Flag public Long flagLong = null;
    @Flag public Float flagFloat = null;
    @Flag public Double flagDouble = null;
  }

  private final TestObject mTest = new TestObject();

  @Test
  public void testUnsetBoolean() {
    FlagParser.init(mTest, new String[]{});
    assertNull(mTest.flagBoolean);
  }

  @Test
  public void testTrueBoolean() {
    FlagParser.init(mTest, new String[]{"--flagBoolean"});
    assertTrue(mTest.flagBoolean);
  }

  @Test
  public void testFalseBoolean() {
    FlagParser.init(mTest, new String[]{"--flagBoolean=false"});
    assertFalse(mTest.flagBoolean);
  }

  @Test
  public void testShort() {
    FlagParser.init(mTest, new String[]{"--flagShort=314"});
    assertEquals(314, (short) mTest.flagShort);
  }

  @Test
  public void testShortOverflow() {
    try {
      FlagParser.init(mTest, new String[]{"--flagShort=32768"});
      fail("32768 should not parse as a short");
    } catch (IllegalFlagValueException ifve) {
      // Expected exception
    }
  }

  @Test
  public void testInteger() {
    FlagParser.init(mTest, new String[]{"--flagInteger=314159"});
    assertEquals((Integer) 314159, mTest.flagInteger);
  }

  @Test
  public void testLong() {
    FlagParser.init(mTest, new String[]{"--flagLong=314159"});
    assertEquals((Long) 314159L, mTest.flagLong);
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

  /** Tests parsing of an explicit null flag. */
  @Test
  public void testDoubleSetToNull() {
    mTest.flagDouble = 1.0;
    FlagParser.init(mTest, new String[]{"--flagDouble"});
    assertNull(mTest.flagDouble);
  }

  @Test
  public void testDoubleEmptyString() {
    try {
      FlagParser.init(mTest, new String[]{"--flagDouble="});
      fail("Empty string is not a valid double value.");
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid command-line argument '--flagDouble=' for flag type class java.lang.Double: "
          + "'' is not a valid value for class java.lang.Double flag declared in "
          + "'org.kiji.common.flags.TestPrimitiveObjectFlag$TestObject.flagDouble'.",
          ifve.getMessage());
    }
  }
}
