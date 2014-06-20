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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.scoring.Parameters;

/** Tests the param parser for enumeration values. */
public class TestEnumParam {
  private static final Logger LOG = LoggerFactory.getLogger(TestEnumParam.class);

  public static enum Values {
    VALUE_ONE,
    VALUE_TWO,
    VALUE_THREE
  }

  private static final class TestObject extends Parameters {
    @Param public Values paramEnum = null;
  }

  @Test
  public void testEnumUnset() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext());
    assertNull(testObj.paramEnum);
  }

  @Test
  public void testEnumValueOne() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramEnum=VALUE_ONE"));
    assertEquals(Values.VALUE_ONE, testObj.paramEnum);
  }

  @Test
  public void testEnumValueOneLowerCase() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramEnum=value_one"));
    assertEquals(Values.VALUE_ONE, testObj.paramEnum);
  }

  @Test
  public void testEnumValueTwo() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("paramEnum=VALUE_TWO"));
    assertEquals(Values.VALUE_TWO, testObj.paramEnum);
  }

  @Test
  public void testEnumUnknown() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("paramEnum=unknown"));
      fail();
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid Values enum parameter argument 'paramEnum=unknown': "
          + "expecting one of VALUE_ONE,VALUE_TWO,VALUE_THREE.",
          ifve.getMessage());
    }
  }
}
