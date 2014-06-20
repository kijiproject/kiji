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
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.scoring.Parameters;

/**
 * Tests flag parser for primitive values as object instances (Boolean, Short, Integer, etc).
 */
public class TestUnsupportedParam {
  private static final Logger LOG = LoggerFactory.getLogger(TestUnsupportedParam.class);

  private static final class TestObject extends Parameters {
    @Param public Object unsupported = null;
  }

  /**
   * Ensure unsupported flag types are detected early and reported even if such flags are not used.
   */
  @Test
  public void testUnusedUnsupported() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext());
      fail("Should fail");
    } catch (UnsupportedParamTypeException upte) {
      LOG.debug("Expected exception: {}", upte.getMessage());
      assertEquals(
          "Unsupported type 'class java.lang.Object' for parameter 'unsupported' "
          + "declared in 'org.kiji.scoring.params.TestUnsupportedParam$TestObject.unsupported'.",
          upte.getMessage());
    }
  }

  @Test
  public void testEnumUnset() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("unsupported"));
      fail("Should fail");
    } catch (UnsupportedParamTypeException upte) {
      LOG.debug("Expected exception: {}", upte.getMessage());
      assertEquals(
          "Unsupported type 'class java.lang.Object' for parameter 'unsupported' "
          + "declared in 'org.kiji.scoring.params.TestUnsupportedParam$TestObject.unsupported'.",
          upte.getMessage());
    }
  }
}
