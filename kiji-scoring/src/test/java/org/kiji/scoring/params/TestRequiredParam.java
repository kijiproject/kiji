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

/** Tests the behavior of required flags. */
public class TestRequiredParam {
  private static final Logger LOG = LoggerFactory.getLogger(TestRequiredParam.class);

  private static final class TestObject extends Parameters {
    @Param(required = true) public String flagRequired = null;
  }

  @Test
  public void testValidRequired() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("flagRequired=value"));
  }

  @Test
  public void testRequiredValidEmpty() {
    TestObject testObj = new TestObject();
    testObj.parse(new TestContext("flagRequired="));
  }


  @Test
  public void testRequiredValidNull() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext("flagRequired"));
      fail();
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Parameter 'flagRequired' is required but has not been specified.",
          ifve.getMessage());
    }
  }

  @Test
  public void testRequiredMissing() {
    try {
      TestObject testObj = new TestObject();
      testObj.parse(new TestContext());
      fail();
    } catch (IllegalParamValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Parameter 'flagRequired' is required but has not been specified.",
          ifve.getMessage());
    }
  }
}
