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


import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.scoring.Parameters;
import org.kiji.scoring.avro.ParameterDescription;
import org.kiji.scoring.avro.ParameterScope;

/** Tests the param parser for enumeration values. */
public class TestDescriptions {
  private static final Logger LOG = LoggerFactory.getLogger(TestEnumParam.class);

  public static enum Values {
    VALUE_ONE,
    VALUE_TWO,
    VALUE_THREE
  }

  private static final class TestObject extends Parameters {
    @Param public boolean paramNativeBoolean = false;
    @Param public short paramNativeShort = 0;
    @Param public int paramNativeInteger = 0;
    @Param public long paramNativeLong = 123L;
    @Param public float paramNativeFloat = 0.0f;
    @Param public double paramNativeDouble = 0.0;

    @Param public Boolean paramBoolean = null;
    @Param public Short paramShort= null;
    @Param public Integer paramInteger = null;
    @Param public Long paramLong = null;
    @Param public Float paramFloat = null;
    @Param public Double paramDouble = null;

    @Param public Values paramEnum = null;

    @Param public String paramString = null;

    @Param public Map<String, String> paramMap = null;
    @Param public Set<String> paramSet = null;
    @Param public List<String> paramList = null;
  }

  /**
   * This doesn't test anything specific at the moment, just that the description extraction works.
   * It's also useful for inspecting the description, e.g the simplified class names.
   */
  @Test
  public void testDescription() {
    TestObject testObj = new TestObject();
    Map<String, ParameterDescription> descriptions = testObj.getDescriptions(ParameterScope.SETUP);
    Assert.assertNotNull(descriptions);
  }
}
