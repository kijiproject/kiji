/**
 * Licensed to WibiData, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
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

package com.wibidata.hadoop.configurator;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TestHadoopConfigurator {
  /**
   * A configured class to test.
   */
  public static class MyConfiguredClass extends Configured {
    /** The default value for the my.string.value configuration varitable. */
    private static final String DEFAULT_STRING_VALUE = "foo";

    @HadoopConf(key="my.string.value", usage="A string value.")
    private String mStringValue = DEFAULT_STRING_VALUE;

    @HadoopConf(key="my.int.value", usage="An integer value.")
    private int mIntValue = 42;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      HadoopConfigurator.configure(this);
    }

    /**
     * Gets the string value.
     *
     * @return The string value.
     */
    public String getStringValue() {
      return mStringValue;
    }

    /**
     * Gets the integer value.
     *
     * @return The integer value.
     */
    public int getIntValue() {
      return mIntValue;
    }
  }

  @Test
  public void testConfigureDefault() {
    Configuration conf = new Configuration();

    MyConfiguredClass instance = ReflectionUtils.newInstance(MyConfiguredClass.class, conf);
    assertEquals("foo", instance.getStringValue());
    assertEquals(42, instance.getIntValue());
  }

  @Test
  public void testConfigure() {
    Configuration conf = new Configuration();
    conf.set("my.string.value", "bar");
    conf.setInt("my.int.value", 12);

    MyConfiguredClass instance = ReflectionUtils.newInstance(MyConfiguredClass.class, conf);
    assertEquals("bar", instance.getStringValue());
    assertEquals(12, instance.getIntValue());
  }
}
