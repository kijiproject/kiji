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

package org.kiji.hadoop.configurator;

import static org.junit.Assert.*;

import java.util.Collection;

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

    /** The conf key for the boolean value. */
    private static final String CONF_BOOLEAN_KEY = "my.boolean.value";

    @HadoopConf(key=CONF_BOOLEAN_KEY)
    private boolean mBooleanValue;

    @HadoopConf(key="my.float.value")
    private float mFloatValue;

    @HadoopConf(key="my.double.value")
    private double mDoubleValue;

    @HadoopConf(key="my.int.value", usage="An integer value.")
    private int mIntValue = 42;

    @HadoopConf(key="my.long.value", defaultValue="456")
    private long mLongValue;

    @HadoopConf(key="my.string.value", usage="A string value.")
    private String mStringValue = DEFAULT_STRING_VALUE;

    @HadoopConf(key="my.string.collection")
    private Collection<String> mStringCollection;

    @HadoopConf(key="my.string.array")
    private String[] mStringArray;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      HadoopConfigurator.configure(this);
    }

    private boolean mYourBoolean;

    @HadoopConf(key="your.boolean.value", defaultValue="true")
    public void setYourBoolean(boolean value) {
      mYourBoolean = value;
    }

    public boolean getYourBoolean() {
      return mYourBoolean;
    }

    private float mYourFloat;

    @HadoopConf(key="your.float.value", defaultValue="1.2")
    public void setYourFloat(float value) {
      mYourFloat = value;
    }

    public float getYourFloat() {
      return mYourFloat;
    }

    private double mYourDouble;

    @HadoopConf(key="your.double.value", defaultValue="3.4")
    public void setYourDouble(double value) {
      mYourDouble = value;
    }

    public double getYourDouble() {
      return mYourDouble;
    }

    private int mYourInt;

    @HadoopConf(key="your.int.value", defaultValue="123")
    public void setYourInt(int value) {
      mYourInt = value;
    }

    public int getYourInt() {
      return mYourInt;
    }

    private long mYourLong;

    @HadoopConf(key="your.long.value", defaultValue="123")
    public void setYourLong(long value) {
      mYourLong = value;
    }

    public long getYourLong() {
      return mYourLong;
    }

    private String mYourString;

    @HadoopConf(key="your.string.value", defaultValue="asdf")
    public void setYourString(String value) {
      mYourString = value;
    }

    public String getYourString() {
      return mYourString;
    }

    /**
     * Gets the boolean value.
     *
     * @return The boolean value.
     */
    public boolean getBooleanValue() {
      return mBooleanValue;
    }

    /**
     * Gets the float value.
     *
     * @return The float value.
     */
    public float getFloatValue() {
      return mFloatValue;
    }

    /**
     * Gets the double value.
     *
     * @return The double value.
     */
    public double getDoubleValue() {
      return mDoubleValue;
    }

    /**
     * Gets the integer value.
     *
     * @return The integer value.
     */
    public int getIntValue() {
      return mIntValue;
    }

    /**
     * Gets the long value.
     *
     * @return The long value.
     */
    public long getLongValue() {
      return mLongValue;
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
     * Gets the string collection.
     *
     * @return The string collection.
     */
    public Collection<String> getStringCollection() {
      return mStringCollection;
    }

    /**
     * Gets the string array.
     *
     * @return The string array.
     */
    public String[] getStringArray() {
      return mStringArray;
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
    conf.setBoolean("my.boolean.value", true);
    conf.setFloat("my.float.value", 3.1f);
    conf.setFloat("my.double.value", 1.9f);
    conf.setInt("my.int.value", 12);
    conf.set("my.string.value", "bar");
    conf.setStrings("my.string.collection", "apple", "banana");
    conf.setStrings("my.string.array", "red", "green", "blue");
    conf.setBoolean("your.boolean.value", true);
    conf.setFloat("your.float.value", 1.0f);
    conf.setFloat("your.double.value", 2.0f);
    conf.setInt("your.int.value", 1);
    conf.setLong("your.long.value", 2L);
    conf.set("your.string.value", "asdf");

    MyConfiguredClass instance = ReflectionUtils.newInstance(MyConfiguredClass.class, conf);
    assertEquals(true, instance.getBooleanValue());
    assertEquals(3.1f, instance.getFloatValue(), 1e-6f);
    assertEquals(1.9, instance.getDoubleValue(), 1e-6);
    assertEquals(12, instance.getIntValue());
    assertEquals(456L, instance.getLongValue());
    assertEquals("bar", instance.getStringValue());
    assertEquals(true, instance.getYourBoolean());
    assertEquals(1.0f, instance.getYourFloat(), 1e-6f);
    assertEquals(2.0, instance.getYourDouble(), 1e-6);
    assertEquals(1, instance.getYourInt());
    assertEquals(2L, instance.getYourLong());
  }

  /**
   * A class configured with a variable of incompatible type to be populated.
   */
  public static class IncompatibleType extends Configured {
    @HadoopConf(key="foo")
    private IncompatibleType mFoo;

    @Override
    public void setConf(Configuration conf) {
      super.setConf(conf);
      HadoopConfigurator.configure(this);
    }
  }

  @Test(expected=HadoopConfigurationException.class)
  public void testIncompatibleType() {
    Configuration conf = new Configuration();
    conf.set("foo", "bar");
    IncompatibleType instance = ReflectionUtils.newInstance(IncompatibleType.class, conf);
  }

  @Test
  public void testInvalidValue() {
    Configuration conf = new Configuration();
    conf.set("my.int.value", "this-is-not-a-number");
    MyConfiguredClass instance = ReflectionUtils.newInstance(MyConfiguredClass.class, conf);
    assertEquals("Should be the default value.", 42, instance.getIntValue());
  }
}
