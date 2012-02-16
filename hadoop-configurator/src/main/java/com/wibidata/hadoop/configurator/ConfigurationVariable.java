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

import java.lang.reflect.Field;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;

/**
 * This class encapsulates everything there is to know about a Hadoop
 * configuration variable declaration.
 */
public class ConfigurationVariable {
  private Field mField;
  private HadoopConf mAnnotation;

  /**
   * Constructs a ConfigurationVariable instance.
   *
   * @param field The variable that has the annotation on it.
   * @param annotation The annotation on the field.
   */
  public ConfigurationVariable(Field field, HadoopConf annotation) {
    mField = field;
    mAnnotation = annotation;
  }

  /**
   * Gets the configuration key that should be used to populate the field.
   *
   * @return The key that was specified in the HadoopConf annotation.
   */
  public String getKey() {
    return mAnnotation.key();
  }

  /**
   * Populates an object's field with the value read from a Configuration instance.
   *
   * @param instance The object to populate.
   * @param conf The configuration to read from.
   * @throws IllegalAccessException If the field cannot be set on the object.
   * @throws HadoopConfigurationException If there is a problem with the annotation definition.
   */
  public void setValue(Object instance, Configuration conf) throws IllegalAccessException {
    final String key = getKey();
    if (null == key) {
      throw new HadoopConfigurationException("Missing 'key' attribute of @HadoopConf on "
          + instance.getClass().getName() + "." + mField.getName());
    }
    if (null == conf.get(key)) {
      // Nothing set in the configuration, so the field will be left alone.
      return;
    }

    if (!mField.isAccessible()) {
      mField.setAccessible(true);
    }

    if (boolean.class == mField.getType()) {
      mField.setBoolean(instance, conf.getBoolean(key, false));
    } else if (float.class == mField.getType()) {
      mField.setFloat(instance, conf.getFloat(key, 0.0f));
    } else if (double.class == mField.getType()) {
      mField.setDouble(instance, conf.getFloat(key, 0.0f));
    } else if (int.class == mField.getType()) {
      mField.setInt(instance, conf.getInt(key, 0));
    } else if (long.class == mField.getType()) {
      mField.setLong(instance, conf.getLong(key, 0L));
    } else if (mField.getType().isAssignableFrom(String.class)) {
      mField.set(instance, conf.get(key, null));
    } else if (mField.getType().isAssignableFrom(Collection.class)) {
      mField.set(instance, conf.getStringCollection(key));
    } else if (String[].class == mField.getType()) {
      mField.set(instance, conf.getStrings(key));
    } else {
      throw new HadoopConfigurationException("Unsupported field type annotated by @HadoopConf: "
          + instance.getClass().getName() + "." + mField.getName());
    }
  }
}
