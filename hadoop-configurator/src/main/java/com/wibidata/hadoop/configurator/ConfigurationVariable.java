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

  public String getKey() {
    // TODO: What happens if the annotation doesn't specify a key?
    return mAnnotation.key();
  }

  public void setValue(Object instance, Configuration conf) throws IllegalAccessException {
    if (null == conf.get(getKey())) {
      // Nothing set in the configuration, so the field will be left alone.
      return;
    }

    if (!mField.isAccessible()) {
      mField.setAccessible(true);
    }

    if (boolean.class == mField.getType()) {
      mField.setBoolean(instance, conf.getBoolean(getKey(), false));
    } else if (float.class == mField.getType()) {
      mField.setFloat(instance, conf.getFloat(getKey(), 0.0f));
    } else if (int.class == mField.getType()) {
      mField.setInt(instance, conf.getInt(getKey(), 0));
    } else if (String.class == mField.getType()) {
      mField.set(instance, conf.get(getKey(), null));
    }
    // TODO long, short, etc.
  }
}
