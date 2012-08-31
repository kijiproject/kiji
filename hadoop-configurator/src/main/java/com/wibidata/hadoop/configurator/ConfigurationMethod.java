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

package com.wibidata.hadoop.configurator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;

/**
 * This class encapsulates everything there is to know about a Hadoop
 * configuration method declaration.
 */
public class ConfigurationMethod {
  private Method mMethod;
  private HadoopConf mAnnotation;

  /**
   * Constructs a ConfigurationMethod instance.
   *
   * @param method The method that has the annotation on it.
   * @param annotation The annotation on the method.
   */
  public ConfigurationMethod(Method method, HadoopConf annotation) {
    mMethod = method;
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
   * Calls an object's method with the value read from a Configuration instance.
   *
   * @param instance The object to populate.
   * @param conf The configuration to read from.
   * @throws IllegalAccessException If the method cannot be called on the object.
   * @throws HadoopConfigurationException If there is a problem with the annotation definition.
   */
  public void call(Object instance, Configuration conf) throws IllegalAccessException {
    final String key = getKey();
    if (null == key) {
      throw new HadoopConfigurationException("Missing 'key' attribute of @HadoopConf on "
          + instance.getClass().getName() + "." + mMethod.getName());
    }

    if (!mMethod.isAccessible()) {
      mMethod.setAccessible(true);
    }

    final Class<?>[] parameterTypes = mMethod.getParameterTypes();
    if (1 != parameterTypes.length) {
      throw new HadoopConfigurationException(
          "Methods annotated with @HadoopConf must have exactly one parameter: "
          + instance.getClass().getName() + "." + mMethod.getName());
    }

    final Class<?> parameterType = parameterTypes[0];

    try {
      try {
        if (boolean.class == parameterType) {
          mMethod.invoke(instance, conf.getBoolean(key, Boolean.parseBoolean(getDefault())));
        } else if (float.class == parameterType) {
          mMethod.invoke(instance, conf.getFloat(key, Float.parseFloat(getDefault())));
        } else if (double.class == parameterType) {
          mMethod.invoke(instance, conf.getFloat(key, Float.parseFloat(getDefault())));
        } else if (int.class == parameterType) {
          mMethod.invoke(instance, conf.getInt(key, Integer.parseInt(getDefault())));
        } else if (long.class == parameterType) {
          mMethod.invoke(instance, conf.getLong(key, Long.parseLong(getDefault())));
        } else if (parameterType.isAssignableFrom(String.class)) {
          mMethod.invoke(instance, conf.get(key, getDefault()));
        } else if (parameterType.isAssignableFrom(Collection.class)) {
          mMethod.invoke(instance, conf.getStringCollection(key));
        } else if (String[].class == parameterType) {
          mMethod.invoke(instance, new Object[] { conf.getStrings(key) });
        } else {
          throw new HadoopConfigurationException(
              "Unsupported method parameter type annotated by @HadoopConf: "
              + instance.getClass().getName() + "." + mMethod.getName());
        }
      } catch (NumberFormatException e) {
        mMethod.invoke(instance, getDefault());
      }
    } catch (InvocationTargetException e) {
      throw new HadoopConfigurationException(e);
    }
  }

  /**
   * Gets the default value specified by the annotation.
   *
   * @return The default value.
   */
  private String getDefault() {
    return mAnnotation.defaultValue();
  }
}
