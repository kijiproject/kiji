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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * The entry point for the HadoopConfigurator system.
 *
 * <p>Clients should call {@link
 * HadoopConfigurator#configure(org.apache.hadoop.conf.Configurable)} within their {@link
 * org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)}
 * method to populate all of their member variables with values read from the {@link
 * org.apache.hadoop.conf.Configuration}.</p>
 *
 * <p>For example:</p>
 *
 * <pre>
 * public class Foo extends Configured {
 *   {@literal @}HadoopConf(key="foo.value", usage="The foo value")
 *   private String fooValue = "defaultValue";
 *
 *   {@literal @}Override
 *   public void setConf(Configuration conf) {
 *     super.setConf(conf);
 *     HadoopConfigurator.configure(this);
 *     // Now fooValue has been populated with the value of conf.get("foo.value").
 *   }
 * }
 * </pre>
 */
public final class HadoopConfigurator {
  /** Disable the constructor for this utility class. */
  private HadoopConfigurator() {}

  /**
   * Populates the instance variables of a {@link org.apache.hadoop.conf.Configurable}
   * instance with values from its {@link org.apache.hadoop.conf.Configuration}.
   *
   * @param instance The instance to configure.
   * @param includeParentClasses Whether to include declared variables in super classes.
   * @throws HadoopConfigurationException If there is an error with the declaration or
   *     assigning the field.
   */
  public static void configure(Configurable instance, boolean includeParentClasses) {
    final Configuration conf = instance.getConf();
    if (null == conf) {
      // No configuration to read from.
      return;
    }

    final List<ConfigurationVariable> variables =
        extractDeclaredVariables(instance.getClass(), includeParentClasses);
    for (ConfigurationVariable variable : variables) {
      try {
        variable.setValue(instance, conf);
      } catch (IllegalAccessException e) {
        throw new HadoopConfigurationException(e);
      }
    }

    final List<ConfigurationMethod> methods =
        extractDeclaredMethods(instance.getClass(), includeParentClasses);
    for (ConfigurationMethod method : methods) {
      try {
        method.call(instance, conf);
      } catch (IllegalAccessException e) {
        throw new HadoopConfigurationException(e);
      }
    }
  }

  /**
   * Populates the instance variables of a {@link org.apache.hadoop.conf.Configurable}
   * instance with values from its {@link org.apache.hadoop.conf.Configuration}.
   *
   * <p>This includes annotations declared in parent classes.</p>
   *
   * @param instance The instance to configure.
   * @throws HadoopConfigurationException If there is an error with the declaration or
   *     assigning the field.
   */
  public static void configure(Configurable instance) {
    configure(instance, true);
  }

  /**
   * Extracts the declared Hadoop configuration variables (fields with
   * {@literal @}HadoopConf annotations) from a class.
   *
   * @param clazz The class to extract conf variable declarations from.
   * @param includeParentClasses If true, also includes declared variables in the super classes.
   * @return The list of configuration variables declared.
   */
  public static List<ConfigurationVariable> extractDeclaredVariables(
      Class<?> clazz, boolean includeParentClasses) {
    final List<ConfigurationVariable> variables = new ArrayList<ConfigurationVariable>();
    do {
      for (Field field : clazz.getDeclaredFields()) {
        final HadoopConf annotation = field.getAnnotation(HadoopConf.class);
        if (null != annotation) {
          variables.add(new ConfigurationVariable(field, annotation));
        }
      }
      clazz = clazz.getSuperclass();
    } while (includeParentClasses && null != clazz);
    return variables;
  }

  /**
   * Extracts the declared Hadoop configuration method (methods with
   * {@literal @}HadoopConf annotations) from a class.
   *
   * @param clazz The class to extract conf method declarations from.
   * @param includeParentClasses If true, also includes declared methods in the super classes.
   * @return The list of configuration methods declared.
   */
  public static List<ConfigurationMethod> extractDeclaredMethods(
      Class<?> clazz, boolean includeParentClasses) {
    final List<ConfigurationMethod> methods = new ArrayList<ConfigurationMethod>();
    do {
      for (Method method : clazz.getDeclaredMethods()) {
        final HadoopConf annotation = method.getAnnotation(HadoopConf.class);
        if (null != annotation) {
          methods.add(new ConfigurationMethod(method, annotation));
        }
      }
      clazz = clazz.getSuperclass();
    } while (includeParentClasses && null != clazz);
    return methods;
  }
}
