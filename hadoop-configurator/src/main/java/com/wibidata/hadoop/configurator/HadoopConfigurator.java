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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * The entry point for the HadoopConfigurator system.
 *
 * <p>Clients should call {@link
 * HadoopConfigurator#configure(org.apache.hadoop.confConfigurable) within their {@link
 * org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)}
 * method to populate all of their member variables with values read from the {@link
 * org.apache.hadoop.conf.Configuration}.</p>
 *
 * <p>For example:</p>
 *
 * <pre>
 * public class Foo extends Configured {
 *   @HadoopConf(key="foo.value", usage="The foo value", defaultValue="")
 *   private String fooValue;
 *
 *   @Override
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
   * @throws HadoopConfigurationException If there is an error.
   */
  public static void configure(Configurable instance) throws HadoopConfigurationException {
    final Configuration conf = instance.getConf();
    if (null == conf) {
      // No configuration to read from.
      return;
    }

    final List<ConfigurationVariable> variables = extractDeclaredVariables(instance);
    for (ConfigurationVariable variable : variables) {
      try {
        variable.setValue(conf);
      } catch (IllegalAccessException e) {
        throw new HadoopConfigurationException(e);
      }
    }
  }

  /**
   * Extracts the declared Hadoop configuration variables (fields with {@literal
   * @}HadoopConf annotations) from an object instance.
   *
   * @param instance The object to extract conf variable declarations from.
   * @return The list of configuration variables declared in the instance or its superclasses.
   */
  private static List<ConfigurationVariable> extractDeclaredVariables(Object instance) {
    List<ConfigurationVariable> variables = new ArrayList<ConfigurationVariable>();
    Class<?> clazz = instance.getClass();
    do {
      for (Field field : clazz.getDeclaredFields()) {
        HadoopConf annotation = field.getAnnotation(HadoopConf.class);
        if (null != annotation) {
          variables.add(new ConfigurationVariable(field, annotation, instance));
        }
      }
    } while (null != (clazz = clazz.getSuperclass()));
    return variables;
  }
}
