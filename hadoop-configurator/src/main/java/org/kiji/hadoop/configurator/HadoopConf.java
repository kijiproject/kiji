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

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Retention;

/**
 * The annotation used on instance (member) variables to tell the HadoopConfigurator
 * system that they should be populated with values from the {@link
 * org.apache.hadoop.conf.Configuration}.
 *
 * @see HadoopConfigurator
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface HadoopConf {
  /**
   * The key used in the {@link org.apache.hadoop.conf.Configuration} for this variable.
   *
   * <p>This field is required.</p>
   */
  String key();

  /**
   * Documents the usage of this configuration variable.
   */
  String usage() default "";

  /**
   * The default value for the configuration variable.
   */
  String defaultValue() default "";
}
