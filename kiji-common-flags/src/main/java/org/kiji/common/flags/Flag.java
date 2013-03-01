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

package org.kiji.common.flags;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates fields that should be assigned to when a command-line is parsed.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Flag {
  /** The name of the flag. */
  String name() default "";

  /** The human-readable usage string. */
  String usage() default "";

  /** Whether the flag should be displayed when {@code --help} is provided. */
  boolean hidden() default false;

  /** An environment variable to use as the default value. */
  String envVar() default "";

  /** Explicit flag parser to use. */
  Class<? extends ValueParser> parser() default ValueParser.class;

  /** Whether the flag is required or optional. */
  boolean required() default false;
}
