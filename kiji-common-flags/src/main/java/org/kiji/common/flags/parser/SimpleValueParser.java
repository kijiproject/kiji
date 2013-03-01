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

package org.kiji.common.flags.parser;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import org.kiji.common.flags.FlagSpec;
import org.kiji.common.flags.IllegalFlagOverrideException;
import org.kiji.common.flags.ValueParser;

/**
 * Base class for parsers of simple, single values (no collections).
 *
 * The default behavior allows such flags to appear multiple times, overriding previous instances.
 * By setting the system property -Dorg.kiji.common.flags.allow.flag.override=true, such flags
 * cannot be overridden anymore (ie. cannot be specified more than once).
 *
 * @param <T> Type of the parsed value.
 */
public abstract class SimpleValueParser<T> implements ValueParser<T> {
  /**
   * Name of the system property that controls whether we allow flags to
   * be overridden when repeated.
   */
  public static final String ALLOW_FLAG_OVERRIDE_PROPERTY =
      "org.kiji.common.flags.allow.flag.override";

  /** {@inheritDoc} */
  @Override
  public boolean parsesSubclasses() {
    return false;
  }

  /** @return whether flag overrides are allowed. */
  private static boolean allowFlagOverride() {
    try {
      return Truth.parse(System.getProperty(ALLOW_FLAG_OVERRIDE_PROPERTY, "true"));
    } catch (IllegalArgumentException iea) {
      throw new IllegalArgumentException(String.format("Invalid value '%s' for property '%s'.",
          System.getProperty(ALLOW_FLAG_OVERRIDE_PROPERTY),
          ALLOW_FLAG_OVERRIDE_PROPERTY));
    }
  }

  /** {@inheritDoc} */
  @Override
  public T parse(FlagSpec flag, List<String> values) {
    Preconditions.checkArgument(!values.isEmpty());

    if (!allowFlagOverride() && (values.size() > 1)) {
      throw new IllegalFlagOverrideException(String.format(
          "Flag '%s' specified multiple times with values: %s",
          flag.getName(), Joiner.on(",").join(values)));
    }

    // Parse all values even if we only use the last flag to override previous instances:
    T parsed = null;
    for (String value : values) {
      parsed = parse(flag, value);
    }
    return parsed;
  }

  /**
   * Parses a value from a string command-line flag.
   *
   * @param flag Specification of the flag being parsed.
   *     Includes the Flag annotation and the field details.
   * @param value The command-line argument associated to this flag.
   *     May be null, empty or non empty.
   * @return the parsed value.
   */
  public abstract T parse(FlagSpec flag, String value);
}
