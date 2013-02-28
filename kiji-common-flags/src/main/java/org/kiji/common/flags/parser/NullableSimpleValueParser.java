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

import org.kiji.common.flags.FlagSpec;

/**
 * Parser for simple flags that evaluates <code>"--flag"</code> as <code>null</code>.
 */
public abstract class NullableSimpleValueParser<T> extends SimpleValueParser<T> {

  /** {@inheritDoc} */
  @Override
  public final T parse(FlagSpec flag, String string) {
    if (string == null) {
      // Handle the case "--flag":
      // TODO: Should we allow "--flag=", "--flag=None" or "--flag=null"?
      return null;
    }
    return parseNonNull(flag, string);
  }

  /**
   * Parses a non-null value from a string command-line flag.
   *
   * @param flag Specification of the flag being parsed.
   *     Includes the Flag annotation and the field details.
   * @param string The string from the command-line to be parsed. Never null.
   * @return the parsed value.
   */
  public abstract T parseNonNull(FlagSpec flag, String string);
}
