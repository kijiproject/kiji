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
import org.kiji.common.flags.IllegalFlagValueException;

/**
 * Parser for command-line flag parameters of type Boolean (object).
 *
 * Notes:
 *   <li> flag evaluates to true if and only if the string value is "true" or "yes"
 *        (case insensitive match).
 *   <li> flag evaluates to false if and only if the string value is "false" or "no"
 *        (case insensitive match).
 *   <li> flag evaluates to null if and only if the string value is "null" or "none"
 *        (case insensitive match).
 *   <li> <code>"--flag"</code> and <code>"--flag="</code> are evaluated as
 *        <code>"--flag=true"</code>
 *   <li> Other inputs are rejected.
 */
public class BooleanParser extends SimpleValueParser<Boolean> {
  /** {@inheritDoc} */
  @Override
  public Class<? extends Boolean> getParsedClass() {
    return Boolean.class;
  }

  /** {@inheritDoc} */
  @Override
  public Boolean parse(FlagSpec flag, String string) {
    if (string == null) {
      // Handle the case: "--flag" without an equal sign:
      return true;
    }
    if (string.isEmpty()) {
      return true;
    }

    try {
      return NullableTruth.parse(string);
    } catch (IllegalArgumentException iae) {
      throw new IllegalFlagValueException(flag, string);
    }
  }
}