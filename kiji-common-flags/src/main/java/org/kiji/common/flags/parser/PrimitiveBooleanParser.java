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

import java.util.Locale;

import org.kiji.common.flags.FlagSpec;
import org.kiji.common.flags.IllegalFlagValueException;

/**
 * Parser for command-line flag parameters of native type boolean.
 *
 * Notes:
 *   <li> flag evaluates to true if and only if the string value is "true" or "yes"
 *        (case insensitive match).
 *   <li> flag evaluates to false if and only if the string value is "false" or "no"
 *        (case insensitive match).
 *   <li> <code>"--flag"</code> is evaluated as <code>"--flag=true"</code>
 *   <li> <code>"--flag="</code> is evaluated as <code>"--flag=true"</code>
 *   <li> Other inputs are rejected.
 */
public class PrimitiveBooleanParser extends SimpleValueParser<Boolean> {
  private static enum Truth {
    TRUE, YES,
    FALSE, NO,
  }

  /** {@inheritDoc} */
  @Override
  public Class<? extends Boolean> getParsedClass() {
    return boolean.class;
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
      final Truth truth = Truth.valueOf(string.toUpperCase(Locale.ROOT));
      switch (truth) {
      case TRUE:
      case YES:
        return true;
      case FALSE:
      case NO:
        return false;
      default:
        throw new RuntimeException("Unexpected truth enum value: " + truth);
      }
    } catch (IllegalArgumentException iae) {
      throw new IllegalFlagValueException(flag, string);
    }
  }
}