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

/**
 * Thrown when the value assigned on the command line cannot be coerced into the Java
 * field because it is an incompatible type.
 */
public class IllegalFlagValueException extends IllegalArgumentException {
  private static final long serialVersionUID = -6219698605704501226L;

  /**
   * Creates a new <code>IllegalFlagValueException</code> instance.
   *
   * @param message Human-readable message detailing the invalid flag.
   */
  public IllegalFlagValueException(String message) {
    super(message);
  }

  /**
   * Creates an <code>IllegalFlagValueException</code> instance.
   *
   * @param spec Specification of the flag being parsed.
   * @param argument Command-line argument being parsed.
   */
  public IllegalFlagValueException(FlagSpec spec, String argument) {
    super(formatError(spec, argument));
  }

  private static String formatError(FlagSpec spec, String argument) {
    if (argument == null) {
      return String.format(
          "Invalid command-line argument '--%s' for flag type %s: "
          + "null is not a valid value for %s flag declared in '%s'.",
          spec.getName(), spec.getTypeName(), spec.getTypeName(), spec);
    } else {
      return String.format(
          "Invalid command-line argument '--%s=%s' for flag type %s: "
          + "'%s' is not a valid value for %s flag declared in '%s'.",
          spec.getName(), argument, spec.getTypeName(), argument, spec.getTypeName(), spec);
    }
  }
}
