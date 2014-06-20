/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.scoring.params;

/**
 * Thrown when the value assigned from a context cannot be coerced into the Java
 * field because it is an incompatible type.
 */
public class IllegalParamValueException extends IllegalArgumentException {
  private static final long serialVersionUID = -6219698605704501226L;

  /**
   * Creates a new <code>IllegalParamValueException</code> instance.
   *
   * @param message Human-readable message detailing the invalid Param.
   */
  public IllegalParamValueException(String message) {
    super(message);
  }

  /**
   * Creates an <code>IllegalParamValueException</code> instance.
   *
   * @param spec Specification of the param being parsed.
   * @param argument Context argument being parsed.
   */
  public IllegalParamValueException(ParamSpec spec, String argument) {
    super(formatError(spec, argument));
  }

  /**
   * Format the error string for the Param and argument.
   * @param spec The ParamSpec
   * @param argument The invalid argument
   * @return A string for the error message.
   */
  private static String formatError(ParamSpec spec, String argument) {
    if (argument == null) {
      return String.format(
          "Invalid parameter argument '%s' for param type %s: "
          + "null is not a valid value for %s param declared in '%s'.",
          spec.getName(), spec.getTypeName(), spec.getTypeName(), spec);
    } else {
      return String.format(
          "Invalid parameter argument '%s=%s' for param type %s: "
          + "'%s' is not a valid value for %s param declared in '%s'.",
          spec.getName(), argument, spec.getTypeName(), argument, spec.getTypeName(), spec);
    }
  }
}
