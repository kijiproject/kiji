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

package org.kiji.scoring.params.parser;

import org.kiji.scoring.params.IllegalParamValueException;
import org.kiji.scoring.params.ParamSpec;

/**
 * Parser for simple params rejecting empty specifications.
 *
 * @param <T> the type of the Parameter.
 */
public abstract class NonNullableSimpleValueParser<T> extends SimpleValueParser<T> {

  /** {@inheritDoc} */
  @Override
  public final T parse(ParamSpec param, String string) {
    if (string == null) {
      throw new IllegalParamValueException(String.format(
          "Invalid %s parameter argument '%s' for param '%s' declared in '%s'.",
          param.getTypeName(), param.getName(), param.getName(), param.toString()));
    }
    return parseNonNull(param, string);
  }

  /**
   * Parses a non-null value from a string context param.
   *
   * @param param Specification of the param being parsed.
   *     Includes the Param annotation and the field details.
   * @param string The string from the context to be parsed. Never null.
   * @return the parsed value.
   */
  public abstract T parseNonNull(ParamSpec param, String string);
}
