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
 * Parser for context param parameters of native type boolean.
 *
 * Notes:
 *   <li> param evaluates to true if and only if the string value is "true" or "yes"
 *        (case insensitive match).
 *   <li> param evaluates to false if and only if the string value is "false" or "no"
 *        (case insensitive match).
 *   <li> Other inputs are rejected.
 */
public class PrimitiveBooleanParser extends SimpleValueParser<Boolean> {
  /** {@inheritDoc} */
  @Override
  public Class<? extends Boolean> getParsedClass() {
    return boolean.class;
  }

  /** {@inheritDoc} */
  @Override
  public Boolean parse(ParamSpec param, String string) {
    if (string == null) {
      // Handle the case: "param" without an equal sign:
      return true;
    }
    if (string.isEmpty()) {
      return true;
    }

    try {
      return Truth.parse(string);
    } catch (IllegalArgumentException iae) {
      throw new IllegalParamValueException(param, string);
    }
  }
}
