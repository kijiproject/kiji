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
 * Parses a command-line argument for the native float type.
 */
public class PrimitiveFloatParser extends NonNullableSimpleValueParser<Float> {
  /** {@inheritDoc} */
  @Override
  public Class<? extends Float> getParsedClass() {
    return float.class;
  }

  /** {@inheritDoc} */
  @Override
  public Float parseNonNull(FlagSpec flag, String string) {
    try {
      return Float.parseFloat(string);
    } catch (NumberFormatException nfe) {
      throw new IllegalFlagValueException(flag, string);
    }
  }
}