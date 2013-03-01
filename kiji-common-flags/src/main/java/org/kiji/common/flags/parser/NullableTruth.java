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

/** Enumeration of nullable truth values. */
public enum NullableTruth {
  TRUE, YES,
  FALSE, NO,
  NULL, NONE;

  /**
   * Parses a string into a Boolean or null.
   *
   * @param string String to parse as a nullable boolean truth value (case insensitive).
   * @return either null or a Boolean value.
   * @throws IllegalArgumentException if the string does not represent null or a truth value.
   */
  public static Boolean parse(String string) {
    final NullableTruth truth = valueOf(string.toUpperCase(Locale.ROOT));
    switch (truth) {
    case TRUE:
    case YES:
      return true;
    case FALSE:
    case NO:
      return false;
    case NULL:
    case NONE:
      return null;
    default:
      throw new RuntimeException("Unexpected truth enum value: " + truth);
    }
  }
}