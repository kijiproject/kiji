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

import java.util.List;

/**
 * Base interface of a parser for flag values.
 *
 * All flag value parsers must implement this interface.
 *
 * @param <T> Type of the parsed value.
 */
public interface ValueParser<T> {

  /** @return the class of the value being parsed. */
  Class<? extends T> getParsedClass();

  /** @return whether this parser also parses subclasses */
  boolean parsesSubclasses();

  /**
   * Parses all the values associated to a command-line flag.
   *
   * @param flag Specification of the flag being parsed.
   *     Includes the Flag annotation and the field details.
   * @param values The many command-line arguments associated to this flag, in order.
   *     The list contains at least one element, and more if the flag has been repeated.
   *     It is up to the parser to decide how to handle multiple values (reject, override,
   *     aggregate, etc).
   * @return the parsed value.
   */
  T parse(FlagSpec flag, List<String> values);
}