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
 * Base interface of a parser for param values.
 *
 * All param value parsers must implement this interface.
 *
 * @param <T> Type of the parsed value.
 */
public interface ValueParser<T> {

  /** @return the class of the value being parsed. */
  Class<? extends T> getParsedClass();

  /** @return whether this parser also parses subclasses */
  boolean parsesSubclasses();

  /**
   * Parses all the values associated to a context param.
   *
   * @param param Specification of the param being parsed.
   *     Includes the Param annotation and the field details.
   * @param value The argument associated to this param.
   * @return the parsed value.
   */
  T parse(ParamSpec param, String value);
}
