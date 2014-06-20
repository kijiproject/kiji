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
 * Exception thrown when the type of a param is not supported.
 *
 * This happens when annotating a field with an <pre>@Param</pre> while there is no declared
 * parser for this field's type.
 */
public class UnsupportedParamTypeException extends RuntimeException {
  /** Generated serial version ID. */
  private static final long serialVersionUID = -1500132821312834934L;

  /**
   * Creates a new <code>UnsupportedParamTypeException</code> instance.
   *
   * @param spec Param descriptor to create an "unsupported" exception for.
   */
  public UnsupportedParamTypeException(ParamSpec spec) {
    super(String.format("Unsupported type '%s' for parameter '%s' declared in '%s'.",
        spec.getTypeName(), spec.getName(), spec.toString()));
  }
}
