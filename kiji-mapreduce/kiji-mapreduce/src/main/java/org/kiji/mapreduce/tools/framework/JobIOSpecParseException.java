/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce.tools.framework;

import org.kiji.annotations.ApiAudience;

/** Thrown when a JobInputSpec or JobOutputSpec cannot be parsed. */
@ApiAudience.Framework
public final class JobIOSpecParseException extends RuntimeException {
  /**
   * @param message A message describing the problem with parsing.
   * @param spec The spec string that could not be parsed.
   */
  public JobIOSpecParseException(String message, String spec) {
    super(message + " [spec=" + spec + "]");
  }
}
