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

package org.kiji.mapreduce;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;

/** Thrown when a KijiProducer fails to produce output. */
@ApiAudience.Public
public final class KijiProducerOutputException extends IOException {
  /**
   * Creates a new <code>KijiProducerOutputException</code> instance.
   *
   * @param message An exception message.
   */
  public KijiProducerOutputException(String message) {
    super(message);
  }
}
