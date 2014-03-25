/**
 * (c) Copyright 2014 WibiData, Inc.
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
package org.kiji.scoring.impl;

import java.util.concurrent.atomic.AtomicLong;

import org.kiji.annotations.ApiAudience;

/**
 * Generator for simple unique IDs.
 *
 * <p>
 *   This class is package private to be used by InternalFreshKijiTableReader. It should not be used
 *   elsewhere.
 * </p>
 */
@ApiAudience.Private
final class UniqueIdGenerator {
  private final AtomicLong mNext = new AtomicLong(0);

  /**
   * Get the next unique identifier.
   *
   * @return the next unique identifier.
   */
  public String getNextUniqueId() {
    return String.valueOf(mNext.getAndIncrement());
  }
}
