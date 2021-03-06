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
package org.kiji.commons;

import java.io.Closeable;

/**
 * Used by instances of RefreshingReference to populate reference
 * values. It is expected that cached values are thread-safe, otherwise
 * any modifications made to the cached object during the refresh cycle
 * could introduce concurrency issues.
 *
 * @param <T> The type of the value that will be cached.
 */
public interface RefreshingLoader<T> extends Closeable {

  /**
   * Returns the initial value that will be cached.
   *
   * @return Returns the initial value to be stored in the cache
   */
  T initial();

  /**
   * Refreshes the value stored in the cache.
   *
   * @param previous The previous value stored in the cache.
   * @return The new value to be stored in the cache.
   */
  T refresh(T previous);

}
