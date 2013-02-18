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

package org.kiji.mapreduce.gather;

import java.io.IOException;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.KijiContext;

/**
 * Context for gatherers. GathererContexts emit key/value pairs.
 *
 * @param <K> Type of the keys to emit.
 * @param <V> Type of the values to emit.
 */
@ApiAudience.Public
@Inheritance.Sealed
public interface GathererContext<K, V> extends KijiContext {
  /**
   * Emits a key/value pair.
   *
   * @param key Emit this key.
   * @param value Emit this value.
   * @throws IOException on I/O error.
   */
  void write(K key, V value) throws IOException;
}
