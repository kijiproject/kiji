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

package org.kiji.mapreduce.context;

import java.io.IOException;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.GathererContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiRowData;

/**
 * Concrete context for gatherers that emit key/value pairs.
 *
 * @param <K> Type of the keys to emit.
 * @param <V> Type of the values to emit.
 */
@ApiAudience.Private
public final class InternalGathererContext<K, V>
    extends InternalKijiContext
    implements GathererContext<K, V> {

  /**
   * Constructs a new context for gatherers.
   *
   * @param context is the Hadoop {@link TaskInputOutputContext} that will back the new context.
   * @throws IOException on I/O error.
   */
  private InternalGathererContext(TaskInputOutputContext<EntityId, KijiRowData, K, V> context)
      throws IOException {
    super(context);
  }

  /**
   * Creates a new context for gatherers.
   *
   * @param context is the Hadoop {@link TaskInputOutputContext} that will back the new context
   *    for MapReduce jobs.
   * @param <K> is the type of key that can be written by the new context.
   * @param <V> is the type of value that can be written by the new context.
   * @return a new context for a gatherer.
   * @throws IOException if there is an I/O error.
   */
  public static <K, V> InternalGathererContext<K, V>
      create(TaskInputOutputContext<EntityId, KijiRowData, K, V> context) throws IOException {
    return new InternalGathererContext<K, V>(context);
  }

  /** {@inheritDoc} */
  @Override
  public void write(K key, V value) throws IOException {
    try {
      getMapReduceContext().write(key, value);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }
}
