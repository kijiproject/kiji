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

import org.kiji.mapreduce.MapReduceContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiRowData;

/**
 * Context for bare MapReduce tasks that emit key/value pairs.
 *
 * @param <K> Type of the keys to emit.
 * @param <V> Type of the values to emit.
 */
public class InternalMapReduceContext<K, V>
    extends InternalKijiContext
    implements MapReduceContext<K, V> {

  /**
   * Initializes a MapReduce context.
   *
   * @param context Underlying Hadoop context.
   * @throws IOException on I/O error.
   */
  public InternalMapReduceContext(TaskInputOutputContext<EntityId, KijiRowData, K, V> context)
      throws IOException {
    super(context);
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
