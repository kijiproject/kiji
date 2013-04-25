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

package org.kiji.mapreduce.lib.reduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;

import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.avro.UnwrappedAvroValueIterable;

/**
 * Base class for reducers that expect the input value to be an Avro
 * Node message.  Subclasses of NodeReducer should be paired with
 * mappers that output (K, Node) pairs and write Nodes to Avro
 * container files.
 *
 * @param <K> The type of the MapReduce input key.
 */
public abstract class NodeReducer<K> extends AvroReducer<K, AvroValue<Node>, Node> {
  @Override
  protected void reduce(K key, Iterable<AvroValue<Node>> values, Context context)
      throws IOException, InterruptedException {
    reduceNodes(key, new UnwrappedAvroValueIterable<Node>(values), context);
  }

  /**
   * An alternative reduce method for subclasses for direct access to
   * iterate over the input values as Nodes instead of having to
   * unwrap the AvroValue container objects.
   *
   * @param key The reduce input key.
   * @param values The reduce input values as Node avro messages.
   * @param context The reduce context.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  protected abstract void reduceNodes(K key, Iterable<Node> values, Context context)
      throws IOException, InterruptedException;

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    return Node.SCHEMA$;
  }
}
