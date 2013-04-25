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

package org.kiji.mapreduce.lib.gather;

import java.io.IOException;


import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;

import org.kiji.mapreduce.avro.AvroValueWriter;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;

import org.kiji.mapreduce.lib.avro.Node;

/**
 * Base class for gatherers that output <key, Node> pairs.
 *
 * @param <K> The type of the MapReduce output key.
 */
public abstract class NodeGatherer<K> extends KijiGatherer<K, AvroValue<Node>>
    implements AvroValueWriter {
  /** A reusable output avro value wrapper for a node. */
  private AvroValue<Node> mNode;

  /** {@inheritDoc} */
  @Override
  public void setup(GathererContext<K, AvroValue<Node>> context) throws IOException {
    super.setup(context);
    mNode = new AvroValue<Node>(null);
  }

  /**
   * Writes a &lt;key, Node&gt; pair.
   *
   * @param key A key for the output pair.
   * @param node A node value to write.
   * @param context The mapper context.
   * @throws IOException If there is an error.
   */
  public void write(K key, Node node, GathererContext<K, AvroValue<Node>> context)
      throws IOException {
    mNode.datum(node);
    context.write(key, mNode);
  }

  @Override
  public Class<?> getOutputValueClass() {
    return AvroValue.class;
  }

  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    return Node.SCHEMA$;
  }
}
