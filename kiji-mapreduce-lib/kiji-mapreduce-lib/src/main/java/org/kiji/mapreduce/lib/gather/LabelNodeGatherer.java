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

import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.lib.avro.Node;

/**
 * Base class for gatherers that output &lt;Node.label, Node&gt; pairs.
 */
public abstract class LabelNodeGatherer extends NodeGatherer<Text> {
  /** A reusable text object for output. */
  private Text mText;

  @Override
  public void setup(GathererContext<Text, AvroValue<Node>> context) throws IOException {
    super.setup(context);
    mText = new Text();
  }

  /**
   * Writes a node with the key as the label.
   *
   * @param node The node to write to the output.
   * @param context The mapper context.
   * @throws IOException If there is an error.
   */
  public void write(Node node, GathererContext<Text, AvroValue<Node>> context)
      throws IOException {
    mText.set(node.getLabel());
    write(mText, node, context);
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

}
