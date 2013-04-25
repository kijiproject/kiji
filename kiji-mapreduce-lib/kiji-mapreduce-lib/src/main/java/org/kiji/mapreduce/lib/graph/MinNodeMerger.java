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

package org.kiji.mapreduce.lib.graph;

import org.kiji.mapreduce.lib.avro.Node;

/**
 * Merge node weights using minimum weight.
 */
public class MinNodeMerger extends AggregateNodeMerger {
  /** {@inheritDoc} */
  @Override
  protected void initialize(Node mergedNode) {
    mergedNode.setWeight(Double.POSITIVE_INFINITY);
  }

  /** {@inheritDoc} */
  @Override
  protected void update(Node mergedNode, Node newNode) {
    if (mergedNode.getWeight() > newNode.getWeight()) {
      mergedNode.setWeight(newNode.getWeight());
    }
  }
}

