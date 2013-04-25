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

import java.util.ArrayList;
import java.util.List;

import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;

/**
 * Abstract parent for merging nodes together by aggregating node weights.
 *
 * <p>Implementing this class usually involves overriding three methods:
 * initialize, update, and close.</p>
 *
 * <p>Calling merge will call initialize once on a newly created, empty node;
 * update once for each node in the list of nodes to be merged; and close once.</p>
 *
 * <p>This class is not thread-safe.</p>
 */
public abstract class AggregateNodeMerger extends NodeMerger {

  /**
   * Supported aggregate types.
   */
  public static enum MergeNodeAggregateType {
    COUNT, SUM, MIN, MAX, MEAN
  }

  /**
   * Returns AggregateNodeMerger from aggregateType.
   *
   * @param aggregateType The aggregation method to use.
   * @return An AggregateNodeMerger.
   */
  public static AggregateNodeMerger getNodeMerger(MergeNodeAggregateType aggregateType) {
    switch (aggregateType) {
      case COUNT:
        return new CountNodeMerger();
      case SUM:
        return new SumNodeMerger();
      case MIN:
        return new MinNodeMerger();
      case MAX:
        return new MaxNodeMerger();
      case MEAN:
        return new MeanNodeMerger();
      default:
        throw new RuntimeException("Unsupported node aggregate.");
    }
  }

  /**
   * Initializes a merged node for aggregation.
   *
   * @param mergedNode The node to initialize.
   */
  protected void initialize(Node mergedNode) {
    mergedNode.setWeight(0.0);
  }

  /**
   * Updates the merged node considering data from the newNode.
   *
   * @param mergedNode The node to merge into.
   * @param newNode The node to be merged.
   */
  protected abstract void update(Node mergedNode, Node newNode);

  /**
   * Closes the merge, finishing any computations needed to compute the node's weight.
   *
   * @param mergedNode The node to close.
   */
  protected void close(Node mergedNode) {
  }

  /** {@inheritDoc} */
  @Override
  public final Node merge(List<Node> nodes) {
    Node mergedNode = new Node();
    initialize(mergedNode);
    List<Edge> edges = new ArrayList<Edge>();
    if (!nodes.isEmpty()) {
      mergedNode.setLabel(nodes.get(0).getLabel());
    }
    for (Node node : nodes) {
      update(mergedNode, node);
      if (null != node.getEdges()) {
        edges.addAll(node.getEdges());
      }
    }
    if (!edges.isEmpty()) {
      mergedNode.setEdges(NodeUtils.mergeEdges(edges, this));
    }
    close(mergedNode);
    return mergedNode;
  }
}
