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
import java.util.ArrayList;
import java.util.List;

import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.graph.AggregateNodeMerger;
import org.kiji.mapreduce.lib.graph.AggregateNodeMerger.MergeNodeAggregateType;
import org.kiji.mapreduce.lib.graph.NodeCopier;
import org.kiji.mapreduce.lib.graph.NodeMerger;
import org.kiji.mapreduce.lib.graph.NodeUtils;

/**
 * Merges nodes with the same label together.
 *
 * <p>Each input collection of nodes with the same input key will be processed together.
 * Within the collection, each group of nodes with the same label will be merged into a
 * single node.</p>
 *
 * <p>When combined with a gatherer that produces (K, Node) pairs, this reducer is the
 * utility knife of aggregate analysis.  The gatherer's output records are grouped by key
 * by the MapReduce framework.  The <code>MergeNodeReducer</code>'s
 * <code>reduce()</code> method is called once per group.  The nodes in the group are
 * merged into a single node (see {@link com.wibidata.core.client.lib.graph.NodeUtils} for the
 * merging algorithm).  The aggregation method of the merged node is determined by the
 * <code>merge.node.aggregate.type</code> configuration variable.<p>
 *
 * <p>This allows you to compute aggregates "sliced" by the gatherer output key.  See the
 * Gather Data Model Example of the User Guide for an example.</p>
 *
 * @param <K> The type of the MapReduce key (the same type is used for input and output).
 */
public class MergeNodeReducer<K> extends NodeValueReducer<K> {
  /** MergeNodeAggregateType contains valid values for this configuration variable. */
  public static final String CONF_MERGE_NODE_AGGREGATE_TYPE = "merge.node.aggregate.type";

  /** Default aggregation type to use when merging nodes. */
  private static final String DEFAULT_MERGE_NODE_AGGREGATE_TYPE
      = MergeNodeAggregateType.SUM.name();

  /** A node copier. */
  private NodeCopier mNodeCopier;

  /** A node merger. */
  protected NodeMerger mNodeMerger;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mNodeCopier = new NodeCopier();
    String aggregateType = context.getConfiguration()
        .get(CONF_MERGE_NODE_AGGREGATE_TYPE, DEFAULT_MERGE_NODE_AGGREGATE_TYPE);
    mNodeMerger = AggregateNodeMerger.getNodeMerger(MergeNodeAggregateType.valueOf(aggregateType));
  }

  /** {@inheritDoc} */
  @Override
  protected void reduceAvro(K key, Iterable<Node> values, Context context)
      throws IOException, InterruptedException {
    // Get all of the nodes for this key into a list.
    List<Node> nodes = new ArrayList<Node>();
    for (Node value : values) {
      nodes.add(mNodeCopier.copy(value));
    }

    // Merge the nodes and write them out.
    for (Node merged : merge(nodes)) {
      if (finalize(merged)) {
        write(key, merged, context);
      }
    }
  }

  /**
   * Gets the correct NodeMerger to use for merging.
   *
   * @param nodes The list of nodes to be merged.
   * @return A NodeMerger.
   */
  protected NodeMerger getNodeMerger(List<Node> nodes) {
    return mNodeMerger;
  }

  /**
   * Merges redundant edges in a list of nodes.
   *
   * @param nodes The nodes to merge.
   * @return The merged list of nodes.
   */
  protected List<Node> merge(List<Node> nodes) {
    return NodeUtils.mergeNodes(nodes, getNodeMerger(nodes));
  }

  /**
   * Merges redundant edges in a list of nodes.
   *
   * @param nodes The nodes to merge.
   * @return The merged list of nodes.
   */
  protected Node mergeGroupedNodes(List<Node> nodes) {
    return NodeUtils.mergeGroupedNodes(nodes, getNodeMerger(nodes));
  }

  /**
   * Last change to modify a node before it is output.  It has already been merged.
   *
   * @param node the merged node.
   * @return true if node may be output, false if it should be filtered.
   */
  protected boolean finalize(Node node) {
    return true;
  }
}
