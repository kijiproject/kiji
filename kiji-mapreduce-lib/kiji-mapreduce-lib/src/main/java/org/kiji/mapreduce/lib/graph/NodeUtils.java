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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;

/**
 * Utility class for avro Node messages.
 */
public final class NodeUtils {
  /** A comparator for edges that orders by decreasing edge weight. */
  public static final EdgeWeightEdgeComparator EDGE_WEIGHT_EDGE_COMPARATOR
      = new EdgeWeightEdgeComparator();
  /** A comparator for edges that orders by lexicographically sorted target node labels. */
  public static final TargetLabelEdgeComparator TARGET_LABEL_EDGE_COMPARATOR
      = new TargetLabelEdgeComparator();

  /**
   * A comparator for edges that assumes numeric target node labels and sorts by
   * decreasing numeric value.
   */
  public static final TargetLabelNumericEdgeComparator TARGET_LABEL_NUMERIC_EDGE_COMPARATOR
      = new TargetLabelNumericEdgeComparator();

  /** No constructor since this a utility class. */
  private NodeUtils() {}

  /**
   * Performs a deep copy of a avro node message.  This does not reuse existing
   * decoders/encoders, so if you're going to do a lot of copying, use a NodeCopier.
   *
   * @param node The node to copy.
   * @return A copy of the node.
   */
  public static Node copy(Node node) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
      SpecificDatumWriter<Node> writer = new SpecificDatumWriter<Node>(Node.SCHEMA$);
      writer.write(node, encoder);

      SpecificDatumReader<Node> reader = new SpecificDatumReader<Node>(Node.SCHEMA$);
      Decoder decoder = DecoderFactory.get().binaryDecoder(
          new ByteArrayInputStream(out.toByteArray()), null);
      return reader.read(null, decoder);
    } catch (IOException e) {
      throw new RuntimeException("Unable to copy node.");
    }
  }

  /**
   * Merges the nodes with the same label together.  Merged nodes have their common edges
   * merged together and their weights summed together.  This is done recursively.
   *
   * @param nodes The list of nodes to merge.
   * @return A list of merged nodes.
   */
  public static List<Node> mergeNodes(List<Node> nodes) {
    return mergeNodes(nodes, new SumNodeMerger());
  }

  /**
   * Merges the nodes with the same label together.  Merged nodes have their common edges
   * merged together and their weights summed together.  This is done recursively.
   *
   * @param nodes The list of nodes to merge.
   * @param nodeMerger The nodeMerger to use.
   * @return A list of merged nodes.
   */
  public static List<Node> mergeNodes(List<Node> nodes, NodeMerger nodeMerger) {
    Map<CharSequence, List<Node>> nodesByLabel = indexNodesByLabel(nodes);
    List<Node> mergedNodes = new ArrayList<Node>();
    for (Map.Entry<CharSequence, List<Node>> entry : nodesByLabel.entrySet()) {
      List<Node> nodeGroup = entry.getValue();
      Node mergedNode = mergeGroupedNodes(nodeGroup, nodeMerger);
      mergedNodes.add(mergedNode);
    }
    return mergedNodes;
  }

  /**
   * Merges nodes together.  Merged nodes have their common edges
   * merged together and their weights summed together.  This is done recursively.
   *
   * @param nodes The list of nodes to merge.
   * @return A list of merged nodes.
   */
  public static Node mergeGroupedNodes(List<Node> nodes) {
    return mergeGroupedNodes(nodes, new SumNodeMerger());
  }

  /**
   * Merges the nodes with the same label together.  Merged nodes have their common edges
   * merged together and their weights summed together.  This is done recursively.
   *
   * @param nodes The list of nodes to merge.
   * @param nodeMerger The nodeMerger to use.
   * @return A list of merged nodes.
   */
  public static Node mergeGroupedNodes(List<Node> nodes, NodeMerger nodeMerger) {
    if (null == nodes || nodes.isEmpty()) {
      throw new IllegalArgumentException("Nodes should not be null and non-empty.");
    }
    Node mergedNode = nodeMerger.merge(nodes);
    return mergedNode;
  }

  /**
   * Creates a map from the Node's label to the Node.
   *
   * @param nodes The list of nodes to index.
   * @return A map from node labels to nodes with that label.
   */
  public static Map<CharSequence, List<Node>> indexNodesByLabel(List<Node> nodes) {
    Map<CharSequence, List<Node>> nodesByLabel
        = new HashMap<CharSequence, List<Node>>();
    for (Node node : nodes) {
      if (!nodesByLabel.containsKey(node.getLabel())) {
        nodesByLabel.put(node.getLabel(), new ArrayList<Node>());
      }
      nodesByLabel.get(node.getLabel()).add(node);
    }
    return nodesByLabel;
  }

  /**
   * Merges any edges with the same label.  The input edges are assumed to point to the
   * same target.
   *
   * @param edges The list of edges to merge.
   * @return The merged list of edges.
   */
  public static List<Edge> mergeEdges(List<Edge> edges) {
    return mergeEdges(edges, new SumNodeMerger());
  }

  /**
   * Merges any edges with the same label.  The input edges are assumed to point to the
   * same target.
   *
   * @param edges The list of edges to merge.
   * @param nodeMerger The nodeMerger to use for the target nodes.
   * @return The merged list of edges.
   */
  public static List<Edge> mergeEdges(List<Edge> edges, NodeMerger nodeMerger) {
    Map<CharSequence, List<Edge>> edgesByTarget = indexEdgesByTarget(edges);
    List<Edge> mergedEdges = new ArrayList<Edge>();
    for (List<Edge> edgeTargetGroup : edgesByTarget.values()) {
      Map<CharSequence, List<Edge>> edgesByLabel = indexEdgesByLabel(edgeTargetGroup);
      for (List<Edge> edgeLabelGroup : edgesByLabel.values()) {
        Edge mergedEdge = new Edge();
        List<Node> nodes = new ArrayList<Node>();
        for (Edge edge : edgeLabelGroup) {
          mergedEdge.setLabel(edge.getLabel());
          mergedEdge.setWeight(mergedEdge.getWeight() + edge.getWeight());
          nodes.add(edge.getTarget());
        }
        List<Node> mergedTarget = mergeNodes(nodes, nodeMerger);
        assert 1 == mergedTarget.size();
        mergedEdge.setTarget(mergedTarget.get(0));
        mergedEdges.add(mergedEdge);
      }
    }
    Collections.sort(mergedEdges, EDGE_WEIGHT_EDGE_COMPARATOR);
    return mergedEdges;
  }

  /**
   * Creates a map from Edge labels to the Edges.
   *
   * @param edges A list of edges to index.
   * @return A map from edge labels to the edges with that label.
   */
  public static Map<CharSequence, List<Edge>> indexEdgesByLabel(List<Edge> edges) {
    Map<CharSequence, List<Edge>> edgesByLabel
        = new HashMap<CharSequence, List<Edge>>();
    for (Edge edge : edges) {
      CharSequence edgeLabel = null == edge.getLabel() ? null : edge.getLabel();
      if (!edgesByLabel.containsKey(edgeLabel)) {
        edgesByLabel.put(edgeLabel, new ArrayList<Edge>());
      }
      edgesByLabel.get(edgeLabel).add(edge);
    }
    return edgesByLabel;
  }

  /**
   * Creates a map from target labels to the Edges.
   *
   * @param edges A list of edges to index.
   * @return A map from target node labels to the edges with those target nodes.
   */
  public static Map<CharSequence, List<Edge>> indexEdgesByTarget(List<Edge> edges) {
    Map<CharSequence, List<Edge>> edgesByTarget
        = new HashMap<CharSequence, List<Edge>>();
    for (Edge edge : edges) {
      if (!edgesByTarget.containsKey(edge.getTarget().getLabel())) {
        edgesByTarget.put(edge.getTarget().getLabel(), new ArrayList<Edge>());
      }
      edgesByTarget.get(edge.getTarget().getLabel()).add(edge);
    }
    return edgesByTarget;
  }

  /**
   * Recursively sorts edges using the given comparator.
   *
   * @param node A node to sort edges in.
   * @param comparator A comparator to use for sorting the edges.
   */
  public static void sortEdges(Node node, Comparator<Edge> comparator) {
    if (null == node.getEdges()) {
      return;
    }
    try {
      Collections.sort(node.getEdges(), comparator);
    } catch (UnsupportedOperationException e) {
      // This is a write-only list, so we need to copy it.
      node.setEdges(new ArrayList<Edge>(node.getEdges()));
      Collections.sort(node.getEdges(), comparator);
    }
    for (Edge edge : node.getEdges()) {
      sortEdges(edge.getTarget(), comparator);
    }
  }
}
