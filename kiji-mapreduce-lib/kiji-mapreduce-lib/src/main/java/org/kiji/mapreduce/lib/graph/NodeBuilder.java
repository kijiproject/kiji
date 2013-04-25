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
import java.util.TreeMap;

import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;

/**
 * Configures and builds a Node.
 */
public class NodeBuilder {
  /** The default weight assigned to a new node. */
  public static final double DEFAULT_NODE_WEIGHT = 1.0;

  /** The wrapped node object that this builder affects. */
  private Node mNode;

  /**
   * Creates a new <code>NodeBuilder</code> instance wrapping an existing Node.
   *
   * @param node An existing node.
   */
  public NodeBuilder(Node node) {
    if (null == node) {
      throw new IllegalArgumentException("node is null");
    }
    mNode = node;
  }

  /**
   * Creates a new <code>NodeBuilder</code> instance using default field values.
   */
  public NodeBuilder() {
    mNode = new Node();
    mNode.setLabel((String) null);
    mNode.setWeight(DEFAULT_NODE_WEIGHT);
  }

  /**
   * Creates a new <code>NodeBuilder</code> instance.
   *
   * @param label A node label.
   */
  @Deprecated
  public NodeBuilder(String label) {
    this(label, DEFAULT_NODE_WEIGHT);
  }

  /**
   * Creates a new <code>NodeBuilder</code> instance.
   *
   * @param label A node label.
   * @param weight A node weight.
   */
  @Deprecated
  public NodeBuilder(String label, double weight) {
    mNode = new Node();
    mNode.setLabel(label);
    mNode.setWeight(weight);
  }

  /**
   * Set the label for the node.
   *
   * @param label A node label.
   * @return The <code>NodeBuilder</code> instance.
   */
  public NodeBuilder setLabel(String label) {
    mNode.setLabel(label);
    return this;
  }

  /**
   * Set the weight for the node.
   *
   * @param weight A node weight.
   * @return The <code>NodeBuilder</code> instance.
   */
  public NodeBuilder setWeight(double weight) {
    mNode.setWeight(weight);
    return this;
  }

  /**
   * Adds an outgoing edge to the node.
   *
   * @param edge An edge.
   * @return The <code>NodeBuilder</code> instance.
   */
  public NodeBuilder addEdge(Edge edge) {
    if (null == mNode.getEdges()) {
      mNode.setEdges(new ArrayList<Edge>());
    }
    mNode.getEdges().add(edge);
    return this;
  }

  /**
   * Adds an outgoing edge to the node.
   *
   * @return The <code>EdgeBuilder</code> for the outgoing edge added.
   */
  @Deprecated
  public EdgeBuilder addEdge() {
    return addEdge((String) null);
  }

  /**
   * Adds an outgoing edge to the node.
   *
   * @param label A label for the new outgoing edge.
   * @return The <code>EdgeBuilder</code> for the outgoing edge added.
   */
  @Deprecated
  public EdgeBuilder addEdge(String label) {
    return addEdge(label, EdgeBuilder.DEFAULT_EDGE_WEIGHT);
  }

  /**
   * Adds an outgoing edge to the node.
   *
   * @param weight A weight for the new outgoing edge.
   * @return The <code>EdgeBuilder</code> for the outgoing edge added.
   */
  @Deprecated
  public EdgeBuilder addEdge(double weight) {
    return addEdge(null, weight);
  }

  /**
   * Adds an outgoing edge to the node.
   *
   * @param label A label for the new outgoing edge.
   * @param weight A weight for the new outgoing edge.
   * @return The <code>EdgeBuilder</code> for the outgoing edge added.
   */
  @Deprecated
  public EdgeBuilder addEdge(String label, double weight) {
    return addEdge(label, weight, null);
  }

  /**
   * Adds an outgoing edge to the node.
   *
   * @param weight A weight for the new outgoing edge.
   * @param target A target node for the new outgoing edge.
   * @return The <code>EdgeBuilder</code> for the outgoing edge added.
   */
  @Deprecated
  public EdgeBuilder addEdge(double weight, Node target) {
    return addEdge(null, weight, target);
  }

  /**
   * Adds an outgoing edge to the node.
   *
   * @param label A label for the new outgoing edge.
   * @param target A target node for the new outgoing edge.
   * @return The <code>EdgeBuilder</code> for the outgoing edge added.
   */
  @Deprecated
  public EdgeBuilder addEdge(String label, Node target) {
    return addEdge(label, EdgeBuilder.DEFAULT_EDGE_WEIGHT, target);
  }

  /**
   * Adds an outgoing edge to the node.
   *
   * @param label A label for the new outgoing edge.
   * @param weight A weight for the new outgoing edge.
   * @param target A target node for the new outgoing edge.
   * @return The <code>EdgeBuilder</code> for the outgoing edge added.
   */
  @Deprecated
  public EdgeBuilder addEdge(String label, double weight, Node target) {
    Edge edge = new Edge();
    addEdge(edge);
    return new EdgeBuilder(edge).setLabel(label).setWeight(weight).setTarget(target);
  }

  /**
   * Adds an annotation to the node.
   *
   * @param key The annotation key.
   * @param value The annotation value.
   * @return The <code>NodeBuilder</code> instance.
   */
  public NodeBuilder addAnnotation(String key, String value) {
    if (null == mNode.getAnnotations()) {
      mNode.setAnnotations(new TreeMap<String, String>());
    }
    mNode.getAnnotations().put(key, value);
    return this;
  }

  /**
   * Returns the built node.
   *
   * @return The built node.
   */
  public Node build() {
    return mNode;
  }
}
