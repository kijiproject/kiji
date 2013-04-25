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

import java.util.TreeMap;

import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;

/**
 * Configures and builds an Edge.
 */
public class EdgeBuilder {
  public static final double DEFAULT_EDGE_WEIGHT = 1.0;

  private Edge mEdge;

  /**
   * Creates a new <code>EdgeBuilder</code> instance wrapping an existing Edge.
   *
   * @param edge An edge.
   */
  public EdgeBuilder(Edge edge) {
    if (null == edge) {
      throw new IllegalArgumentException("edge is null");
    }
    mEdge = edge;
  }

  /**
   * Build an edge from scratch with default values.
   */
  public EdgeBuilder() {
    mEdge = new Edge();
    mEdge.setLabel((String) null);
    mEdge.setWeight(DEFAULT_EDGE_WEIGHT);
    mEdge.setTarget(null);
  }

  /**
   * Creates a new <code>EdgeBuilder</code> instance.
   *
   * @param weight An edge weight.
   */
  @Deprecated
  public EdgeBuilder(double weight) {
    this(null, weight, null);
  }

  /**
   * Creates a new <code>EdgeBuilder</code> instance.
   *
   * @param label An edge label.
   */
  @Deprecated
  public EdgeBuilder(String label) {
    this(label, DEFAULT_EDGE_WEIGHT);
  }

  /**
   * Creates a new <code>EdgeBuilder</code> instance.
   *
   * @param label An edge label.
   * @param weight An edge weight.
   */
  @Deprecated
  public EdgeBuilder(String label, double weight) {
    this(label, weight, null);
  }

  /**
   * Creates a new <code>EdgeBuilder</code> instance.
   *
   * @param weight An edge weight.
   * @param target The target node of the edge.
   */
  @Deprecated
  public EdgeBuilder(double weight, Node target) {
    this(null, weight, target);
  }

  /**
   * Creates a new <code>EdgeBuilder</code> instance.
   *
   * @param label An edge label.
   * @param target The target node of the edge.
   */
  @Deprecated
  public EdgeBuilder(String label, Node target) {
    this(label, DEFAULT_EDGE_WEIGHT, target);
  }

  /**
   * Creates a new <code>EdgeBuilder</code> instance.
   *
   * @param label An edge label.
   * @param weight An edge weight.
   * @param target The target node of the edge.
   */
  @Deprecated
  public EdgeBuilder(String label, double weight, Node target) {
    mEdge = new Edge();
    mEdge.setLabel(label);
    mEdge.setWeight(weight);
    mEdge.setTarget(target);
  }

  /**
   * Sets the label of the edge.
   *
   * @param label An edge label.
   * @return The <code>EdgeBuilder</code> instance.
   */
  public EdgeBuilder setLabel(String label) {
    mEdge.setLabel(label);
    return this;
  }

  /**
   * Sets the weight of the edge.
   *
   * @param weight An edge weight.
   * @return The <code>EdgeBuilder</code> instance.
   */
  public EdgeBuilder setWeight(double weight) {
    mEdge.setWeight(weight);
    return this;
  }

  /**
   * Sets the target node of the edge.
   *
   * @param target The target node.
   * @return The <code>EdgeBuilder</code> instance.
   */
  public EdgeBuilder setTarget(Node target) {
    mEdge.setTarget(target);
    return this;
  }

  /**
   * Sets the target node of the edge.
   *
   * @param label The label for the target node.
   * @return The <code>EdgeBuilder</code> instance.
   */
  @Deprecated
  public EdgeBuilder setTarget(String label) {
    return setTarget(label, NodeBuilder.DEFAULT_NODE_WEIGHT);
  }

  /**
   * Sets the target node of the edge.
   *
   * @param weight The weight of the target node.
   * @return The <code>EdgeBuilder</code> instance.
   */
  @Deprecated
  public EdgeBuilder setTarget(double weight) {
    return setTarget(null, weight);
  }

  /**
   * Sets the target node of the edge.
   *
   * @param label The label of the target node.
   * @param weight The weight of the target node.
   * @return The <code>EdgeBuilder</code> instance.
   */
  @Deprecated
  public EdgeBuilder setTarget(String label, double weight) {
    target(label, weight);
    return this;
  }

  /**
   * Gets a node builder for the target node of the edge.
   *
   * @return The <code>NodeBuilder</code> instance for the target of the edge.
   */
  public NodeBuilder target() {
    return target(null);
  }

  /**
   * Sets the target node of the edge and returns a builder for it.
   *
   * @param label The label of the target node.
   * @return The <code>NodeBuilder</code> instance for the target of the edge.
   */
  public NodeBuilder target(String label) {
    return target(label, NodeBuilder.DEFAULT_NODE_WEIGHT);
  }

  /**
   * Sets the target node of the edge and returns a builder for it.
   *
   * @param weight The weight of the of the target node.
   * @return The <code>NodeBuilder</code> instance for the target of the edge.
   */
  public NodeBuilder target(double weight) {
    return target(null, weight);
  }

  /**
   * Sets the target node of the edge and returns a builder for it.
   *
   * @param label The label of the target node.
   * @param weight The weight of the target node.
   * @return The <code>NodeBuilder</code> instance for the target of the edge.
   */
  public NodeBuilder target(String label, double weight) {
    Node target = new Node();
    mEdge.setTarget(target);
    return new NodeBuilder(target).setLabel(label).setWeight(weight);
  }

  /**
   * Adds an annotation to the edge.
   *
   * @param key The annotation key.
   * @param value The annotation value.
   * @return The <code>EdgeBuilder</code> instance.
   */
  public EdgeBuilder addAnnotation(String key, String value) {
    if (null == mEdge.getAnnotations()) {
      mEdge.setAnnotations(new TreeMap<String, String>());
    }
    mEdge.getAnnotations().put(key, value);
    return this;
  }

  /**
   * Returns the edge.
   *
   * @return The built edge.
   */
  public Edge build() {
    return mEdge;
  }
}
