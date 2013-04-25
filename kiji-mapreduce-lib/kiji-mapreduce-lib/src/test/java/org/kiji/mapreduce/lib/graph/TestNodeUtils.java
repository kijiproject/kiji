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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;

public class TestNodeUtils {
  public static Node getNode() {
    return new NodeBuilder()
        .setLabel("foo")
        .setWeight(1.0)
        .addEdge(new EdgeBuilder()
            .setLabel("bar")
            .setWeight(2.0)
            .setTarget(new NodeBuilder()
                .setLabel("baz")
                .setWeight(3.0)
                .build())
            .build())
        .build();
  }

  @Test
  public void testCopy() {
    Node node2 = NodeUtils.copy(getNode());
    assertTrue(getNode() != node2);
    assertEquals(0, getNode().compareTo(node2));

    // Modify and edge and make sure they are no longer equal
    // to verify they are not pointing to the same list of edges.
    node2.getEdges().get(0).setLabel("car");
    assertFalse(node2.getEdges().get(0).getLabel()
        .equals((getNode().getEdges().get(0).getLabel())));
  }

  @Test
  public void testMergeNodes() {
    Node node2 = NodeUtils.copy(getNode());

    List<Node> nodes = new ArrayList<Node>(2);
    nodes.add(getNode());
    nodes.add(node2);

    List<Node> merged = NodeUtils.mergeNodes(nodes);
    assertEquals(1, merged.size());

    Node expected = new Node();
    expected.setLabel("foo");
    expected.setWeight(2.0);
    expected.setEdges(new ArrayList<Edge>());
    expected.getEdges().add(new Edge());
    expected.getEdges().get(0).setLabel("bar");
    expected.getEdges().get(0).setWeight(4.0);
    expected.getEdges().get(0).setTarget(new Node());
    expected.getEdges().get(0).getTarget().setLabel("baz");
    expected.getEdges().get(0).getTarget().setWeight(6.0);
    assertEquals(0, expected.compareTo(merged.get(0)));
  }

  @Test
  public void testIndexNodesByLabel() {
    Node node2 = NodeUtils.copy(getNode());

    List<Node> nodes = new ArrayList<Node>(2);
    nodes.add(getNode());
    nodes.add(node2);
    Map<CharSequence, List<Node>> index = NodeUtils.indexNodesByLabel(nodes);
    assertEquals(1, index.size());
    assertEquals(2, index.get("foo").size());
    assertEquals(getNode(), index.get("foo").get(0));
    assertEquals(node2, index.get("foo").get(1));
  }
}
