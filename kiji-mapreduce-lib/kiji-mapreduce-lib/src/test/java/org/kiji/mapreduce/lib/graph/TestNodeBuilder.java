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

import org.junit.Test;

import org.kiji.mapreduce.lib.avro.Node;

public class TestNodeBuilder {
  @Test
  public void testBuildWithMethods() {
    Node node = new NodeBuilder()
        .setLabel("foo")
        .setWeight(2.0)
        .addEdge(new EdgeBuilder()
            .setLabel("bar")
            .setWeight(3.0)
            .setTarget(new NodeBuilder()
                .setLabel("baz")
                .addAnnotation("a", "car")
                .build())
            .build())
        .build();

    assertEquals("foo", node.getLabel().toString());
    assertEquals(2.0, node.getWeight(), 0.00001);
    assertEquals(1, node.getEdges().size());
    assertEquals("bar", node.getEdges().get(0).getLabel().toString());
    assertEquals(3.0, node.getEdges().get(0).getWeight(), 0.00001);
    assertEquals("baz", node.getEdges().get(0).getTarget().getLabel().toString());
    assertEquals(1, node.getEdges().get(0).getTarget().getAnnotations().size());
    assertEquals("car", node.getEdges().get(0).getTarget().getAnnotations().get("a").toString());
    assertEquals(1.0, node.getEdges().get(0).getTarget().getWeight(), 0.00001);
  }

  @Test
  @Deprecated
  public void testBuildWithConstructors() {
    Node node = new NodeBuilder("foo", 2.0)
        .addEdge(new EdgeBuilder("bar", 3.0,
                new NodeBuilder("baz").addAnnotation("a", "car").build()).build())
        .build();

    assertEquals("foo", node.getLabel().toString());
    assertEquals(2.0, node.getWeight(), 0.00001);
    assertEquals(1, node.getEdges().size());
    assertEquals("bar", node.getEdges().get(0).getLabel().toString());
    assertEquals(3.0, node.getEdges().get(0).getWeight(), 0.00001);
    assertEquals("baz", node.getEdges().get(0).getTarget().getLabel().toString());
    assertEquals(1, node.getEdges().get(0).getTarget().getAnnotations().size());
    assertEquals("car", node.getEdges().get(0).getTarget().getAnnotations().get("a").toString());
    assertEquals(1.0, node.getEdges().get(0).getTarget().getWeight(), 0.00001);
  }

  @Test
  @Deprecated
  public void testBuildGoogleProtocolBufferStyle() {
    NodeBuilder builder = new NodeBuilder("foo", 2.0);
    builder.addEdge("bar", 3.0).target().setLabel("baz").addAnnotation("a", "car");
    Node node = builder.build();

    assertEquals("foo", node.getLabel().toString());
    assertEquals(2.0, node.getWeight(), 0.00001);
    assertEquals(1, node.getEdges().size());
    assertEquals("bar", node.getEdges().get(0).getLabel().toString());
    assertEquals(3.0, node.getEdges().get(0).getWeight(), 0.00001);
    assertEquals("baz", node.getEdges().get(0).getTarget().getLabel().toString());
    assertEquals(1, node.getEdges().get(0).getTarget().getAnnotations().size());
    assertEquals("car", node.getEdges().get(0).getTarget().getAnnotations().get("a").toString());
    assertEquals(1.0, node.getEdges().get(0).getTarget().getWeight(), 0.00001);
  }

  @Test
  public void testSimpleBuildForCommonCase() {
    NodeBuilder builder = new NodeBuilder("foo");
    builder.addEdge(2.0).setTarget("bar");
    builder.addEdge(3.0).setTarget("car");
    Node node = builder.build();

    assertEquals("foo", node.getLabel().toString());
    assertEquals(1.0, node.getWeight(), 0.00001);
    assertEquals(2, node.getEdges().size());
    assertEquals(2.0, node.getEdges().get(0).getWeight(), 0.00001);
    assertEquals("bar", node.getEdges().get(0).getTarget().getLabel().toString());
    assertEquals(1.0, node.getEdges().get(0).getTarget().getWeight(), 0.00001);
    assertEquals(3.0, node.getEdges().get(1).getWeight(), 0.00001);
    assertEquals("car", node.getEdges().get(1).getTarget().getLabel().toString());
    assertEquals(1.0, node.getEdges().get(0).getTarget().getWeight(), 0.00001);
  }

  @Test
  @Deprecated
  public void testOneLiner() {
    Node node = new NodeBuilder("foo")
        .addEdge(new EdgeBuilder(2.0).setTarget("bar").build())
        .build();
    assertEquals("foo", node.getLabel().toString());
    assertEquals(1.0, node.getWeight(), 0.00001);
    assertEquals(1, node.getEdges().size());
    assertEquals(2.0, node.getEdges().get(0).getWeight(), 0.00001);
    assertEquals("bar", node.getEdges().get(0).getTarget().getLabel().toString());
    assertEquals(1.0, node.getEdges().get(0).getTarget().getWeight(), 0.00001);
  }
}
