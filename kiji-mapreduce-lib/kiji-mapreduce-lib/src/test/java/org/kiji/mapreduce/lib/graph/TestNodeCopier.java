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
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import org.kiji.mapreduce.lib.avro.Node;

public class TestNodeCopier {
  @Test
  public void testCopy() {
    NodeCopier copier = new NodeCopier();

    Node original = new NodeBuilder()
        .setLabel("foo")
        .setWeight(1.0)
        .addEdge(new EdgeBuilder()
            .setLabel("-")
            .setWeight(2.0)
            .setTarget(new NodeBuilder()
                .setLabel("bar")
                .setWeight(3.0)
                .build())
            .build())
        .build();

    Node copy = copier.copy(original);

    assertEquals("foo", copy.getLabel().toString());
    assertEquals(1.0, copy.getWeight(), 0.0001);
    assertNotNull(copy.getEdges());
    assertEquals(1, copy.getEdges().size());
    assertEquals(2.0, copy.getEdges().get(0).getWeight(), 0.0001);
    assertEquals("bar", copy.getEdges().get(0).getTarget().getLabel().toString());
    assertEquals(3.0, copy.getEdges().get(0).getTarget().getWeight(), 0.0001);

    Node dupe = copier.copy(copy);

    assertEquals("foo", dupe.getLabel().toString());
    assertEquals(1.0, dupe.getWeight(), 0.0001);
    assertNotNull(dupe.getEdges());
    assertEquals(1, dupe.getEdges().size());
    assertEquals(2.0, dupe.getEdges().get(0).getWeight(), 0.0001);
    assertEquals("bar", dupe.getEdges().get(0).getTarget().getLabel().toString());
    assertEquals(3.0, dupe.getEdges().get(0).getTarget().getWeight(), 0.0001);
  }
}
