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

import java.util.List;

import com.google.common.collect.Lists;

import org.kiji.mapreduce.lib.avro.Edge;

public final class TestEdgeUtils {
  /**
   * Util class, private constructor.
   */
  private TestEdgeUtils() {
  }

  public static List<Edge> getEdges() {
    final Edge e0 = new EdgeBuilder()
        .setLabel("apple")
        .setWeight(0.0)
        .setTarget(new NodeBuilder()
            .setLabel("sunday")
            .setWeight(1.0)
            .build())
        .build();
    final Edge e1 = new EdgeBuilder()
        .setLabel("banana")
        .setWeight(1.0)
        .setTarget(new NodeBuilder()
            .setLabel("monday")
            .setWeight(2.0)
            .build())
        .build();
    final Edge e2 = new EdgeBuilder()
        .setLabel("carrot")
        .setWeight(2.0)
        .setTarget(new NodeBuilder()
            .setLabel("tuesday")
            .setWeight(3.0)
            .build())
        .build();
    return Lists.newArrayList(e0, e1, e2);
  }
}
