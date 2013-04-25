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

import java.util.Comparator;

import org.kiji.mapreduce.lib.avro.Edge;

/**
 * Compares Edge messages by only looking at their weight, and sorts
 * them in decreasing order.
 */
public class EdgeWeightEdgeComparator implements Comparator<Edge> {

  @Override
  public int compare(Edge o1, Edge o2) {
    if (o1.getWeight() > o2.getWeight()) {
      return -1;
    }
    if (o1.getWeight() < o2.getWeight()) {
      return 1;
    }
    if (o1.getTarget().getWeight() > o2.getTarget().getWeight()) {
      return -1;
    }
    if (o1.getTarget().getWeight() < o2.getTarget().getWeight()) {
      return 1;
    }
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.getClass() == getClass();
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException();
  }
}
