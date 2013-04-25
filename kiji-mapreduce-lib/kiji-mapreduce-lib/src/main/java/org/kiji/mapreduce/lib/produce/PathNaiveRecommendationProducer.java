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

package org.kiji.mapreduce.lib.produce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.avro.Path;
import org.kiji.mapreduce.lib.graph.NodeBuilder;
import org.kiji.mapreduce.lib.graph.NodeUtils;
import org.kiji.mapreduce.produce.ProducerContext;

/**
 * An implementation of the <code>NaiveRecommendationProducer</code> that computes the
 * weights for generated recommendations by aggregating the nodes/edges along the path from the
 * entity to the recommended item.
 */
public abstract class PathNaiveRecommendationProducer extends NaiveRecommendationProducer {
  /**
   * Computes the confidence of a recommendation given its path from the entity the
   * relationship graph.
   *
   * @param path The path from <i>the entity we are recommending items to</i> to <i>the item we
   *     are recommending</i>.
   * @return The confidence in the recommendation.
   */
  protected abstract double getConfidence(org.kiji.mapreduce.lib.avro.Path path);

  /** {@inheritDoc} */
  @Override
  protected Node recommend(Node relationships, ProducerContext context)
      throws IOException {
    // The relationships variable is a subgraph representing the current entity and its
    // relationship to other entities via affinity and relatedness.
    //
    // The relationships graph's first level of outgoing edges represent the "likes"
    // relationship; if there is an edge from A to B, we say that A "likes" B.  The
    // second level of edges represent the "relates to" relationship; if there is an edge
    // from A to B, we say that A "relates to" B.  Here is an example:
    //
    //       "likes"     "relates to"
    //
    // (self) ------> (a) ---------> (b)
    //        \           \--------> (c)
    //         \----> (d) ---------> (e)
    //
    // In this example, (self) likes (a) and (d); (a) relates to (b) and (c); (d) relates
    // to (e).  The goal of this method is to collapse this relationship graph into one
    // that has only direct paths from (self) to (b), (c), and (e), since those are the
    // elements we want to recommend.  We will use the sum of weights along the edges of
    // the path as the confidence.
    //
    // TODO(gwu): Is this collapse operation useful? If so, it should be added to NodeUtils.

    NodeBuilder recommendations = new NodeBuilder(relationships.getLabel());
    for (Edge affinityEdge : relationships.getEdges()) {
      if (null == affinityEdge.getTarget().getEdges()) {
        continue;
      }
      for (Edge relatedEdge : affinityEdge.getTarget().getEdges()) {
        // Construct a path from the entity to the recommendation.
        Path path = new Path();
        path.setStart(relationships);
        path.setEdges(new ArrayList<Edge>());
        path.getEdges().add(affinityEdge);
        path.getEdges().add(relatedEdge);

        // Compute the confidence based on the path.
        double confidence = getConfidence(path);

        // Add the recommendation.
        recommendations.addEdge(confidence, relatedEdge.getTarget());
      }
    }

    Node recommendationsGraph = recommendations.build();
    if (null == recommendationsGraph.getEdges() || recommendationsGraph.getEdges().isEmpty()) {
      // No recommendations.
      return null;
    }

    // The recommendations graph at this point has outgoing edges directly to the
    // items we would like to recommend.  However, there may be duplicates if an item was
    // related to more than one thing the entity "liked".  Merge those duplicates
    // together by summing the edge weights.  This also sorts the edges by decreasing
    // weight.
    List<Node> mergedRecommendations = NodeUtils.mergeNodes(
        Collections.singletonList(recommendationsGraph));

    // Since we only gave one input to mergeNodes(), we can only have one output.
    assert 1 == mergedRecommendations.size();

    // Return the merged (duplicates removed) recommendations.
    return mergedRecommendations.get(0);
  }
}
