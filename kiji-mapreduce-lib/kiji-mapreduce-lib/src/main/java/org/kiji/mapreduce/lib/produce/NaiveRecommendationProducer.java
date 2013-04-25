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
import java.util.Map;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.lib.avro.Edge;
import org.kiji.mapreduce.lib.avro.Node;
import org.kiji.mapreduce.lib.graph.NodeBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiRowData;

/**
 * A producer that generates recommendations based on a precomputed model of independent
 * association rules.
 *
 * <p>This producer attempts to generate recommendations based on the following
 * assumptions:</p>
 * <ul>
 *   <li>
 *     Each entity/row <i>E</i> in the table "likes" a set of entities, each with an
 *     associated magnitude (weight).  Call this weighted vector of liked entities
 *     <i>affinities(E)</i>.
 *   </li>
 *   <li>
 *     Each affinity <i>A</i> in <i>affinities(E)</i> has a weighted set of related
 *     (associated) items <i>related(A)</i>.  Furthermore, <i>related(A)</i> does not
 *     depend on any other elements of <i>affinities(E)</i> (this naive independence
 *     assumption is where this recommender get its name).
 *   </li>
 *   <li>
 *     The recommendations for <i>E</i> can be expressed as a function of the set of
 *     <i>related(A)</i> for each <i>A</i> in <i>affinities(E)</i>.
 *   </li>
 * </ul>
 *
 * <p>This abstract base class assumes that <i>related(x)</i> can be looked up in a
 * precomputed association model accessible via the "associations" required
 * <code>KeyValueStore</code>.
 *
 * <p>Subclasses must implement:</p>
 * <ul>
 *   <li>
 *     <code>getDataRequest()</code> to specify which cells of the row are required to
 *     determine <i>affinities(x)</i>.
 *   </li>
 *   <li>
 *     <code>getOutputColumn()</code> to specify where recommendations will be written.
 *   </li>
 *   <li>
 *     <code>getAffinities()</code> to compute <i>affinities(x)</i> for an entity <i>x</i>.
 *   </li>
 *   <li>
 *     <code>recommend()</code> to combine the set of weighted related items into a single
 *     weighted vector of recommendations.
 *   </li>
 * </ul>
 */
public abstract class NaiveRecommendationProducer extends KijiProducer {
  /** The name of the key/value store where associations (related items) can be looked up. */
  public static final String ASSOCIATIONS_STORE = "associations";

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return Collections.<String, KeyValueStore<?, ?>>singletonMap(
        ASSOCIATIONS_STORE, UnconfiguredKeyValueStore.<String, Node>builder().build());
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, ProducerContext context)
      throws IOException {
    // Extract the affinities for the current input entity (row).
    Node affinities = getAffinities(input, context);
    if (null == affinities) {
      throw new IOException("getAffinities() returned null for entity " + input.getEntityId());
    }
    if (null == affinities.getEdges()) {
      affinities.setEdges(new ArrayList<Edge>());
    }

    // Get the related items for this entity's affinities.
    Node relationships = getRelationships(affinities, context);

    // Compute the recommendations from the entity's affinity relationships.
    Node recommendations = recommend(relationships, context);

    // Output the recommendations.
    if (null != recommendations) {
      write(recommendations, context);
    }
  }

  /**
   * Returns a graph node that has outgoing weighted edges to a set of "liked" items.
   *
   * <p>An edge pointing to item <i>I</i> with weight <i>W</i> means that the current
   * entity has an affinity for <i>I</i> with magnitude <i>W</i>.</p>
   *
   * @param input The input from the row, determined by getDataRequest().
   * @param context The producer context.
   * @return A central node linked to each of this entity's affinities. The weight of each
   *     edge linking an affinity to this central node represents the weight of the
   *     affinity. The central may not be null, but may be empty (no edges).
   * @throws IOException If there is an error.
   */
  protected abstract Node getAffinities(KijiRowData input, ProducerContext context)
      throws IOException;

  /**
   * Finds the set of items that are related to a set of affinities.
   *
   * @param affinities A node where each edge represents a weighted affinity.
   * @param context The producer context.
   * @return The <code>affinities</code> node, where each affinity has an
   *     outgoing edge for each item it is related to, weighted by its "relatedness."
   * @throws IOException If there is an error.
   */
  protected Node getRelationships(Node affinities, ProducerContext context)
      throws IOException {
    for (Edge weightedAffinity : affinities.getEdges()) {
      weightedAffinity.setTarget(
          getRelatedItems(weightedAffinity.getTarget().getLabel(), context));
    }
    return affinities;
  }

  /**
   * Looks up an entity's related items in the association model via the KeyValue store.
   *
   * @param entityId The entity id to look up associations (related items) for.
   * @param context The producer context.
   * @return The entity's node in the relationship graph. Its outgoing edges represent its
   *     direct relationships.  This will not return null; if there are no related items,
   *     it will return a node with no outgoing edges.
   * @throws IOException If there is an error.
   */
  protected Node getRelatedItems(String entityId, ProducerContext context)
      throws IOException {
    KeyValueStoreReader<String, Node> associationsStore = context.getStore(ASSOCIATIONS_STORE);
    assert null != associationsStore;

    Node associations = associationsStore.get(entityId);
    if (null == associations) {
      // No associations, just return an empty node.
      return new NodeBuilder(entityId).build();
    }
    return associations;
  }

  /**
   * Uses the information about the entity's relationships (affinities and their related
   * items) to produce a single weighted vector of recommendations.
   *
   * <p>The <code>relationships</code> parameter is a subgraph representing the current
   * entity and its relationship to other entities via affinity and relatedness.</p>
   *
   * <p>The relationships graph's first level of outgoing edges represent the "likes"
   * relationship; if there is an edge from A to B, we say that A "likes" B.  The
   * second level of edges represent the "relates to" relationship; if there is an edge
   * from A to B, we say that A "relates to" B.  Here is an example:</p>
   *
   * <pre>
   *       "likes"     "relates to"
   *
   * (self) ------&gt; (a) ---------&gt; (b)
   *        \           \--------&gt; (c)
   *         \----&gt; (d) ---------&gt; (e)
   * </pre>
   *
   * <p>In this example, (self) likes (a) and (d); (a) relates to (b) and (c); (d) relates
   * to (e).</p>
   *
   * @param relationships A node representing the current entity.  It has an outgoing edge
   *     for each affinity, and a transitive outgoing edge for each item related to that
   *     affinity.
   * @param context The producer context.
   * @return A node with an outgoing edge for each recommendation, where the edge weight
   *     is the confidence in the recommendation. May be null for no recommendations.
   * @throws IOException If there is an error.
   */
  protected abstract Node recommend(Node relationships, ProducerContext context)
      throws IOException;

  /**
   * Uses the generated recommendations to generate producer output.
   *
   * <p>The default implementation simply writes the entire node to the output column at
   * the current timestamp.</p>
   *
   * @param recommendations A node with an outgoing edge for each recommendation. The
   *     edge weight is the confidence in the recommendation.
   * @param context The producer context.
   * @throws IOException If there is an error.
   */
  public void write(Node recommendations, ProducerContext context)
      throws IOException {
    context.put(recommendations);
  }
}
