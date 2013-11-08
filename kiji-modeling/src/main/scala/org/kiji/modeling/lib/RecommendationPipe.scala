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

package org.kiji.modeling.lib

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.TupleConversions

import org.kiji.express.repl.Implicits.pipeToRichPipe
import org.kiji.modeling.framework.ModelPipeConversions

/**
 * This class provides extensions to Scalding's RichPipe in order to perform recommendations. There
 * must be an [[scala.math.Ordering]] defined for the IDs used for recommendations
 * (e.g. product IDs), i.e. there must be a way to compare them. For most commonly used data
 * types (e.g. Int, String, etc.) this is already provided by Scala.
 *
 * @param pipe is the underlying Cascading Pipe.
 */
class RecommendationPipe(val pipe: Pipe)
    extends FieldConversions
    with TupleConversions
    with ModelPipeConversions {
  /**
   * This function takes profile information (e.g. a history of purchases) or order data and outputs
   * smaller subsets of co-occurring items. If the minSetSize and maxSetSize is 2, it will create
   * tuples of items that are found within the same history/order. This will typically be used to
   * calculate the numerator (raw count) for Jaccard similarity, or the number of N-grams.
   * Mathematically, if N = number of orders, m = minSetSize and M = maxSetSize, the number of
   * resulting subsets will be = N choose m + N choose (m+1) + N choose (m+2) ... N choose M
   *
   * @param fieldSpec mapping from the field(s) which represent the order/purchase history to the
   *     field that will hold the resulting N-grams.
   * @param minSetSize is the minimum size of the subset or N-gram. Optional.
   * @param maxSetSize is the maximum size of the subset or N-gram. Optional. Care must be taken
   *     while choosing this value as the number of resulting tuples might be too large to hold in
   *     memory at a single mapper or may take a while to generate.
   * @param separator is used to create the identifier string for the itemset.
   * @tparam T is the data type of the element in the order/purchase history.
   * @return a RichPipe with the specified output field which holds the resulting tuples.
   */
  def prepareItemSets[T](
      fieldSpec: (Fields, Fields),
      minSetSize: Int = 2,
      maxSetSize: Int = 5,
      separator: String = ",")
      (implicit ordering: Ordering[T]): Pipe = {
    val (fromFields, toFields) = fieldSpec
    pipe
        .flatMapTo(fromFields -> toFields) {
          basket: Seq[T] => {
            (minSetSize to maxSetSize).flatMap(basket.sorted.combinations)
          }
        }
        .map(toFields -> toFields) { itemSets: Seq[T] => itemSets.mkString(separator) }
  }

  /**
   * This function joins every row in the pipe with the size of the group it belongs to. The
   * parameter fromFields determines the criteria for the group.
   *
   * @param fieldSpec a mapping from the fields on which to group to the output field name.
   * @return a pipe containing the groups, along with a field for their size.
   */
  def joinWithGroupCount(fieldSpec : (Fields, Fields)) : Pipe = {
    val (fromFields, toField) = fieldSpec
    val total = pipe.groupBy(fromFields) { _.size(toField) }.project(fromFields, toField)
    pipe.joinWithSmaller(fromFields-> fromFields, total)
  }

  /**
   * This function joins every row in the pipe with the total count of the rows. This is useful
   * when performing normalization. Note: This operation is very slow, considering it must be sent
   * to a single reducer to count the rows, followed by crossing this data with all the rows.
   *
   * @param toField is the name of the field that will hold the total in the pipe.
   * @return a pipe with a new field attached, which holds the total count.
   */
  def joinWithCount(toField: Fields): Pipe = {
    val total = pipe.groupAll { _.size(toField) }.project(toField)
    pipe.crossWithTiny(total)
  }

  /**
   * Filter itemsets (which are N-grams of entities that occur together in some context, e.g.
   * products in an order) by a threshold value called supportThreshold. This is usually used to
   * filter only "important" occurrences. This is typically expressed as a fraction, rather than
   * an absolute value. The denominator for this fraction needs to be provided either as a constant
   * or a pipe and a field within it.
   *
   * @param fieldSpec is the mapping of the field in this pipe that contains the co-occurring
   *     N-grams to the resultant field that will store the support of this N-gram.
   * @param normalizingPipe is a pipe that may contain the normalizing constant. Optional.
   * @param normalizingConstant is a normalizing constant.
   * @param normalizingField is either the name of the field in the normalizing pipe that contains
   *     the normalizing constant or is the name of the field to insert into the pipe if you have
   *     provided a normalizingConstant.
   * @param supportThreshold is the cut-off value for filtering the itemsets of importance.
   * @param numReducers is used if you have more than 1 reducer available to run this function.
   * @return the pipe containing itemsets that satisfy the supportThreshold with a field for their
   *     support.
   */
  def filterBySupport(fieldSpec: (Fields, Fields),
      normalizingPipe: Option[Pipe],
      normalizingConstant: Option[Double],
      normalizingField: Fields,
      supportThreshold: Double,
      numReducers: Int = 1): Pipe = {
    val (itemsetField, resultField) = fieldSpec
    // Exactly one of normalizingConstant and normalizingPipe must be supplied to this function
    require (normalizingConstant.isDefined ^ normalizingPipe.isDefined)
    var tempPipe = pipe.groupBy(itemsetField) { _.reducers(numReducers).size('itemSetFrequency) }
    if (normalizingConstant.isDefined) {
      tempPipe = tempPipe.insert(normalizingField, normalizingConstant.get)
    } else {
      tempPipe = tempPipe.crossWithTiny(normalizingPipe.get)
    }
    tempPipe
        .map(('itemSetFrequency, normalizingField) -> resultField) {
          fields: (Long, Double) => {
            val (frequency, normalizer) = fields
            frequency/normalizer
          }
        }
        .filter(resultField) { support: Double => (support >= supportThreshold) }
  }

  /**
   * Calculates the Jaccard Similarity between two pairs of items. Assumes the data is stored
   * in an entity-centric manner.
   *
   * @param fieldSpec is the mapping from the field containing items related to one entity
   *     (e.g. purchases of 1 person) to the field that will hold the result.
   * @param separator to use between pairs of items. Default = ","
   * @tparam T is the type of the incoming item IDs.
   * @return a pipe containing pairs of items with their Jaccard similarity. The result field
   *     will contain a double.
   */
  def simpleItemItemJaccardSimilarity[T](
      fieldSpec: (Fields, Fields),
      separator: String = ",")
      (implicit ordering: Ordering[T]): Pipe = {
    val (itemsetField, resultField) = fieldSpec
    // For each item, find the number of entities (orders, customers) this appears in
    // and keep it around in a pipe to be used for calculating the union
    val productCounts = pipe.flatMap(itemsetField -> 'itemId) {
          itemset: Seq[T] => itemset.distinct
        }
        .groupBy('itemId) { _.size('itemCount) }

    // first calculate the intersection, i.e. number of rows in which two items occur together
    pipe.prepareItemSets[T](itemsetField -> 'itemPairs, 2, 2)
        .groupBy('itemPairs) { _.size('itemPairCount) }
        // now calculate union, i.e. number of pairs in which either one of the other item appear
        .map('itemPairs -> ('item1, 'item2)) {
          itemPair: String => {
            val items = itemPair.split(separator)
            (items(0), items(1))
          }
        }
        // join with the count for the number of entities in which the first product appears
        .joinWithSmaller('item1 -> 'itemId, productCounts)
        .rename('itemCount -> 'item1Count)
        .project('itemPairCount, 'item1, 'item2, 'item1Count)
        // join with the count for the number of entities in which the second product appears
        .joinWithSmaller('item2 -> 'itemId, productCounts)
        .rename('itemCount -> 'item2Count)
        .project('item1, 'item2, 'itemPairCount, 'item1Count, 'item2Count)
        // find the union
        .map(('itemPairCount, 'item1Count, 'item2Count) -> 'itemUnionCount) {
          counts: (Long, Long, Long) => {
            val (itemPairCount, item1Count, item2Count) = counts
            // |union(A, B)| = |A| + |B| + |Intersection(A, B)|
            item1Count + item2Count - itemPairCount
          }
        }
        // find the Jaccard similarity
        .map(('itemPairCount, 'itemUnionCount) -> resultField) {
          counts: (Long, Long) => counts._1.toDouble / counts._2
        }
  }
}
