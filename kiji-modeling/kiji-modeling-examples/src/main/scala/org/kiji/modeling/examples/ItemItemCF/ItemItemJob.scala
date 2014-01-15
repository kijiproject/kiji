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

package org.kiji.modeling.examples.ItemItemCF

import scala.math.sqrt
import scala.collection.JavaConverters._

import cascading.pipe.Pipe
import cascading.pipe.joiner.LeftJoin
import com.twitter.scalding._
import org.apache.avro.util.Utf8
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.express._
import org.kiji.express.flow._

import org.kiji.modeling.examples.ItemItemCF.avro._

/**
 * Contains common functionality for different phases in the item-item CF flow.
 */
abstract class ItemItemJob(args: Args) extends KijiJob(args) {

  /**
   * Extract the item ID and item rating from a `FlowCell`.
   *
   * @param slice The sequence of cells (should be only one) with ratings for this item.
   * @return The item ID and rating (as a pair).
   */
  def extractItemIdAndRating(slice: Seq[FlowCell[Double]]): Seq[(Long,Double)] = {
    slice.map { cell => (cell.qualifier.toLong, cell.datum) }
  }

  /**
   * Query the user-ratings table to produce a pipe with tuples containing a user, item, and
   * rating.
   *
   * @param specificUser An optional specific user for whom to filter the pipe.  Useful for querying
   * only a single user.
   * @return A pipe of the form `'userId`, `'itemId`, `'rating`.
   */
  def createUserRatingsPipe(specificUser: Option[Long] = None): Pipe = {

    val userRatingsPipe: Pipe = KijiInput.builder
        .withTableURI(args("ratings-table-uri"))
        .withColumnSpecs(ColumnFamilyInputSpec.builder.withFamily("ratings").build -> 'ratingInfo)
        .build
        .read

        // Extract the userIds
        .map('entityId -> 'userId) { eid: EntityId => eid.components(0) }

    val filteredUserRatingsPipe = specificUser match {
      case None => userRatingsPipe
      case Some(myUser: Long) => userRatingsPipe.filter('userId) { x: Long => x == myUser }
    }

    filteredUserRatingsPipe
        // Extract the ratings
        .flatMap('ratingInfo -> ('itemId, 'rating)) { extractItemIdAndRating }
        .project('userId, 'itemId, 'rating)
  }

  /**
   * Extract the item Id and similarity from a sequence of `FlowCell`s containing Avro records of
   * most-similar-item vectors.
   *
   * @param slice The sequence of cells (should be only one) containing the similarity vector for
   * the given item.
   * @return The most-similar items as a `Seq[(itemId, rating)]`.
   */
  def extractItemIdAndSimilarity(
      slice: Seq[FlowCell[AvroSortedSimilarItems]]): Seq[(Long, Double)] = {
    slice.flatMap { cell => {
      // Get a Scala List of the similar items and similarities
      val topItems = cell.datum.getTopItems.asScala
      topItems.map { sim: AvroItemSimilarity => (sim.getItemId.toLong, sim.getSimilarity.toDouble) }
    }}}


  /**
   * Query the item-item similarities table to produce a pipe in which each tuple contains an item
   * ID, the ID of a similar item, and the similarity.
   *
   * @param specificItems An optional set of items with which to filter the item IDs in the pipe.
   * Use this parameter to limit the pipe to similarity vectors for only a set of items.
   * @return The pipe.
   */
  def createMostSimilarItemsPipe(specificItems: Option[Set[Long]] = None): Pipe = {
    // Get the most similar items to this item
    // Extract them out of the AvroSortedSimilarItems
    val mostSimilarPipe = KijiInput.builder
        .withTableURI(args("similarity-table-uri"))
        .withColumnSpecs(
            QualifiedColumnInputSpec.builder
                .withColumn("most_similar", "most_similar")
                .withSchemaSpec(SchemaSpec.Specific(classOf[AvroSortedSimilarItems]))
                .build -> 'most_similar)
        .build

        // We care about only the data for one item
        .map('entityId -> 'itemId) { eid: EntityId => eid.components(0) }

    val filteredPipe = specificItems match {
      case None => mostSimilarPipe
      case Some(itemsSet) => mostSimilarPipe.filter('itemId)(itemsSet.contains)
    }


    filteredPipe
        // Extract out the itemId and similarity score for the similar items
        .flatMap('most_similar -> ('similarItem, 'similarity)) { extractItemIdAndSimilarity }
        .project('itemId, 'similarItem, 'similarity)
        .rename('itemId -> 'itemToScoreId)
  }


  /**
   * Read in the movie titles and attach them to another stream with movie IDs in a given field.
   *
   * @param pipe The pipe to which to attach movie titles.
   * @param movieIdField The field within `pipe` that contains the movie IDs to use in the join.
   * @return The `pipe` with movie titles instead of movie IDs.
   */
  def attachMovieTitles(pipe: Pipe, movieIdField: Symbol): Pipe = {
      KijiInput.builder
          .withTableURI(args("titles-table-uri"))
          .withColumns("info:title" -> 'title)
          .build
          // Get the movieIds from the entity IDs
          .map('entityId -> 'movieId) { eid: EntityId => eid.components(0) }
          // Extract the actual movie title
          .map('title -> 'title) { cellseq: Seq[FlowCell[CharSequence]] => {
            assert(cellseq.size == 1)
            cellseq.head.datum.toString
          }}
          .joinWithLarger('movieId -> movieIdField, pipe)
          .discard('movieId)
  }
}
