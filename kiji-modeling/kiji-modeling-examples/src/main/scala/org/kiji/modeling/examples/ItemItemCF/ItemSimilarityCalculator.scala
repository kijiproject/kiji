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
import scala.collection.JavaConverters.seqAsJavaListConverter

import cascading.pipe.Pipe
import cascading.pipe.joiner.LeftJoin
import com.twitter.scalding._
import com.twitter.scalding.mathematics._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.express._
import org.kiji.express.flow._

import org.kiji.modeling.examples.ItemItemCF.avro._
import org.kiji.modeling.framework.ModelPipeConversions

/**
 * Calculate item-item similarity.
 *
 * Get mean rating / user:
 * - Read in all of the data from the table
 * - Group by user
 * - Get the mean
 * - Create a (user, mean) stream
 *
 * Get the mean-adjusted ratings for all of the items
 * - Read in all of the data from the table
 * - Group by user
 * - Join with the (tiny) (user, mean) stream
 * - Subtract the mean from all of the ratings
 * - Create a (user, item, normalized-rating) stream
 *
 * Get the cosine similarity of all of the pairs for items
 *
 * @param args passed in from the command line.
 */
class ItemSimilarityCalculator(args: Args) extends ItemItemJob(args) with ModelPipeConversions {

  val logger: Logger = LoggerFactory.getLogger(classOf[ItemSimilarityCalculator])

  /**
   * Sort (itemA, itemB, similarity) tuples by similarity (for a given pair of tuples) and store in
   * Avro records.  Remember for a tuple (itemA, itemB, similarity) to populate the similarity
   * vectors for itemA *and* for itemB.
   *
   * @param simPipe A pipe containing the (itemA, itemB, similarity) item-item similarity scores.
   * @return A pipe with `AvroSortedSimilarItems` objects containing sorted arrays of the similarity
   * vectors for each itemd.
   */
  def createSimilarityRecordsPipe(simPipe: Pipe): Pipe = {

    // Group by the first item in the pair and sort to get a sorted list of item similarities.
    simPipe
        .groupBy('itemA) {
          _.sortWithTake(('itemB, 'similarity) -> 'topSimPairs, args("model-size").toInt)
              { (x: (Long, Double), y: (Long, Double)) => x._2 > y._2 } }
          // Now we have tuples of ('itemA, 'topSimPairs = List[(Long, Double)]

        .map('topSimPairs -> 'mostSimilar) { x: List[(Long, Double)] => {
          val simList = x.map { y: (Long, Double) => new AvroItemSimilarity(y._1, y._2) }
          new AvroSortedSimilarItems(simList.asJava)
        }}
        .rename('itemA, 'item)
        .project('item, 'mostSimilar)
  }

  // Read in user ratings for various items.
  val userRatingsPipe = createUserRatingsPipe()

  // Calculate the adjusted cosine similarity.
  val itemItemSimilarityPipe = userRatingsPipe
      .adjustedCosineSimilarity[Long, Long](('userId, 'itemId, 'rating) -> ('itemA, 'itemB,
        'similarity))
        .filter('similarity) { sim: Double => sim > 0 }

  // Sort by similarity and create avro records to store in a Kiji table.
  // The Avro records contain a list of the top N most-similar items for the item in question.
  val simRecordsPipe = createSimilarityRecordsPipe(itemItemSimilarityPipe)
      .map('item -> 'entityId) { item: Long => EntityId(item) }
      .project('entityId, 'mostSimilar)

  // Write these top N most-similar items to a table.
  simRecordsPipe.write(KijiOutput.builder
      .withTableURI(args("similarity-table-uri"))
      .withColumnSpecs(Map(
          'mostSimilar -> QualifiedColumnOutputSpec.builder
              .withColumn("most_similar", "most_similar")
              .withSchemaSpec(SchemaSpec.Specific(classOf[AvroSortedSimilarItems]))
              .build))
     .build)
}
