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

import com.twitter.scalding.Args
import com.twitter.scalding.Csv
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Calculate the score for an item for a user by taking the weighted average of that user's ratings
 * for the K most similar items to the item in question.
 *
 * @param args passed in from the command line.
 */
class ItemScorer(args: Args) extends ItemItemJob(args) {
  val logger: Logger = LoggerFactory.getLogger(classOf[ItemScorer])

  val usersAndItems: Set[(Long, Long)] = args("users-and-items")
      .split(",")
      .map { userAndItem: String => {
        val Array(user,item) = userAndItem.split(":")
        (user.toLong, item.toLong)
      }}.toSet

  val usersSet = usersAndItems.map( _._1 ).toSet
  val itemsSet = usersAndItems.map( _._2 ).toSet

  // Get the most similar items to this item.
  // Extract them out of the AvroSortedSimilarItems.
  val mostSimilarPipe = createMostSimilarItemsPipe()

  // Read in user ratings for various items.
  val userRatingsPipe = createUserRatingsPipe()
      .filter('userId)(usersSet.contains)

  // Select only the most similar items that the user has rated.
  val mostSimilarItemsThatUserHasRatedPipe = mostSimilarPipe
      .joinWithSmaller('similarItem -> 'itemId, userRatingsPipe)
      .project('userId, 'itemToScoreId, 'similarItem, 'similarity, 'rating)
      .filter('userId, 'itemToScoreId) { x: (Long, Long) => {
        val (userId, itemToScoreId) = x
        usersSet.contains(userId) && itemsSet.contains(itemToScoreId)
      }}

  // Sort, and then take the top K.
  val neighborsPipe = mostSimilarItemsThatUserHasRatedPipe
      .groupBy(('userId, 'itemToScoreId))
      { _.sortedReverseTake[(Long, Long, Double, Long, Double)] (
          ('userId, 'itemToScoreId, 'similarity, 'similarItem, 'rating)
              -> 'res, args("k").toInt) }
      .flattenTo[(Long, Long, Double, Long, Double)] ('res ->
          ('userId, 'itemToScoreId, 'similarity, 'similarItem, 'rating))

  // Sum of all of the similarities is the denominator.
  val denom = neighborsPipe
      .groupBy(('userId, 'itemToScoreId)) { _.sum[Double]('similarity -> 'denom) }
      .project('userId, 'itemToScoreId, 'denom)

  val numer = neighborsPipe
      .map(('similarity, 'rating) -> ('scoreTerm)) { x: (Double, Double) => x._1 * x._2 }
      .groupBy(('userId, 'itemToScoreId)) { _.sum[Double]('scoreTerm -> 'numer) }
      .project('userId, 'itemToScoreId, 'numer)
      .rename(('userId, 'itemToScoreId) -> ('userIdNumer, 'itemToScoreIdNumer))

  val scorePipe = denom
      .joinWithSmaller(('userId, 'itemToScoreId) ->
          ('userIdNumer, 'itemToScoreIdNumer), numer)
      .map(('numer, 'denom) -> 'score) { x: (Double, Double) => x._1 / x._2 }
      .project('userId, 'itemToScoreId, 'score)

      // Filter out any movie pairs not in the original user input.
      .filter(('userId, 'itemToScoreId))(usersAndItems.contains)

  // Attach the actual movie titles.
  val formattedPipe = attachMovieTitles(scorePipe, 'itemToScoreId)
      .map('score -> 'score_str) { score: Double => "%.4f".format(score) }
      .project('userId, 'itemToScoreId, 'score_str, 'title)

  if (args("output-mode") == "test") {
    formattedPipe.write(Csv("score"))
  } else {
    formattedPipe.write(Csv("score", fields=('userId, 'itemToScoreId, 'score_str, 'title)))
  }
}
