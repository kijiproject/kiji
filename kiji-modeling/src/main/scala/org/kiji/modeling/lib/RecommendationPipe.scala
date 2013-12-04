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

import scala.math.sqrt

import cascading.pipe.Pipe
import cascading.pipe.joiner.LeftJoin
import cascading.tuple.Fields
import com.twitter.algebird.Ring
import com.twitter.scalding.FieldConversions
import com.twitter.scalding.GroupBuilder
import com.twitter.scalding.RichPipe
import com.twitter.scalding.TupleConversions
import com.twitter.scalding.mathematics.Matrix
import com.twitter.scalding.mathematics.RowVector

import org.kiji.express.repl.Implicits.pipeToRichPipe
import org.kiji.modeling.framework.ModelPipeConversions

// For implicit conversions
import Matrix._

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
   * Compute support for itemsets (N-grams of entities that occur together in some context, e.g.
   * products in an order) in a pipe containing sets of orders.
   * This is typically expressed as a fraction, rather than an absolute value.
   * The denominator for this fraction needs to be provided either
   * as a constant or a pipe and a field within it.
   *
   * @param fieldSpec is the mapping of the field in this pipe that contains the co-occurring
   *     N-grams to the resultant field that will store the support of this N-gram.
   * @param normalizingPipe is a pipe that may contain the normalizing constant. Optional.
   * @param normalizingConstant is a normalizing constant.
   * @param normalizingField is either the name of the field in the normalizing pipe that contains
   *     the normalizing constant or is the name of the field to insert into the pipe if you have
   *     provided a normalizingConstant.
   * @param numReducers is used if you have more than 1 reducer available to run this function.
   * @return the pipe containing itemsets and their supports.
   */
  def support(fieldSpec: (Fields, Fields),
      normalizingPipe: Option[Pipe],
      normalizingConstant: Option[Double],
      normalizingField: Fields,
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
  }

  /**
   * Given a pipe containing exhaustive itemsets with their support values,
   * contrive association rules of the form
   * LHS (antecedent itemset) --> RHS (consequent itemset)
   * and then compute lift and confidence for these rules.
   * For itemsets LHS and RHS with corresponding supports support(LHS) and support(RHS),
   * confidence(LHS --> RHS) = support(LHS union RHS) / support(LHS)
   * lift(LHS --> RHS) = conf(LHS --> RHS) / support(RHS).
   * The minimum and maximum sizes of LHS and RHS may be provided as parameters,
   * but ought to be small for reasonably fast computation.
   *
   * These definitions can be found on the Wikipedia page for
   * <a href="http://en.wikipedia.org/wiki/Association_rule_learning">
   * Association rule learning
   * </a>.
   *
   * Input pipe must contain: itemset (string of comma-separated items) and support.
   * Output pipe will contain: LHS itemset, RHS itemset, confidence, lift, support(LHS union RHS).
   *
   * @param lhsMinSize minimum size of the LHS
   * @param lhsMaxSize maximum size of the LHS
   * @param rhsMinSize minimum size of the RHS
   * @param rhsMaxSize maximum size of the RHS
   * @param separator for itemset strings
   */
  def confidenceAndLift(
      lhsMinSize: Int = 1,
      lhsMaxSize: Int = 1,
      rhsMinSize: Int = 1,
      rhsMaxSize: Int = 1,
      separator: String = ","): Pipe = {
    pipe
        // Filter out itemsets which are too big or too small.
        .filter('itemset) {
          field: String => {
            val itemSetSize = field.split(separator).size
            itemSetSize <= lhsMaxSize + rhsMaxSize &&
                itemSetSize > Math.min(lhsMinSize, rhsMinSize)
          }
        }
        // Generate all possible LHS itemsets by taking subsets of the itemsets from input pipe.
        // Generate corresponding RHS itemsets.
        // Make sure LHS and RHS are requested sizes.
        .flatMap('itemset -> ('lhs, 'rhs)) {
          itemset: String => {
            val itemsetSplit = itemset.split(separator).map(_.trim).distinct.sorted.toList
            // LHS ought to be strict subset.
            (lhsMinSize to Math.min(lhsMaxSize, itemsetSplit.size - 1))
                .flatMap(itemsetSplit.combinations)
                .map(combination => {
                  val lhs = combination.sorted
                  val rhs = itemsetSplit.filterNot(element => lhs.contains(element)).sorted
                  (lhs, rhs)
                })
                // Remove incorrect sizes.
                .filter(fields => {
                  val (lhsSize, rhsSize) = (fields._1.size, fields._2.size)
                  lhsSize <= lhsMaxSize && lhsSize >= lhsMinSize &&
                      rhsSize <= rhsMaxSize && rhsSize >= rhsMinSize
                })
                // Make string.
                .map(fields => {
                  val (lhs, rhs) = fields
                  (lhs.mkString(separator), rhs.mkString(separator))
                })
                .toList
          }
        }
        .rename(('itemset, 'support) -> ('union, 'supportUnion))
        .project('union, 'supportUnion, 'lhs, 'rhs)
        // Acquire supports for LHS itemsets if they exist in the original pipe.
        .joinWithSmaller('lhs -> 'itemset, pipe)
        .rename('support -> 'supportlhs)
        .project('union, 'supportUnion, 'lhs, 'supportlhs, 'rhs)
        // Acquire supports for RHS itemsets if they exist in the original pipe.
        .joinWithSmaller('rhs -> 'itemset, pipe)
        .rename('support -> 'supportrhs)
        .project('union, 'supportUnion, 'lhs, 'supportlhs, 'rhs, 'supportrhs)
        // Compute confidence and lift.
        .map(('supportUnion, 'supportlhs, 'supportrhs) -> ('confidence, 'lift)) {
          fields: (Double, Double, Double) => {
            val (supportUnion, supportlhs, supportrhs) = fields
            val confidence = supportUnion/supportlhs
            val lift = confidence/supportrhs
            (confidence, lift)
          }
        }
        .project('lhs, 'rhs, 'confidence, 'lift, 'supportUnion)
  }

  /**
   * Create a `Matrix` from a user pipe by projecting on user-specified fields indicated the row
   * identifiers, column identifiers, and values in the matrix.
   *
   * For item-item collaborative filtering, the rows will presumably be user IDs, the columns will
   * be item IDs, and the values will be ratings.
   *
   * @param fields The name of three fields (row, col, rating) in the user's pipe.
   * @tparam R The type of the row identifiers in the `Pipe`.
   * @tparam C The type of the column identifiers in the `Pipe`.
   * @return A Scalding `Matrix` formed from the input pipe.
   */
  private def pipeToUserItemMatrix[R <% Ordered[R], C <% Ordered[C]](
      fields: Fields): Matrix[R, C, Double] = {
    if (3 != fields.size()) {
      throw new Exception("Pipe to convert to matrix should have exactly three input fields " +
          "(row, column, value).")
    }

    pipe
        .project(fields)
        .rename(fields -> ('userId, 'itemId, 'rating))
        .toMatrix[R, C, Double]('userId, 'itemId, 'rating)
  }

  /**
   * Create a `Pipe` from a Scalding `Matrix` of similarities.  In doing so, remove any similarity
   * tuples in which the two items (or users) are identical.
   *
   * @param fields Three user fields (for item-item similarity: first item, second item, similarity)
   *     to populate with the Matrix elements.
   * @param similarityMatrix The Scalding `Matrix` containing similarity scores.
   * @tparam C The type of the column identifiers in the `Pipe`.
   * @return A pipe containing item pairs and their similarity scores.
   */
  private def similarityMatrixToPipe[C <% Ordered[C]](
      fields: Fields,
      similarityMatrix: Matrix[C, C, Double]): Pipe = {

    if (3 != fields.size()) {
      throw new Exception("Pipe to produce from a matrix should have exactly three input fields.")
    }

    similarityMatrix
        .pipeAs('itemA, 'itemB, 'score)
        // Don't report similarity of an item with itself!
        .filter('itemA, 'itemB, 'score) { fields: (C, C, Double) =>
          val (itemA, itemB, score) = fields
          (itemA != itemB)
        }
        .rename(('itemA, 'itemB, 'score) -> fields)
  }

  /**
   * Calculates cosine similarity, returning a pipe of tuples of the form (first item, second item,
   * similarity score).
   *
   * This method implements the algorithm described in
   * <a href=http://files.grouplens.org/papers/www10_sarwar.pdf>
   * "Item-Based Collaborative Filtering Recommendation Algorithms" by Sarwar, et al</a>, Section
   * 3.1.1.
   *
   * @param fieldSpec contains the (row ID, column ID, rating) fields in the current pipe and the
   *     (first item, second item, similarity) fields in the output pipe.  For item-item similarity,
   *     the rows will be user IDs and the columns will be item IDs.
   * @tparam R The type of the incoming row IDs.
   * @tparam C The type of the incoming column IDs.
   * @return A pipe containing tuples of (first item, second item, similarity).
   */
  def cosineSimilarity[R <% Ordered[R], C <% Ordered[C]](fieldSpec: (Fields, Fields)): Pipe = {

    // Convert pipe into a matrix with rows for users and columns for items
    val userRatingsMatrix: Matrix[R, C, Double] =
        pipeToUserItemMatrix[R, C](fieldSpec._1)

    // Normalize every column (item vector) in the matrix.  Scalding has a row normalization method
    // but not a column normalization method, so we have to transpose the matrix (tranpose should be
    // free in Scalding's matrix API - We are just juxtaposing the row and column fields).
    val preparedMatrix = userRatingsMatrix
      .transpose
      .rowL2Normalize
      .transpose

    val similarityMatrix = (preparedMatrix.transpose * preparedMatrix)

    similarityMatrixToPipe[C](fieldSpec._2, similarityMatrix)
  }

  /**
   * Calculates cosine similarity, returning a pipe of tuples of the form (first item, second item,
   * similarity score).  In adjust cosine similarity, we center the column vectors around the mean
   * values of the rows before computing cosine similarity.
   *
   * This method implements the algorithm described in
   * <a href=http://files.grouplens.org/papers/www10_sarwar.pdf>
   * "Item-Based Collaborative Filtering Recommendation Algorithms" by Sarwar, et al</a>, Section
   * 3.1.3.
   *
   * @param fieldSpec contains the (row ID, column ID, rating) fields in the current pipe and the
   *     (first item, second item, similarity) fields in the output pipe.  For item-item similarity,
   *     the rows will be user IDs and the columns will be item IDs.
   * @tparam R is the type of the incoming row IDs.
   * @tparam C is the type of the incoming column IDs.
   * @return A pipe containing tuples of (first item, second item, similarity).
   */
  def adjustedCosineSimilarity[R <% Ordered[R], C <% Ordered[C]](
      fieldSpec: (Fields, Fields)): Pipe = {

    // Convert pipe into a matrix with rows for users and columns for items
    val userRatingsMatrix: Matrix[R, C, Double] =
        pipeToUserItemMatrix[R, C](fieldSpec._1)

    val preparedMatrix = userRatingsMatrix
      // Compute the mean of every row in the matrix and recenter the values in each row around the
      // mean
      .rowMeanCentering
      // Normalize every column (item vector) in the matrix.  Scalding has a row normalization
      // method but not a column normalization method, so we have to transpose the matrix (tranpose
      // should be free in Scalding's matrix API - We are just juxtaposing the row and column
      // fields).
      .transpose
      .rowL2Normalize
      .transpose

    val similarityMatrix = (preparedMatrix.transpose * preparedMatrix)

    similarityMatrixToPipe[C](fieldSpec._2, similarityMatrix)
  }

  /**
   * Calculates Jaccard correlation-based similarity, returning a pipe of tuples of the form (first
   * item, second item, similarity score).
   *
   * This method implements the standard
   * <a href=http://en.wikipedia.org/wiki/Jaccard_index>Jaccard index formula</a>.
   *
   * @param fieldSpec contains the (row ID, column ID, rating) fields in the current pipe and the
   *     (first item, second item, similarity) fields in the output pipe.  For item-item similarity,
   *     the rows will be user IDs and the columns will be item IDs.
   * @tparam R is the type of the incoming row IDs.
   * @tparam C is the type of the incoming column IDs.
   * @return A pipe containing tuples of (first item, second item, similarity).
   */
  def jaccardSimilarity[R <% Ordered[R], C <% Ordered[C]]
      (fieldSpec: (Fields, Fields)): Pipe = {

    // Convert pipe into a matrix with rows for users and columns for items
    val userRatingsMatrix: Matrix[R, C, Int] = fieldSpec._1.size() match {
      // If the user has specified ratings, then binarize
      case 3 => pipeToUserItemMatrix[R, C](fieldSpec._1).binarizeAs[Int]

      // If not, just add 1 for all of the user / item pairs
      case 2 => pipe
        .project(fieldSpec._1)
        .rename(fieldSpec._1 -> ('userId, 'itemId))
        // Insert a default value of "1" for a rating here, since the subsequent code relies upon
        // having a numerical rating.
        .map('userId -> ('userId, 'rating)) { x: R => (x, 1) }
        .toMatrix[R, C, Int]('userId, 'itemId, 'rating)

      case _ => throw new Exception(
          "Pipe to convert to matrix should have exactly two or three input fields.")
    }

    // Compute the number of ratings for each item.
    // (The "sumRowVectors" method in Scalding creates a row vector containing the sum of each
    // column vector.  The nomenclature is somewhat confusing, but follows a Matlab naming
    // convention.)
    val myNormVector: RowVector[C, Int] = userRatingsMatrix.sumRowVectors

    // Create a matrix in which the the value in row i, col j is the number of users who have rated
    // both item i and item j.
    val dotProductMatrix = userRatingsMatrix.transpose * userRatingsMatrix

    // This code is slightly tricky and comes from the Scalding Matrix library example that does
    // Jaccard similarity.

    // Create a matrix in which entry i,j is:
    // - if the number of users who have interacted with both i and j > 0, then the number of users
    //   who have interacted with i (the value for column i in myNormVector).
    // - if no user has interacted with both i and j, then zero
    val rowUnionMatrix = dotProductMatrix
        .zip(myNormVector)
        .mapValues(pair => pair._2)

    val colUnionMatrix = dotProductMatrix
        .zip(myNormVector.transpose)
        .mapValues(pair => pair._2)

    val unionMatrix: Matrix[C, C, Int] = rowUnionMatrix + colUnionMatrix - dotProductMatrix
    val similarityMatrix: Matrix[C, C, Double] = dotProductMatrix
        .zip(unionMatrix)
        .mapValues( pair => pair._1.toDouble / pair._2.toDouble )

    similarityMatrixToPipe[C](fieldSpec._2, similarityMatrix)
  }

  /**
   * Calculates Pearson correlation-based similarity, returning a pipe of tuples of the form (first
   * item, second item, similarity score).
   *
   * This method implements the algorithm described in
   * <a href=http://files.grouplens.org/papers/www10_sarwar.pdf>
   * "Item-Based Collaborative Filtering Recommendation Algorithms" by Sarwar, et al</a>, Section
   * 3.1.2.
   *
   * @param fieldSpec contains the (row ID, column ID, rating) fields in the current pipe and the
   *     (first item, second item, similarity) fields in the output pipe.  For item-item similarity,
   *     the rows will be user IDs and the columns will be item IDs.
   * @tparam R is the type of the incoming row IDs.
   * @tparam C is the type of the incoming column IDs.
   * @return A pipe containing tuples of (first item, second item, similarity).
   */
  def pearsonSimilarity[R <% Ordered[R], C <% Ordered[C]](
      fieldSpec: (Fields, Fields)): Pipe = {

    def createRatingPairs(itemsAndRatings: List[(C, Double)]):
        Iterable[(C, C, Double, Double)] = {
      for {
        (itemA: C, ratingA: Double) <- itemsAndRatings
        (itemB: C, ratingB: Double) <- itemsAndRatings
        //if (itemA < itemB)
        if (itemA != itemB)
      } yield (itemA, itemB, ratingA, ratingB)
    }

    def ratingPairListToPearson(ratings: List[(Double, Double)]): Double = {
      val ratingsA = ratings.map { _._1 }
      val ratingsB = ratings.map { _._2 }

      // Compute the mean-adjusted, normalized vectors for itemA and itemB
      val normalizedRatings: List[List[Double]] = { for {
        ratingsVector <- List(ratingsA, ratingsB)
      } yield {
        val mean: Double = ratingsVector.sum / ratingsVector.size
        val meanAdjustedVector: List[Double] = ratingsVector.map { _ - mean }
        val norm: Double = sqrt(meanAdjustedVector
            .foldLeft(0.0) { (currentSum: Double, rating: Double) => currentSum + rating*rating })

        meanAdjustedVector.map { _ / norm }
      } }.toList

      // The Pearson correlation is now just the dot product of the two vectors
      val List(normalizedRatingsA, normalizedRatingsB) = normalizedRatings

      val similarity: Double = normalizedRatingsA.zip(normalizedRatingsB)
          .foldLeft(0.0) { (currentSum: Double, ratings: (Double, Double)) =>
            currentSum + ratings._1 * ratings._2 }

      similarity
    }

    def calculatePearson(gb: GroupBuilder): GroupBuilder = {
      gb.mapList[(Double, Double), (Double)](('ratingA, 'ratingB) -> 'similarity) {
        ratingPairListToPearson }
    }

    if (3 != fieldSpec._1.size()) {
      throw new Exception("Pipe to convert to matrix should have exactly three input fields.")
    }

    pipe
        .project(fieldSpec._1)
        .rename(fieldSpec._1 -> ('userId, 'itemId, 'rating))
        // Group by user to get all of the items that a given user has rated (store in a list) We
        // assume that we can fit the item-rating vector for a single user within memory.
        .groupBy('userId) { _.toList[(C, Double)](('itemId, 'rating) -> 'ratingList) }

        // Emit (itemA, itemB, ratingA, ratingB) tuples for all of the itemA, itemB that a given
        // user has rated.
        .flatMapTo('ratingList -> ('itemA, 'itemB, 'ratingA, 'ratingB)) { createRatingPairs }

        // Group by item pair and product a pair of vectors, each of which contains all of the
        // ratings for the item in question from users that are common with the other item, and
        // then compute the Pearson correlation.
        .groupBy('itemA, 'itemB) { calculatePearson }
        .project('itemA, 'itemB, 'similarity)
        .rename(('itemA, 'itemB, 'similarity) -> fieldSpec._2)
  }
}



