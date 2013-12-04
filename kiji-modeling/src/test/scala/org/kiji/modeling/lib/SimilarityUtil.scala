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

import scala.math.abs
import scala.math.sqrt
import scala.util.Random

/**
 * A vector of ratings for a given item by various users.
 *
 * @param ratings A map from user IDs to ratings for the given item.
 */
case class ItemVector(ratings: Map[Long, Double]) {
  def dotProductWith(that: ItemVector): Double = {
    ratings.foldLeft(0.0) { (currentSum: Double, keyAndVal: (Long, Double)) =>
      currentSum + keyAndVal._2 * that.ratings.getOrElse(keyAndVal._1, 0.0) } }

  /**
    * Compute the mean of this vector and return a copy of the vector centered aroudn the mean.
    *
    * @return The mean-centered version of this vector.
    */
  def meanCentered: ItemVector = {
    val avg: Double = ratings.values.sum / ratings.size
    ItemVector(ratings.mapValues { _ - avg })
  }

  /** Return number of elements in the vector. */
  def length: Int = ratings.size

  /** Return the magnitude of the vector (square root of the sum of squares of its values). */
  def magnitude: Double = sqrt(dotProductWith(this))
}

/** Declare a constant for filtering out small similarity values. */
object SimilarityChecker {
  val ratingEpsilon: Double = 0.00001
}

/**
 * Test various similarity measurements in modeling.
 */
abstract class SimilarityChecker {

  /**
   * Create a random user-item rating matrix, implemented as a Map from users to maps of items to
   * ratings.
   *
   * @param numUsers The number of users in the user-item matrix.
   * @param numItemsPerUser The number of items that every user rates in the user-item matrix.
   * @param numTotalItems The number of distinct items in the user-item matrix (every user will rate
   *     a random set of `numItemsPerUser` from the `numTotalItems` total possible items).
   * @return the user-item matrix, as a map of maps.
   */
  protected def createRandomRatings(
      numUsers: Int = 20,
      numItemsPerUser: Int = 10,
      numTotalItems: Int = 20): Map[Long, Map[Long, Double]] = {

    // Create a bunch of random ratings for one particular user
    def createRandomRatingListForSingleUser: Map[Long, Double] = {
      require(numTotalItems >= numItemsPerUser)
      // Get a uniqe set of items
      val itemIds: IndexedSeq[Int] =
          Random.shuffle((0 until numTotalItems).toSeq)
          .take(numItemsPerUser)
          .toSet
          .toIndexedSeq

      val ratings = for (itemId <- itemIds) yield {
        //val rating: Double = ((Random.nextDouble() * 4).toInt) / 4.0d
        val rating: Double = ((Random.nextDouble() * 4).toInt) + 1.0d
        (itemId.toLong + 100L, rating)
      }
      ratings.toMap
    }

    val userRatings = {
      for ( userId <- 0 until numUsers ) yield {
        (userId.toLong, createRandomRatingListForSingleUser)
      }
    }.toMap

    userRatings
  }

  /**
   * Create a Map from items to vectors of (user, rating) pairs.
   * @param userItemRatings The user-item rating matrix, represented as a Map of users to maps of
   *     items to ratings.
   * @return A Map of items to vectors of (user, rating) pairs.
   */
  protected def createItemVectors(userItemRatings: Map[Long, Map[Long, Double]]):
      Map[Long, ItemVector] = {

    // Need to get a map from items -> (map users -> ratings)
    val itemUserRatings = { for {
      user <- userItemRatings.keys
      (item, rating) <- userItemRatings(user)
    } yield (item, (user, rating))
    }.groupBy(_._1).mapValues { tupleList => tupleList.map { _._2}.toMap }

    val itemVectors: Map[Long, ItemVector] = {
      for ( itemId <- itemUserRatings.keys ) yield {
        val itemRatings: Map[Long, Double] = itemUserRatings(itemId)
        val itemVec: ItemVector = ItemVector(itemRatings)
        (itemId, itemVec)
      } }.toMap
    itemVectors
  }

  /**
   * Return a list of (user, item, rating) tuples.
   *
   * @return a list user (user, item, rating) tuples.
   */
  def getRatings: List[(Long, Long, Double)] = ratings
      .toList
      .flatMap { userAndItems =>
        val (user, itemRatings) = userAndItems
        itemRatings.map { itemAndRating => (user, itemAndRating._1, itemAndRating._2) }
      }

  val ratings = createRandomRatings()
  val itemIds: Set[Long] = ratings.flatMap { userAndItems => userAndItems._2.keys }.toSet
  val itemVectors = createItemVectors(ratings)

  /**
   * Compute similarities.
   *
   * @return The item-item similarities.
   */
  def computeSimilarities: Map[(Long, Long), Double] = {
    val sims = for {
      itemA: Long <-itemIds
      itemB: Long <- itemIds
      if (itemA != itemB)
      similarity <- computeVectorPairSimilarity(itemVectors(itemA), itemVectors(itemB))
    } yield ((itemA, itemB), similarity)
    sims.toMap
  }

  def computeVectorPairSimilarity(vecA: ItemVector, vecB: ItemVector): Option[Double]
}

/** Test cosine similarity similarity function. */
protected[modeling] class VerifyCosineSimilarity extends SimilarityChecker {
  def computeVectorPairSimilarity(vecA: ItemVector, vecB: ItemVector): Option[Double] = {
    val sim = vecA.dotProductWith(vecB) / (vecA.magnitude * vecB.magnitude)
    if (sim > SimilarityChecker.ratingEpsilon) Some(sim) else None
  }
}

/** Test cosine similarity similarity function. */
protected[modeling] class VerifyPearsonSimilarity extends SimilarityChecker {
  def computeVectorPairSimilarity(vecA: ItemVector, vecB: ItemVector): Option[Double] = {
    // Remove any of the non-common items
    val commonItems: Set[Long] = vecA.ratings.keys.toSet & vecB.ratings.keys.toSet

    val vecACen = ItemVector(vecA.ratings.filter( kv => commonItems.contains(kv._1))).meanCentered
    val vecBCen = ItemVector(vecB.ratings.filter( kv => commonItems.contains(kv._1))).meanCentered

    val sim = vecACen.dotProductWith(vecBCen) / (vecACen.magnitude * vecBCen.magnitude)
    if (sim > SimilarityChecker.ratingEpsilon) Some(sim) else None
  }
}

/** Test Jaccard similarity similarity function. */
protected[modeling] class VerifyJaccardSimilarity extends SimilarityChecker {
  def computeVectorPairSimilarity(vecA: ItemVector, vecB: ItemVector): Option[Double] = {
    val itemsA = vecA.ratings.keys.toSet
    val itemsB = vecB.ratings.keys.toSet

    val sim: Double = (itemsA & itemsB).size.toDouble / (itemsA | itemsB).size

    if (sim > SimilarityChecker.ratingEpsilon) Some(sim) else None
  }
}

/** Test adjusted-cosine similarity similarity function. */
protected[modeling] class VerifyAdjustedCosineSimilarity extends SimilarityChecker {
  /** Center ratings around the user's mean. */
  override def createItemVectors(userItemRatings: Map[Long, Map[Long, Double]]):
      Map[Long, ItemVector] = {

    val centeredUserItemRatings: Map[Long, Map[Long, Double]] = userItemRatings.mapValues {
      userVec: Map[Long, Double] =>
        val userMean: Double = userVec.values.sum / userVec.size
        userVec.mapValues( _ - userMean)
    }
    super.createItemVectors(centeredUserItemRatings)
  }

  def computeVectorPairSimilarity(vecA: ItemVector, vecB: ItemVector): Option[Double] = {
    val sim = vecA.dotProductWith(vecB) / (vecA.magnitude * vecB.magnitude)
    if (sim > SimilarityChecker.ratingEpsilon) Some(sim) else None
  }
}

