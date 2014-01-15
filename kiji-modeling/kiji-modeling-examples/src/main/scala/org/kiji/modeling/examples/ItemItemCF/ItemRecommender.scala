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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.express._
import org.kiji.express.flow._

import org.kiji.modeling.examples.ItemItemCF.avro._

/**
 * Recommend items based on a collection of items.
 *
 * @param args passed in from the command line.
 */
class ItemRecommender(args: Args) extends ItemItemJob(args) {
  val logger: Logger = LoggerFactory.getLogger(classOf[ItemRecommender])

  val items: Set[Long] = args("items")
      .split(",")
      .map(_.toLong)
      .toSet

  // Get the most similar items
  val mostSimilarPipe = createMostSimilarItemsPipe(Some(items))
      // Now we have the most similar items to all given items.
      // We don't need to know to which item any of these potential recommended items is
      // similar.
      .project('similarItem, 'similarity)
      .groupBy('similarItem) { _.sum('similarity) }

  // Attach the actual movie titles
  val formattedPipe = attachMovieTitles(mostSimilarPipe, 'similarItem)
      .groupAll { _.sortBy('similarity) }
      .map('similarity -> 'score_str) { score: Double => "%.4f".format(score) }
      .project('similarItem, 'score_str, 'title)

    formattedPipe.write(Csv("recommendation", fields=('similarItem, 'score_str, 'title)))
}
