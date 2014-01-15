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

import scala.collection.mutable
import scala.math.abs
import scala.collection.JavaConverters._

import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.specific.SpecificRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.commons.io.IOUtils
import java.io.InputStream

import org.kiji.express._
import org.kiji.express.flow._
import org.kiji.express.flow.util.ResourceUtil.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.util.InstanceBuilder

import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout

import org.kiji.schema.shell.api.Client
import org.kiji.modeling.examples.ItemItemCF.avro._

/**
 * Checks that the item-similarity calculation works correctly by running a simple, contrived
 * example.
 */
@RunWith(classOf[JUnitRunner])
class ItemSimilarityCalculatorSuite extends ItemItemSuite {

  val logger: Logger = LoggerFactory.getLogger(classOf[ItemSimilarityCalculatorSuite])


  /* Description of the test case:

    - User 100 average rating is 3.3
    - User 101 average rating is 3.3

    - Adjusted ratings for (user,item) are:

      - 100,10 -> 1.7
      - 100,11 -> 1.7
      - 100,20 -> -3.3
      - 101,10 -> 1.7
      - 101,11 -> 1.7
      - 101,21 -> -3.3

  - Dot product between items 10 and 11:

                  1.7 * 1.7 + 1.7 * 1.7
                  ---------------------
    sqrt(1.7*1.7 + 1.7*1.7) * sqrt(1.7*1 + 1.7*1.7)

    = 1.0

  - The only other valid similarities are between 10/11 and 20/21.  All of them have the same
    score:

    1.7 * -3.3 / ( sqrt(1.7*1.7 + 1.7*1.7) + sqrt(-3.3*-3.3) ) = -0.707

  - All of the negative ratings should be filtered out.

   */

  /**
   * Checks the results of the item-item similarity calculator.
   * @param output The output from the item-item similarity calculator (would normally go into a
   * `KijiTable`.
   */
  def validateOutput(
      output: mutable.Buffer[(EntityId, List[FlowCell[AvroSortedSimilarItems]])]): Unit = {
    assert(output.size === 2)
    val pairs2scores = output
        .map { x: (EntityId, List[FlowCell[AvroSortedSimilarItems]]) => {
          val (eid, simItems) = x
          assert(simItems.size === 1)
          val itemA: Long = eid.components(0).asInstanceOf[Long]
          val topItems = simItems.head.datum.getTopItems.asScala
          assert(topItems.size === 1)
          val sim: AvroItemSimilarity = topItems.head
          val itemB: Long = sim.getItemId
          val similarity: Double = sim.getSimilarity
          ((itemA, itemB), similarity)
        }}
        .toMap
    assert(pairs2scores.contains((10,11)))
    assert(pairs2scores((10,11)) ~= 1.0)

    assert(pairs2scores.contains((11,10)))
    assert(pairs2scores((11,10)) ~= 1.0)
  }

  test("Similarity of two items with same users and same ratings is 1 - local") {
    val jobTest = JobTest(new ItemSimilarityCalculator(_))
        .arg("ratings-table-uri", userRatingsUri)
        .arg("similarity-table-uri", itemItemSimilaritiesUri)
        .arg("model-size", "10")
        .source(kijiInputUserRatings, userRatingsSlices)
        .sink(kijiOutputItemSimilarities) {validateOutput}

    // Run in local mode.
    jobTest.run.finish
  }

   test("Similarity of two items with same users and same ratings is 1 - hadoop") {
    val jobTest = JobTest(new ItemSimilarityCalculator(_))
        .arg("ratings-table-uri", userRatingsUri)
        .arg("similarity-table-uri", itemItemSimilaritiesUriHadoop)
        .arg("model-size", "10")
        .source(kijiInputUserRatings, userRatingsSlices)
        .sink(kijiOutputItemSimilaritiesHadoop) {validateOutput}

    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }
}

