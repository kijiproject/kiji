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
import com.twitter.scalding.Csv
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

@RunWith(classOf[JUnitRunner])
/**
 * Make sure that the scorer runs.
 */
class ItemScorerSuite extends ItemItemSuite {
  val logger: Logger = LoggerFactory.getLogger(classOf[ItemSimilarityCalculatorSuite])

  val myUser: Long = 100L
  val myItem: Long = 10L

  // Example in which user 100 and user 101 have both given item 0 5 stars.
  // Each has also reviewed another item and given that item a different rating, (this ensures
  // that the mean-adjusted rating is not a zero).
  val avroSortedSimilarItems = new AvroSortedSimilarItems(
      List(new AvroItemSimilarity(11, 0.5)).asJava
  )

  val itemSimSlices: List[(EntityId, Seq[FlowCell[AvroSortedSimilarItems]])] = List(
    (EntityId(myItem), List(
        FlowCell[AvroSortedSimilarItems](
            "most_similar",
            "most_similar",
            version,
            avroSortedSimilarItems)
          )))

  def validateOutput(output: mutable.Buffer[(Long, Long, String, String)]): Unit = {
    println(output)
  }

  test("Test that ItemScorer does not crash outright!") {
    val jobTest = JobTest(new ItemScorer(_))
        .arg("similarity-table-uri", itemItemSimilaritiesUri)
        .arg("ratings-table-uri", userRatingsUri)
        .arg("users-and-items", myUser + ":" + myItem)
        .arg("titles-table-uri", titlesUri)
        .arg("k", "30")
        // This annoying argument is necessary because the test code won't work with the
        // application-mode Csv output, because the application-mode Csv output specifies a field
        // ordering.
        .arg("output-mode", "test")
        .source(KijiInput.builder
            .withTableURI(itemItemSimilaritiesUri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
                .withColumn("most_similar", "most_similar")
                .withSchemaSpec(SchemaSpec.Specific(classOf[AvroSortedSimilarItems]))
                .build -> 'most_similar)
            .build, itemSimSlices)
        .source(kijiInputUserRatings, userRatingsSlices)
        .source(kijiInputTitles, titlesSlices)
        .sink(Csv("score")) {validateOutput}


    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    //jobTest.runHadoop.finish
  }
}

