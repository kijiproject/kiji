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

import scala.math.abs

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
import org.kiji.express.KijiSuite
import org.kiji.express.flow._
import org.kiji.express.flow.util.ResourceUtil.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.util.InstanceBuilder

import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

import org.kiji.schema.shell.api.Client
import org.kiji.modeling.examples.ItemItemCF.avro._

// Hacks for convenient comparison of doubles:
case class Precision(val p:Double)

class WithAlmostEquals(d:Double) {
    def ~=(d2:Double)(implicit p:Precision) = (d-d2).abs <= p.p
}

/**
 * Abstract class containing common code for all of the test suites.
 */
abstract class ItemItemSuite extends KijiSuite {
  /**
   * Read any text file on the class path.  Used here for reading a DDL file to create a test
   * `KijiTable` from a DDL description.
   *
   * @param resourcePath The path to the text file.
   * @return The contents of the file.
   */
  def readResource(resourcePath: String): String = {
    try {
      val istream: InputStream =
        classOf[ItemItemSuite].getClassLoader().getResourceAsStream(resourcePath)
      val content: String = IOUtils.toString(istream)
      istream.close
      content
    } catch {
      case e: Exception => "Problem reading resource \"" + resourcePath + "\""
    }
  }

  /**
   * Create a test KijiTable by executing a DDL file in a schema-shell.
   *
   * @param ddlName The path of the DDL file to execute.
   * @param tableName The name of the table that the DDL file will create (this needs to agree with
   * the DDL file).
   * @param instanceName Optional `KijiInstance` name.
   * @return A `KijiTable` ready for use in unit tests!
   */
  def makeTestKijiTableFromDDL(
    ddlName: String,
    tableName: String,
    instanceName: String = "default_%s".format(counter.incrementAndGet())
  ): KijiTable = {

    // Create the table
    val ddl: String = readResource(ddlName)

    // Create the instance
    val kiji: Kiji = new InstanceBuilder(instanceName).build()
    val kijiUri: KijiURI = kiji.getURI()

    val client: Client = Client.newInstance(kijiUri)
    client.executeUpdate(ddl)

    // Get a pointer to the instance
    val table: KijiTable = kiji.openTable(tableName)
    kiji.release()
    return table
  }

  /** More trickery for comparing doubles. */
  implicit def add_~=(d:Double) = new WithAlmostEquals(d)

  /** This is good enough double-comparison accuracy! */
  implicit val precision = Precision(0.001)

  // Create test versions of the tables for user ratings and for similarities
  val userRatingsUri: String = doAndRelease(
      makeTestKijiTableFromDDL(
        ddlName = "user_ratings.ddl",
        tableName = "user_ratings"))
      { table: KijiTable => table.getURI().toString() }

  val itemItemSimilaritiesUri: String = doAndRelease(
      makeTestKijiTableFromDDL(
        ddlName = "item_item_similarities.ddl",
        tableName = "item_item_similarities"))
      { table: KijiTable => table.getURI().toString() }

  val itemItemSimilaritiesUriHadoop: String = doAndRelease(
      makeTestKijiTableFromDDL(
        ddlName = "item_item_similarities.ddl",
        tableName = "item_item_similarities"))
      { table: KijiTable => table.getURI().toString() }

  val titlesUri: String = doAndRelease(
      makeTestKijiTableFromDDL(
        ddlName = "titles.ddl",
        tableName = "movie_titles"))
      { table: KijiTable => table.getURI().toString() }

  // Example in which user 100 and user 101 have both given item 0 5 stars.
  // Each has also reviewed another item and given that item a different rating, (this ensures
  // that the mean-adjusted rating is not a zero).
  val version: Long = 0L
  val userRatingsSlices: List[(EntityId, Seq[FlowCell[Double]])] = List(
    (EntityId(100L), List(
        //                     item           score
        FlowCell[Double]("ratings", "10", version, 5.0),
        FlowCell[Double]("ratings", "11", version, 5.0),
        FlowCell[Double]("ratings", "20", version, 0.0))),
    (EntityId(101L), List(
        FlowCell[Double]("ratings", "10", version, 5.0),
        FlowCell[Double]("ratings", "11", version, 5.0),
        FlowCell[Double]("ratings", "21", version, 0.0))))

  val titlesSlices: List[(EntityId, Seq[FlowCell[String]])] = List(
    (EntityId(10L), List(FlowCell("info", "title", version, "Movie 10"))),
    (EntityId(11L), List(FlowCell("info", "title", version, "Movie 11"))),
    (EntityId(20L), List(FlowCell("info", "title", version, "Movie 20"))),
    (EntityId(21L), List(FlowCell("info", "title", version, "Movie 21")))
  )

  val kijiInputUserRatings = KijiInput.builder
      .withTableURI(userRatingsUri)
      .withColumnSpecs(ColumnFamilyInputSpec.builder
          .withFamily("ratings").build -> 'ratingInfo)
      .build

  val kijiOutputItemSimilarities = KijiOutput.builder
      .withTableURI(itemItemSimilaritiesUri)
      .withColumnSpecs(Map('mostSimilar -> QualifiedColumnOutputSpec.builder
          .withColumn("most_similar", "most_similar")
          .withSchemaSpec(SchemaSpec.Specific(classOf[AvroSortedSimilarItems]))
          .build))
      .build

 val kijiOutputItemSimilaritiesHadoop = KijiOutput.builder
      .withTableURI(itemItemSimilaritiesUriHadoop)
      .withColumnSpecs(Map('mostSimilar -> QualifiedColumnOutputSpec.builder
          .withColumn("most_similar", "most_similar")
          .withSchemaSpec(SchemaSpec.Specific(classOf[AvroSortedSimilarItems]))
          .build))
      .build

  val kijiInputTitles = KijiInput.builder
      .withTableURI(titlesUri)
      .withColumns("info:title" -> 'title)
      .build
}
