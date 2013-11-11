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

import java.io.File

import com.google.common.io.Files
import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.TextLine
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.Cell
import org.kiji.express.KijiSuite
import org.kiji.express.flow.KijiInput
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.modeling.framework.KijiModelingJob
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class RecommendationPipeSuite extends KijiSuite {
  val testLayoutDesc: TableLayoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE)
  testLayoutDesc.setName("OrdersTable")

  // The data consists of the following 3 orders stored in a Kiji table:
  // {milk, bread, coke}
  // {milk, bread, butter, flour}
  // {butter, flour}
  val kiji: Kiji = new InstanceBuilder("default")
      .withTable(testLayoutDesc)
      .withRow("row1")
      .withFamily("family")
      .withQualifier("column").withValue("milk, bread, coke")
      .withRow("row2")
      .withFamily("family")
      .withQualifier("column").withValue("milk, bread, butter, flour")
      .withRow("row3")
      .withFamily("family")
      .withQualifier("column").withValue("butter, flour")
      .build()

  val inputUri: KijiURI = doAndRelease(kiji.openTable("OrdersTable")) {
    table: KijiTable => table.getURI
  }
  // Hack to set the mode correctly. Scalding sets the mode in JobTest
  // which makes the tests below run in HadoopTest mode instead of Hadoop mode whenever they are
  // run after another test that uses JobTest.
  // Remove this after the bug in Scalding is fixed.
  com.twitter.scalding.Mode.mode = Hdfs(false, HBaseConfiguration.create())

  /**
   * This test goes over the orders in the table and creates itemsets of size 2 from them.
   * This is the first step in finding which products co-occur frequently.
   */
  test("Prepare Itemsets Job runs properly") {
    class PrepareItemsetsJob(args: Args) extends KijiModelingJob(args) {
      KijiInput(args("input"), "family:column" -> 'slice)
        .map('slice -> 'order) {
          slice: Seq[Cell[CharSequence]] =>
              slice.head.datum.toString.split(",").map(_.trim).toList
        }
        .prepareItemSets[String]('order -> 'itemset, 2, 2)
        .write(TextLine(args("output")))
    }
    val outputDir: File = Files.createTempDir()
    new PrepareItemsetsJob(Args("--input " + inputUri.toString + " --output " +
        outputDir.getAbsolutePath)).run
    val lines = doAndClose(scala.io.Source.fromFile(outputDir.getAbsolutePath + "/part-00000")) {
      source: scala.io.Source => source.mkString
    }
    val expectedOutput = Array(
        "butter,flour",
        "bread,butter",
        "bread,flour",
        "bread,milk",
        "butter,flour",
        "butter,milk",
        "flour,milk",
        "bread,coke",
        "bread,milk",
        "coke,milk"
    )
    assert(expectedOutput.sameElements(lines.split("\n")))
    FileUtils.deleteDirectory(outputDir)
  }

  /**
   * Assumes every row in the table above represents an order. This test goes over the orders,
   * groups them by product. It then joins the orders with the size the groups of constituent
   * products. Thus an order like {bread, flour} will result in two tuples:
   * {bread, flour}, bread, #of orders with bread; and
   * {bread, flour}, flour, #of orders containing flour.
   */
  test("Joining with Group Count works properly") {
    class JoinWithGroupCountJob(args: Args) extends KijiModelingJob(args) {
      KijiInput(args("input"), "family:column" -> 'slice)
        .flatMap('slice -> 'product) {
          slice: Seq[Cell[CharSequence]] => slice.head.datum.toString.split(",").map(_.trim)
        }
        .joinWithGroupCount('product -> 'count)
        .write(TextLine(args("output")))
    }
    val outputDir: File = Files.createTempDir()
    new JoinWithGroupCountJob(Args("--input " + inputUri.toString + " --output " +
        outputDir.getAbsolutePath)).run
    val lines = doAndClose(scala.io.Source.fromFile(outputDir.getAbsolutePath + "/part-00000")) {
      source: scala.io.Source => source.mkString
    }
    assert(lines.split("\n").size == 9)
    val linesOfInterest = lines.split("\n").filter(_.contains("milk, bread, coke"))
    assert(linesOfInterest.size == 3)
    assert(linesOfInterest.count("""coke\s+1""".r.findAllIn(_).nonEmpty) == 1)
    FileUtils.deleteDirectory(outputDir)
  }

  test("Joining with Count works properly") {
    class JoinWithCountJob(args: Args) extends KijiModelingJob(args) {
      KijiInput(args("input"), "family:column" -> 'slice)
          .joinWithCount('grpCount)
          .write(TextLine(args("output")))
    }
    val outputDir: File = Files.createTempDir()
    new JoinWithCountJob(Args("--input " + inputUri.toString + " --output " +
        outputDir.getAbsolutePath)).run
    val lines = doAndClose(scala.io.Source.fromFile(outputDir.getAbsolutePath + "/part-00000")) {
      source: scala.io.Source => source.mkString
    }
    assert(lines.split("\n").map(_.contains("3")).size == 3)
    FileUtils.deleteDirectory(outputDir)
  }

  /**
   * This tests runs a frequent itemset miner with the total number of orders provided. Assumes
   * every row in the table above represents an order.
   */
  test("Filter by Support works with Constant Normalizer") {
    class FrequentItemsetFinderJob(args: Args) extends KijiModelingJob(args) {
      KijiInput(args("input"), "family:column" -> 'slice)
          .map('slice -> 'order) {
            slice: Seq[Cell[CharSequence]] =>
                slice.head.datum.toString.split(",").map(_.trim).toList
          }
          .prepareItemSets[String]('order -> 'itemset, 2, 2)
          .filterBySupport('itemset -> 'support, None, Some(3.0), 'norm, 0.5)
          .write(TextLine(args("output")))
    }
    val outputDir: File = Files.createTempDir()
    new FrequentItemsetFinderJob(Args("--input " + inputUri.toString + " --output " +
        outputDir.getAbsolutePath)).run
    val lines = doAndClose(scala.io.Source.fromFile(outputDir.getAbsolutePath + "/part-00000")) {
        source: scala.io.Source => source.mkString
    }
    val expected = Array("bread,milk\t2\t3.0\t0.6666666666666666",
        "butter,flour\t2\t3.0\t0.6666666666666666")
    assert(lines.split("\n").sameElements(expected))
    FileUtils.deleteDirectory(outputDir)
  }

  /**
   * This test runs an actual frequent itemset miner. Assumes every row in the table above
   * represents an order.
   */
  test("Filter by Support works with Normalizing Pipe") {
    class FrequentItemsetFinderJob(args: Args) extends KijiModelingJob(args) {
      val totalOrders = KijiInput(args("input"), "family:column" -> 'slice)
          .groupAll { _.size('norm)}

      KijiInput(args("input"), "family:column" -> 'slice)
          .map('slice -> 'order) {
            slice: Seq[Cell[CharSequence]] =>
                slice.head.datum.toString.split(",").map(_.trim).toList
          }
        .prepareItemSets[String]('order -> 'itemset, 2, 2)
        .filterBySupport('itemset -> 'support, Some(totalOrders), None, 'norm, 0.5)
        .write(TextLine(args("output")))
    }
    val outputDir: File = Files.createTempDir()
    new FrequentItemsetFinderJob(Args("--input " + inputUri.toString + " --output " +
        outputDir.getAbsolutePath)).run
    val lines = doAndClose(scala.io.Source.fromFile(outputDir.getAbsolutePath + "/part-00000")) {
      source: scala.io.Source => source.mkString
    }
    val expected = Array("bread,milk\t2\t3\t0.6666666666666666",
        "butter,flour\t2\t3\t0.6666666666666666")
    assert(lines.split("\n").sameElements(expected))
    FileUtils.deleteDirectory(outputDir)
  }

  // Assumes every row in the Kiji table above represents a user.
  test("Jaccard distance works correctly") {
    class JaccardDistanceCalculator(args: Args) extends KijiModelingJob(args) {
      KijiInput(args("input"), "family:column" -> 'slice)
        .map('slice -> 'userPurchases) {
          slice: Seq[Cell[CharSequence]] =>
              slice.head.datum.toString.split(",").map(_.trim).toList
        }
        .simpleItemItemJaccardSimilarity[String]('userPurchases -> 'result)
        .write(TextLine(args("output")))
    }
    val outputDir: File = Files.createTempDir()
    new JaccardDistanceCalculator(Args("--input " + inputUri.toString + " --output " +
        outputDir.getAbsolutePath)).run
    val lines = doAndClose(scala.io.Source.fromFile(outputDir.getAbsolutePath + "/part-00000")) {
      source: scala.io.Source => source.mkString
    }
    val expected = Array("bread\tbutter\t1\t2\t2\t3\t0.3333333333333333",
        "bread\tcoke\t1\t2\t1\t2\t0.5",
        "bread\tflour\t1\t2\t2\t3\t0.3333333333333333",
        "butter\tflour\t2\t2\t2\t2\t1.0",
        "bread\tmilk\t2\t2\t2\t2\t1.0",
        "butter\tmilk\t1\t2\t2\t3\t0.3333333333333333",
        "coke\tmilk\t1\t1\t2\t2\t0.5",
        "flour\tmilk\t1\t2\t2\t3\t0.3333333333333333")
    assert(lines.split("\n").sameElements(expected))
    FileUtils.deleteDirectory(outputDir)
  }
}
