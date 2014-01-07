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
import scala.collection.mutable
import scala.math.abs

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.google.common.io.Files
import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.express.KijiSuite
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.util.ResourceUtil.doAndClose
import org.kiji.express.flow.util.ResourceUtil.doAndRelease
import org.kiji.modeling.framework.KijiModelingJob
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

@RunWith(classOf[JUnitRunner])
class RecommendationPipeSuite extends KijiSuite {
  private val logger: Logger = LoggerFactory.getLogger(classOf[RecommendationPipeSuite])
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
      KijiInput.builder
          .withTableURI(args("input"))
          .withColumns("family:column" -> 'slice)
          .build
          .map('slice -> 'order) {
            slice: Seq[FlowCell[CharSequence]] =>
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
      KijiInput.builder
          .withTableURI(args("input"))
          .withColumns("family:column" -> 'slice)
          .build
          .flatMap('slice -> 'product) {
            slice: Seq[FlowCell[CharSequence]] => slice.head.datum.toString.split(",").map(_.trim)
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
      KijiInput.builder
          .withTableURI(args("input"))
          .withColumns("family:column" -> 'slice)
          .build
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
      KijiInput.builder
          .withTableURI(args("input"))
          .withColumns("family:column" -> 'slice)
          .build
          .map('slice -> 'order) {
            slice: Seq[FlowCell[CharSequence]] =>
                slice.head.datum.toString.split(",").map(_.trim).toList
          }
          .prepareItemSets[String]('order -> 'itemset, 2, 2)
          .support('itemset -> ('frequency, 'support), None, Some(3.0), 'norm)
          .filter('support) { support: Double => (support >= 0.5) }
          .debug
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
        .map(_.split("\t").toSet)
    assert(lines.split("\n").map{ x: String => x.split("\t").toSet}.sameElements(expected))
    FileUtils.deleteDirectory(outputDir)
  }

  /**
   * This test runs an actual frequent itemset miner. Assumes every row in the table above
   * represents an order.
   */
  test("Filter by Support works with Normalizing Pipe") {
    class FrequentItemsetFinderJob(args: Args) extends KijiModelingJob(args) {
      val totalOrders = KijiInput.builder
          .withTableURI(args("input"))
          .withColumns("family:column" -> 'slice)
          .build
          .groupAll { _.size('norm)}

      KijiInput.builder
          .withTableURI(args("input"))
          .withColumns("family:column" -> 'slice)
          .build
          .map('slice -> 'order) {
            slice: Seq[FlowCell[CharSequence]] =>
                slice.head.datum.toString.split(",").map(_.trim).toList
          }
        .prepareItemSets[String]('order -> 'itemset, 2, 2)
        .support('itemset -> ('frequency, 'support), Some(totalOrders), None, 'norm)
        .filter('support) { support: Double => (support >= 0.5) }
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
        .map(_.split("\t").toSet)
    assert(lines.split("\n").map{ x: String => x.split("\t").toSet}.sameElements(expected))
    FileUtils.deleteDirectory(outputDir)
  }

  /**
   * This tests runs a frequent itemset miner with the total number of orders provided.
   * Assumes every row in the table above represents an order.
   * Computes lift and confidence.
   */
  test("Compute lift and confidence") {
    class FrequentItemsetFinderJob(args: Args) extends KijiModelingJob(args) {
      KijiInput.builder
          .withTableURI(args("input"))
          .withColumns("family:column" -> 'slice)
          .build
          .map('slice -> 'order) {
            slice: Seq[FlowCell[CharSequence]] =>
                slice.head.datum.toString.split(",").map(_.trim).toList
          }
          .prepareItemSets[String]('order -> 'itemset, 1, 3)
          .support('itemset -> ('frequency, 'support), None, Some(3.0), 'norm)
          .confidenceAndLift(lhsMinSize = 1, lhsMaxSize = 5, rhsMinSize = 1, rhsMaxSize = 5)
          .write(TextLine(args("output")))
    }
    val outputDir: File = Files.createTempDir()
    new FrequentItemsetFinderJob(Args("--input " + inputUri.toString + " --output " +
        outputDir.getAbsolutePath)).run
    val lines = doAndClose(scala.io.Source.fromFile(outputDir.getAbsolutePath + "/part-00000")) {
        source: scala.io.Source => source.mkString
    }
    val expected = Set("flour\tbread\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "milk\tbread\t1.0\t1.5\t0.6666666666666666\t2\t3.0",
        "butter\tbread\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "coke\tbread\t1.0\t1.5\t0.3333333333333333\t1\t3.0",
        "flour\tbutter\t1.0\t1.5\t0.6666666666666666\t2\t3.0",
        "bread\tbutter\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "milk\tbutter\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "bread\tcoke\t0.5\t1.5\t0.3333333333333333\t1\t3.0",
        "milk\tcoke\t0.5\t1.5\t0.3333333333333333\t1\t3.0",
        "bread\tflour\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "butter\tflour\t1.0\t1.5\t0.6666666666666666\t2\t3.0",
        "milk\tflour\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "butter\tmilk\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "bread\tmilk\t1.0\t1.5\t0.6666666666666666\t2\t3.0",
        "coke\tmilk\t1.0\t1.5\t0.3333333333333333\t1\t3.0",
        "flour\tmilk\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "bread,flour\tmilk\t1.0\t1.5\t0.3333333333333333\t1\t3.0",
        "bread,coke\tmilk\t1.0\t1.5\t0.3333333333333333\t1\t3.0",
        "bread,butter\tmilk\t1.0\t1.5\t0.3333333333333333\t1\t3.0",
        "butter\tbread,flour\t0.5\t1.5\t0.3333333333333333\t1\t3.0",
        "milk\tbread,flour\t0.5\t1.5\t0.3333333333333333\t1\t3.0",
        "butter\tbread,milk\t0.5\t0.75\t0.3333333333333333\t1\t3.0",
        "coke\tbread,milk\t1.0\t1.5\t0.3333333333333333\t1\t3.0",
        "flour\tbread,milk\t0.5\t0.75\t0.3333333333333333\t1\t3.0")
        .map { _.split("\t").toSet }
    assert(expected.subsetOf(lines.split("\n").toSet.map { x:String => x.split("\t").toSet }))
    FileUtils.deleteDirectory(outputDir)
  }

  /**
   * Utility function to test difference item-item similarity metrics.
   *
   * @param userRatings List of user ratings (essentially the user-item rating matrix).
   * @param expectedSimilarities List of correct item-item similarity pairs.
   * @param similarityCalculator A function that takes a `RecommendationPipe` and returns a
   *     function from (`Fields`, `Fields`) to a `Pipe`.  Typically this function just calls the
   *     item-item similarity method for a pipe, e.g., it is of the form:
   *     `val fn = (pipe: RecommendationPipe) => pipe.itemItemCosineSimilarity[Long, Long] _`
   */
  private def testItemItemSimilarity(
    userRatings: List[(Long, Long, Double)],
    expectedSimilarities: Map[(Long, Long), Double],
    similarityCalculator: RecommendationPipe => (((Fields, Fields)) => Pipe)
  ): Unit = {

    /** Compares two floating point similarities. */
    def floatEq(a: Double, b: Double, epsilon: Double = 0.00001): Boolean = {
      abs(a-b) < epsilon
    }

    /**
     * Assuming that we have gotten rid of all of the negative similarities, we should see the
     * following:
     * - 10,11 have similarity of 1.0
     */
    def validateSimilarity(outputBuffer: mutable.Buffer[(Long, Long, Double)]): Unit = {
      val pairs2scores: Map[(Long,Long), Double] = outputBuffer.map { x: (Long, Long, Double) =>
        val (itemA, itemB, similarity) = x
        ((itemA, itemB), similarity)
      }
      .filter( pairAndSim => pairAndSim._2 > SimilarityChecker.ratingEpsilon)
      .toMap

      logger.debug("Got: " + pairs2scores
          .toList
          .sortWith((x, y) => if (x._1._1 != y._1._1) x._1._1 < y._1._1 else x._1._2 < y._1._2))
      logger.debug("Expected: " + expectedSimilarities
          .toList
          .sortWith((x, y) => if (x._1._1 != y._1._1) x._1._1 < y._1._1 else x._1._2 < y._1._2))

      // Print out any differences
      val foundPairs: Set[(Long, Long)] = pairs2scores.keys.toSet
      val expectedPairs: Set[(Long, Long)] = expectedSimilarities.keys.toSet

      logger.debug("Found but not expected: " + (foundPairs &~ expectedPairs))
      logger.debug("Found but not expected: " + (expectedPairs &~ foundPairs))

      assert(pairs2scores.size === expectedSimilarities.size)
      pairs2scores.keys.foreach{ key: (Long, Long) =>
        val kijiScore = pairs2scores(key)
        val expectedScore = expectedSimilarities(key)
        if (!floatEq(kijiScore, expectedScore)) {
          logger.debug(key.toString)
        }
        assert(floatEq(pairs2scores(key), expectedSimilarities(key)))
      }
    }

    /** Split up an input text line into `'userId`, `'itemId`, and `'rating`. */
    def tokenize(line: String): (Long, Long, Double) = {
      val toks: Array[String] = line.split(",")
      assert(toks.size === 3, "What is with this line? " + line)
      (toks(0).toLong, toks(1).toLong, toks(2).toDouble)
    }

    // Simple job to run a pipeline with user ratings to calculate similarities
    class SimilarityCalculator(args: Args) extends KijiModelingJob(args) {
        val pipe = TextLine(args("input"))
            .read
            // Put "x" on the end of these field names to test the ability of the item-item
            // similarity code to handle different field names.
            .map('line -> ('userIdx, 'itemIdx, 'ratingx)) { x: String => tokenize(x) }

        // This is fairly hideous...
        val simCalcMethod: ((Fields, Fields)) => Pipe = similarityCalculator(pipe)
        val simPipe = simCalcMethod(
            ('userIdx, 'itemIdx, 'ratingx),
            ('itemAx, 'itemBx, 'similarityx))
        simPipe.write(TextLine(args("output")))
    }

    val jobTest = JobTest(new SimilarityCalculator(_))
        .arg("input", "inputFile")
        .arg("output", "outputFile")
        .source(TextLine("inputFile"), userRatings.map { ratingTuple: (Long, Long, Double) =>
          val (userId, itemId, rating) = ratingTuple
          "%s,%s,%s".format(userId,itemId,rating)
        }.zipWithIndex.map { x: (String, Int) =>
          val (line, index) = x
          ((index+1).toString, line)
        })
        .sink(TextLine("outputFile"))(validateSimilarity)

    // Run in local mode.
    jobTest.run.finish
  }

  test("Cosine similarity works correctly.") {
    val fn = (pipe: RecommendationPipe) => pipe.cosineSimilarity[Long, Long] _
    val checker = new VerifyCosineSimilarity()
    testItemItemSimilarity(checker.getRatings, checker.computeSimilarities, fn)
  }

  test("Adjusted-cosine similarity works correctly.") {
    val fn = (pipe: RecommendationPipe) => pipe.adjustedCosineSimilarity[Long, Long] _
    val checker = new VerifyAdjustedCosineSimilarity()
    testItemItemSimilarity(checker.getRatings, checker.computeSimilarities, fn)
  }

  test("Pearson similarity works correctly.") {
    val fn = (pipe: RecommendationPipe) => pipe.pearsonSimilarity[Long, Long] _
    val checker = new VerifyPearsonSimilarity()
    testItemItemSimilarity(checker.getRatings, checker.computeSimilarities, fn)
  }

  test("Jaccard similarity works correctly.") {
    val fn = (pipe: RecommendationPipe) => pipe.jaccardSimilarity[Long, Long] _
    val checker = new VerifyJaccardSimilarity()
    testItemItemSimilarity(checker.getRatings, checker.computeSimilarities, fn)
  }

  // Assumes every row in the Kiji table above represents a user.
  // More exciting Jaccard test.
  test("More abstract Jaccard distance test works.") {

    class JaccardDistanceCalculator(args: Args) extends KijiModelingJob(args) {
      TextLine(args("input"))
        .read
        .flatMap('line -> 'item) { line: String => line.split(",") }
        .map('offset -> 'entityId) { offset: Int => EntityId(offset) }
        .jaccardSimilarity[EntityId, String](
            ('entityId, 'item) -> ('itemA, 'itemB, 'similarity))
        .mapTo(('itemA, 'itemB, 'similarity) -> 'result) { fields: (String, String, Double) =>
          val (itemA, itemB, sim) = fields
          "%s,%s,%g".format(itemA, itemB, sim)
        }
        .write(TextLine(args("output")))
    }

    val input: Seq[(Int, String)] = Seq(
        "milk,bread,coke",
        "milk,bread,butter,flour",
        "butter,flour")
        .zipWithIndex
        .map{ x: (String, Int) =>
          val (msg, linenumber) = x
          (linenumber + 1, msg)
        }

    val expected = Set(
        "bread,butter,0.333333",
        "bread,coke,0.500000",
        "bread,flour,0.333333",
        "bread,milk,1.00000",
        "butter,bread,0.333333",
        "butter,flour,1.00000",
        "butter,milk,0.333333",
        "coke,bread,0.500000",
        "coke,milk,0.500000",
        "flour,bread,0.333333",
        "flour,butter,1.00000",
        "flour,milk,0.333333",
        "milk,bread,1.00000",
        "milk,butter,0.333333",
        "milk,coke,0.500000",
        "milk,flour,0.333333")

    def validateOutput(output: mutable.Buffer[String]): Unit = {
      val actual: Set[String] = output.toSet
      assert(actual === expected)
    }

    val jobTest = JobTest(new JaccardDistanceCalculator(_))
        .arg("input", "inputFile")
        .arg("output", "outputFile")
        .source(TextLine("inputFile"), input)
        .sink(TextLine("outputFile")) {validateOutput}

    jobTest.run.finish
  }

  test("Counting tuples on pipe in parallel works correctly") {
    // Create input with offset = some integer and line = that integer.toString
    val input = (1 to 20)
        .zipWithIndex
        .map { x: (Int, Int) => val (data, line) = x; (line+1, data.toString) }

    class TupleCounter(args: Args) extends KijiModelingJob(args) {
      TextLine(args("input"))
          .read
          .count('total)
          .write(TextLine(args("output")))
    }

    val expected: Set[Float] = Set(20.0f)

    def validateOutput(output: mutable.Buffer[String]): Unit = {
      val actual: Set[Float] = output.toSet.map { x: String => x.toFloat }
      assert(actual === expected)
    }

    val jobTest = JobTest(new TupleCounter(_))
        .arg("input", "inputFile")
        .arg("output", "outputFile")
        .source(TextLine("inputFile"), input)
        .sink(TextLine("outputFile")) { validateOutput }

    jobTest.run.finish
  }
}
