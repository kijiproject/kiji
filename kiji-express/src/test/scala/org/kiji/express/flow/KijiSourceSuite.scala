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

package org.kiji.express.flow

import scala.collection.mutable.Buffer

import java.util.UUID

import com.twitter.scalding._

import org.kiji.express.AvroRecord
import org.kiji.express.AvroEnum
import org.kiji.express.AvroValue
import org.kiji.express.Cell
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

class KijiSourceSuite
    extends KijiSuite {
  import KijiSourceSuite._

  /** Simple table layout to use for tests. */
  val simpleLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Table layout using Avro schemas to use for tests. */
  val avroLayout: KijiTableLayout = layout("avro-types.json")

  /** Input tuples to use for word count tests. */
  def wordCountInput(uri: String): List[(EntityId, KijiSlice[String])] = {
    List(
        ( EntityId(uri)("row01"), slice("family:column1", (1L, "hello")) ),
        ( EntityId(uri)("row02"), slice("family:column1", (2L, "hello")) ),
        ( EntityId(uri)("row03"), slice("family:column1", (1L, "world")) ),
        ( EntityId(uri)("row04"), slice("family:column1", (3L, "hello")) ))
  }

  /**
   * Validates output from [[com.twitter.scalding.examples.WordCountJob]].
   *
   * @param outputBuffer containing data that output buffer has in it after the job has been run.
   */
  def validateWordCount(outputBuffer: Buffer[(String, Int)]) {
    val outMap = outputBuffer.toMap

    // Validate that the output is as expected.
    assert(3 === outMap("hello"))
    assert(1 === outMap("world"))
  }

  test("a word-count job that reads from a Kiji table is run using Scalding's local mode") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new WordCountJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)("family:column1" -> 'word), wordCountInput(uri))
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .run
        .finish
  }

  test("a word-count job that reads from a Kiji table is run using Hadoop") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new WordCountJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)("family:column1" -> 'word), wordCountInput(uri))
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .runHadoop
        .finish
  }

  /** Input tuples to use for import tests. */
  val importMultipleTimestamps: List[(String, String)] = List(
      ( "0", "1 eid1 word1" ),
      ( "1", "3 eid1 word2" ),
      ( "2", "5 eid2 word3" ),
      ( "3", "7 eid2 word4" ))

  /**
   * Validates output from [[org.kiji.express.flow.KijiSourceSuite.ImportJob]].
   *
   * @param outputBuffer containing data that the Kiji table has in it after the job has been run.
   */
  def validateMultipleTimestamps(outputBuffer: Buffer[(EntityId, KijiSlice[String])]) {
    assert(outputBuffer.size === 2)

    // There should be two Cells in each of the KijiSlices
    assert(outputBuffer(0)._2.cells.size === 2)
    assert(outputBuffer(1)._2.cells.size === 2)

    // KijiSlices order the most recent timestamp first.
    assert(outputBuffer(0)._2.cells(0).datum === "word2")
    assert(outputBuffer(0)._2.cells(1).datum === "word1")
    assert(outputBuffer(1)._2.cells(0).datum === "word4")
    assert(outputBuffer(1)._2.cells(1).datum === "word3")
  }

  test("An import job with multiple timestamps imports all timestamps in local mode.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new MultipleTimestampsImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), importMultipleTimestamps)
        .sink(KijiOutput(uri, 'timestamp)('word -> "family:column1"))(validateMultipleTimestamps)
        // Run the test job.
        .run
        .finish
  }

  test("An import with multiple timestamps imports all timestamps using Hadoop.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new MultipleTimestampsImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), importMultipleTimestamps)
        .sink(KijiOutput(uri, 'timestamp)('word -> "family:column1"))(validateMultipleTimestamps)
        // Run the test job.
        .run
        .finish
  }

  /** Input tuples to use for import tests. */
  val importInput: List[(String, String)] = List(
      ( "0", "hello hello hello world world hello" ),
      ( "1", "world hello   world      hello" ))

  /**
   * Validates output from [[org.kiji.express.flow.KijiSourceSuite.ImportJob]].
   *
   * @param outputBuffer containing data that the Kiji table has in it after the job has been run.
   */
  def validateImport(outputBuffer: Buffer[(EntityId, KijiSlice[String])]) {
    assert(10 === outputBuffer.size)

    // Perform a non-distributed word count.
    val wordCounts: (Int, Int) = outputBuffer
        // Extract words from each row.
        .flatMap { row =>
          val (_, slice) = row
          slice.cells
              .map { cell: Cell[String] =>
              cell.datum
              }
        }
        // Count the words.
        .foldLeft((0, 0)) { (counts, word) =>
          // Unpack the counters.
          val (helloCount, worldCount) = counts

          // Increment the appropriate counter and return both.
          word.toString() match {
            case "hello" => (helloCount + 1, worldCount)
            case "world" => (helloCount, worldCount + 1)
          }
        }

    // Make sure that the counts are as expected.
    assert((6, 4) === wordCounts)
  }

  test("an import job that writes to a Kiji table is run using Scalding's local mode") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new ImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), importInput)
        .sink(KijiOutput(uri)('word -> "family:column1"))(validateImport)
        // Run the test job.
        .run
        .finish
  }

  test("an import job that writes to a Kiji table is run using Hadoop") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new ImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), importInput)
        .sink(KijiOutput(uri)('word -> "family:column1"))(validateImport)
        // Run the test job.
        .runHadoop
        .finish
  }

  val importWithTimeInput = (0L, "Line-0") :: (1L, "Line-1") :: (2L, "Line-2") :: Nil

  /**
   * Validates output from [[org.kiji.express.flow.KijiSourceSuite.ImportJobWithTime]].
   *
   * @param outputBuffer containing data that the Kiji table has in it after the job has been run.
   */
  def validateImportWithTime(outputBuffer: Buffer[(EntityId, KijiSlice[String])]) {
    // There should be one cell per row in the output.
    val cellsPerRow = outputBuffer.unzip._2.map { m => (m.getFirst().version, m.getFirst().datum) }
    // Sort by timestamp.
    val cellsSortedByTime = cellsPerRow.sortBy { case(ts, line) => ts }
    // Verify contents.
    (0 until 2).foreach { index =>
      assert( (index.toLong, "Line-" + index) == cellsSortedByTime(index) )
    }
  }

  test("an import job that writes to a Kiji table with timestamps is run using Scalding's local "
      + "mode") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new ImportJobWithTime(_))
    .arg("input", "inputFile")
    .arg("output", uri)
    .source(TextLine("inputFile"), importWithTimeInput)
    .sink(KijiOutput(uri, 'offset)('line -> "family:column1"))(validateImportWithTime)
    // Run the test job.
    .run
    .finish
  }

  test("an import job that writes to a Kiji table with timestamps is run using Hadoop") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new ImportJobWithTime(_))
    .arg("input", "inputFile")
    .arg("output", uri)
    .source(TextLine("inputFile"), importWithTimeInput)
    .sink(KijiOutput(uri, 'offset)('line -> "family:column1"))(validateImportWithTime)
    // Run the test job.
    .runHadoop
    .finish
  }

  // Input tuples to use for version count tests.
  def versionCountInput(uri: String): List[(EntityId, KijiSlice[String])] = {
    List(
        ( EntityId(uri)("row01"), slice("family:column1", (10L, "two"), (20L, "two")) ),
        ( EntityId(uri)("row02"), slice("family:column1",
            (10L, "three"),
            (20L, "three"),
            (30L, "three") ) ),
        ( EntityId(uri)("row03"), slice("family:column1", (10L, "hello")) ))
  }

  test("a job that requests maxVersions gets them") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    def validateVersionCount(outputBuffer: Buffer[(Int, Int)]) {
      val outMap = outputBuffer.toMap
      // There should be two rows with 2 returned versions
      // since one input row has 3 versions but we only requested two.
      assert(1 == outMap(1))
      assert(2 == outMap(2))
    }

    // Build test job.
    val source =
        KijiInput(uri)(Map((Column("family:column1", versions=2) -> 'words)))
    JobTest(new VersionsJob(source)(_))
        .arg("output", "outputFile")
        .source(source, versionCountInput(uri))
        .sink(Tsv("outputFile"))(validateVersionCount)
        // Run the test job.
        .run
        .finish
  }

  test("a job that requests a time range gets them") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }


    def validateVersionCount(outputBuffer: Buffer[(Int, Int)]) {
      val outMap = outputBuffer.toMap
      // There should be two rows with 2 returned versions
      // since one input row has 3 versions but we only requested two.
      assert(1 === outMap.size)
      assert(2 === outMap(1))
    }

    // Build test job.
    val source = KijiInput(uri, timeRange=Between(15L, 25L))("family:column1" -> 'words)
    JobTest(new VersionsJob(source)(_))
        .arg("output", "outputFile")
        .source(source, versionCountInput(uri))
        .sink(Tsv("outputFile"))(validateVersionCount)
        // Run the test job.
        .run
        .finish
  }

  def missingValuesInput(uri: String): List[(EntityId, KijiSlice[String], KijiSlice[String])] = {
    List(
        (EntityId(uri)("row01"), slice("family:column1", (10L, "hello")),
            slice("family:column2", (10L, "hello")) ),
        (EntityId(uri)("row02"), slice("family:column1", (10L, "hello")), missing() ),
        (EntityId(uri)("row03"), slice("family:column1", (10L, "world")),
            slice("family:column2", (10L, "world")) ),
        (EntityId(uri)("row04"), slice("family:column1", (10L, "hello")),
            slice("family:column2", (10L, "hello"))))
  }

  test("Default for missing values is skipping the row.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    def validateMissingValuesSize(outputBuffer: Buffer[(String, String)]) {
      assert(3 === outputBuffer.size)
    }

    // Build test job.
    JobTest(new TwoColumnJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)("family:column1" -> 'word1, "family:column2" -> 'word2),
            missingValuesInput(uri))
        .sink(Tsv("outputFile"))(validateMissingValuesSize)
        // Run the test job.
        .runHadoop
        .finish
  }


  test("replacing missing values succeeds") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }
    def validateMissingValuesReplaced(outputBuffer: Buffer[(EntityId, String)]) {
      assert(4 === outputBuffer.size)
      val eid_hello:EntityId = EntityId(uri)("row01")
      val eid_missings: EntityId = EntityId(uri)("row02")
      val outMap = outputBuffer.toMap
      assert("hellos" == outMap(eid_hello))
      assert("missings" == outMap(eid_missings))
    }
    // Build test job.
    JobTest(new PluralizeReplaceJob(_))
      .arg("input", uri)
      .arg("output", "outputFile")
      .source(KijiInput(uri)(
        Map(
          Column("family:column1") -> 'word1,
          Column("family:column2")
      .replaceMissingWith("missing") -> 'word2)), missingValuesInput(uri))
      .sink(Tsv("outputFile"))(validateMissingValuesReplaced)
    // Run the test job.
      .run
      .finish
  }

  // TODO(CHOP-39): Write this test.
  test("a word-count job that uses the type-safe api is run") {
    pending
  }

  // TODO(CHOP-40): Write this test.
  test("a job that uses the matrix api is run") {
    pending
  }

  test("test conversion of column value of type string between java and scala in Hadoop mode") {
    def validateSimpleAvroChecker(outputBuffer: Buffer[(String, Int)]) {
      val outMap = outputBuffer.toMap
      // Validate that the output is as expected.
      intercept[java.util.NoSuchElementException]{outMap("false")}
      assert(6 === outMap("true"))
    }
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }
    // Input tuples to use for avro/scala conversion tests.
    val avroCheckerInput: List[(EntityId, KijiSlice[String])] = List(
        ( EntityId(uri)("row01"), slice("family:column1", (10L,"two"), (20L, "two")) ),
        ( EntityId(uri)("row02"), slice("family:column1",
            (10L, "three"),
            (20L, "three"),
            (30L, "three") ) ),
        ( EntityId(uri)("row03"), slice("family:column1", (10L, "hello")) ))
    // Build test job.
    val testSource = KijiInput(uri)(Map((Column("family:column1", versions=all) -> 'word)))
    JobTest(new AvroToScalaChecker(testSource)(_))
      .arg("input", uri)
      .arg("output", "outputFile")
      .source(testSource, avroCheckerInput)
      .sink(Tsv("outputFile"))(validateSimpleAvroChecker)
    // Run the test job.
      .runHadoop
      .finish
  }

  test("A job that reads using the generic API is run.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    val specificRecord = new HashSpec()
    specificRecord.setHashType(HashType.MD5)
    specificRecord.setHashSize(13)
    specificRecord.setSuppressKeyMaterialization(true)
    def genericReadInput(uri: String): List[(EntityId, KijiSlice[HashSpec])] = {
      List((EntityId(uri)("row01"), slice("family:column3", (10L, specificRecord))))
    }

    def validateGenericRead(outputBuffer: Buffer[(Int, Int)]): Unit = {
      assert (1 === outputBuffer.size)
      // There exactly 1 record with hash_size of 13.
      assert ((13, 1) === outputBuffer(0))
    }

    val jobTest = JobTest(new GenericAvroReadJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri) (Map (Column("family:column3") -> 'records)),
            genericReadInput(uri))
        .sink(Tsv("outputFile"))(validateGenericRead)

    // Run in local mode
    jobTest.run.finish

    // Run in hadoop mode
    jobTest.runHadoop.finish
  }

  test("A job that writes using the generic API is run.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input to use with Text source.
    val genericWriteInput: List[(String, String)] = List(
        ( "0", "one" ),
        ( "1", "two" ))

    // Validates the output buffer contains the same as the input buffer.
    def validateGenericWrite(outputBuffer: Buffer[(EntityId, KijiSlice[AvroRecord])]): Unit = {
      val outMap = outputBuffer.toMap
      assert("word_one" === outMap(EntityId(uri)("one")).getFirstValue()("field1").asString)
      assert("word_two" === outMap(EntityId(uri)("two")).getFirstValue()("field1").asString)

      assert("one" === outMap(EntityId(uri)("one")).getFirstValue()("field2").asEnumName)
      assert("two" === outMap(EntityId(uri)("two")).getFirstValue()("field2").asEnumName)

      assert(null == outMap(EntityId(uri)("one")).getFirstValue()("field3"))

      assert(0 === outMap(EntityId(uri)("one")).getFirstValue()("field4").asList.size)

      assert(0 === outMap(EntityId(uri)("one")).getFirstValue()("field5").asMap.size)
    }

    val jobTest = JobTest(new GenericAvroWriteJob(_))
      .arg("input", "inputFile")
      .arg("output", uri)
      .source(TextLine("inputFile"), genericWriteInput)
      .sink(KijiOutput(uri)('genericRecord -> "family:column4"))(
          validateGenericWrite)

    // Run in local mode.
    jobTest.run.finish

    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test ("A job that writes to map-type column families is run.") {
    // URI of the Kiji table to use.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input text.
    val mapTypeInput: List[(String, String)] = List(
        ("0", "dogs 4"),
        ("1", "cats 5"),
        ("2", "fish 3"))

    // Validate output.
    def validateMapWrite(outputBuffer: Buffer[(EntityId, KijiSlice[AvroRecord])]): Unit = {
      assert (1 === outputBuffer.size)
      val outputSlice = outputBuffer(0)._2
      val outputSliceMap = outputSlice.groupByQualifier
      assert (4 === outputSliceMap("dogs").getFirstValue)
      assert (5 === outputSliceMap("cats").getFirstValue)
      assert (3 === outputSliceMap("fish").getFirstValue)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new MapWriteJob(_))
        .arg("input", "inputFile")
        .arg("table", uri)
        .source(TextLine("inputFile"), mapTypeInput)
        .sink(KijiOutput(uri)(Map(MapFamily("searches")('terms) -> 'resultCount)))(validateMapWrite)

    // Run the test.
    jobTest.run.finish
    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test ("A job that writes to map-type column families with numeric column qualifiers is run.") {
    // URI of the Kiji table to use.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Create input using mapSlice.
    val mapTypeInput: List[(EntityId, KijiSlice[String])] = List(
        ( EntityId(uri)("0row"), mapSlice("animals", ("0column", 0L, "0 dogs")) ),
        ( EntityId(uri)("1row"), mapSlice("animals", ("0column", 0L, "1 cat")) ),
        ( EntityId(uri)("2row"), mapSlice("animals", ("0column", 0L, "2 fish")) ))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 3)
      val outputSet = outputBuffer.map { value: Tuple1[String] =>
        value._1
      }.toSet
      assert (outputSet.contains("0 dogs"), "Failed on \"0 dogs\" test")
      assert (outputSet.contains("1 cat"), "Failed on \"1 cat\" test")
      assert (outputSet.contains("2 fish"), "Failed on \"2 fish\" test")
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new MapSliceJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)("animals" -> 'terms), mapTypeInput)
        .sink(Tsv("outputFile"))(validateTest)

    // Run the test.
    jobTest.run.finish
    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }
}

/** Companion object for KijiSourceSuite. Contains helper functions and test jobs. */
object KijiSourceSuite extends KijiSuite {

  /**
   * A job that extracts the most recent string value from the column "family:column1" for all rows
   * in a Kiji table, and then counts the number of occurrences of those strings across rows.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class WordCountJob(args: Args) extends KijiJob(args) {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    KijiInput(args("input"))("family:column1" -> 'word)
        // Sanitize the word.
        .map('word -> 'cleanword) { words: KijiSlice[String] =>
          words.getFirstValue()
              .toString()
              .toLowerCase()
        }
        // Count the occurrences of each word.
        .groupBy('cleanword) { occurences => occurences.size }
        // Write the result to a file.
        .write(Tsv(args("output")))
  }

  /**
   * A job that takes the most recent string value from the column "family:column1" and adds
   * the letter 's' to the end of it. It passes through the column "family:column2" without
   * any changes.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class TwoColumnJob(args: Args) extends KijiJob(args) {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    KijiInput(args("input"))("family:column1" -> 'word1, "family:column2" -> 'word2)
        .map('word1 -> 'pluralword) { words: KijiSlice[String] =>
          words.getFirstValue().toString() + "s"
        }
        .write(Tsv(args("output")))
  }


 /**
  * A job that takes the most recent string value from the column "family:column1" and adds
  * the letter 's' to the end of it. It passes through the column "family:column2" without
  * any changes, replacing missing values with the string "missing".
  *
  * @param args to the job. Two arguments are expected: "input", which should specify the URI
  *     to the Kiji table the job should be run on, and "output", which specifies the output
  *     Tsv file.
  */
  class PluralizeReplaceJob(args: Args) extends KijiJob(args) {
    KijiInput(args("input"))(
        Map(
            Column("family:column1") -> 'word1,
            Column("family:column2")
                .replaceMissingWith("missing") -> 'word2))
    .map('word2 -> 'pluralword) { words: KijiSlice[String] =>
      words.getFirst().datum + "s"
    }
    .discard(('word1, 'word2))
    .write(Tsv(args("output")))
  }

  /**
   * A job that requests specific number of versions and buckets the results by the number of
   * versions.  The result is pairs of number of versions and the number of rows with that number
   * of versions.
   *
   * @param source that the job will use.
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class VersionsJob(source: KijiSource)(args: Args) extends KijiJob(args) {
    source
        // Count the size of words (number of versions).
        .map('words -> 'versioncount) { words: KijiSlice[String] =>
          words.size
        }
        .groupBy('versioncount) (_.size)
        .write(Tsv(args("output")))
  }

  class MultipleTimestampsImportJob(args: Args) extends KijiJob(args) {
    // Setup input.
    TextLine(args("input"))
        .read
        // Get the words in each line.
        .map('line -> ('timestamp, 'entityId, 'word)) { line: String =>
          val Array(timestamp, eid, token) = line.split("\\s+")

          (timestamp.toLong, EntityId(args("output"))(eid), token)
        }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput(args("output"), 'timestamp)('word -> "family:column1"))
  }

  /**
   * A job that, for each line in a text file, splits the line into words,
   * and for each word occurrence writes a copy of the word to the column "family:column1" in a
   * row for that word in a Kiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class ImportJob(args: Args) extends KijiJob(args) {
    // Setup input.
    TextLine(args("input"))
        .read
        // Get the words in each line.
        .flatMap('line -> 'word) { line : String => line.split("\\s+") }
        // Generate an entityId for each word.
        .map('word -> 'entityId) { _: String =>
            EntityId(args("output"))(UUID.randomUUID().toString()) }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput(args("output"))('word -> "family:column1"))
  }

  /**
   * A job that, given lines of text, writes each line to row for the line in a Kiji table,
   * at the column "family:column1", with the offset provided for each line being used as the
   * timestamp.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class ImportJobWithTime(args: Args) extends KijiJob(args) {
    // Setup input.
    TextLine(args("input"))
        .read
        // Generate an entityId for each line.
        .map('line -> 'entityId) { EntityId(args("output"))(_: String) }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput(args("output"), 'offset)('line -> "family:column1"))
  }

  /**
   * A job that given input from a Kiji table, ensures the type is accurate.
   *
   * @param source that the job will use.
   * @param args to the job. The input URI for the table and the output file.
   */
  class AvroToScalaChecker(source: KijiSource)(args: Args) extends KijiJob(args) {
    source
        .flatMap('word -> 'matches) { word: KijiSlice[String] =>
          word.cells.map { cell: Cell[String] =>
            val value = cell.datum
            if (value.isInstanceOf[String]) {
              "true"
            } else {
              "false"
            }
          }
        }
        .groupBy('matches) (_.size)
        .write(Tsv(args("output")))
  }

  /**
   * A job that uses the generic API, getting the "hash_size" field from a generic record, and
   * writes the number of records that have a certain hash_size.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class GenericAvroReadJob(args: Args) extends KijiJob(args) {
    KijiInput(args("input"))("family:column3" -> 'records)
    .map('records -> 'hashSizeField) { slice: KijiSlice[AvroValue] =>
        slice.getFirst match {
          case Cell(_, _, _, record: AvroRecord) => {
            record("hash_size").asInt
          }
        }
    }
    .groupBy('hashSizeField)(_.size)
    .write(Tsv(args("output")))
  }

  /**
   * A job that uses the generic API, creating a record containing the text from the input,
   * and writing it to a Kiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class GenericAvroWriteJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("output")
    TextLine(args("input"))
        .read
        .map('offset -> 'timestamp) { offset: String => offset.toLong }
        // Generate an entityId for each line.
        .map('line -> 'entityId) { EntityId(tableUri)(_: String) }
        .map('line -> 'genericRecord) { text: String =>
          AvroRecord(
              "field1" -> "word_%s".format(text),
              "field2" -> AvroEnum(text),
              "field3" -> null,
              "field4" -> List[Int](),
              "field5" -> Map[String, Int]())
        }
        // Write the results to the "family:column4" column of a Kiji table.
        .write(KijiOutput(tableUri)('genericRecord -> "family:column4"))
  }

  /**
   * A job that writes to a map-type column family.  It takes text from the input and uses it as
   * search terms and the number of results returned for that term.  All of them belong to the same
   * entity, "my_eid".
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class MapWriteJob(args: Args) extends KijiJob(args) {
    TextLine(args("input"))
        .read
        // Create an entity ID for each line (always the same one, here)
        .map('line -> 'entityId) { line: String => EntityId(args("table"))("my_eid") }
        // Get the number of result for each search term
        .map('line -> ('terms, 'resultCount)) { line: String =>
          (line.split(" ")(0), line.split(" ")(1).toInt)
        }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput(args("table"))(Map(MapFamily("searches")('terms) -> 'resultCount)))
  }

  /**
   * A job that tests map-type column families using KijiSlice and outputs the results to a TSV.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class MapSliceJob(args: Args) extends KijiJob(args) {
    KijiInput(args("input"))("animals" -> 'terms)
        .map('terms -> 'values) { terms: KijiSlice[String] => terms.getFirstValue }
        .project('values)
        .write(Tsv(args("output")))
  }
}
