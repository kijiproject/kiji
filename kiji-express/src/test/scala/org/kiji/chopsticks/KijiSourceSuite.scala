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

package org.kiji.chopsticks

import scala.collection.mutable.Buffer
import java.util.UUID

import com.twitter.scalding._

import org.kiji.chopsticks.DSL._
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.schema.EntityId
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

class KijiSourceSuite
    extends KijiSuite {
  import KijiSourceSuite._

  /** Table layout to use for tests. */
  val layout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Input tuples to use for word count tests. */
  val wordCountInput: List[(EntityId, KijiSlice[String])] = List(
      ( id("row01"), slice("family:column1", (1L, "hello")) ),
      ( id("row02"), slice("family:column1", (2L, "hello")) ),
      ( id("row03"), slice("family:column1", (1L, "world")) ),
      ( id("row04"), slice("family:column1", (3L, "hello")) ))

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
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new WordCountJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)("family:column1" -> 'word), wordCountInput)
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .run
        .finish
  }

  test("a word-count job that reads from a Kiji table is run using Hadoop") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new WordCountJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)("family:column1" -> 'word), wordCountInput)
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .runHadoop
        .finish
  }

  /** Input tuples to use for import tests. */
  val importInput: List[(String, String)] = List(
      ( "0", "hello hello hello world world hello" ),
      ( "1", "world hello   world      hello" ))

  /**
   * Validates output from [[org.kiji.chopsticks.KijiSourceSuite.ImportJob]].
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
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
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
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
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
   * Validates output from [[org.kiji.chopsticks.KijiSourceSuite.ImportJobWithTime]].
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
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
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
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
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
  val versionCountInput: List[(EntityId, KijiSlice[String])] = List(
    ( id("row01"), slice("family:column1", (10L, "two"), (20L, "two")) ),
    ( id("row02"), slice("family:column1",
      (10L, "three"),
      (20L, "three"),
      (30L, "three") ) ),
    ( id("row03"), slice("family:column1", (10L, "hello")) ))

  test("a job that requests maxVersions gets them") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
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
        .source(source, versionCountInput)
        .sink(Tsv("outputFile"))(validateVersionCount)
        // Run the test job.
        .run
        .finish
  }

  test("a job that requests a time range gets them") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
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
    val source = KijiInput(uri, timeRange=TimeRange.Between(15L, 25L))("family:column1" -> 'words)
    JobTest(new VersionsJob(source)(_))
        .arg("output", "outputFile")
        .source(source, versionCountInput)
        .sink(Tsv("outputFile"))(validateVersionCount)
        // Run the test job.
        .run
        .finish
  }
  val missingValuesInput: List[(EntityId, KijiSlice[String], KijiSlice[String])]
  = List(
    (id("row01"), slice("family:column1", (10L, "hello")),
      slice("family:column2", (10L, "hello")) ),
    (id("row02"), slice("family:column1", (10L, "hello")), missing() ),
    (id("row03"), slice("family:column1", (10L, "world")),
      slice("family:column2", (10L, "world")) ),
    (id("row04"), slice("family:column1", (10L, "hello")),
      slice("family:column2", (10L, "hello"))))

  test("Default for missing values is skipping the row.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
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
            missingValuesInput)
        .sink(Tsv("outputFile"))(validateMissingValuesSize)
        // Run the test job.
        .runHadoop
        .finish
  }


  test("replacing missing values succeeds") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
      table.getURI().toString()
    }
    def validateMissingValuesReplaced(outputBuffer: Buffer[(String, String)]) {
      assert(4 === outputBuffer.size)
      assert(outputBuffer(0)._2 == "hellos")
      assert(outputBuffer(1)._2 == "missings")
    }
    // Build test job.
    JobTest(new PluralizeReplaceJob(_))
      .arg("input", uri)
      .arg("output", "outputFile")
      .source(KijiInput(uri)(
        Map(
          Column("family:column1") -> 'word1,
          Column("family:column2")
      .replaceMissingWith(
        slice("family:column2", (0L, "missing"))) -> 'word2)),
    missingValuesInput)
      .sink(Tsv("outputFile"))(validateMissingValuesReplaced)
    // Run the test job.
      .run
    // note for reviewer: Running this with .runHadoop fails because Utf8 is not
    // serializable, and the KijiScheme gets serialized.  Hopefully if we eventually
    // use String or CharSequence instead, this won't be a problem
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
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
      table.getURI().toString()
    }
    // Input tuples to use for version count tests.
    val avroCheckerInput: List[(EntityId, KijiSlice[String])] = List(
        ( id("row01"), slice("family:column1", (10L,"two"), (20L, "two")) ),
        ( id("row02"), slice("family:column1",
            (10L, "three"),
            (20L, "three"),
            (30L, "three") ) ),
        ( id("row03"), slice("family:column1", (10L, "hello")) ))
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
  class WordCountJob(args: Args) extends Job(args) {
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
  class TwoColumnJob(args: Args) extends Job(args) {
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
  class PluralizeReplaceJob(args: Args) extends Job(args) {
    KijiInput(args("input"))(
        Map(
            Column("family:column1") -> 'word1,
            Column("family:column2")
                .replaceMissingWith(
                        slice("family:column2", (0L, "missing"))) -> 'word2))
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
  class VersionsJob(source: KijiSource)(args: Args) extends Job(args) {
    source
        // Count the size of words (number of versions).
        .map('words -> 'versioncount) { words: KijiSlice[String] =>
          words.size
        }
        .groupBy('versioncount) (_.size)
        .write(Tsv(args("output")))
  }

  /**
   * A job that, for each line in a text file, splits the line into words,
   * and for each word occurrence writes a copy of the word to the column "family:column1" in a
   * row for that word in a Kiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class ImportJob(args: Args) extends Job(args) {
    // Setup input.
    TextLine(args("input"))
        .read
        // Get the words in each line.
        .flatMap('line -> 'word) { line : String => line.split("\\s+") }
        // Generate an entityId for each word.
        .map('word -> 'entityId) { _: String => id(UUID.randomUUID().toString()) }
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
  class ImportJobWithTime(args: Args) extends Job(args) {
    // Setup input.
    TextLine(args("input"))
    .read
    // Generate an entityId for each line.
    .map('line -> 'entityId) { id(_: String) }
    // Write the results to the "family:column1" column of a Kiji table.
    .write(KijiOutput(args("output"), 'offset)('line -> "family:column1"))
  }

  class AvroToScalaChecker(source: KijiSource)(args: Args) extends Job(args) {
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
}
