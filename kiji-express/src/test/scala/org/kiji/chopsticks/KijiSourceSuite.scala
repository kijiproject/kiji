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

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import java.util.NavigableMap
import java.util.UUID

import com.twitter.scalding._
import org.apache.avro.util.Utf8

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
  val layout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE)

  /** Input tuples to use for word count tests. */
  val wordCountInput: List[(EntityId, NavigableMap[Long, Utf8])] = List(
      ( id("row01"), singleton(new Utf8("hello")) ),
      ( id("row02"), singleton(new Utf8("hello")) ),
      ( id("row03"), singleton(new Utf8("world")) ),
      ( id("row04"), singleton(new Utf8("hello")) ))

  /**
   * Validates output from [[com.twitter.scalding.examples.WordCountJob]].
   *
   * @param outputBuffer containing data that the Kiji table has in it after the job has been run.
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
        .source(KijiInput(uri)("family:column" -> 'word), wordCountInput)
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
        .source(KijiInput(uri)("family:column" -> 'word), wordCountInput)
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
  def validateImport(outputBuffer: Buffer[(EntityId, NavigableMap[Long, Utf8])]) {
    assert(10 === outputBuffer.size)

    // Perform a non-distributed word count.
    val wordCounts: (Int, Int) = outputBuffer
        // Extract words from each row.
        .flatMap { row =>
          val (_, timeline) = row
          timeline
              .asScala
              .map { case (_, word) => word }
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
        .sink(KijiOutput(uri)('word -> "family:column"))(validateImport)
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
        .sink(KijiOutput(uri)('word -> "family:column"))(validateImport)
        // Run the test job.
        .runHadoop
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
}

/** Companion object for KijiSourceSuite. Contains helper functions and test jobs. */
object KijiSourceSuite extends KijiSuite {
  /** Convenience method for getting the latest value in a timeline. */
  def getMostRecent[T](timeline: NavigableMap[Long, T]): T = timeline.firstEntry().getValue()

  /**
   * A job that extracts the most recent string value from the column "family:column" for all rows
   * in a Kiji table, and then counts the number of occurrences of those strings across rows.
   *
   * @param args to the job. One argument named "input" is expected, which should specify the URI
   *     to the Kiji table the job should be run on.
   */
  class WordCountJob(args: Args) extends Job(args) {
    // Setup input to bind values from the "family:column" column to the symbol 'word.
    KijiInput(args("input"))("family:column" -> 'word)
        // Sanitize the word.
        .map('word -> 'cleanword) { words: NavigableMap[Long, Utf8] =>
          getMostRecent(words)
              .toString()
              .toLowerCase()
        }
        // Count the occurrences of each word.
        .groupBy('cleanword) { occurences => occurences.size }
        // Write the result to a file.
        .write(Tsv(args("output")))
  }

  /**
   * A job that, for each line in a text file, splits the line into words,
   * and for each word occurrence writes a copy of the word to the column "family:column" in a
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
        // Write the results to the "family:column" column of a Kiji table.
        .write(KijiOutput(args("output"))('word -> "family:column"))
  }
}
