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

import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.Tsv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.util.Resources.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

/**
 * A job that extracts the most recent string value from the column "family:column1" for all rows
 * in a Kiji table, and then concatenates those strings into a single word.
 *
 * @param args to the job. Two arguments are expected: "input", which should specify the URI
 *     to the Kiji table the job should be run on, and "output", which specifies the output
 *     Tsv file.
 */
class WordConcatJob(args: Args) extends KijiJob(args) {
  // Setup input to bind values from the "family:column1" column to the symbol 'word.
  KijiInput(
      args("input"),
      Map(ColumnInputSpec("family:column1", all, paging = PagingSpec.Cells(3)) -> 'word))
    // Sanitize the word.
    .map('word -> 'cleanword) { words: Seq[FlowCell[CharSequence]] =>
      words.foldLeft("")((a: String, b: FlowCell[CharSequence]) => a + b.datum.toString)
    }
    // Count the occurrences of each word.
    .groupBy('cleanword) { occurences => occurences.size }
    // Write the result to a file.
    .write(Tsv(args("output")))
}

/**
 * A job that extracts the most recent string value from the column "family:column1" for all rows
 * in a Kiji table, and then concatenates those strings into a single word.
 *
 * @param args to the job. Two arguments are expected: "input", which should specify the URI
 *     to the Kiji table the job should be run on, and "output", which specifies the output
 *     Tsv file.
 */
class WordCountFlatMapJob(args: Args) extends KijiJob(args) {
  // Setup input to bind values from the "family:column1" column to the symbol 'word.
  KijiInput(
      args("input"),
      Map(ColumnInputSpec("family:column1", all, paging = PagingSpec.Cells(3)) -> 'word))

      // Sanitize the word.
      .flatMap('word -> 'word) { words: Seq[FlowCell[CharSequence]] =>
          words
      }.map('word -> 'cleanword) {word: FlowCell[CharSequence] => word.datum.toString}
      // Count the occurrences of each word.
      .groupBy('cleanword) { occurences => occurences.size }
      // Write the result to a file.
      .write(Tsv(args("output")))
}

@RunWith(classOf[JUnitRunner])
class PagedCellsSuite extends KijiSuite {
  /** Simple table layout to use for tests. The row keys are hashed. */
  val simpleLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  test("a word-concat job that reads from a Kiji table is run using Scalding's local mode") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    /** Input tuples to use for word count tests. */
    def wordCountInput(uri: String): List[(EntityId, Seq[FlowCell[String]])] = {
      List((EntityId("row01"), slice("family:column1",(1L, "hello"), (2L, "world"),(3L, "hello"),
          (4L, "hello"))))
    }

    /**
     * Validates output from WordConcatJob
     *
     * @param outputBuffer containing data that output buffer has in it after the job has been run.
     */
    def validateWordConcat(outputBuffer: Buffer[(String, Int)]) {
      val outMap = outputBuffer.toMap
      assert(1 === outMap("hellohelloworldhello"))
    }

    // Build test job.
    JobTest(new WordConcatJob(_))
      .arg("input", uri)
      .arg("output", "outputFile")
      .source(
          KijiInput(
            uri,
            Map(ColumnInputSpec("family:column1", all, paging = PagingSpec.Cells(3)) -> 'word)),
          wordCountInput(uri))
      .sink(Tsv("outputFile"))(validateWordConcat)
      // Run the test job.
      .runHadoop
      .finish
  }

  test("a word-count job that reads from a Kiji table is run using Scalding's local mode") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    /** Input tuples to use for word count tests. */
    def wordCountInput(uri: String): List[(EntityId, Seq[FlowCell[String]])] = {
      List((EntityId("row01"), slice("family:column1",(1L, "hello"), (2L, "world"),(3L, "hello"),
        (4L, "hello"))))
    }

    /**
     * Validates output from WordConcatJob
     *
     * @param outputBuffer containing data that output buffer has in it after the job has been run.
     */
    def validateWordConcat(outputBuffer: Buffer[(String, Int)]) {
      val outMap = outputBuffer.toMap
      assert(1 === outMap("hellohelloworldhello"))
    }

    def validateWordCount(outputBuffer: Buffer[(String, Int)]) {
      val outMap = outputBuffer.toMap

      // Validate that the output is as expected.
      assert(3 === outMap("hello"))
      assert(1 === outMap("world"))
    }

    val column1 = ColumnInputSpec(
        column = "family:column1",
        maxVersions = all,
        paging = PagingSpec.Cells(3)
    )

    // Build test job.
    JobTest(new WordCountFlatMapJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(
            KijiInput(
                uri,
                Map(column1 -> 'word)),
            wordCountInput(uri))
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .runHadoop
        .finish
  }
}
