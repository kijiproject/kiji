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

import java.io.File

import scala.collection.mutable.Buffer

import com.google.common.io.Files
import com.twitter.scalding._

import org.kiji.chopsticks.DSL._
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.schema.EntityId
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

class ScriptRunnerSuite extends KijiSuite {
  /** Table layout to use for tests. */
  val layout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Input tuples to use for word count tests. */
  val wordCountInput: List[(EntityId, KijiSlice[String])] = List(
      ( id("row01"), slice("family:column1", (10L, "hello")) ),
      ( id("row02"), slice("family:column1", (10L, "hello")) ),
      ( id("row03"), slice("family:column1", (10L, "world")) ),
      ( id("row04"), slice("family:column1", (10L, "hello")) ))

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

  /** Test script to run. */
  val scriptString: String =
    """
import java.util.NavigableMap

import com.twitter.scalding._
import org.apache.avro.util.Utf8

import org.kiji.chopsticks.DSL._
import org.kiji.chopsticks.KijiSlice

KijiInput("%s")("family:column1" -> 'word)
    // Sanitize the word.
    .map('word -> 'cleanword) { words: KijiSlice[String] =>
      words
          .getFirstValue()
          .toLowerCase()
    }
    // Count the occurrences of each word.
    .groupBy('cleanword) { occurences => occurences.size }
    // Write the result to a file.
    .write(Tsv("outputFile"))
    """

  test("An script can be compiled and run as a local job.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
      table.getURI().toString()
    }
    val runner = new ScriptRunner()

    val tempDir: File = Files.createTempDir()
    val classes: File = new File("%s/classes".format(tempDir.getPath()))
    classes.mkdir()
    val jobc = runner.compileScript(scriptString.format(uri), classes)

    // Build test job.
    JobTest(jobc)
        .source(KijiInput(uri)("family:column1" -> 'word), wordCountInput)
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .run
        .finish
  }

  // TODO(CHOP-60): Write this test.
  test("An script can be compiled and run as a hadoop job.") {
    pending
  }
}
