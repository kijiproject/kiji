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

package org.kiji.express.repl

import scala.collection.mutable.Buffer

import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.JobTest
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.Tsv
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.KijiOutput
import org.kiji.express.flow.util.Resources.doAndRelease
import org.kiji.express.KijiSuite
import org.kiji.express.repl.Implicits._
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

@RunWith(classOf[JUnitRunner])
class KijiPipeToolSuite extends KijiSuite {
  /** Table layout to use for tests. */
  val layout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Input tuples to use for word count tests. */
  def wordCountInput(uri: String): List[(EntityId, Seq[FlowCell[String]])] = {
    List(
      ( EntityId("row01"), slice("family:column1", (1L, "hello")) ),
      ( EntityId("row02"), slice("family:column1", (2L, "hello")) ),
      ( EntityId("row03"), slice("family:column1", (1L, "world")) ),
      ( EntityId("row04"), slice("family:column1", (3L, "hello")) ))
  }

  /**
   * Validates output from the word count tests.

   * @param outputBuffer containing data that output buffer has in it after the job has been run.
   */
  def validateWordCount(outputBuffer: Buffer[(String, Int)]) {
    val outMap = outputBuffer.toMap

    // Validate that the output is as expected.
    assert(3 === outMap("hello"))
    assert(1 === outMap("world"))
  }

  // Create test Kiji table.
  val uri: String = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
    table.getURI().toString()
  }

  // A name for a dummy file for test job output.
  val outputFile: String = "outputFile"

  // A job obtained by converting a Cascading Pipe to a KijiPipe, which is then used to obtain
  // a Scalding Job from the pipe.
  def jobToRun(args: Args): Job = {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    KijiInput.builder
        .withTableURI(uri)
        .withColumns("family:column1" -> 'word)
        .build
    // Sanitize the word.
    .map('word -> 'cleanword) { words: Seq[FlowCell[CharSequence]] =>
      words.head.datum
          .toString()
          .toLowerCase()
    }
    // Count the occurrences of each word.
    .groupBy('cleanword) { occurences => occurences.size }
    // Write the result to a file.
    .write(Tsv("outputFile"))
    .getJob(args)
  }

  /** The job tester we'll use to run the test job in either local or hadoop mode. */
  val jobTest = JobTest(jobToRun(_))
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("family:column1" -> 'word)
          .build, wordCountInput(uri))
      .sink(Tsv("outputFile"))(validateWordCount)

  test("A KijiPipeTool can be used to obtain a Scalding job that is run in local mode.") {
    jobTest.run.finish
  }

  test("A KijiPipeTool can be used to obtain a Scalding job that is run with Hadoop.") {
    jobTest.runHadoop.finish
  }

  test("A KijiPipe can be implicitly converted to a KijiPipeTool,") {

    // Run test case in local mode so we can specify the input file.
    Mode.mode = Local(true)

    val tempFolder = new TemporaryFolder()
    tempFolder.create()
    val inputFile = tempFolder.newFile("input-source")

    // Implicitly create a KijiPipe, then call KijiPipeTool's run() method on it.
    Tsv(inputFile.getAbsolutePath, fields = ('l, 's)).read
        .insert('entityId, EntityId("foo"))
        .write(KijiOutput.builder.withTableURI(uri).build)
        .packGenericRecordTo(('l, 's) -> 'record)(SimpleRecord.getClassSchema)
        .run
  }
}
