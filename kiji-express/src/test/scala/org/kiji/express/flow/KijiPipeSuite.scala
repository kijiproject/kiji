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

import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.repl.Implicits._
import org.kiji.express.util.Resources._
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.KijiTable

class KijiPipeSuite extends KijiSuite {
  /** Table layout to use for tests. */
  val layout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Input tuples to use for word count tests. */
  def wordCountInput(uri: String): List[(EntityId, KijiSlice[String])] = {
    List(
      ( EntityId(uri)("row01"), slice("family:column1", (1L, "hello")) ),
      ( EntityId(uri)("row02"), slice("family:column1", (2L, "hello")) ),
      ( EntityId(uri)("row03"), slice("family:column1", (1L, "world")) ),
      ( EntityId(uri)("row04"), slice("family:column1", (3L, "hello")) ))
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
    KijiInput(uri)("family:column1" -> 'word)
    // Sanitize the word.
    .map('word -> 'cleanword) { words: KijiSlice[String] =>
      words.getFirstValue()
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
      .source(KijiInput(uri)("family:column1" -> 'word), wordCountInput(uri))
      .sink(Tsv("outputFile"))(validateWordCount)

  test("A KijiPipe can be used to obtain a Scalding job that is run in local mode.") {
    jobTest.run.finish
  }

  test("A KijiPipe can be used to obtain a Scalding job that is run with Hadoop.") {
    jobTest.runHadoop.finish
  }

  test("A KijiPipe can pack tuples into AvroRecords.") {
    val packingInput: List[(String, String)] = List(
        ( "0", "1 eid1 word1" ),
        ( "1", "3 eid1 word2" ),
        ( "2", "5 eid2 word3" ),
        ( "3", "7 eid2 word4" ))

    def validatePacking(outputBuffer: Buffer[(Int, String, Int, String)]) {
      assert(AvroRecord("line" -> "1 eid1 word1", "length" -> 12).toString === outputBuffer(0)._4)
    }

    def packTupleJob(args: Args): Job = {
      val cascadingPipe = TextLine(args("input")).read
          .map ('line -> 'length) { line: String => line.length }
      new KijiPipe(cascadingPipe)
          .packAvro(('line, 'length) -> 'record)
          .write(Tsv(args("output")))
          .getJob(args)
    }

    val jobTest = JobTest(packTupleJob(_))
        .arg("input", "inputFile")
        .arg("output", "outputFile")
        .source(TextLine("inputFile"), packingInput)
        .sink(Tsv("outputFile"))(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiPipe can pack tuples into AvroRecords when the table layout has a 'CLASS' type.") {
    val avroLayout: KijiTableLayout = layout("avro-types.json")

    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    val packingInput: List[(String, String)] = List(( "0", "1 eid1 word1" ))

    def validatePacking(outputBuffer: Buffer[(EntityId, KijiSlice[AvroRecord])]) {
      // Get the first record from the first KijiSlice.
      val outputRecord = outputBuffer(0)._2.getFirstValue
      assert("1 eid1 word1" === outputRecord("line").asString)
      assert(12 === outputRecord("length").asInt)
    }

    def packTupleJob(args: Args): Job = {
      val cascadingPipe = TextLine(args("input")).read
          .map ('line -> 'length) { line: String => line.length }
          .map ('offset -> 'entityId) { offset: String => EntityId(args("output"))(offset) }
      new KijiPipe(cascadingPipe)
          .packAvro(('line, 'length) -> 'record)
          .write(KijiOutput(args("output"))('record -> "family:column5"))
          .getJob(args)
    }

    val jobTest = JobTest(packTupleJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), packingInput)
        .sink(KijiOutput(uri)('record -> "family:column5"))(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiPipe can unpack tuples from AvroRecords.") {
    val avroLayout: KijiTableLayout = layout("avro-types.json")

    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    val specificRecord = new HashSpec()
    specificRecord.setHashType(HashType.MD5)
    specificRecord.setHashSize(13)
    specificRecord.setSuppressKeyMaterialization(true)

    def unpackingInput(uri: String): List[(EntityId, KijiSlice[HashSpec])] = {
      List((EntityId(uri)("row01"), slice("family:column3", (10L, specificRecord))))
    }

    def validatePacking(outputBuffer: Buffer[(String, String, String)]) {
      assert(AvroEnum("MD5").toString === outputBuffer(0)._1)
      assert(AvroInt(13).toString === outputBuffer(0)._2)
      assert(AvroBoolean(true).toString === outputBuffer(0)._3)
    }

    def unpackTupleJob(args: Args): Job = {
      val cascadingPipe = KijiInput(args("input"))("family:column3" -> 'slice)
          .map('slice -> 'record) { slice: KijiSlice[AvroRecord] => slice.getFirstValue }
      new KijiPipe(cascadingPipe)
          .unpackAvro('record -> ('hashtype, 'hashsize, 'suppress))
          .project('hashtype, 'hashsize, 'suppress)
          .write(Tsv(args("output")))
          .getJob(args)
    }

    val jobTest = JobTest(unpackTupleJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)(Map (Column("family:column3") -> 'slice)), unpackingInput(uri))
        .sink(Tsv("outputFile"))(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }
}
