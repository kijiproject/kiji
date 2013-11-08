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
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.avro.generic.GenericRecord
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.Cell
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType
import org.kiji.schema.layout.KijiTableLayout

@RunWith(classOf[JUnitRunner])
class KijiJobSuite extends KijiSuite {
  val avroLayout: KijiTableLayout = layout("layout/avro-types.json")
  val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
    table.getURI().toString()
  }

  test("A KijiJob can run with a pipe that uses packAvro.") {
    val packingInput: List[(String, String)] = List(
        ( "0", "length 8" ),
        ( "1", "length  9" ),
        ( "2", "length  10" ),
        ( "3", "length   11" ))

    def validatePacking(outputBuffer: Buffer[(Int, String, String)]) {
      val inputMap = packingInput.unzip._2.groupBy(_.length).mapValues(_.head)
      outputBuffer.foreach { t =>
        assert(inputMap(t._1) === t._2)
        assert(t._3 === "default-value")
      }
    }

    val jobTest = JobTest(new PackTupleJob(_))
        .arg("input", "inputFile")
        .arg("output", "outputFile")
        .source(TextLine("inputFile"), packingInput)
        .sink(Tsv("outputFile"))(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob can unpack a specific record.") {

    val specificRecord = new HashSpec(HashType.MD5, 13, true)
    val entityId = EntityId("row01")
    val slice = new KijiSlice(Seq(Cell("family", "column3", 10L, specificRecord)))
    val input = List(entityId -> slice)

    def validate(outputBuffer: Buffer[(String, Int, Boolean)]) {
      val t = outputBuffer.head
      assert(HashType.MD5.toString === t._1)
      assert(13 === t._2)
      assert(true === t._3)
    }

    val jobTest = JobTest(new UnpackTupleJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri, Map(ColumnRequestInput("family:column3") -> 'slice)), input)
        .sink(Tsv("outputFile"))(validate)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob implicitly converts KijiSources to KijiPipes so you can call Kiji methods.") {
    class UnpackTupleJob(args: Args) extends KijiJob(args) {
      KijiInput(args("input"), "family:column3" -> 'slice)
          .map('slice -> 'record) { slice: KijiSlice[GenericRecord] => slice.getFirstValue() }
          .unpackTo('record -> ('hashtype, 'hashsize, 'suppress))
          .write(Tsv(args("output")))
    }
  }

  test("A KijiJob is not run if the Kiji instance in the output doesn't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .map ('line -> 'entityId) { line: String => EntityId(line) }
        .write(KijiOutput(args("output"), 'line -> "family:column1"))
    }

    val nonexistentInstanceURI: String = KijiURI.newBuilder(uri)
        .withInstanceName("nonexistent_instance")
        .build()
        .toString

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
        .arg("input", "inputFile")
        .arg("output", nonexistentInstanceURI)
        .source(TextLine("inputFile"), basicInput)
        .sink(KijiOutput(nonexistentInstanceURI, 'line -> "family:column1"))(validateBasicJob)

    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }
    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_instance"))
  }

  test("A KijiJob is not run if the Kiji table in the output doesn't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .write(KijiOutput(args("output"), 'line -> "family:column1"))
    }

    val nonexistentTableURI: String = KijiURI.newBuilder(uri)
        .withTableName("nonexistent_table")
        .build()
        .toString

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
        .arg("input", "inputFile")
        .arg("output", nonexistentTableURI)
        .source(TextLine("inputFile"), basicInput)
        .sink(KijiOutput(nonexistentTableURI, 'line -> "family:column1"))(validateBasicJob)

    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_table"))
  }

  test("A KijiJob is not run if any of the columns don't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .write(KijiOutput(args("output"), 'line -> "family:nonexistent_column"))
    }

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), basicInput)
        .sink(KijiOutput(uri, 'line -> "family:nonexistent_column"))(validateBasicJob)

    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_column"))
  }
}

class PackTupleJob(args: Args) extends KijiJob(args) {
  TextLine(args("input")).read
      .map ('line -> 'l) { line: String => line.length }
      .rename('line -> 's)
      .packGenericRecordTo(('l, 's) -> 'record)(SimpleRecord.getClassSchema)
      .unpackTo('record -> ('l, 's, 'o))
      .write(Tsv(args("output")))
}

class UnpackTupleJob(args: Args) extends KijiJob(args) {
  KijiInput(args("input"), "family:column3" -> 'slice)
      .map('slice -> 'record) { slice: KijiSlice[GenericRecord] => slice.getFirstValue }
      .unpackTo[GenericRecord]('record -> ('hash_type, 'hash_size,
      'suppress_key_materialization))
      .write(Tsv(args("output")))
}

