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

import org.kiji.express.AvroBoolean
import org.kiji.express.AvroEnum
import org.kiji.express.AvroInt
import org.kiji.express.AvroRecord
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType
import org.kiji.schema.layout.KijiTableLayout

class KijiJobSuite extends KijiSuite {
  val avroLayout: KijiTableLayout = layout("avro-types.json")
  val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
    table.getURI().toString()
  }

  test("A KijiJob can run with a pipe that uses packAvro.") {
    val packingInput: List[(String, String)] = List(
        ( "0", "1 eid1 word1" ),
        ( "1", "3 eid1 word2" ),
        ( "2", "5 eid2 word3" ),
        ( "3", "7 eid2 word4" ))

    def validatePacking(outputBuffer: Buffer[(Int, String, Int, String)]) {
      assert(AvroRecord("line" -> "1 eid1 word1", "length" -> 12).toString === outputBuffer(0)._4)
    }

    class PackTupleJob(args: Args) extends KijiJob(args) {
      TextLine(args("input")).read
          .map ('line -> 'length) { line: String => line.length }
          .packAvro(('line, 'length) -> 'record)
          .write(Tsv(args("output")))
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

  test("A KijiJob can run with a pipe that uses unpackAvro.") {
    val specificRecord = new HashSpec()
    specificRecord.setHashType(HashType.MD5)
    specificRecord.setHashSize(13)
    specificRecord.setSuppressKeyMaterialization(true)

    val unpackingInput: List[(EntityId, KijiSlice[HashSpec])] =
      List((EntityId("row01"), slice("family:column3", (10L, specificRecord))))

    def validatePacking(outputBuffer: Buffer[(String, String, String)]) {
      assert(AvroEnum("MD5").toString === outputBuffer(0)._1)
      assert(AvroInt(13).toString === outputBuffer(0)._2)
      assert(AvroBoolean(true).toString === outputBuffer(0)._3)
    }

    class UnpackTupleJob(args: Args) extends KijiJob(args) {
      KijiInput(args("input"))("family:column3" -> 'slice)
          .map('slice -> 'record) { slice: KijiSlice[AvroRecord] => slice.getFirstValue }
          .unpackAvro('record -> ('hashtype, 'hashsize, 'suppress))
          .project('hashtype, 'hashsize, 'suppress)
          .write(Tsv(args("output")))
    }

    val jobTest = JobTest(new UnpackTupleJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri)(Map (Column("family:column3") -> 'slice)), unpackingInput)
        .sink(Tsv("outputFile"))(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob implicitly converts KijiSources to KijiPipes so you can call Kiji methods.") {
    // This class won't actually run because slice will contain a KijiSlice which can't be
    // unpacked.  This tests only tests that this compiles.  Eventually we may have methods
    // on KijiPipe that we need to use directly after KijiInput (for example, we may decide to
    // return only the first value in the slice if only 1 value is requested).
    class UnpackTupleJob(args: Args) extends KijiJob(args) {
      KijiInput(args("input"))("family:column3" -> 'slice)
          .unpackAvro('slice -> ('hashtype, 'hashsize, 'suppress))
          .project('hashtype, 'hashsize, 'suppress)
          .write(Tsv(args("output")))
    }
  }

  test("A KijiJob is not run if the Kiji instance in the output doesn't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .map ('line -> 'entityId) { line: String => EntityId(line) }
        .write(KijiOutput(args("output"))('line -> "family:column1"))
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
        .sink(KijiOutput(nonexistentInstanceURI)('line -> "family:column1"))(validateBasicJob)

    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }
    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_instance"))
  }

  test("A KijiJob is not run if the Kiji table in the output doesn't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .write(KijiOutput(args("output"))('line -> "family:column1"))
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
        .sink(KijiOutput(nonexistentTableURI)('line -> "family:column1"))(validateBasicJob)

    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_table"))
  }

  test("A KijiJob is not run if any of the columns don't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .write(KijiOutput(args("output"))('line -> "family:nonexistent_column"))
    }

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), basicInput)
        .sink(KijiOutput(uri)('line -> "family:nonexistent_column"))(validateBasicJob)

    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_column"))
  }
}
