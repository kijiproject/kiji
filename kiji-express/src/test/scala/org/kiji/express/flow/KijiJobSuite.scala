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

import scala.collection.mutable

import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import cascading.tuple.Fields
import org.apache.avro.specific.SpecificRecord
import org.kiji.express.Cell
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout

@RunWith(classOf[JUnitRunner])
class KijiJobSuite extends KijiSuite {
  val avroLayout: KijiTableLayout = layout("layout/avro-types.json")
  val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
    table.getURI().toString()
  }

  val rawInputs: List[(Long, String)] = List(
    (1, "input 1"),
    (2, "input 2"),
    (3, "input 3"),
    (4, "input 4"),
    (5, "input 5"))

  val eids: List[EntityId] = List("row-1", "row-2", "row-3", "row-4", "row-5").map(EntityId(_))

  val genericInputs: List[GenericRecord] = {
    val builder = new GenericRecordBuilder(SimpleRecord.getClassSchema)
    rawInputs.map { case (l: Long, s: String) => builder.set("l", l).set("s", s).build }
  }

  val specificInputs: List[SimpleRecord] = {
    val builder = SimpleRecord.newBuilder()
    rawInputs.map { case (l: Long, s: String) => builder.setL(l).setS(s).build }
  }

  def validateUnpacking(output: mutable.Buffer[(Long, String, String)]): Unit = {
    val inputMap = rawInputs.toMap
    output.foreach { case (l: Long, s: String, o: String) =>
      assert(inputMap(l) === s)
      assert("default-value" === o)
    }
  }

  test("A KijiJob can pack a generic Avro record.") {
    def validatePacking(outputs: mutable.Buffer[(EntityId, KijiSlice[GenericRecord])]) {
      val inputMap = rawInputs.toMap
      outputs.foreach { case (_: EntityId, slice: KijiSlice[GenericRecord]) =>
        val record = slice.getFirstValue()
        assert(inputMap(record.get("l").asInstanceOf[Long]) === record.get("s"))
        assert("default-value" === record.get("o"))
      }
    }

    val jobTest = JobTest(new PackGenericRecordJob(_))
        .arg("input", "inputFile")
        .arg("uri", uri)
        .source(Tsv("inputFile", fields = new Fields("l", "s")), rawInputs)
        .sink(KijiOutput(uri, 'record -> "family:simple"))(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  // TODO: EXP-290 - Fix testing support for outputting specific avro records
  ignore("A KijiJob can pack a specific Avro record.") {
    def validatePacking(outputs: mutable.Buffer[(EntityId, KijiSlice[SimpleRecord])]) {
      val inputMap = rawInputs.toMap
      outputs.foreach { case (_: EntityId, slice: KijiSlice[SimpleRecord]) =>
        val record = slice.getFirstValue()
        assert(inputMap(record.getL) === record.getS)
        assert("default-value" === record.getO)
      }
    }

    val jobTest = JobTest(new PackSpecificRecordJob(_))
        .arg("input", "inputFile")
        .arg("uri", uri)
        .source(Tsv("inputFile", fields = new Fields("l", "s")), rawInputs)
        .sink(KijiOutput(uri,
            Map('record ->
                QualifiedColumnRequestOutput("family", "simple", classOf[SimpleRecord]))))(
                    validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob can unpack a generic record.") {
    val slices: List[KijiSlice[GenericRecord]] = genericInputs.map { record: GenericRecord =>
      new KijiSlice(List(Cell("family", "simple", record)))
    }
    val input: List[(EntityId, KijiSlice[GenericRecord])] = eids.zip(slices)

    val jobTest = JobTest(new UnpackGenericRecordJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri,
            Map(QualifiedColumnRequestInput("family", "simple", SimpleRecord.getClassSchema)
                -> 'slice)), input)
        .sink(Tsv("outputFile"))(validateUnpacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob can unpack a specific record.") {
    val slices: List[KijiSlice[SpecificRecord]] = specificInputs.map { record: SpecificRecord =>
      new KijiSlice(List(Cell("family", "simple", record)))
    }
    val input: List[(EntityId, KijiSlice[SpecificRecord])] = eids.zip(slices)

    val jobTest = JobTest(new UnpackSpecificRecordJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput(uri,
      Map(QualifiedColumnRequestInput("family", "simple", classOf[SimpleRecord])
          -> 'slice)), input)
        .sink(Tsv("outputFile"))(validateUnpacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
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

    def validateBasicJob(outputBuffer: mutable.Buffer[String]) { /** Nothing to validate. */ }

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

    def validateBasicJob(outputBuffer: mutable.Buffer[String]) { /** Nothing to validate. */ }

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

    def validateBasicJob(outputBuffer: mutable.Buffer[String]) { /** Nothing to validate. */ }

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

class PackGenericRecordJob(args: Args) extends KijiJob(args) {
  Tsv(args("input"), fields = ('l, 's)).read
      .packGenericRecordTo(('l, 's) -> 'record)(SimpleRecord.getClassSchema)
      .insert('entityId, EntityId("foo"))
      .write(KijiOutput(args("uri"), 'record -> "family:simple"))
}

class PackSpecificRecordJob(args: Args) extends KijiJob(args) {
  Tsv(args("input"), fields = ('l, 's)).read
      .packTo[SimpleRecord](('l, 's) -> 'record)
      .insert('entityId, EntityId("foo"))
      .write(KijiOutput(args("uri"),
          Map('record -> QualifiedColumnRequestOutput("family", "simple", classOf[SimpleRecord]))))
}

class UnpackGenericRecordJob(args: Args) extends KijiJob(args) {
  KijiInput(args("input"),
      Map(QualifiedColumnRequestInput("family", "simple", SimpleRecord.getClassSchema) -> 'slice))
      .mapTo('slice -> 'record) { slice: KijiSlice[GenericRecord] => slice.getFirstValue() }
      .unpackTo[GenericRecord]('record -> ('l, 's, 'o))
      .write(Tsv(args("output")))
}

class UnpackSpecificRecordJob(args: Args) extends KijiJob(args) {
  KijiInput(args("input"),
      Map(QualifiedColumnRequestInput("family", "simple", classOf[SimpleRecord]) -> 'slice))
      .map('slice -> 'record) { slice: KijiSlice[SimpleRecord] => slice.getFirstValue }
      .unpackTo[SimpleRecord]('record -> ('l, 's, 'o))
      .write(Tsv(args("output")))
}
