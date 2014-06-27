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

import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiJobSuiteSampleData
import org.kiji.express.KijiSuite
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI

@RunWith(classOf[JUnitRunner])
class KijiJobSuite extends KijiSuite {
  import KijiJobSuiteSampleData._

  val uri: String = ResourceUtil.doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
    table.getURI.toString
  }

  def validateUnpacking(output: mutable.Buffer[(Long, String, String)]): Unit = {
    val inputMap = rawInputs.toMap
    output.foreach { case (l: Long, s: String, o: String) =>
      assert(inputMap(l) === s)
      assert("default-value" === o)
    }
  }

  test("A KijiJob can pack a generic Avro record.") {
    def validatePacking(outputs: mutable.Buffer[(EntityId, Seq[FlowCell[GenericRecord]])]) {
      val inputMap = rawInputs.toMap
      outputs.foreach { case (_: EntityId, slice: Seq[FlowCell[GenericRecord]]) =>
        val record = slice.head.datum
        assert(inputMap(record.get("l").asInstanceOf[Long]) === record.get("s"))
        assert("default-value" === record.get("o"))
      }
    }

    val jobTest = JobTest(new PackGenericRecordJob(_))
        .arg("input", "inputFile")
        .arg("uri", uri)
        .source(Tsv("inputFile", fields = new Fields("l", "s")), rawInputs)
        .sink(KijiOutput.builder
            .withTableURI(uri)
            .withColumns('record -> "family:simple")
            .build
        )(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob can pack a specific Avro record.") {
    def validatePacking(outputs: mutable.Buffer[(EntityId, Seq[FlowCell[SimpleRecord]])]) {
      val inputMap = rawInputs.toMap
      outputs.foreach { case (_: EntityId, slice: Seq[FlowCell[SimpleRecord]]) =>
        val record = slice.head.datum
        assert(inputMap(record.getL) === record.getS)
        assert("default-value" === record.getO)
      }
    }

    val jobTest = JobTest(new PackSpecificRecordJob(_))
        .arg("input", "inputFile")
        .arg("uri", uri)
        .source(Tsv("inputFile", fields = new Fields("l", "s")), rawInputs)
        .sink(KijiOutput.builder
            .withTableURI(uri)
            .withColumnSpecs('record -> QualifiedColumnOutputSpec.builder
                .withFamily("family")
                .withQualifier("simple")
                .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
                .build)
            .build
        )(validatePacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob can unpack a generic record.") {
    val slices: List[Seq[FlowCell[GenericRecord]]] = genericInputs.map { record: GenericRecord =>
      List(FlowCell("family", "simple", datum = record))
    }
    val input: List[(EntityId, Seq[FlowCell[GenericRecord]])] = eids.zip(slices)

    val jobTest = JobTest(new UnpackGenericRecordJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
                .withColumn("family", "simple")
                .withSchemaSpec(SchemaSpec.Generic(SimpleRecord.getClassSchema))
                .build -> 'slice)
            .build, input)
        .sink(Tsv("outputFile"))(validateUnpacking)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiJob can unpack a specific record.") {
    val slices: List[Seq[FlowCell[SpecificRecord]]] = specificInputs
        .map { record: SpecificRecord =>
          List(FlowCell("family", "simple", datum = record))
        }
    val input: List[(EntityId, Seq[FlowCell[SpecificRecord]])] = eids.zip(slices)

    val jobTest = JobTest(new UnpackSpecificRecordJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
                .withColumn("family", "simple")
                .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
                .build -> 'slice)
            .build, input)
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
        .write(KijiOutput.builder
            .withTableURI(args("output"))
            .withColumns('line -> "family:column1")
            .build)
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
        .sink(KijiOutput.builder
            .withTableURI(nonexistentInstanceURI)
            .withColumns('line -> "family:column1")
            .build
        )(validateBasicJob)

    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }
    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getCause.getMessage.contains("nonexistent_instance"))
  }

  test("A KijiJob is not run if the Kiji table in the output doesn't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .write(KijiOutput.builder
            .withTableURI(args("output"))
            .withColumns('line -> "family:column1")
            .build)
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
        .sink(KijiOutput.builder
            .withTableURI(nonexistentTableURI)
            .withColumns('line -> "family:column1")
            .build
        )(validateBasicJob)

    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish }
    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)
    assert(localException.getMessage.contains("nonexistent_table"))
  }

  test("A KijiJob is not run if any of the columns don't exist.") {
    class BasicJob(args: Args) extends KijiJob(args) {
      TextLine(args("input"))
        .write(KijiOutput.builder
            .withTableURI(args("output"))
            .withColumns('line -> "family:nonexistent_column")
            .build)
    }

    val basicInput: List[(String, String)] = List[(String, String)]()

    def validateBasicJob(outputBuffer: mutable.Buffer[String]) { /** Nothing to validate. */ }

    val jobTest = JobTest(new BasicJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), basicInput)
        .sink(KijiOutput.builder
            .withTableURI(uri)
            .withColumns('line -> "family:nonexistent_column")
            .build
        )(validateBasicJob)

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
      .write(KijiOutput.builder
          .withTableURI(args("uri"))
          .withColumns('record -> "family:simple")
          .build)
}

class PackSpecificRecordJob(args: Args) extends KijiJob(args) {
  Tsv(args("input"), fields = ('l, 's)).read
      .packTo[SimpleRecord](('l, 's) -> 'record)
      .insert('entityId, EntityId("foo"))
      .write(KijiOutput.builder
          .withTableURI(args("uri"))
          .withColumnSpecs('record -> QualifiedColumnOutputSpec.builder
              .withFamily("family")
              .withQualifier("simple")
              .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
              .build)
          .build)
}

class UnpackGenericRecordJob(args: Args) extends KijiJob(args) {
  KijiInput.builder
      .withTableURI(args("input"))
      .withColumnSpecs(QualifiedColumnInputSpec.builder
          .withColumn("family", "simple")
          .withSchemaSpec(SchemaSpec.Generic(SimpleRecord.getClassSchema))
          .build -> 'slice)
      .build
      .mapTo('slice -> 'record) { slice: Seq[FlowCell[GenericRecord]] => slice.head.datum }
      .unpackTo[GenericRecord]('record -> ('l, 's, 'o))
      .write(Tsv(args("output")))
}

class UnpackSpecificRecordJob(args: Args) extends KijiJob(args) {
  KijiInput.builder
      .withTableURI(args("input"))
      .withColumnSpecs(QualifiedColumnInputSpec.builder
          .withColumn("family", "simple")
          .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
          .build -> 'slice)
      .build
      .map('slice -> 'record) { slice: Seq[FlowCell[SimpleRecord]] => slice.head.datum }
      .unpackTo[SimpleRecord]('record -> ('l, 's, 'o))
      .write(Tsv(args("output")))
}
