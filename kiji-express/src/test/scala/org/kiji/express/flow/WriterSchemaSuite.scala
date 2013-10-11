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

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema

import org.kiji.schema.layout.KijiTableLayout
import org.kiji.express._
import org.kiji.express.util.Resources._
import org.kiji.schema.{KijiURI, Kiji, KijiTable}
import scala.collection.mutable.Buffer
import com.twitter.scalding.{Args, JobTest}
import com.twitter.scalding.TextLine

class WriterSchemaSuite extends KijiSuite {
  import WriterSchemaSuite._

  /** Table layout using Avro schemas to use for tests. The row keys are formatted. */
  val avroLayout1_3: KijiTableLayout = layout("layout/avro-types-1.3.json")

  test("A job that writes to a layout-1.3 table and uses the default schema runs.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout1_3)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input to use with Text source.
    val writeInput: List[(String, String)] = List(
      ( "0", "one" ),
      ( "1", "two" ))

    // Validates the output buffer contains the records written with the default schema.
    def validateWrite(outputBuffer: Buffer[(EntityId, KijiSlice[AvroRecord])]): Unit = {
      val outMap = outputBuffer.toMap

      val record1 = outMap(EntityId("one")).getFirstValue()
      val record2 = outMap(EntityId("two")).getFirstValue()

      assert(
        "word_one" === record1("contained_string").asString)
      assert(
        "word_two" === record2("contained_string").asString)

      // Check that other fields don't exist (they were written with the default reader schema):
      assert(!record1.map.contains("field2"))
      assert(!record1.map.contains("field3"))
    }

    val jobTest = JobTest(new DefaultSchemaWriteJob(_))
      .arg("input", "inputFile")
      .arg("output", uri)
      .source(TextLine("inputFile"), writeInput)
      .sink(KijiOutput(uri,
          Map('genericRecord -> Column("family:column_validated").useDefaultReaderSchema))
      )(validateWrite)

    // Run in local mode.
    jobTest.run.finish

    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A job that writes to a layout-1.3 table and specifies a user schema runs.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout1_3)) { table: KijiTable =>
      table.getURI.toString
    }

    // Get the ID of the user defined schema.
    val userDefinedSchemaId: Long = doAndRelease(
      Kiji.Factory.open(KijiURI.newBuilder(uri).build())) { kiji: Kiji =>
      kiji.getSchemaTable.getOrCreateSchemaId(userDefinedSchema)
    }

    // Input to use with Text source.
    val writeInput: List[(String, String)] = List(
      ( "0", "one" ),
      ( "1", "two" ))

    // Validates the output buffer contains the records written with the default schema.
    def validateWrite(outputBuffer: Buffer[(EntityId, KijiSlice[AvroRecord])]): Unit = {
      val outMap = outputBuffer.toMap

      val record1 = outMap(EntityId("one")).getFirstValue()
      val record2 = outMap(EntityId("two")).getFirstValue()

      assert(
        "word_one" === record1("contained_string").asString)
      assert(
        "word_two" === record2("contained_string").asString)

      // Check that field2 exists but field3 doesn't (it's not in the specified writer schema):
      assert(record1.map.contains("field2"))
      assert(!record1.map.contains("field3"))
    }

    val jobTest = JobTest(new UserSpecifiedSchemaWriteJob(_))
      .arg("input", "inputFile")
      .arg("output", uri.toString)
      .source(TextLine("inputFile"), writeInput)
      .sink(KijiOutput(uri,
          Map('genericRecord ->
              Column("family:column_validated").withSchemaId(userDefinedSchemaId)))
      )(validateWrite)

    // Run in local mode.
    jobTest.run.finish

    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A job that writes to a layout-1.3 table column without a default schema runs.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout1_3)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input to use with Text source.
    val writeInput: List[(String, String)] = List(
      ( "0", "one" ),
      ( "1", "two" ))

    // Validates the output buffer contains the records written with the default schema.
    def validateWrite(outputBuffer: Buffer[(EntityId, KijiSlice[AvroRecord])]): Unit = {
      val outMap = outputBuffer.toMap

      val record1 = outMap(EntityId("one")).getFirstValue()
      val record2 = outMap(EntityId("two")).getFirstValue()

      assert(
        "word_one" === record1("contained_string").asString)
      assert(
        "word_two" === record2("contained_string").asString)

      // Check that other fields exist (they were written with the writer schema):
      assert(record1.map.contains("field2"))
      assert(record1.map.contains("field3"))

      assert(record1("field2").asInt === 1)
      assert(record1("field3").asEnumName === "three")
    }

    val jobTest = JobTest(new NoDefaultWriteJob(_))
      .arg("input", "inputFile")
      .arg("output", uri)
      .source(TextLine("inputFile"), writeInput)
      .sink(KijiOutput(uri, 'genericRecord -> "family:column_compatibility"))(validateWrite)

    // Run in local mode.
    jobTest.run.finish

    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A KijiExpress job will not run if a writer schema should be specified and isn't.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout1_3)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input to use with Text source.
    val writeInput: List[(String, String)] = List(
      ( "0", "one" ),
      ( "1", "two" ))

    // Validates the output buffer contains the records written with the default schema.
    def validateWrite(outputBuffer: Buffer[(EntityId, KijiSlice[AvroRecord])]): Unit = {
      val outMap = outputBuffer.toMap

      val record1 = outMap(EntityId("one")).getFirstValue()
      val record2 = outMap(EntityId("two")).getFirstValue()

      assert(
        "word_one" === record1("contained_string").asString)
      assert(
        "word_two" === record2("contained_string").asString)

      // Check that other fields exist (they were written with the writer schema):
      assert(record1.map.contains("field2"))
      assert(record1.map.contains("field3"))

      assert(record1("field2").asInt === 1)
      assert(record1("field3").asEnumName === "three")
    }

    val jobTest = JobTest(new NoSchemaSpecifiedWriteJob(_))
      .arg("input", "inputFile")
      .arg("output", uri)
      .source(TextLine("inputFile"), writeInput)
      .sink(KijiOutput(uri, 'genericRecord -> "family:column_validated"))(validateWrite)

    // Run in local mode.
    val localException = intercept[InvalidKijiTapException] { jobTest.run.finish}

    // Run in hadoop mode.
    val hadoopException = intercept[InvalidKijiTapException] { jobTest.runHadoop.finish }

    assert(localException.getMessage === hadoopException.getMessage)

    // Check the message includes the problem column.
    assert(localException.getMessage.contains("family:column_validated"))
  }
}

object WriterSchemaSuite {
  /**
   * A job that writes out to a Kiji table, using the default reader schema.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class DefaultSchemaWriteJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("output")
    TextLine(args("input"))
      .read
      .map('offset -> 'timestamp) { offset: String => offset.toLong }
      // Generate an entityId for each line.
      .map('line -> 'entityId) { EntityId(_: String) }
      .map('line -> 'genericRecord) { text: String =>
      AvroRecord(
        "contained_string" -> "word_%s".format(text),
        "field2" -> { if (text.equals("one")) 1; else 2; },
        "field3" -> AvroEnum("three")
      )
    }
      // Write the results to the "family:column_validated" column of a Kiji table using the
      // default schema.
      .write(KijiOutput(tableUri,
          Map('genericRecord -> Column("family:column_validated").useDefaultReaderSchema)))
  }

  // User-defined schema for use in UserSpecifiedSchemaWriteJob.
  val userDefinedSchema = {
    val fields: List[Schema.Field] = List(
        new Schema.Field("contained_string", Schema.create(Schema.Type.STRING), "contained", null),
        new Schema.Field("field2", Schema.create(Schema.Type.INT), "field 2", null))
    val record =
        Schema.createRecord("stringcontainer", "user-created schema", "org.kiji.schema.avro", false)
    record.setFields(fields.toSeq.asJava)
    record
  }

  /**
   * A job that writes out to a Kiji table, not using the default reader schema even though it
   * exists, and using a user-specified schema instead.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class UserSpecifiedSchemaWriteJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("output")

    // Get the ID of the user defined schema.
    val userDefinedSchemaId: Long = doAndRelease(
        Kiji.Factory.open(KijiURI.newBuilder(tableUri).build())) { kiji: Kiji =>
      kiji.getSchemaTable.getOrCreateSchemaId(userDefinedSchema)
    }

    TextLine(args("input"))
      .read
      .map('offset -> 'timestamp) { offset: String => offset.toLong }
      // Generate an entityId for each line.
      .map('line -> 'entityId) { EntityId(_: String) }
      .map('line -> 'genericRecord) { text: String =>
      AvroRecord(
        "contained_string" -> "word_%s".format(text),
        "field2" -> { if (text.equals("one")) 1; else 2; },
        "field3" -> AvroEnum("three")
      )
    }
      // Write the results to the "family:column_validated" column of a Kiji table.
      .write(KijiOutput(tableUri,
          Map('genericRecord ->
              Column("family:column_validated").withSchemaId(userDefinedSchemaId))))
  }

  /**
   * A job that writes out to a Kiji table, to a column without a default reader schema.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class NoDefaultWriteJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("output")
    TextLine(args("input"))
      .read
      .map('offset -> 'timestamp) { offset: String => offset.toLong }
      // Generate an entityId for each line.
      .map('line -> 'entityId) { EntityId(_: String) }
      .map('line -> 'genericRecord) { text: String =>
      AvroRecord(
        "contained_string" -> "word_%s".format(text),
        "field2" -> { if (text.equals("one")) 1; else 2; },
        "field3" -> AvroEnum("three")
      )
    }
      // Write the results to the "family:column_compatbility" column of a Kiji table.
      .write(KijiOutput(tableUri, 'genericRecord -> "family:column_compatibility"))
  }

  /**
   * A job that writes out to a Kiji table, to a column without a default reader schema,
   * without specifying a writer schema.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class NoSchemaSpecifiedWriteJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("output")
    TextLine(args("input"))
      .read
      .map('offset -> 'timestamp) { offset: String => offset.toLong }
      // Generate an entityId for each line.
      .map('line -> 'entityId) { EntityId(_: String) }
      .map('line -> 'genericRecord) { text: String =>
      AvroRecord(
        "contained_string" -> "word_%s".format(text),
        "field2" -> { if (text.equals("one")) 1; else 2; },
        "field3" -> AvroEnum("three")
      )
    }
      // Write the results to the "family:column_compatbility" column of a Kiji table.
      .write(KijiOutput(tableUri, 'genericRecord -> "family:column_validated"))
  }
}
