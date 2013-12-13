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

import java.util.UUID

import scala.collection.mutable.Buffer

import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.avro.generic.GenericRecord
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.SchemaSpec.Specific
import org.kiji.express.flow.util.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiTable
import org.kiji.schema.avro.AvroValidationPolicy
import org.kiji.schema.avro.HashSpec
import org.kiji.schema.avro.HashType
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.avro.TestRecord
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.layout.TableLayoutBuilder

@RunWith(classOf[JUnitRunner])
class KijiSourceSuite
    extends KijiSuite {
  import KijiSourceSuite._

  /** Simple table layout to use for tests. The row keys are hashed. */
  val simpleLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Table layout using Avro schemas to use for tests. The row keys are formatted. */
  val avroLayout: KijiTableLayout = layout("layout/avro-types.json")

  /** Input tuples to use for word count tests. */
  def wordCountInput(uri: String): List[(EntityId, Seq[FlowCell[String]])] = {
    List(
        ( EntityId("row01"), slice("family:column1", (1L, "hello")) ),
        ( EntityId("row02"), slice("family:column1", (2L, "hello")) ),
        ( EntityId("row03"), slice("family:column1", (1L, "world")) ),
        ( EntityId("row04"), slice("family:column1", (3L, "hello")) ))
  }

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

  test("a word-count job that reads from a Kiji table is run using Scalding's local mode") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new WordCountJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumns("family:column1" -> 'word)
            .build, wordCountInput(uri))
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .run
        .finish
  }

  test("a word-count job that reads from a Kiji table is run using Hadoop") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new WordCountJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumns("family:column1" -> 'word)
            .build, wordCountInput(uri))
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .runHadoop
        .finish
  }

  /** Input tuples to use for import tests. */
  val importMultipleTimestamps: List[(String, String)] = List(
      ( "0", "1 eid1 word1" ),
      ( "1", "3 eid1 word2" ),
      ( "2", "5 eid2 word3" ),
      ( "3", "7 eid2 word4" ))

  /**
   * Validates output from [[org.kiji.express.flow.KijiSourceSuite.ImportJob]].
   *
   * @param outputBuffer containing data that the Kiji table has in it after the job has been run.
   */

  def validateMultipleTimestamps(outputBuffer: Buffer[(EntityId, Seq[FlowCell[CharSequence]])]) {
    assert(outputBuffer.size === 2)

    // There should be two Cells written to each column.
    assert(outputBuffer(0)._2.size === 2)
    assert(outputBuffer(1)._2.size === 2)

    assert(outputBuffer(0)._2.head.datum.toString === "word2")
    assert(outputBuffer(0)._2.last.datum.toString === "word1")
    assert(outputBuffer(1)._2.head.datum.toString === "word4")
    assert(outputBuffer(1)._2.last.datum.toString === "word3")
  }

  test("An import job with multiple timestamps imports all timestamps in local mode.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new MultipleTimestampsImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), importMultipleTimestamps)
        .sink(KijiOutput.builder.
            withTableURI(uri)
            .withTimestampField('timestamp)
            .withColumns('word -> "family:column1")
            .build
        )(validateMultipleTimestamps)
        // Run the test job.
        .run
        .finish
  }

  test("An import with multiple timestamps imports all timestamps using Hadoop.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new MultipleTimestampsImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), importMultipleTimestamps)
        .sink(KijiOutput.builder
            .withTableURI(uri)
            .withTimestampField('timestamp)
            .withColumns('word -> "family:column1")
            .build
        )(validateMultipleTimestamps)
        // Run the test job.
        .run
        .finish
  }

  /** Input tuples to use for import tests. */
  val importInput: List[(String, String)] = List(
      ( "0", "hello hello hello world world hello" ),
      ( "1", "world hello   world      hello" ))

  /**
   * Validates output from [[org.kiji.express.flow.KijiSourceSuite.ImportJob]].
   *
   * @param outputBuffer containing data that the Kiji table has in it after the job has been run.
   */

  def validateImport(outputBuffer: Buffer[(EntityId, Seq[FlowCell[CharSequence]])]) {
    assert(10 === outputBuffer.size)

    // Perform a non-distributed word count.
    val wordCounts: (Int, Int) = outputBuffer
        // Extract words from each row.
        .flatMap { row =>
          val (_, slice) = row
          slice
              .map { cell: FlowCell[CharSequence] =>
              cell.datum
              }
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
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new ImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), importInput)
        .sink(KijiOutput.builder
            .withTableURI(uri)
            .withColumns('word -> "family:column1")
            .build
        )(validateImport)
        // Run the test job.
        .run
        .finish
  }

  test("an import job that writes to a Kiji table is run using Hadoop") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new ImportJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(TextLine("inputFile"), importInput)
        .sink(KijiOutput.builder
            .withTableURI(uri)
            .withColumns('word -> "family:column1")
            .build
        )(validateImport)
        // Run the test job.
        .runHadoop
        .finish
  }

  val importWithTimeInput = (0L, "Line-0") :: (1L, "Line-1") :: (2L, "Line-2") :: Nil

  /**
   * Validates output from [[org.kiji.express.flow.KijiSourceSuite.ImportJobWithTime]].
   *
   * @param outputBuffer containing data that the Kiji table has in it after the job has been run.
   */
  def validateImportWithTime(outputBuffer: Buffer[(EntityId, Seq[FlowCell[CharSequence]])]) {
    // There should be one cell per row in the output.
    val cellsPerRow = outputBuffer.unzip._2.map { m => (m.head.version, m.head.datum) }
    // Sort by timestamp.
    val cellsSortedByTime = cellsPerRow.sortBy { case(ts, line) => ts }
    // Verify contents.
    (0 until 2).foreach { index =>
      val cell = cellsSortedByTime(index)
      assert(index.toLong === cell._1)
      assert("Line-" + index === cell._2.toString)
    }
  }

  test("an import job that writes to a Kiji table with timestamps is run using local mode") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new ImportJobWithTime(_))
    .arg("input", "inputFile")
    .arg("output", uri)
    .source(TextLine("inputFile"), importWithTimeInput)
    .sink(KijiOutput.builder
        .withTableURI(uri)
        .withTimestampField('offset)
        .withColumns('line -> "family:column1")
        .build
    )(validateImportWithTime)
    // Run the test job.
    .run
    .finish
  }

  test("an import job that writes to a Kiji table with timestamps is run using Hadoop") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Build test job.
    JobTest(new ImportJobWithTime(_))
    .arg("input", "inputFile")
    .arg("output", uri)
    .source(TextLine("inputFile"), importWithTimeInput)
    .sink(KijiOutput.builder
        .withTableURI(uri)
        .withTimestampField('offset)
        .withColumns('line -> "family:column1")
        .build
    )(validateImportWithTime)
    // Run the test job.
    .runHadoop
    .finish
  }

  // Input tuples to use for version count tests.
  def versionCountInput(uri: String): List[(EntityId, Seq[FlowCell[String]])] = {
    List(
        ( EntityId("row01"), slice("family:column1", (10L, "two"), (20L, "two")) ),
        ( EntityId("row02"), slice("family:column1",
            (10L, "three"),
            (20L, "three"),
            (30L, "three") ) ),
        ( EntityId("row03"), slice("family:column1", (10L, "hello")) ))
  }

  test("a job that requests maxVersions gets them") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    def validateVersionCount(outputBuffer: Buffer[(Int, Int)]) {
      val outMap = outputBuffer.toMap
      // There should be two rows with 2 returned versions
      // since one input row has 3 versions but we only requested two.
      assert(1 == outMap(1))
      assert(2 == outMap(2))
    }

    // Build test job.
    val source =
        KijiInput.builder
            .withTableURI(uri)
            .withColumnSpecs(QualifiedColumnInputSpec.builder
                .withColumn("family", "column1")
                .withMaxVersions(2)
                .build -> 'words)
            .build
    JobTest(new VersionsJob(source)(_))
        .arg("output", "outputFile")
        .source(source, versionCountInput(uri))
        .sink(Tsv("outputFile"))(validateVersionCount)
        // Run the test job.
        .run
        .finish
  }

  test("a job that requests a time range gets them") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }


    def validateVersionCount(outputBuffer: Buffer[(Int, Int)]) {
      val outMap = outputBuffer.toMap
      // There should be two rows with 2 returned versions
      // since one input row has 3 versions but we only requested two.
      assert(1 === outMap.size)
      assert(2 === outMap(1))
    }

    // Build test job.
    val source = KijiInput.builder
        .withTableURI(uri)
        .withTimeRange(Between(15L, 25L))
        .withColumns("family:column1" -> 'words)
        .build
    JobTest(new VersionsJob(source)(_))
        .arg("output", "outputFile")
        .source(source, versionCountInput(uri))
        .sink(Tsv("outputFile"))(validateVersionCount)
        // Run the test job.
        .run
        .finish
  }

  test("Specific records can be returned to JobTest's validate method") {
    val uri: String = doAndRelease(makeTestKiji()) { kiji: Kiji =>
      val baseDesc: TableLayoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.SCHEMA_REG_TEST)
      baseDesc.setVersion("layout-1.3.0")

      val desc = new TableLayoutBuilder(baseDesc, kiji)
          .withAvroValidationPolicy(new KijiColumnName("info:fullname"), AvroValidationPolicy.NONE)
          .build()

      // Create test Kiji table.
      doAndRelease {
        kiji.createTable(desc)
        kiji.openTable(desc.getName)
      } { table: KijiTable =>
        table.getURI.toString
      }
    }

    // Build test job.
    class TestSpecificRecordWriteJob(args: Args) extends KijiJob(args) {
      Tsv(args("input"), ('entityId, 'fullname))
          .write(KijiOutput.builder
              .withTableURI(args("output"))
              .withColumnSpecs('fullname -> QualifiedColumnOutputSpec.builder
                  .withFamily("info")
                  .withQualifier("fullname")
                  .withSchemaSpec(SchemaSpec.Specific(classOf[TestRecord]))
                  .build)
              .build
          )
    }

    val inputRecord1 = TestRecord
        .newBuilder()
        .setA("foo")
        .setB(2)
        .setC(4)
        .build()
    val inputRecord2 = TestRecord
        .newBuilder()
        .setA("bar")
        .setB(1)
        .setC(3)
        .build()
    val inputRecord3 = TestRecord
        .newBuilder()
        .setA("baz")
        .setB(9)
        .setC(8)
        .build()
    val inputRecords = Seq(
        (EntityId("row01"), inputRecord1),
        (EntityId("row02"), inputRecord2),
        (EntityId("row03"), inputRecord3)
    )

    def validateSpecificWrite(outputBuffer: Buffer[(EntityId, Seq[FlowCell[TestRecord]])]) {
      val outputMap = outputBuffer.toMap
      assert(outputMap(EntityId("row01")).head.datum.getClass === classOf[TestRecord])
      assert(outputMap(EntityId("row02")).head.datum.getClass === classOf[TestRecord])
      assert(outputMap(EntityId("row03")).head.datum.getClass === classOf[TestRecord])
      assert(outputMap(EntityId("row01")).head.datum === inputRecord1)
      assert(outputMap(EntityId("row02")).head.datum === inputRecord2)
      assert(outputMap(EntityId("row03")).head.datum === inputRecord3)
    }

    JobTest(new TestSpecificRecordWriteJob(_))
        .arg("input", "inputFile")
        .arg("output", uri)
        .source(Tsv("inputFile", new Fields("entityId", "fullname")), inputRecords)
        .sink(
            KijiOutput.builder
                .withTableURI(uri)
                .withColumnSpecs('fullname -> QualifiedColumnOutputSpec.builder
                    .withFamily("info")
                    .withQualifier("fullname")
                    .withSchemaSpec(SchemaSpec.Specific(classOf[TestRecord]))
                    .build)
                .build
        )(validateSpecificWrite)
        .run
        .runHadoop
        .finish
  }

  // TODO(EXP-7): Write this test.
  test("a word-count job that uses the type-safe api is run") {
    pending
  }

  // TODO(EXP-6): Write this test.
  test("a job that uses the matrix api is run") {
    pending
  }

  test("test conversion of column value of type string between java and scala in Hadoop mode") {
    def validateSimpleAvroChecker(outputBuffer: Buffer[(String, Int)]) {
      val outMap = outputBuffer.toMap
      // Validate that the output is as expected.
      intercept[java.util.NoSuchElementException]{outMap("false")}
      assert(6 === outMap("true"))
    }
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }
    // Input tuples to use for avro/scala conversion tests.
    val avroCheckerInput: List[(EntityId, Seq[FlowCell[String]])] = List(
        ( EntityId("row01"), slice("family:column1", (10L,"two"), (20L, "two")) ),
        ( EntityId("row02"), slice("family:column1",
            (10L, "three"),
            (20L, "three"),
            (30L, "three") ) ),
        ( EntityId("row03"), slice("family:column1", (10L, "hello")) ))
    // Build test job.
    val testSource = KijiInput.builder
        .withTableURI(uri)
        .withColumnSpecs(QualifiedColumnInputSpec.builder
            .withColumn("family", "column1")
            .withMaxVersions(all)
            .build -> 'word)
        .build
    JobTest(new AvroToScalaChecker(testSource)(_))
      .arg("input", uri)
      .arg("output", "outputFile")
      .source(testSource, avroCheckerInput)
      .sink(Tsv("outputFile"))(validateSimpleAvroChecker)
    // Run the test job.
      .runHadoop
      .finish
  }

  test("A job that reads using the generic API is run.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    val specificRecord = new HashSpec()
    specificRecord.setHashType(HashType.MD5)
    specificRecord.setHashSize(13)
    specificRecord.setSuppressKeyMaterialization(true)
    def genericReadInput(uri: String): List[(EntityId, Seq[FlowCell[HashSpec]])] = {
      List((EntityId("row01"), slice("family:column3", (10L, specificRecord))))
    }

    def validateGenericRead(outputBuffer: Buffer[(Int, Int)]): Unit = {
      assert (1 === outputBuffer.size)
      // There exactly 1 record with hash_size of 13.
      assert ((13, 1) === outputBuffer(0))
    }

    val jobTest = JobTest(new GenericAvroReadJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumns("family:column3" -> 'records)
            .build,
            genericReadInput(uri))
        .sink(Tsv("outputFile"))(validateGenericRead)

    // Run in local mode
    jobTest.run.finish

    // Run in hadoop mode
    jobTest.runHadoop.finish
  }

  test("A job that reads using the specific API is run.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    val specificRecord = new HashSpec()
    specificRecord.setHashType(HashType.MD5)
    specificRecord.setHashSize(13)
    specificRecord.setSuppressKeyMaterialization(true)
    def genericReadInput(uri: String): List[(EntityId, Seq[FlowCell[HashSpec]])] = {
      List((EntityId("row01"), slice("family:column3", (10L, specificRecord))))
    }

    def validateSpecificRead(outputBuffer: Buffer[(Int, Int)]): Unit = {
      assert (1 === outputBuffer.size)
      // There exactly 1 record with hash_size of 13.
      assert ((13, 1) === outputBuffer(0))
    }

    // Defining the KijiSource directly like this is unfortunate, but necessary to make sure that
    // the KijiSource referenced here and the one used within SpecificAvroReadJob are identical (the
    // KijiSources are used as keys for a map of buffers for test code).
    val ksource = new KijiSource(
        tableAddress = uri,
        timeRange = All,
        timestampField = None,
        inputColumns = Map('records -> ColumnInputSpec(
          "family:column3", schemaSpec = Specific(classOf[HashSpec]))),
        outputColumns = Map('records -> QualifiedColumnOutputSpec.builder
            .withColumn("family", "column3")
            .build)
    )

    val jobTest = JobTest(new SpecificAvroReadJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(ksource, genericReadInput(uri))
        .sink(Tsv("outputFile"))(validateSpecificRead)

    // Run in local mode
    jobTest.run.finish

    // Run in hadoop mode
    jobTest.runHadoop.finish
  }

  test("A job that writes using the generic API is run.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input to use with Text source.
    val genericWriteInput: List[(String, String)] = List(
        ( "0", "zero" ),
        ( "1", "one" ))

    // Validates the output buffer contains the same as the input buffer.
    def validateGenericWrite(outputBuffer: Buffer[(EntityId, Seq[FlowCell[GenericRecord]])]) {
      val inputMap: Map[Long, String] = genericWriteInput.map { t => t._1.toLong -> t._2 }.toMap
      outputBuffer.foreach { t: (EntityId, Seq[FlowCell[GenericRecord]]) =>
        val entityId = t._1
        val record = t._2.head.datum

        val s = record.get("s").asInstanceOf[String]
        val l = record.get("l").asInstanceOf[Long]

        assert(entityId(0) === s)
        assert(inputMap(l) === s)
      }
    }

    val jobTest = JobTest(new GenericAvroWriteJob(_))
      .arg("input", "inputFile")
      .arg("output", uri)
      .source(TextLine("inputFile"), genericWriteInput)
      .sink(KijiOutput.builder
          .withTableURI(uri)
          .withColumns('record -> "family:column4")
          .build
      )(validateGenericWrite)

    // Run in local mode.
    jobTest.run.finish

    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test ("A job that writes to map-type column families is run.") {
    // URI of the Kiji table to use.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Input text.
    val mapTypeInput: List[(String, String)] = List(
        ("0", "dogs 4"),
        ("1", "cats 5"),
        ("2", "fish 3"))

    // Validate output.
    def validateMapWrite(
        outputBuffer: Buffer[(EntityId,Seq[FlowCell[GenericRecord]])]
    ): Unit = {
      assert (1 === outputBuffer.size)
      val outputSlice = outputBuffer(0)._2
      val outputSliceMap = outputSlice.groupBy(_.qualifier)
      assert (4 === outputSliceMap("dogs").head.datum)
      assert (5 === outputSliceMap("cats").head.datum)
      assert (3 === outputSliceMap("fish").head.datum)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new MapWriteJob(_))
        .arg("input", "inputFile")
        .arg("table", uri)
        .source(TextLine("inputFile"), mapTypeInput)

        .sink(KijiOutput.builder
            .withTableURI(uri)
            .withColumnSpecs('resultCount -> ColumnFamilyOutputSpec.builder
                .withFamily("searches")
                .withQualifierSelector('terms)
                .build)
            .build
        )(validateMapWrite)

    // Run the test.
    jobTest.run.finish
    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test ("A job that writes to map-type column families with numeric column qualifiers is run.") {
    // URI of the Kiji table to use.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Create input using mapSlice.
    val mapTypeInput: List[(EntityId, Seq[FlowCell[String]])] = List(
        ( EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs")) ),
        ( EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat")) ),
        ( EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish")) ))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 3)
      val outputSet = outputBuffer.map { value: Tuple1[String] =>
        value._1
      }.toSet
      assert (outputSet.contains("0 dogs"), "Failed on \"0 dogs\" test")
      assert (outputSet.contains("1 cat"), "Failed on \"1 cat\" test")
      assert (outputSet.contains("2 fish"), "Failed on \"2 fish\" test")
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new MapSliceJob(_))
        .arg("input", uri)
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumns("animals" -> 'terms)
            .build, mapTypeInput)
        .sink(Tsv("outputFile"))(validateTest)

    // Run the test.
    jobTest.run.finish
    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A job that joins two pipes, on string keys, is run in both local and hadoop mode.") {
    // URI of the Kiji table to use.
    val uri: String = doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Create input from Kiji table.
    val joinKijiInput: List[(EntityId, Seq[FlowCell[String]])] = List(
        ( EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs")) ),
        ( EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat")) ),
        ( EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish")) ))

    // Create input from side data.
    val sideInput: List[(String, String)] = List( ("0", "0row"), ("1", "2row") )

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinOnStringsJob(_))
        .arg("input", uri)
        .arg("side-input", "sideInputFile")
        .arg("output", "outputFile")
        .source(KijiInput.builder
            .withTableURI(uri)
            .withColumns("animals" -> 'animals)
            .build, joinKijiInput)
        .source(TextLine("sideInputFile"), sideInput)
        .sink(Tsv("outputFile"))(validateTest)

    // Run the test in local mode.
    jobTest.run.finish

    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }
}

/** Companion object for KijiSourceSuite. Contains test jobs. */
object KijiSourceSuite {
  /**
   * A job that extracts the most recent string value from the column "family:column1" for all rows
   * in a Kiji table, and then counts the number of occurrences of those strings across rows.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class WordCountJob(args: Args) extends KijiJob(args) {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("family:column1" -> 'word)
        .build
        // Sanitize the word.
        .map('word -> 'cleanword) { words:Seq[FlowCell[CharSequence]] =>
          words.head.datum
              .toString()
              .toLowerCase()
        }
        // Count the occurrences of each word.
        .groupBy('cleanword) { occurences => occurences.size }
        // Write the result to a file.
        .write(Tsv(args("output")))
  }

  /**
   * A job that takes the most recent string value from the column "family:column1" and adds
   * the letter 's' to the end of it. It passes through the column "family:column2" without
   * any changes.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class TwoColumnJob(args: Args) extends KijiJob(args) {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("family:column1" -> 'word1, "family:column2" -> 'word2)
        .build
        .map('word1 -> 'pluralword) { words: Seq[FlowCell[CharSequence]] =>
          words.head.datum.toString() + "s"
        }
        .write(Tsv(args("output")))
  }

  /**
   * A job that requests specific number of versions and buckets the results by the number of
   * versions.  The result is pairs of number of versions and the number of rows with that number
   * of versions.
   *
   * @param source that the job will use.
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class VersionsJob(source: KijiSource)(args: Args) extends KijiJob(args) {
    source
        // Count the size of words (number of versions).
        .map('words -> 'versioncount) { words: Seq[FlowCell[String]]=>
          words.size
        }
        .groupBy('versioncount) (_.size)
        .write(Tsv(args("output")))
  }

  class MultipleTimestampsImportJob(args: Args) extends KijiJob(args) {
    // Setup input.
    TextLine(args("input"))
        .read
        // new  the words in each line.
        .map('line -> ('timestamp, 'entityId, 'word)) { line: String =>
          val Array(timestamp, eid, token) = line.split("\\s+")
          (timestamp.toLong, EntityId(eid), token)
        }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput.builder
            .withTableURI(args("output"))
            .withTimestampField('timestamp)
            .withColumns('word -> "family:column1")
            .build)
  }

  /**
   * A job that, for each line in a text file, splits the line into words,
   * and for each word occurrence writes a copy of the word to the column "family:column1" in a
   * row for that word in a Kiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class ImportJob(args: Args) extends KijiJob(args) {
    // Setup input.
    TextLine(args("input"))
        .read
        // new  the words in each line.
        .flatMap('line -> 'word) { line : String => line.split("\\s+") }
        // Generate an entityId for each word.
        .map('word -> 'entityId) { _: String =>
            EntityId(UUID.randomUUID().toString()) }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput.builder
            .withTableURI(args("output"))
            .withColumns('word -> "family:column1")
            .build)
  }

  /**
   * A job that, given lines of text, writes each line to row for the line in a Kiji table,
   * at the column "family:column1", with the offset provided for each line being used as the
   * timestamp.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class ImportJobWithTime(args: Args) extends KijiJob(args) {
    // Setup input.
    TextLine(args("input"))
        .read
        // Generate an entityId for each line.
        .map('line -> 'entityId) { EntityId(_: String) }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput.builder
            .withTableURI(args("output"))
            .withTimestampField('offset)
            .withColumns('line -> "family:column1")
            .build)
  }

  /**
   * A job that given input from a Kiji table, ensures the type is accurate.
   *
   * @param source that the job will use.
   * @param args to the job. The input URI for the table and the output file.
   */
  class AvroToScalaChecker(source: KijiSource)(args: Args) extends KijiJob(args) {
    source

        .flatMap('word -> 'matches) { word: Seq[FlowCell[CharSequence]] =>
          word.map { cell: FlowCell[CharSequence] =>
            val value = cell.datum
            if (value.isInstanceOf[CharSequence]) {
              "true"
            } else {
              "false"
            }
          }
        }
        .groupBy('matches) (_.size)
        .write(Tsv(args("output")))
  }

  /**
   * A job that uses the generic API, getting the "hash_size" field from a generic record, and
   * writes the number of records that have a certain hash_size.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class GenericAvroReadJob(args: Args) extends KijiJob(args) {
    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("family:column3" -> 'records)
        .build
        .map('records -> 'hashSizeField) { slice: Seq[FlowCell[GenericRecord]] =>
          slice.head match {
            case FlowCell(_, _, _, record: GenericRecord) => {
              record
                  .get("hash_size")
                  .asInstanceOf[Int]
            }
          }
        }
        .groupBy('hashSizeField)(_.size)
        .write(Tsv(args("output")))
  }

  /**
   * A job that uses the generic API, getting the "hash_size" field from a generic record, and
   * writes the number of records that have a certain hash_size.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   */
  class SpecificAvroReadJob(args: Args) extends KijiJob(args) {
    // Want to read some data out to 'records and then write it back to a Tsv
    val ksource = new KijiSource(
        tableAddress = args("input"),
        timeRange = All,
        timestampField = None,
        inputColumns = Map('records -> ColumnInputSpec(
            "family:column3", schemaSpec = Specific(classOf[HashSpec]))),
        outputColumns = Map('records -> QualifiedColumnOutputSpec.builder
            .withColumn("family", "column3")
            .build))
    ksource
        .map('records -> 'hashSizeField) { slice: Seq[FlowCell[HashSpec]] =>
          val FlowCell(_, _, _, record) = slice.head
          record.getHashSize
        }
        .groupBy('hashSizeField)(_.size)
        .write(Tsv(args("output")))
  }

  /**
   * A job that uses the generic API, creating a record containing the text from the input,
   * and writing it to a Kiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class GenericAvroWriteJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("output")
    TextLine(args("input"))
        .read
        .map('offset -> 'timestamp) { offset: String => offset.toLong }
        .map('offset -> 'l) { offset: String => offset.toLong }
        // Generate an entityId for each line.
        .map('line -> 'entityId) { EntityId(_: String) }
        .rename('line -> 's)
        .packGenericRecord(('l, 's) -> 'record)(SimpleRecord.getClassSchema)
        // Write the results to the "family:column4" column of a Kiji table.
        .project('entityId, 'record)
        .write(KijiOutput.builder
            .withTableURI(args("output"))
            .withColumns('record -> "family:column4")
            .build)
  }

  /**
   * A job that writes to a map-type column family.  It takes text from the input and uses it as
   * search terms and the number of results returned for that term.  All of them belong to the same
   * entity, "my_eid".
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the path to a
   *     text file, and "output", which specifies the URI to a Kiji table.
   */
  class MapWriteJob(args: Args) extends KijiJob(args) {
    TextLine(args("input"))
        .read
        // Create an entity ID for each line (always the same one, here)
        .map('line -> 'entityId) { line: String => EntityId("my_eid") }
        // new  the number of result for each search term
        .map('line -> ('terms, 'resultCount)) { line: String =>
          (line.split(" ")(0), line.split(" ")(1).toInt)
        }
        // Write the results to the "family:column1" column of a Kiji table.
        .write(KijiOutput.builder
            .withTableURI(args("table"))
            .withColumnSpecs('resultCount -> ColumnFamilyOutputSpec.builder
                .withFamily("searches")
                .withQualifierSelector('terms)
                .build)
            .build)
  }

  /**
   * A job that tests map-type column families using sequences of cells and outputs the results to
   * a TSV.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class MapSliceJob(args: Args) extends KijiJob(args) {
    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("animals" -> 'terms)
        .build
        .map('terms -> 'values) { terms: Seq[FlowCell[CharSequence]] => terms.head.datum }
        .project('values)
        .write(Tsv(args("output")))
  }

  /**
   * A job that tests joining two pipes, on String keys.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class JoinOnStringsJob(args: Args) extends KijiJob(args) {
    val sidePipe = TextLine(args("side-input"))
        .read
        .map('line -> 'entityId) { line: String => EntityId(line) }

    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("animals" -> 'animals)
        .build
        .map('animals -> 'terms) { animals: Seq[FlowCell[CharSequence]] =>
          animals.head.datum.toString.split(" ")(0) + "row" }
        .discard('entityId)
        .joinWithSmaller('terms -> 'line, sidePipe)
        .write(Tsv(args("output")))
  }
}

