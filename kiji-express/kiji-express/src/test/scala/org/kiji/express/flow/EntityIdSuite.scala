/**
 * (c) Copyright 2014 WibiData, Inc.
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
import scala.collection.mutable.Buffer

import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.TextLine
import com.twitter.scalding.Tsv
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.EntityId.HashedEntityId
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.express.flow.util.TestingResourceUtil
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

/**
 * Unit tests for [[org.kiji.express.flow.EntityId]].
 */
@RunWith(classOf[JUnitRunner])
class EntityIdSuite extends KijiSuite {

  import org.kiji.express.flow.EntityIdSuite._

  /** Table layout with formatted entity IDs to use for tests. */
  val formattedEntityIdLayout: KijiTableLayout =
      TestingResourceUtil.layout(KijiTableLayouts.FORMATTED_RKF)
  // Create a table to use for testing
  val formattedTableUri: KijiURI =
      ResourceUtil.doAndRelease(makeTestKijiTable(formattedEntityIdLayout)) { table: KijiTable =>
        table.getURI
      }

  /** Table layout with hashed entity IDs to use for tests. */
  val hashedEntityIdLayout: KijiTableLayout =
      TestingResourceUtil.layout(KijiTableLayouts.HASHED_FORMATTED_RKF)
  // Create a table to use for testing
  val hashedTableUri: KijiURI =
      ResourceUtil.doAndRelease(makeTestKijiTable(hashedEntityIdLayout)) { table: KijiTable =>
        table.getURI
      }

  val configuration: Configuration = HBaseConfiguration.create()

  val formattedEidFactory = EntityIdFactory.getFactory(formattedEntityIdLayout)
  val hashedEidFactory = EntityIdFactory.getFactory(hashedEntityIdLayout)

  // ------- "Unit tests" for comparisons and creation. -------
  test("Create an Express EntityId from a Kiji EntityId and vice versa in a formatted table.") {
    val expressEid = EntityId("test", "1", "2", 1, 7L)
    val kijiEid = expressEid.toJavaEntityId(formattedEidFactory)
    val expected: java.util.List[AnyRef] =
        Seq[AnyRef]("test", "1", "2", 1: java.lang.Integer, 7L: java.lang.Long).asJava

    assert(expected === kijiEid.getComponents)

    val recreate = EntityId.fromJavaEntityId(kijiEid)

    assert(expressEid === recreate)
    assert(recreate(0) === "test")
  }

  test("Create an Express EntityId from a Kiji EntityId and vice versa in a hashed table.") {
    val origKijiEid = hashedEidFactory.getEntityId("test")

    val expressEid = HashedEntityId(origKijiEid.getHBaseRowKey)
    val expressToKijiEid = expressEid.toJavaEntityId(hashedEidFactory)

    val recreate = EntityId.fromJavaEntityId(expressToKijiEid)
    assert(recreate.components.equals(List(origKijiEid.getHBaseRowKey)))
  }

  test("Creating an EntityId from a Hashed table fails if there is more than one component.") {
    val eid: EntityId = EntityId("one", 2)
    val exception = intercept[org.kiji.schema.EntityIdException] {
      eid.toJavaEntityId(hashedEidFactory)
    }
    assert(exception.getMessage.contains("Too many components"))
  }

  test("Test equality between two EntityIds.") {
    val eidComponents1: EntityId = EntityId("test", 1)
    val eidComponents2: EntityId = EntityId("test", 1)

    assert(eidComponents1 === eidComponents2)
    assert(eidComponents2 === eidComponents1)
  }

  test("Test comparison between two EntityIds.") {
    val eidComponents1: EntityId = EntityId("test", 2)
    val eidComponents2: EntityId = EntityId("test", 3)

    assert(eidComponents2 > eidComponents1)
    assert(eidComponents1 < eidComponents2)
  }

  test("Test comparison between two EntityIds with different lengths.") {
    val eidComponents1: EntityId = EntityId("test", 2)
    val eidComponents2: EntityId = EntityId("test", 2, 1)

    assert(eidComponents2 > eidComponents1)
    assert(eidComponents1 < eidComponents2)
  }

  test("Test comparison between two EntityIds with different formats fails.") {
    val eidComponents1: EntityId = EntityId("test", 2)
    val eidComponents2: EntityId = EntityId("test", 2L)

    val exception = intercept[EntityIdFormatMismatchException] {
      eidComponents1 < eidComponents2
    }

    // Exception message should be something like:
    // Mismatched Formats: Components: [java.lang.String,java.lang.Integer] and  Components:
    // [java.lang.String,java.lang.Long] do not match.

    assert(exception.getMessage.contains("String"))
    assert(exception.getMessage.contains("Integer"))
    assert(exception.getMessage.contains("Long"))
  }

  // ------- "integration tests" for joins. -------
  /** Simple table layout to use for tests. The row keys are hashed. */
  val simpleLayout: KijiTableLayout =
    TestingResourceUtil.layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Table layout using Avro schemas to use for tests. The row keys are formatted. */
  val avroLayout: KijiTableLayout = TestingResourceUtil.layout("layout/avro-types.json")

  test("Runs a job that joins two pipes, on user-created EntityIds.") {
    // Create main input.
    val mainInput: List[(String, String)] = List(
      ("0", "0row"),
      ("1", "1row"),
      ("2", "2row"))

    // Create input from side data.
    val sideInput: List[(String, String)] = List(("0", "0row"), ("1", "2row"))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinUserEntityIdsJob(_))
      .arg("input", "mainInputFile")
      .arg("side-input", "sideInputFile")
      .arg("output", "outputFile")
      .source(TextLine("mainInputFile"), mainInput)
      .source(TextLine("sideInputFile"), sideInput)
      .sink(Tsv("outputFile"))(validateTest)

    // Run the test in local mode.
    jobTest.run.finish

    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("Runs a job that joins two pipes, on user-created and from a table (formatted) EntityIds.") {
    // URI of the Kiji table to use.
    val uri: String = ResourceUtil.doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI.toString
    }

    // Create input from Kiji table.
    val joinKijiInput: List[(EntityId, Seq[FlowCell[String]])] = List(
      (EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs"))),
      (EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat"))),
      (EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish"))))

    // Create input from side data.
    val sideInput: List[(String, String)] = List(("0", "0row"), ("1", "2row"))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinUserAndFormattedFromTableJob(_))
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

  test("Runs a job that joins two pipes, on EntityIds from a table (hashed), in local mode.") {
    // URI of the hashed Kiji table to use.
    val uri: String =
        ResourceUtil.doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
          table.getURI.toString
        }

    // Create input from hashed Kiji table.
    val joinInput1: List[(EntityId, Seq[FlowCell[String]])] = List(
      (EntityId("0row"), slice("family:column1", (0L, "0 dogs"))),
      (EntityId("1row"), slice("family:column1", (0L, "1 cat"))),
      (EntityId("2row"), slice("family:column1", (0L, "2 fish"))))


    // Create input from hashed Kiji table.
    val joinInput2: List[(EntityId, Seq[FlowCell[String]])] = List(
      (EntityId("0row"), slice("family:column2", (0L, "0 boop"))),
      (EntityId("2row"), slice("family:column2", (1L, "1 cat")))
      )

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinHashedEntityIdsJob(_))
      .arg("input1", uri)
      .arg("input2", uri)
      .arg("output", "outputFile")
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("family:column1" -> 'animals)
          .build, joinInput1)
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("family:column2" -> 'slice)
          .build, joinInput2)
      .sink(Tsv("outputFile"))(validateTest)

    // Run the test in local mode.
    jobTest.run.finish
  }

  test("Runs a job that joins two pipes, on EntityIds from a table (hashed), in hadoop mode.") {
    // URI of the hashed Kiji table to use.
    val uri: String =
        ResourceUtil.doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
          table.getURI.toString
        }

    // Create input from hashed Kiji table.
    val joinInput1: List[(EntityId, Seq[FlowCell[String]])] = List(
      (EntityId("0row"), slice("family:column1", (0L, "0 dogs"))),
      (EntityId("1row"), slice("family:column1", (0L, "1 cat"))),
      (EntityId("2row"), slice("family:column1", (0L, "2 fish"))))

    // Create input from hashed Kiji table.
    val joinInput2: List[(EntityId, Seq[FlowCell[String]])] = List(
      (EntityId("0row"), slice("family:column2", (0L, "0 boop"))),
      (EntityId("2row"), slice("family:column2", (0L, "2 beep"))))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinHashedEntityIdsJob(_))
      .arg("input1", uri)
      .arg("input2", uri)
      .arg("output", "outputFile")
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("family:column1" -> 'animals)
          .build, joinInput1)
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("family:column2" -> 'slice)
          .build, joinInput2)
      .sink(Tsv("outputFile"))(validateTest)

    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("A job that joins two pipes, on EntityIds from a table (formatted) in local mode.") {
    // URI of a formatted Kiji table to use.
    val uri: String = ResourceUtil.doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI.toString
    }

    // Create input from formatted Kiji table.
    val joinInput1: List[(EntityId, Seq[FlowCell[Int]])] = List(
      (EntityId("0row"), mapSlice("searches", ("0column", 0L, 0))),
      (EntityId("2row"), mapSlice("searches", ("0column", 0L, 2))))

    // Create input from formatted Kiji table.
    val joinInput2: List[(EntityId, Seq[FlowCell[String]])] = List(
      (EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs"))),
      (EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat"))),
      (EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish"))))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinFormattedEntityIdsJob(_))
      .arg("input1", uri)
      .arg("input2", uri)
      .arg("output", "outputFile")
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("searches" -> 'searches)
          .build, joinInput1)
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("animals" -> 'animals)
          .build, joinInput2)
      .sink(Tsv("outputFile"))(validateTest)

    // Run the test in local mode.
    jobTest.run.finish
  }

  test("A job that joins two pipes, on EntityIds from a table (formatted) in hadoop mode.") {
    // URI of a formatted Kiji table to use.
    val uri: String = ResourceUtil.doAndRelease(makeTestKijiTable(avroLayout)) { table: KijiTable =>
      table.getURI.toString
    }

    // Create input from formatted Kiji table.
    val joinInput1: List[(EntityId, Seq[FlowCell[Int]])] = List(
      (EntityId("0row"), mapSlice("searches", ("0column", 0L, 0))),
      (EntityId("2row"), mapSlice("searches", ("0column", 0L, 2))))

    // Create input from formatted Kiji table.
    val joinInput2: List[(EntityId, Seq[FlowCell[String]])] = List(
      (EntityId("0row"), mapSlice("animals", ("0column", 0L, "0 dogs"))),
      (EntityId("1row"), mapSlice("animals", ("0column", 0L, "1 cat"))),
      (EntityId("2row"), mapSlice("animals", ("0column", 0L, "2 fish"))))

    // Validate output.
    def validateTest(outputBuffer: Buffer[Tuple1[String]]): Unit = {
      assert(outputBuffer.size === 2)
    }

    // Create the JobTest for this test.
    val jobTest = JobTest(new JoinFormattedEntityIdsJob(_))
      .arg("input1", uri)
      .arg("input2", uri)
      .arg("output", "outputFile")
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("searches" -> 'searches)
          .build, joinInput1)
      .source(KijiInput.builder
          .withTableURI(uri)
          .withColumns("animals" -> 'animals)
          .build, joinInput2)
      .sink(Tsv("outputFile"))(validateTest)

    // Run the test in hadoop mode.
    jobTest.runHadoop.finish
  }
}

/** Companion object for EntityIdSuite. Contains test jobs. */
object EntityIdSuite {
  /**
   * A job that tests joining two pipes, on user-constructed EntityIds.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class JoinUserEntityIdsJob(args: Args) extends KijiJob(args) {
    val sidePipe = TextLine(args("side-input"))
        .read
        .map('line -> 'entityId) { line: String => EntityId(line) }
        .project('entityId)

    TextLine(args("input"))
        .map('line -> 'entityId) { line: String => EntityId(line) }
        .joinWithSmaller('entityId -> 'entityId, sidePipe)
        .write(Tsv(args("output")))
  }

  /**
   * A job that tests joining two pipes, one with a user-constructed EntityId and one with
   * a formatted EntityId from a Kiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class JoinUserAndFormattedFromTableJob(args: Args) extends KijiJob(args) {
    val sidePipe = TextLine(args("side-input"))
        .read
        .map('line -> 'entityId) { line: String => EntityId(line) }
        .project('entityId)

    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("animals" -> 'animals)
        .build

        .map('animals -> 'terms) { animals: Seq[FlowCell[CharSequence]] => animals.toString }
        .joinWithSmaller('entityId -> 'entityId, sidePipe)
        .write(Tsv(args("output")))
  }

  /**
   * A job that tests joining two pipes, one with a user-constructed EntityId and one with
   * a hashed EntityId from a Kiji table.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class JoinUserAndHashedFromTableJob(args: Args) extends KijiJob(args) {
    val sidePipe = TextLine(args("side-input"))
        .read
        .map('line -> 'entityId) { line: String => EntityId(line) }
        .project('entityId)

    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("family:column1" -> 'slice)
        .build
        .map('slice -> 'terms) { slice: Seq[FlowCell[CharSequence]] => slice.head.datum.toString }
        .joinWithSmaller('entityId -> 'entityId, sidePipe)
        .write(Tsv(args("output")))
  }

  /**
   * A job that tests joining two pipes, on EntityIds from a table with row key format HASHED.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class JoinHashedEntityIdsJob(args: Args) extends KijiJob(args) {
    val pipe1 = KijiInput.builder
        .withTableURI(args("input1"))
        .withColumns("family:column1" -> 'animals)
        .build

    KijiInput.builder
        .withTableURI(args("input2"))
        .withColumns("family:column2" -> 'slice)
        .build
        .map('animals -> 'animal) {
          slice: Seq[FlowCell[CharSequence]] => slice.head.datum.toString
        }

    KijiInput.builder
        .withTableURI(args("input2"))
        .withColumns("family:column2" -> 'slice)
        .build
        .map('slice -> 'terms) { slice:Seq[FlowCell[CharSequence]] => slice.head.datum.toString }
        .joinWithSmaller('entityId -> 'entityId, pipe1)
        .write(Tsv(args("output")))
  }

  /**
   * A job that tests joining two pipes, on EntityIds from a table with row key format formatted.
   *
   * @param args to the job. Two arguments are expected: "input", which specifies the URI to a
   *     Kiji table, and "output", which specifies the path to a text file.
   */
  class JoinFormattedEntityIdsJob(args: Args) extends KijiJob(args) {
    val pipe1 = KijiInput.builder
        .withTableURI(args("input1"))
        .withColumns("searches" -> 'searches)
        .build
        .map('searches -> 'term) { slice:Seq[FlowCell[Int]] => slice.head.datum }

    KijiInput.builder
        .withTableURI(args("input2"))
        .withColumns("animals" -> 'animals)
        .build
        .map('animals -> 'animal) {
          slice: Seq[FlowCell[CharSequence]] => slice.head.datum.toString
        }
        .joinWithSmaller('entityId -> 'entityId, pipe1)
        .write(Tsv(args("output")))
  }
}
