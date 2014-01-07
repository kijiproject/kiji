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

import com.twitter.scalding.JobTest
import com.twitter.scalding.Tsv
import com.twitter.scalding.Args
import org.apache.hadoop.hbase.util.Bytes
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.RowFilterSpec.KijiRandomRowFilterSpec
import org.kiji.express.flow.RowFilterSpec.NoRowFilterSpec
import org.kiji.express.flow.RowRangeSpec.AllRows
import org.kiji.express.flow.RowRangeSpec.BetweenRows
import org.kiji.express.flow.RowRangeSpec.FromRow
import org.kiji.express.flow.RowRangeSpec.UntilRow
import org.kiji.express.flow.util.ResourceUtil.doAndRelease
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

@RunWith(classOf[JUnitRunner])
class RowSpecSuite extends KijiSuite {
  /** Simple table layout to use for tests. The row keys are hashed. */
  val simpleLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  /** Sample row keys. */
  val eidFactory = EntityIdFactory.getFactory(simpleLayout)
  val eid1 = EntityId.fromJavaEntityId(eidFactory.getEntityIdFromHBaseRowKey(Bytes.toBytes("1")))
  val eid2 = EntityId.fromJavaEntityId(eidFactory.getEntityIdFromHBaseRowKey(Bytes.toBytes("2")))
  val eid3 = EntityId.fromJavaEntityId(eidFactory.getEntityIdFromHBaseRowKey(Bytes.toBytes("3")))
  val eid4 = EntityId.fromJavaEntityId(eidFactory.getEntityIdFromHBaseRowKey(Bytes.toBytes("4")))
  val eid5 = EntityId.fromJavaEntityId(eidFactory.getEntityIdFromHBaseRowKey(Bytes.toBytes("5")))

  /** Sample input tuples. */
  def sampleInput(uri: String): List[(EntityId, Seq[FlowCell[String]])] = {
    List(
        ( eid1, slice("family:column1", (1L, "one")) ),
        ( eid2, slice("family:column1", (2L, "two")) ),
        ( eid3, slice("family:column1", (3L, "three")) ),
        ( eid4, slice("family:column1", (4L, "four")) ),
        ( eid5, slice("family:column1", (5L, "five")) ))
  }

  /** Set up the JobTest. */
  def scanJobTest(
      uri: String,
      rowRangeSpec: RowRangeSpec,
      rowFilterSpec: RowFilterSpec,
      validateScan: Buffer[Tuple1[String]] => Unit): JobTest = {
    JobTest(new IntervalScanJob(_, rowRangeSpec, rowFilterSpec))
      .arg("input", uri)
      .arg("output", "outputFile")
      .source(
          KijiInput.builder
              .withTableURI(uri)
              .withColumns("family:column1" -> 'word)
              .withRowRangeSpec(rowRangeSpec)
              .withRowFilterSpec(rowFilterSpec)
              .build,
          sampleInput(uri))
      .sink(Tsv("outputFile"))(validateScan)
  }

  test("Interval scans start from startEntityId.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Method to validate interval scan.
    def validateScan(outputBuffer: Buffer[Tuple1[String]]) {
      val outputSet = outputBuffer.map { value: Tuple1[String] =>
        value._1
      }.toSet
      assert(outputSet.size === 3)
      assert(outputSet.contains("three"))
      assert(outputSet.contains("four"))
      assert(outputSet.contains("five"))
    }

    // Set up row specification.
    val rowRangeSpec: RowRangeSpec = FromRow(eid3)
    val rowFilterSpec = NoRowFilterSpec

    // Build test job.
    val jobTest = scanJobTest(uri, rowRangeSpec, rowFilterSpec, validateScan)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("Interval scans until limitEntityId.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Method to validate interval scan.
    def validateScan(outputBuffer: Buffer[Tuple1[String]]) {
      val outputSet = outputBuffer.map { value: Tuple1[String] =>
        value._1
      }.toSet
      assert(outputSet.size === 3)
      assert(outputSet.contains("one"))
      assert(outputSet.contains("two"))
      assert(outputSet.contains("three"))
    }

    // Set up row specification.
    val rowRangeSpec: RowRangeSpec = UntilRow(eid4)
    val rowFilterSpec = NoRowFilterSpec

    // Build test job.
    val jobTest = scanJobTest(uri, rowRangeSpec, rowFilterSpec, validateScan)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("Random row filter scans selects no rows when selection chance is 0.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Method to validate interval scan.
    def validateScan(outputBuffer: Buffer[Tuple1[String]]) {
      val outputSet = outputBuffer.map { value: Tuple1[String] =>
        value._1
      }.toSet
      assert(outputSet.size === 0)
    }

    // Set up row specification.
    val rowRangeSpec: RowRangeSpec = AllRows
    val rowFilterSpec = KijiRandomRowFilterSpec(0.0F)


    // Build test job.
    val jobTest = scanJobTest(uri, rowRangeSpec, rowFilterSpec, validateScan)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  test("Random row filter scans selects all rows when selection chance is 1.0.") {
    // Create test Kiji table.
    val uri: String = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    // Method to validate interval scan.
    def validateScan(outputBuffer: Buffer[Tuple1[String]]) {
      val outputSet = outputBuffer.map { value: Tuple1[String] =>
        value._1
      }.toSet
      assert(outputSet.size === 5)
    }

    // Set up row specification.
    val rowRangeSpec: RowRangeSpec = AllRows
    val rowFilterSpec = KijiRandomRowFilterSpec(1.0F)

    // Build test job.
    val jobTest = scanJobTest(uri, rowRangeSpec, rowFilterSpec, validateScan)

    // Run in local mode.
    jobTest.run.finish
    // Run in hadoop mode.
    jobTest.runHadoop.finish
  }

  /**
   * A job that scans a table based on the row specification.
   *
   * @param args to the job. Two arguments are expected: "input", which should specify the URI
   *     to the Kiji table the job should be run on, and "output", which specifies the output
   *     Tsv file.
   * @param rowRangeSpec specification of the row range to scan.
   * @param rowFilterSpec specification of the row filter to scan with.
   */
  class IntervalScanJob(args: Args,
      rowRangeSpec: RowRangeSpec,
      rowFilterSpec: RowFilterSpec) extends KijiJob(args) {
    // Setup input to bind values from the "family:column1" column to the symbol 'word.
    KijiInput.builder
        .withTableURI(args("input"))
        .withColumns("family:column1" -> 'word)
        .withRowRangeSpec(rowRangeSpec)
        .withRowFilterSpec(rowFilterSpec)
        .build
        // Sanitize the word.
        .map('word -> 'cleanword) { words:Seq[FlowCell[CharSequence]] =>
          words.head.datum.toString().toLowerCase()
        }
        .project('cleanword)
        // Write the result to a file.
        .write(Tsv(args("output")))
  }
}
