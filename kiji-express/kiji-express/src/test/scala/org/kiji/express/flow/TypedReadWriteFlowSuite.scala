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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.express.flow

import java.util.UUID

import scala.Some
import scala.collection.JavaConversions.asScalaIterator

import org.junit
import org.junit.Before
import com.twitter.scalding.TypedSink
import com.twitter.scalding.TypedPipe
import com.twitter.scalding.Mode
import com.twitter.scalding.Args
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import org.apache.avro.util.Utf8
import org.apache.hadoop.mapred.JobConf

import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.Kiji
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.express.avro.SimpleRecord
import org.kiji.schema.util.InstanceBuilder
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef
import org.kiji.express.flow.util.ResourceUtil

class TypedReadWriteFlowSuite extends KijiClientTest{

  private var kiji: Kiji = null

  private def kijiSink (kijiUri:KijiURI): TypedSink[Seq[ExpressColumnOutput[_]]] = {
    KijiOutput.typedSinkForTable(kijiUri)
  }

  @Before
  def setupKijiForTypedTest(): Unit = {
    kiji = createTestKiji()
  }

  def setupKijiTableForTest(): KijiTable = {
    val testTableName = "%s_%s"
        .format(
            this.getClass.getSimpleName,
            UUID.randomUUID().toString
        )
        .replace("-", "_")
        .replace("$", "_")
    val testTableDDL =
        """
          |CREATE TABLE %s
          |    ROW KEY FORMAT RAW
          |    PROPERTIES (
          |        VALIDATION = STRICT
          |    )
          |    WITH LOCALITY GROUP default (
          |        MAXVERSIONS = INFINITY,
          |        TTL = FOREVER,
          |        COMPRESSED WITH NONE,
          |        FAMILY family
          |        (
          |            column column1 "string",
          |            column column2 "string",
          |            column column3 WITH SCHEMA CLASS org.kiji.express.avro.SimpleRecord,
          |            column column4 WITH SCHEMA CLASS org.kiji.express.avro.SimpleRecord
          |        )
          |    );
        """.stripMargin.format(testTableName, testTableName, testTableName)

    ResourceUtil.executeDDLString(kiji, testTableDDL)
    kiji.openTable(testTableName)
  }

  /**
   * Default input source used for the tests in this suite.
   *
   * @param kijiUri The uri for the kiji table.
   */
  private def defaultTestColumnInputSource(kijiUri: KijiURI) = KijiInput.typedBuilder
      .withTableURI(kijiUri)
      .withColumnSpecs(
          List(
              QualifiedColumnInputSpec
                  .builder
                  .withColumn("family", "column1")
                  .withMaxVersions(Int.MaxValue)
                  .build,
              QualifiedColumnInputSpec
                  .builder
                  .withColumn("family", "column2")
                  .withMaxVersions(Int.MaxValue)
                  .build,
              QualifiedColumnInputSpec
                  .builder
                  .withColumn("family", "column3")
                  .withMaxVersions(Int.MaxValue)
                  .withSchemaSpec(SchemaSpec.Specific(classOf[SimpleRecord]))
                  .build)
      ).build


  def validateAndDeleteWrites[T](
      row: String,
      columns: Seq[KijiColumnName],
      table: KijiTable,
      expected: Set[_]
  ): Unit = {
    ResourceUtil.doAndClose(table.openTableReader()) { reader =>
      val dataRequestBuilder: KijiDataRequestBuilder = KijiDataRequest.builder()
      columns.foreach { col: KijiColumnName =>
        dataRequestBuilder
            .addColumns(ColumnsDef.create()
                .withMaxVersions(Int.MaxValue)
                .add(col))}

      val rowData: KijiRowData = reader.get(
          table.getEntityId(row),
          dataRequestBuilder.build())

      val kijiCellSet: Set[KijiCell[_]] = columns.flatMap{ colName: KijiColumnName =>
        rowData.iterator[T](colName.getFamily, colName.getQualifier).toList
      }.toSet

      val actualSet:Set[_] = kijiCellSet.map { cell : KijiCell[_] => cell.getData}
      val entityId: JEntityId = table.getEntityId(row)

      ResourceUtil.doAndClose(table.openTableWriter()) { writer =>
        kijiCellSet.foreach { cell: KijiCell[_] =>
          writer.deleteCell(
              entityId,
              cell.getColumn.getFamily,
              cell.getColumn.getQualifier,
              cell.getTimestamp)
        }
      }
      assert(
          actualSet == expected,
          "actual: %s\nexpected: %s\noutput.".format(
              actualSet,
              expected))
    }
  }

  /**
   * Runs the test using the functions passed in as the parameters and validates the output.
   *
   * @param kijiTable The Kiji table used for the test.
   * @param jobInput  A function that takes in KijiUri as a parameter and returns an instance of
   *     TypedKijiSource.
   * @param jobBody A function that maps the input of the job to a Seq[ExpressColumnOutput] for the
   *     purpose of writing it to the sink.
   * @param sink The sink to be used for writing the test data.
   */
  def runTypedTest[T](
      kijiTable: KijiTable,
      jobInput: KijiURI => TypedKijiSource[ExpressResult],
      jobBody: TypedPipe[ExpressResult] => TypedPipe[Seq[ExpressColumnOutput[T]]],
      sink: TypedSink[Seq[ExpressColumnOutput[T]]],
      //validation params
      validationRow: String,
      validationColumns: Seq[KijiColumnName],
      expectedSet: Set[_]
  ): Unit = {
    class TypedKijiTest(args: Args) extends KijiJob(args) {
      jobBody(jobInput(kijiTable.getURI)).write(sink)
    }

    // Run in local mode.
    val localModeArgs = Mode.putMode(Local(strictSources = true), Args(List()))
    new TypedKijiTest(localModeArgs).run
    validateAndDeleteWrites(validationRow, validationColumns, kijiTable, expectedSet)

    //Run in Hdfs mode
    val hdfsModeArgs =
        Mode.putMode(Hdfs(strict = true, conf = new JobConf(getConf)), Args(Nil))
    new TypedKijiTest(hdfsModeArgs).run
    validateAndDeleteWrites(validationRow, validationColumns, kijiTable, expectedSet)
  }


  @junit.Test
  def testReadWriteTwoColumns() {

    val kijiTable: KijiTable = setupKijiTableForTest()

    new InstanceBuilder(kiji)
        .withTable(kijiTable)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column1")
                        .withValue("v1")
                    .withQualifier("column2")
                        .withValue("v2")
                    .build()

    def body(source: TypedPipe[ExpressResult]): TypedPipe[Seq[ExpressColumnOutput[Utf8]]] = {
      source.map { row: ExpressResult =>
        val mostRecent1 = row.mostRecentCell[Utf8]("family", "column1")
        val col1 = ExpressColumnOutput(
            EntityId("row2"),
            "family",
            "column1",
            mostRecent1.datum,
            version = Some(mostRecent1.version))

        val mostRecent2 = row.mostRecentCell[Utf8]("family", "column2")
        val col2 = ExpressColumnOutput(
            EntityId("row2"),
            "family",
            "column2",
            mostRecent2.datum,
            version = Some(mostRecent2.version))
        Seq(col1, col2)
      }
    }

    val columns: Seq[KijiColumnName] = List("column1", "column2").map{ col: String =>
      KijiColumnName.create("family", col)}
    try{
      runTypedTest[Utf8](
          kijiTable,
          defaultTestColumnInputSource,
          body,
          kijiSink(kijiTable.getURI),
          validationRow = "row2",
          validationColumns = columns,
          expectedSet = Array("v1", "v2").map { s => new Utf8(s) }.toSet)
    }
    finally {
      kijiTable.release()
    }
  }


  @junit.Test
  def testMultipleWritesOneColumn(): Unit = {
    val kijiTable: KijiTable = setupKijiTableForTest()

    new InstanceBuilder(kiji)
        .withTable(kijiTable)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column1")
                        .withValue(1l, "v1")
                        .withValue(2l, "v2")
                        .withValue(3l, "v3")
                        .withValue(4l, "v4")
                        .build()

    def body(source: TypedPipe[ExpressResult]): TypedPipe[Seq[ExpressColumnOutput[Utf8]]] = {
      source.map { row: ExpressResult =>
        row.qualifiedColumnCells[Utf8]("family", "column1")
            .map {
              cell: FlowCell[Utf8] =>
                ExpressColumnOutput(
                    EntityId("row1"),
                    "family",
                    "column2",
                    cell.datum,
                    version = Some(cell.version))
        }.toSeq
      }
    }

    try{
      runTypedTest[Utf8](
          kijiTable,
          defaultTestColumnInputSource,
          body,
          kijiSink(kijiTable.getURI),
          validationRow = "row1",
          validationColumns = Seq(KijiColumnName.create("family", "column2")),
          expectedSet = Array("v1", "v2", "v3", "v4").map { s => new Utf8(s) }.toSet)
    }
    finally {
      kijiTable.release()
    }
  }

  @junit.Test
  def testMultipleWriteSameVersion(): Unit =  {

    val kijiTable: KijiTable = setupKijiTableForTest()

    new InstanceBuilder(kiji)
        .withTable(kijiTable)
        .withRow("row1")
            .withFamily("family")
                .withQualifier("column1")
                    .withValue(1l, "v1")
                    .withValue(1l, "v2")
                    .build()

    def body(source: TypedPipe[ExpressResult]): TypedPipe[Seq[ExpressColumnOutput[Utf8]]] = {
      source.map { row: ExpressResult =>
        row.qualifiedColumnCells[Utf8]("family", "column1")
          .map {
          cell: FlowCell[Utf8] =>
            ExpressColumnOutput(
                EntityId("row1"),
                "family",
                "column2",
                cell.datum,
                version = Some(cell.version))
        }.toSeq
      }
    }

    try{
      runTypedTest[Utf8](
          kijiTable,
          defaultTestColumnInputSource,
          body,
          kijiSink(kijiTable.getURI),
          validationRow = "row1",
          validationColumns = Seq(KijiColumnName.create("family", "column2")),
          expectedSet = Set(new Utf8("v2")))
    }
    finally {
      kijiTable.release()
    }
  }

  @junit.Test
  def testReadWriteAvro(): Unit = {

    val kijiTable: KijiTable = setupKijiTableForTest()
    val simpleRecord1 = SimpleRecord.newBuilder().setL(4L).setS("4").build()
    val simpleRecord2 = SimpleRecord.newBuilder().setL(5L).setS("5").build()

    new InstanceBuilder(kiji)
        .withTable(kijiTable)
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("column3")
                        .withValue(1l, simpleRecord1)
                        .withValue(2l, simpleRecord2)
                    .build()

    def body(
        source: TypedPipe[ExpressResult]
    ): TypedPipe[Seq[ExpressColumnOutput[SimpleRecord]]] = {
        source.map { result: ExpressResult =>
          result.qualifiedColumnCells("family", "column3").map { cell : FlowCell[SimpleRecord] =>
            ExpressColumnOutput(
                EntityId("row1"),
                "family",
                "column4",
                cell.datum,
                SchemaSpec.Specific(classOf[SimpleRecord]),
                version = Some(cell.version))
          }.toSeq
        }
    }

    try{
      runTypedTest[SimpleRecord](
          kijiTable,
          defaultTestColumnInputSource,
          body,
          kijiSink(kijiTable.getURI),
          validationRow = "row1",
          validationColumns = Seq(KijiColumnName.create("family", "column4")),
          expectedSet = Set(
              SimpleRecord.newBuilder().setL(4L).setS("4").build(),
              SimpleRecord.newBuilder().setL(5L).setS("5").build()))
    }
    finally {
      kijiTable.release()
    }
  }
}
