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

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.Buffer

import java.io.OutputStream
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.collect.Maps
import com.google.common.base.Objects
import com.twitter.scalding.AccessMode
import com.twitter.scalding.Test
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Write
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.Cell
import org.kiji.express.EntityId
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.flow.framework.KijiTap
import org.kiji.express.flow.framework.LocalKijiScheme
import org.kiji.express.flow.framework.LocalKijiTap
import org.kiji.express.flow.framework.OutputContext
import org.kiji.express.util.GenericCellSpecs
import org.kiji.express.util.Resources._
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.CellSpec

/**
 * A read or write view of a Kiji table.
 *
 * A Scalding `Source` provides a view of a data source that can be read as Scalding tuples. It
 * is comprised of a Cascading tap [[cascading.tap.Tap]], which describes where the data is and how
 * to access it, and a Cascading Scheme [[cascading.scheme.Scheme]], which describes how to read
 * and interpret the data.
 *
 * When reading from a Kiji table, a `KijiSource` will provide a view of a Kiji table as a
 * collection of tuples that correspond to rows from the Kiji table. Which columns will be read
 * and how they are associated with tuple fields can be configured,
 * as well as the time span that cells retrieved must belong to.
 *
 * When writing to a Kiji table, a `KijiSource` views a Kiji table as a collection of tuples that
 * correspond to cells from the Kiji table. Each tuple to be written must provide a cell address
 * by specifying a Kiji `EntityID` in the tuple field `entityId`, a value to be written in a
 * configurable field, and (optionally) a timestamp in a configurable field.
 *
 * End-users cannot directly obtain instances of `KijiSource`. Instead,
 * they should use the factory methods provided as part of the [[org.kiji.express.flow]] module.
 *
 * @param tableAddress is a Kiji URI addressing the Kiji table to read or write to.
 * @param timeRange that cells read must belong to. Ignored when the source is used to write.
 * @param timestampField is the name of a tuple field that will contain cell timestamp when the
 *     source is used for writing. Specify `None` to write all
 *     cells at the current time.
 * @param inputColumns is a one-to-one mapping from field names to Kiji columns. The columns in the
 *     map will be read into their associated tuple fields.
 * @param outputColumns is a one-to-one mapping from field names to Kiji columns. Values from the
 *     tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class KijiSource private[express] (
    val tableAddress: String,
    val timeRange: TimeRange,
    val timestampField: Option[Symbol],
    val inputColumns: Map[Symbol, ColumnInputSpec] = Map(),
    val outputColumns: Map[Symbol, ColumnOutputSpec] = Map()
) extends Source {
  import KijiSource._

  private type HadoopScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]

  /** The URI of the target Kiji table. */
  private val tableUri: KijiURI = KijiURI.newBuilder(tableAddress).build()

  /** A Kiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  private val kijiScheme: KijiScheme =
      new KijiScheme(
          timeRange,
          timestampField,
          convertKeysToStrings(inputColumns),
          convertKeysToStrings(outputColumns)
      )

  /** A Kiji scheme intended to be used with Scalding/Cascading's local mode. */
  private val localKijiScheme: LocalKijiScheme =
      new LocalKijiScheme(
          timeRange,
          timestampField,
          convertKeysToStrings(inputColumns),
          convertKeysToStrings(outputColumns)
      )

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the hadoop runner.
   */
  override val hdfsScheme: HadoopScheme = kijiScheme
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[HadoopScheme]

  /**
   * Creates a Scheme that writes to/reads from a Kiji table for usage with
   * the local runner.
   */
  override val localScheme: LocalScheme = localKijiScheme
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[LocalScheme]

  /**
   * Create a connection to the physical data source (also known as a Tap in Cascading)
   * which, in this case, is a [[org.kiji.schema.KijiTable]].
   *
   * @param readOrWrite Specifies if this source is to be used for reading or writing.
   * @param mode Specifies which job runner/flow planner is being used.
   * @return A tap to use for this data source.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    /** Combination of normal input columns and input versions of the output columns (the latter are
     * needed for reading back written results) */
    def getInputColumnsForTesting: Map[String, ColumnInputSpec] = {
      val testingInputColumnsFromReads = inputColumnSpecifyAllData(
          convertKeysToStrings(inputColumns))
      val testingInputColumnsFromWrites = inputColumnSpecifyAllData(
          convertKeysToStrings(outputColumns)
          .mapValues { x: ColumnOutputSpec => ColumnInputSpec(x.columnName.toString) })
      testingInputColumnsFromReads ++ testingInputColumnsFromWrites
    }

    val tap: Tap[_, _, _] = mode match {
      // Production taps.
      case Hdfs(_,_) => new KijiTap(tableUri, kijiScheme).asInstanceOf[Tap[_, _, _]]
      case Local(_) => new LocalKijiTap(tableUri, localKijiScheme).asInstanceOf[Tap[_, _, _]]

      // Test taps.
      case HadoopTest(conf, buffers) => {
        readOrWrite match {
          case Read => {
            val scheme = kijiScheme
            populateTestTable(tableUri, buffers(this), scheme.getSourceFields, conf)

            new KijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
          case Write => {
            val scheme = new TestKijiScheme(
                timestampField,
                getInputColumnsForTesting,
                convertKeysToStrings(outputColumns))

            new KijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
        }
      }
      case Test(buffers) => {
        readOrWrite match {
          // Use Kiji's local tap and scheme when reading.
          case Read => {
            val scheme = localKijiScheme
            populateTestTable(
                tableUri,
                buffers(this),
                scheme.getSourceFields,
                HBaseConfiguration.create())

            new LocalKijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }

          // After performing a write, use TestLocalKijiScheme to populate the output buffer.
          case Write => {
            val scheme = new TestLocalKijiScheme(
                buffers(this),
                timeRange,
                timestampField,
                getInputColumnsForTesting,
                convertKeysToStrings(outputColumns))

            new LocalKijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
        }
      }

      // Delegate any other tap types to Source's default behaviour.
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }

 override def toString: String = {
   Objects
       .toStringHelper(this)
       .add("tableAddress", tableAddress)
       .add("timeRange", timeRange)
       .add("timestampField", timestampField)
       .add("inputColumns", inputColumns)
       .add("outputColumns", outputColumns)
       .toString
  }

  override def equals(other: Any): Boolean = {
    other match {
      case source: KijiSource => {
        Objects.equal(tableAddress, source.tableAddress) &&
        Objects.equal(inputColumns, source.inputColumns) &&
        Objects.equal(outputColumns, source.outputColumns) &&
        Objects.equal(timestampField, source.timestampField) &&
        Objects.equal(timeRange, source.timeRange)
      }
      case _ => false
    }
  }

  override def hashCode(): Int =
      Objects.hashCode(tableAddress, inputColumns, outputColumns, timestampField, timeRange)
}

/**
 * Contains a private, inner class used by [[org.kiji.express.flow.KijiSource]] when working with
 * tests.
 */
object KijiSource {
  /**
   * Convert scala columns definition into its corresponding java variety.
   *
   * @param columnMap Mapping from field name to Kiji column name.
   * @return Java map from field name to column definition.
   */
  private[express] def convertKeysToStrings[T <: Any](columnMap: Map[Symbol, T])
      : Map[String, T] = {
    columnMap.map { case (symbol, column) => (symbol.name, column) }
  }

  /**
   * Takes a buffer containing rows and writes them to the table at the specified uri.
   *
   * @param tableUri of the table to populate.
   * @param rows Tuples to write to populate the table with.
   * @param fields Field names for elements in the tuple.
   * @param configuration defining the cluster to use.
   */
  private def populateTestTable(
      tableUri: KijiURI,
      rows: Buffer[Tuple],
      fields: Fields,
      configuration: Configuration) {
    doAndRelease(Kiji.Factory.open(tableUri)) { kiji: Kiji =>
      // Layout to get the default reader schemas from.
      val layout = withKijiTable(tableUri, configuration) { table: KijiTable =>
        table.getLayout
      }

      val eidFactory = EntityIdFactory.getFactory(layout)

      // Write the desired rows to the table.
      withKijiTableWriter(tableUri, configuration) { writer: KijiTableWriter =>
        rows.foreach { row: Tuple =>
          val tupleEntry = new TupleEntry(fields, row)
          val iterator = fields.iterator()

          // Get the entity id field.
          val entityIdField = iterator.next().toString
          val entityId = tupleEntry
            .getObject(entityIdField)
            .asInstanceOf[EntityId]

          // Iterate through fields in the tuple, adding each one.
          while (iterator.hasNext) {
            val field = iterator.next().toString

            // Get the timeline to be written.
            val cells: Seq[Cell[Any]] = tupleEntry
                .getObject(field)
                .asInstanceOf[Seq[Cell[Any]]]

            // Write the timeline to the table.
            cells.foreach { cell: Cell[Any] =>
              writer.put(
                  entityId.toJavaEntityId(eidFactory),
                  cell.family,
                  cell.qualifier,
                  cell.version,
                  cell.datum
              )
            }
          }
        }
      }
    }
  }

  private[express] def newGetAllData(col: ColumnInputSpec): ColumnInputSpec = {
    ColumnInputSpec(
        col.columnName.toString,
        Integer.MAX_VALUE,
        col.filter,
        col.paging,
        col.schemaSpec)
  }

  /**
   * Returns a map from field name to column input spec where the column input spec has been
   * configured as an output column.
   *
   * This is used in tests, when we use KijiScheme to read tuples from a Kiji table, and we want
   * to read all data in all of the columns, so the test can inspect all data in the table.
   *
   * @param columns to transform.
   * @return transformed map where the column input specs are configured for output.
   */
  private def inputColumnSpecifyAllData(
      columns: Map[String, ColumnInputSpec]): Map[String, ColumnInputSpec] = {
    columns.mapValues(newGetAllData)
        // Need this to make the Map serializable (issue with mapValues)
        .map(identity)
  }

  /**
   * A LocalKijiScheme that loads rows in a table into the provided buffer. This class
   * should only be used during tests.
   *
   * @param buffer to fill with post-job table rows for tests.
   * @param timeRange of timestamps to read from each column.
   * @param timestampField is the name of a tuple field that will contain cell timestamp when the
   *     source is used for writing. Specify the empty field name to write all
   *     cells at the current time.
   * @param inputColumns is a map of Scalding field name to ColumnInputSpec.
   * @param outputColumns is a map of ColumnOutputSpec to Scalding field name.
   */
  private class TestLocalKijiScheme(
      val buffer: Buffer[Tuple],
      timeRange: TimeRange,
      timestampField: Option[Symbol],
      inputColumns: Map[String, ColumnInputSpec],
      outputColumns: Map[String, ColumnOutputSpec])
      extends LocalKijiScheme(
          timeRange,
          timestampField,
          inputColumnSpecifyAllData(inputColumns),
          outputColumns) {
    override def sinkCleanup(
        process: FlowProcess[Properties],
        sinkCall: SinkCall[OutputContext, OutputStream]) {
      // Store the output table.
      val conf: JobConf = HadoopUtil
          .createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))
      val uri: KijiURI = KijiURI
          .newBuilder(conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI))
          .build()

      // Read table into buffer.
      withKijiTable(uri, conf) { table: KijiTable =>
        val layout = table.getLayout

        // Determine what type of record to use (generic, specific).
        val cellSpecOverrides: Map[KijiColumnName, CellSpec] = outputColumns
            .values
            .map { column => (column.columnName, column.schemaSpec) }
            .collect {
              case (name, SchemaSpec.DefaultReader) => {
                val cellSpec = layout.getCellSpec(name).setUseDefaultReaderSchema()
                (name, cellSpec)
              }
              case (name, SchemaSpec.Writer) => {
                val cellSpec = layout.getCellSpec(name).setUseWriterSchema()
                (name, cellSpec)
              }
              case (name, SchemaSpec.Generic(avroSchema)) => {
                val cellSpec = layout.getCellSpec(name).setReaderSchema(avroSchema)
                (name, cellSpec)
              }
              case (name, SchemaSpec.Specific(avroClass)) => {
                val cellSpec = layout.getCellSpec(name).setSpecificRecord(avroClass)
                (name, cellSpec)
              }
            }
            .toMap

        // Open a table reader that reads data using the generic api.
        val readerFactory = table.getReaderFactory
        doAndClose(readerFactory.openTableReader(cellSpecOverrides.asJava)) { reader =>
          // We also want the entire timerange, so the test can inspect all data in the table.
          val request: KijiDataRequest = KijiScheme.buildRequest(All, inputColumns.values)

          doAndClose(reader.getScanner(request)) { scanner: KijiRowScanner =>
            val rows: Iterator[KijiRowData] = scanner.iterator().asScala
            rows.foreach { row: KijiRowData =>
              val tuple = KijiScheme
                  .rowToTuple(
                      // Use input columns that are based on the output columns
                      inputColumns,
                      getSourceFields,
                      timestampField,
                      row,
                      table.getURI,
                      conf
                  )

              val newTupleValues = tuple
                  .iterator()
                  .asScala
                  .map {
                    // This converts stream into a list to force the stream to compute all of the
                    // transformations that have been applied lazily to it. This is necessary
                    // because some of the transformations applied in KijiScheme#rowToTuple have
                    // dependencies on an open connection to a schema table.
                    case stream: Stream[_] => stream.toList
                    case x => x
                  }
                  .toSeq

              buffer += new Tuple(newTupleValues: _*)
            }
          }
        }
      }

      super.sinkCleanup(process, sinkCall)
    }
  }

  /**
   * Merges an input column mapping with an output column mapping producing an input column mapping.
   * This is used to configure input columns for reading back written data on a source that has just
   * been used as a sink.
   *
   * @param inputs describing which columns to request and what fields to associate them with.
   * @param outputs describing which columns fields should be output to.
   * @return a merged mapping from field names to input column requests.
   */
  private def mergeColumnMapping(
      inputs: Map[String, ColumnInputSpec],
      outputs: Map[String, ColumnOutputSpec]
  ): Map[String, ColumnInputSpec] = {
    def mergeEntry(
        inputs: Map[String, ColumnInputSpec],
        entry: (String, ColumnOutputSpec)
    ): Map[String, ColumnInputSpec] = {
      val (fieldName, columnRequest) = entry
      val input = ColumnInputSpec(
          column = columnRequest.columnName.getName,
          maxVersions = Int.MaxValue,
          schemaSpec = columnRequest.schemaSpec
      )

      inputs + ((fieldName, input))
    }

    outputs
        .foldLeft(inputs)(mergeEntry)
  }

  /**
   * A KijiScheme that loads rows in a table into the provided buffer. This class should only be
   * used during tests.
   *
   * @param timestampField is the name of a tuple field that will contain cell timestamp when the
   *                       source is used for writing. Specify the empty field name to write all
   *                       cells at the current time.
   * @param columns Scalding field name to Kiji column name mapping.
   */
  private class TestKijiScheme(
      timestampField: Option[Symbol],
      inputColumns: Map[String, ColumnInputSpec],
      outputColumns: Map[String, ColumnOutputSpec])
      extends KijiScheme(
          All,
          timestampField,
          mergeColumnMapping(inputColumns, outputColumns),
          outputColumns) {
  }
}
