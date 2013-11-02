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
import scala.collection.mutable.Buffer

import java.io.OutputStream
import java.util.Properties
import java.util.NoSuchElementException

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

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

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.Cell
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.flow.framework.KijiTap
import org.kiji.express.flow.framework.LocalKijiScheme
import org.kiji.express.flow.framework.LocalKijiTap
import org.kiji.express.flow.framework.OutputContext
import org.kiji.express.util.AvroUtil
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
 * @param loggingInterval The interval at which to log skipped rows.
 * @param columns is a one-to-one mapping from field names to Kiji columns. When reading,
 *     the columns in the map will be read into their associated tuple fields. When
 *     writing, values from the tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Experimental
final class KijiSource private[express] (
    val tableAddress: String,
    val timeRange: TimeRange,
    val timestampField: Option[Symbol],
    val loggingInterval: Long,
    val inputColumns: Map[Symbol, ColumnRequestInput] = Map(),
    val outputColumns: Map[Symbol, ColumnRequestOutput] = Map())
    extends Source {
  import KijiSource._

  private type HadoopScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]

  /** The URI of the target Kiji table. */
  private val tableUri: KijiURI = KijiURI.newBuilder(tableAddress).build()

  /** A Kiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  private val kijiScheme: KijiScheme =
      new KijiScheme(timeRange, timestampField, loggingInterval, convertKeysToStrings(inputColumns),
        convertKeysToStrings(outputColumns))

  /** A Kiji scheme intended to be used with Scalding/Cascading's local mode. */
  private val localKijiScheme: LocalKijiScheme = {
    new LocalKijiScheme(
      timeRange,
      timestampField,
      convertKeysToStrings(inputColumns),
      convertKeysToStrings(outputColumns))
  }

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
    def getInputColumnsForTesting: Map[String, ColumnRequestInput] = {
      val testingInputColumnsFromReads = inputColumnRequestsAllData(
          convertKeysToStrings(inputColumns))
      val testingInputColumnsFromWrites = inputColumnRequestsAllData(
          convertKeysToStrings(outputColumns)
          .mapValues { x: ColumnRequestOutput => ColumnRequestInput(x.getColumnName.toString) })
      testingInputColumnsFromReads ++ testingInputColumnsFromWrites
    }

    def getInputColumnsToCheckWrites(icols: Map[String, ColumnRequestOutput]):
        Map[String, ColumnRequestInput] = { icols.mapValues( { x: ColumnRequestOutput =>
            ColumnRequestInput(x.getColumnName.toString())
        })}

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
                loggingInterval,
                getInputColumnsForTesting,
                outputColumnRequestsAllData(convertKeysToStrings(outputColumns)))

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
                outputColumnRequestsAllData(convertKeysToStrings(outputColumns)))

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
    ("KijiSource(table: %s, timeRange: %s, timestampField: %s, loggingInterval: %s, " +
        "inputColumns: %s, outputColumns: %s)").format(
            tableAddress,
            timeRange,
            timestampField match {
              case None => None
              case Some(tsField) => tsField
            },
            loggingInterval,
            inputColumns,
            outputColumns)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case source: KijiSource =>
        Objects.equal(tableAddress, source.tableAddress) &&
        Objects.equal(inputColumns, source.inputColumns) &&
        Objects.equal(outputColumns, source.outputColumns) &&
        Objects.equal(timestampField, source.timestampField) &&
        Objects.equal(timeRange, source.timeRange)
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
      val schemaTable = kiji.getSchemaTable

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
              .asInstanceOf[KijiSlice[Any]]
              .cells

            // Write the timeline to the table.
            cells.map { cell: Cell[Any] =>
              val readerSchema = layout
                .getCellSchema(new KijiColumnName(cell.family, cell.qualifier))
                .getDefaultReader
              val schema: Option[Schema] =
                Option(readerSchema) match {
                  case Some(defaultSchema) =>
                    Some(KijiScheme.resolveSchemaFromJSONOrUid(defaultSchema, schemaTable))
                  case None => None
                }

              val datum = AvroUtil.encodeToJava(cell.datum, schema)
              writer.put(
                entityId.toJavaEntityId(eidFactory),
                cell.family,
                cell.qualifier,
                cell.version,
                datum)
            }
          }
        }
      }
    }
  }

  /**
   * Returns a map from field name to column request where the column request has been
   * configured as an output column.
   *
   * This is used in tests, when we use KijiScheme to read tuples from a Kiji table, and we want
   * to read all data in all of the columns, so the test can inspect all data in the table.
   * Therefore, we convert ColumnRequestOutput objects into ColumnRequestInput objects.
   *
   * @param columns to transform.
   * @return transformed map where the column requests are configured for output.
   */
  private def inputColumnRequestsAllData(
      columns: Map[String, ColumnRequestInput]): Map[String, ColumnRequestInput] = {
    columns.mapValues(_.newGetAllData)
        // Need this to make the Map serializable (issue with mapValues)
        .map(identity)
  }

  private def outputColumnRequestsAllData(
      columns: Map[String, ColumnRequestOutput]): Map[String, ColumnRequestOutput] = {
    columns.mapValues(_.newGetAllData)
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
   * @param columns is a Scalding field name to Kiji column name mapping.
   */
  private class TestLocalKijiScheme(
      val buffer: Buffer[Tuple],
      timeRange: TimeRange,
      timestampField: Option[Symbol],
      inputColumns: Map[String, ColumnRequestInput],
      outputColumns: Map[String, ColumnRequestOutput])
      extends LocalKijiScheme(
          timeRange,
          timestampField,
          inputColumnRequestsAllData(inputColumns),
          outputColumnRequestsAllData(outputColumns)) {
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
        // Open a table reader that reads data using the generic api.
        val readerFactory = table.getReaderFactory
        val genericSpecs = GenericCellSpecs(table)
        doAndClose(readerFactory.openTableReader(genericSpecs)) { reader: KijiTableReader=>
          // We also want the entire timerange, so the test can inspect all data in the table.
          val request: KijiDataRequest = KijiScheme.buildRequest(All, inputColumns.values)

          doAndClose(reader.getScanner(request)) { scanner: KijiRowScanner =>
            val rows: Iterator[KijiRowData] = scanner.iterator().asScala
            rows.foreach { row: KijiRowData =>
              KijiScheme
                  .rowToTuple(
                      // Use input columns that are based on the output columns
                      inputColumns,
                      getSourceFields,
                      timestampField,
                      row,
                      table.getURI,
                      conf)
                  .foreach { tuple => buffer += tuple }
            }
          }
        }
      }

      super.sinkCleanup(process, sinkCall)
    }
  }

  /**
   * A KijiScheme that loads rows in a table into the provided buffer. This class should only be
   * used during tests.
   *
   * @param timestampField is the name of a tuple field that will contain cell timestamp when the
   *                       source is used for writing. Specify the empty field name to write all
   *                       cells at the current time.
   * @param loggingInterval The interval at which to log skipped rows.
   * @param columns Scalding field name to Kiji column name mapping.
   */
  private class TestKijiScheme(
      timestampField: Option[Symbol],
      loggingInterval: Long,
      inputColumns: Map[String, ColumnRequestInput],
      outputColumns: Map[String, ColumnRequestOutput])
      extends KijiScheme(
          All,
          timestampField,
          loggingInterval,
          inputColumns,
          outputColumns)
}
