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

package org.kiji.express.flow.framework

import java.io.InputStream
import java.io.OutputStream
import java.util.Properties

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapred.JobConf

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.ColumnOutputSpec
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.RowFilterSpec
import org.kiji.express.flow.RowRangeSpec
import org.kiji.express.flow.TimeRangeSpec
import org.kiji.express.flow.util.ResourceUtil._
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequest.Column
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiTableReader.KijiScannerOptions
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.ColumnReaderSpec
import org.kiji.schema.{EntityId => JEntityId}

/**
 * Encapsulates the state required to read from a Kiji table locally, for use in
 * [[org.kiji.express.flow.framework.LocalKijiScheme]].
 *
 * @param reader that has an open connection to the desired Kiji table.
 * @param scanner that has an open connection to the desired Kiji table.
 * @param iterator that maintains the current row pointer.
 * @param tableUri of the kiji table.
 */
@ApiAudience.Private
@ApiStability.Stable
final private[express] case class InputContext(
    reader: KijiTableReader,
    scanner: KijiRowScanner,
    iterator: Iterator[KijiRowData],
    tableUri: KijiURI,
    configuration: Configuration
)

/**
 * A local version of [[org.kiji.express.flow.framework.KijiScheme]] that is meant to be used with
 * Cascading's local job runner. [[org.kiji.express.flow.framework.KijiScheme]] and
 * LocalKijiScheme both define how to read and write the data stored in a Kiji table.
 *
 * This scheme is meant to be used with [[org.kiji.express.flow.framework.LocalKijiTap]] and
 * Cascading's local job runner. Jobs run with Cascading's local job runner execute on
 * your local machine instead of a cluster. This can be helpful for testing or quick jobs.
 *
 * In KijiExpress, LocalKijiScheme is used in tests.  See [[org.kiji.express.flow.KijiSource]]'s
 * `TestLocalKijiScheme` class.
 *
 * This scheme is responsible for converting rows from a Kiji table that are input to a
 * Cascading flow into Cascading tuples (see
 * `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to a Kiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * Note: LocalKijiScheme logs every row that was skipped because of missing data in a column. It
 * lacks the parameter `loggingInterval` in [[org.kiji.express.flow.framework.KijiScheme]] that
 * configures how many skipped rows will be logged.
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When KijiScheme is
 * serialized, the result is not persisted anywhere making serialVersionUID unnecessary.
 *
 * Note: If sourcing from a KijiTable, it is never closed.  The reason for this is that if any of
 * the columns in the request are paged, they might still need an open KijiTable for the rest of
 * the flow.  It is expected that any job using this as a source is not long-running and is
 * contained to a single JVM.
 *
 * @see [[org.kiji.express.flow.framework.KijiScheme]]
 *
 * @param uri of table to be written to.
 * @param timeRange to include from the Kiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param inputColumns is a one-to-one mapping from field names to Kiji columns. The columns in the
 *     map will be read into their associated tuple fields.
 * @param outputColumns is a one-to-one mapping from field names to Kiji columns. Values from the
 *     tuple fields will be written to their associated column.
 */
@ApiAudience.Framework
@ApiStability.Stable
@Inheritance.Sealed
private[express] case class LocalKijiScheme(
    private[express] val uri: KijiURI,
    private[express] val timeRange: TimeRangeSpec,
    private[express] val timestampField: Option[Symbol],
    private[express] val inputColumns: Map[String, ColumnInputSpec] = Map(),
    private[express] val outputColumns: Map[String, ColumnOutputSpec] = Map(),
    private[express] val rowRangeSpec: RowRangeSpec,
    private[express] val rowFilterSpec: RowFilterSpec)
    extends Scheme[Properties, InputStream, OutputStream, InputContext, DirectKijiSinkContext] {

  /** Set the fields that should be in a tuple when this source is used for reading and writing. */
  setSourceFields(KijiScheme.buildSourceFields(inputColumns.keys ++ outputColumns.keys))
  setSinkFields(KijiScheme.buildSinkFields(outputColumns, timestampField))

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Kiji table.
   *
   * @param process flow being built.
   * @param tap that is being used with this scheme.
   * @param conf is an unused Properties object that is a stand-in for a job configuration object.
   */
  override def sourceConfInit(
      process: FlowProcess[Properties],
      tap: Tap[Properties, InputStream, OutputStream],
      conf: Properties) {
    // No-op. Setting options in a java Properties object is not going to help us read from
    // a Kiji table.
  }

  /**
   * Sets up any resources required to read from a Kiji table.
   *
   * @param process currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]) {

    val conf: JobConf =
      HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))

    // Build the input context.
    withKijiTable(uri, conf) { table: KijiTable =>
      val request = KijiScheme.buildRequest(table.getLayout, timeRange, inputColumns.values)
      val reader = LocalKijiScheme.openReaderWithOverrides(table, request)

      // Set up scanning options.
      val eidFactory = EntityIdFactory.getFactory(table.getLayout())
      val scannerOptions = new KijiScannerOptions()
      scannerOptions.setKijiRowFilter(
          rowFilterSpec.toKijiRowFilter.getOrElse(null))
      scannerOptions.setStartRow(
        rowRangeSpec.startEntityId match {
          case Some(entityId) => entityId.toJavaEntityId(eidFactory)
          case None => null
        }
      )
      scannerOptions.setStopRow(
        rowRangeSpec.limitEntityId match {
          case Some(entityId) => entityId.toJavaEntityId(eidFactory)
          case None => null
        }
      )
      val scanner = reader.getScanner(request, scannerOptions)
      val tableUri = table.getURI
      val context = InputContext(reader, scanner, scanner.iterator.asScala, tableUri, conf)

      sourceCall.setContext(context)
    }
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row in the table.
   *
   * @param process is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   * @return <code>true</code> if another row was read and it was converted to a tuple,
   *     <code>false</code> if there were no more rows to read.
   */
  override def source(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]): Boolean = {
    val context: InputContext = sourceCall.getContext
    if (context.iterator.hasNext) {
      // Get the current row.
      val row: KijiRowData = context.iterator.next()
      val result: Tuple = KijiScheme.rowToTuple(
        inputColumns,
        getSourceFields,
        timestampField,
        row)

      // Set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      process.increment(KijiScheme.CounterGroupName, KijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the input.
    }
  }

  /**
   * Cleans up any resources used to read from a Kiji table.
   *
   * Note: This does not close the KijiTable!  If one of the columns of the request was paged,
   * it will potentially still need access to the Kiji table even after the tuples have been
   * sourced.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  override def sourceCleanup(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]) {
    // Set the context to null so that we no longer hold any references to it.
    sourceCall.setContext(null)
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
   */
  override def sinkConfInit(
      process: FlowProcess[Properties],
      tap: Tap[Properties, InputStream, OutputStream],
      conf: Properties) {
    // No-op. Setting options in a java Properties object is not going to help us write to
    // a Kiji table.
  }

  /**
   * Sets up any resources required to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkPrepare(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {

    val conf: JobConf =
      HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))

    withKijiTable(uri, conf) { table =>
    // Set the sink context to an opened KijiTableWriter.
      sinkCall.setContext(
        DirectKijiSinkContext(
          EntityIdFactory.getFactory(table.getLayout),
          table.getWriterFactory.openBufferedWriter()))
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sink(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {
    val DirectKijiSinkContext(eidFactory, writer) = sinkCall.getContext
    val tuple: TupleEntry = sinkCall.getOutgoingEntry

    // Get the entityId.
    val eid: JEntityId = tuple
        .getObject(KijiScheme.EntityIdField)
        .asInstanceOf[EntityId]
        .toJavaEntityId(eidFactory)

    // Get a timestamp to write the values to, if it was specified by the user.
    val version: Long = timestampField
        .map(field => tuple.getLong(field.name))
        .getOrElse(HConstants.LATEST_TIMESTAMP)

    outputColumns.foreach { case (field, column) =>
      val value = tuple.getObject(field)

      val qualifier: String = column match {
        case qc: QualifiedColumnOutputSpec => qc.qualifier
        case cf: ColumnFamilyOutputSpec => tuple.getString(cf.qualifierSelector.name)
      }

      writer.put(eid, column.columnName.getFamily, qualifier, version, column.encode(value))
    }
  }

  /**
   * Cleans up any resources used to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkCleanup(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]) {
    val writer = sinkCall.getContext.writer
    writer.flush()
    writer.close()
    sinkCall.setContext(null)
  }
}

private[express] object LocalKijiScheme {
  /**
   * Opens a Kiji table reader correctly specifying column schema overrides from a KijiDataRequest.
   *
   * @param table to use to open a reader.
   * @param request for data from the target Kiji table.
   * @return a Kiji table reader. Close this reader when it is no longer needed.
   */
  def openReaderWithOverrides(table: KijiTable, request: KijiDataRequest): KijiTableReader = {
    val overrides: Map[KijiColumnName, ColumnReaderSpec] = request
        .getColumns
        .asScala
        .map { column: Column => (column.getColumnName, column.getReaderSpec) }
        .toMap
    table.getReaderFactory.readerBuilder()
        .withColumnReaderSpecOverrides(overrides.asJava)
        .buildAndOpen()
  }
}
