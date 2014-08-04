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

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
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
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.express.flow.util.ResourceUtil.withKijiTable

/**
 * A local version of [[org.kiji.express.flow.framework.KijiScheme]] that is meant to be used with
 * Cascading's local job runner. [[org.kiji.express.flow.framework.KijiScheme]] and
 * LocalKijiScheme both define how to read and write the data stored in a Kiji table.
 *
 * [[LocalKijiScheme]] extends trait [[org.kiji.express.flow.framework.BaseLocalKijiScheme]],
 * which holds method implementations of [[cascading.scheme.Scheme]] that are common to both
 * [[LocalKijiScheme]] and [[TypedLocalKijiScheme]] for running jobs locally.
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
 * @see [[org.kiji.express.flow.framework.KijiScheme]]
 * @see [[org.kiji.express.flow.framework.BaseLocalKijiScheme]]
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
 extends BaseLocalKijiScheme {

  /** Set the fields that should be in a tuple when this source is used for reading and writing. */
  setSourceFields(KijiScheme.buildSourceFields(inputColumns.keys ++ outputColumns.keys))
  setSinkFields(KijiScheme.buildSinkFields(outputColumns, timestampField))

  /**
   * Sets up any resources required to read from a Kiji table.
   *
   * @param process currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]) {

    val jobConfiguration: JobConf =
        HadoopUtil.createJobConf(process.getConfigCopy, new JobConf(HBaseConfiguration.create()))
    withKijiTable(uri, jobConfiguration) { table: KijiTable =>
      // Build the input context.
      val request = BaseKijiScheme.buildRequest(table.getLayout, timeRange, inputColumns.values)
      val context: InputContext = BaseLocalKijiScheme.configureInputContext(
          table,
          jobConfiguration,
          rowFilterSpec,
          rowRangeSpec,
          request)
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
      process.increment(BaseKijiScheme.CounterGroupName, BaseKijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the input.
    }
  }

  /**
   * Sets up any resources required to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkPrepare(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputStream]
  ) {
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
}
