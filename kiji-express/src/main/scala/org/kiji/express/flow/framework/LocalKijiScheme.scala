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
import java.util.HashMap
import java.util.{Map => JMap}
import java.util.Properties

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.asScalaIteratorConverter

import cascading.flow.FlowProcess
import cascading.flow.hadoop.util.HadoopUtil
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.ColumnRequestInput
import org.kiji.express.flow.ColumnRequestOutput
import org.kiji.express.flow.TimeRange
import org.kiji.express.util.GenericCellSpecs
import org.kiji.express.util.SpecificCellSpecs
import org.kiji.express.util.Resources._
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.CellSpec
import org.kiji.schema.layout.KijiTableLayout

/**
 * Encapsulates the state required to read from a Kiji table locally, for use in
 * [[org.kiji.express.flow.framework.LocalKijiScheme]].
 *
 * @param reader that has an open connection to the desired Kiji table.
 * @param scanner that has an open connection to the desired Kiji table.
 * @param iterator that maintains the current row pointer.
 * @param tableUri of the kiji table.
 */
private[express] case class InputContext(
    reader: KijiTableReader,
    scanner: KijiRowScanner,
    iterator: Iterator[KijiRowData],
    tableUri: KijiURI,
    configuration: Configuration)

/**
 * Encapsulates the state required to write to a Kiji table.
 *
 * @param writer that has an open connection to the desired Kiji table.
 * @param tableUri of the Kiji table.
 * @param layout of the kiji table.
 */
private[express] case class OutputContext(
    writer: KijiTableWriter,
    tableUri: KijiURI,
    kiji: Kiji,
    layout: KijiTableLayout,
    configuration: Configuration)

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
 * @see [[org.kiji.express.flow.framework.KijiScheme]]
 *
 * @param timeRange to include from the Kiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param columns mapping tuple field names to requests for Kiji columns.
 */
@ApiAudience.Framework
@ApiStability.Experimental
private[express] class LocalKijiScheme(
    private[express] val timeRange: TimeRange,
    private[express] val timestampField: Option[Symbol],
    private[express] val icolumns: Map[String, ColumnRequestInput] = Map(),
    private[express] val ocolumns: Map[String, ColumnRequestOutput] = Map())
    extends Scheme[Properties, InputStream, OutputStream, InputContext, OutputContext] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[LocalKijiScheme])

  /** Set the fields that should be in a tuple when this source is used for reading and writing. */
  setSourceFields(KijiScheme.buildSourceFields(icolumns.keys ++ ocolumns.keys))
  setSinkFields(KijiScheme.buildSinkFields(ocolumns, timestampField))

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
    val conf: JobConf = HadoopUtil.createJobConf(
        process.getConfigCopy,
        new JobConf(HBaseConfiguration.create()))
    val uriString: String = conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)
    val kijiUri: KijiURI = KijiURI.newBuilder(uriString).build()

    // Build the input context.
    withKijiTable(kijiUri, conf) { table: KijiTable =>
      val request = KijiScheme.buildRequest(timeRange, icolumns.values)
      val reader = {
        val allCellSpecs: JMap[KijiColumnName, CellSpec] = new HashMap[KijiColumnName, CellSpec]()
        allCellSpecs.putAll(GenericCellSpecs(table))
        allCellSpecs.putAll(SpecificCellSpecs.buildCellSpecs(table.getLayout, icolumns).asJava)
        table.getReaderFactory.openTableReader(allCellSpecs)
      }
      val scanner = reader.getScanner(request)
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
      val result: Tuple =
          KijiScheme.rowToTuple(
              icolumns,
              getSourceFields,
              timestampField,
              row,
              context.tableUri,
              context.configuration)

      // Set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      process.increment(KijiScheme.counterGroupName, KijiScheme.counterSuccess, 1)
      return true // We set a result tuple, return true for success.
    } else {
      return false // We reached the end of the input.
    }
  }

  /**
   * Cleans up any resources used to read from a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  override def sourceCleanup(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]) {
    val context: InputContext = sourceCall.getContext
    context.reader.close()
    context.scanner.close()

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
      sinkCall: SinkCall[OutputContext, OutputStream]) {
    // Open a table writer.
    val conf: JobConf = HadoopUtil.createJobConf(process.getConfigCopy,
        new JobConf(HBaseConfiguration.create()))
    val uriString: String = conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(uriString).build()

    val kiji = Kiji.Factory.open(uri)
    doAndRelease(kiji.openTable(uri.getTable)) { table: KijiTable =>
      // Set the sink context to an opened KijiTableWriter.
      sinkCall
        .setContext(OutputContext(table.openTableWriter(), uri, kiji, table.getLayout, conf))
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
      sinkCall: SinkCall[OutputContext, OutputStream]) {
    // Retrieve writer from the scheme's context.
    val OutputContext(writer, tableUri, kiji, layout, configuration) = sinkCall.getContext

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry
    KijiScheme.putTuple(
        ocolumns,
        tableUri,
        kiji,
        timestampField,
        output,
        writer,
        layout,
        configuration)
  }

  /**
   * Cleans up any resources used to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkCleanup(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[OutputContext, OutputStream]) {
    sinkCall.getContext.writer.close()
    sinkCall.getContext.kiji.release()
    // Set the context to null so that we no longer hold any references to it.
    sinkCall.setContext(null)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case scheme: LocalKijiScheme => {
        icolumns == scheme.icolumns &&
        ocolumns == scheme.ocolumns &&
            timestampField == scheme.timestampField &&
            timeRange == scheme.timeRange
      }
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(icolumns, ocolumns, timestampField, timeRange)
}
