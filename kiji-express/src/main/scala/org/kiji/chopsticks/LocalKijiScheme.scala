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

package org.kiji.chopsticks

import scala.collection.JavaConverters._
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import com.google.common.base.Objects

/**
 * Encapsulates the state required to read from a Kiji table.
 *
 * @param reader that has an open connection to the desired Kiji table.
 * @param scanner that has an open connection to the desired Kiji table.
 * @param iterator that maintains the current row pointer.
 */
private[chopsticks] case class InputContext(
    reader: KijiTableReader,
    scanner: KijiRowScanner,
    iterator: Iterator[KijiRowData])

/**
 * A scheme that can source and sink data from a Kiji table.
 *
 * <p>This scheme is responsible for converting rows from a Kiji table that are input to a
 * Cascading flow into Cascading tuples (see
 * [[#source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)]]) and writing output
 * data from a Cascading flow to a Kiji table
 * (see [[#sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)]]). This scheme is meant
 * to be used with [[LocalKijiTap]] and Cascading's local job runner. Jobs run with Cascading's
 * local job runner execute on your local machine instead of a cluster. This can be helpful for
 * testing or quick jobs.</p>
 *
 * <p>Note: Warnings about a missing serialVersionUID are ignored here. When KijiScheme is
 * serialized, the result is not persisted anywhere making serialVersionUID unnecessary.</p>
 */
@ApiAudience.Framework
@ApiStability.Unstable
class LocalKijiScheme(
    private val timeRange: TimeRange,
    private val timestampField: Option[Symbol],
    private val columns: Map[String, ColumnRequest])
    extends Scheme[Properties, InputStream, OutputStream, InputContext, KijiTableWriter] {

  /** Set the fields that should be in a tuple when this source is used for reading and writing. */
  setSourceFields(KijiScheme.buildSourceFields(columns.keys))
  setSinkFields(KijiScheme.buildSinkFields(columns.keys, timestampField))

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Kiji table.
   *
   * @param process Current Cascading flow being built.
   * @param tap The tap that is being used with this scheme.
   * @param conf The job configuration object.
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
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   */
  override def sourcePrepare(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]) {
    val uriString: String = process.getConfigCopy().getProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(uriString).build()

    // Build the input context.
    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(uri.getTable())) { table: KijiTable =>
        val request = KijiScheme.buildRequest(timeRange, columns.values)
        val reader = table.openTableReader()
        val scanner = reader.getScanner(request)
        val context = InputContext(reader, scanner, scanner.iterator.asScala)

        sourceCall.setContext(context)
      }
    }
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row in the table.
   *
   * @param process Current Cascading flow being run.
   * @param sourceCall Object containing the context for this source.
   * @return <code>true</code> if another row was read and it was converted to a tuple,
   *     <code>false</code> if there were no more rows to read.
   */
  override def source(
      process: FlowProcess[Properties],
      sourceCall: SourceCall[InputContext, InputStream]): Boolean = {
    val context: InputContext = sourceCall.getContext()
    if (context.iterator.hasNext) {
      // Get the current row.
      val row: KijiRowData = context.iterator.next()

      val result: Tuple = KijiScheme.rowToTuple(columns, getSourceFields(), timestampField, row)
      sourceCall.getIncomingEntry().setTuple(result)

      true
    } else {
      false
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
    val context: InputContext = sourceCall.getContext()
    context.reader.close()
    context.scanner.close()
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
      sinkCall: SinkCall[KijiTableWriter, OutputStream]) {
    // Open a table writer.
    val uriString: String = process.getConfigCopy().getProperty(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(uriString).build()

    // TODO: Check and see if Kiji.Factory.open should be passed the configuration object in
    //     process.
    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(uri.getTable())) { table: KijiTable =>
        // Set the sink context to an opened KijiTableWriter.
        sinkCall.setContext(table.openTableWriter())
      }
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
      sinkCall: SinkCall[KijiTableWriter, OutputStream]) {
    // Retrieve writer from the scheme's context.
    val writer: KijiTableWriter = sinkCall.getContext()

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry()
    KijiScheme.putTuple(columns, getSinkFields(), timestampField, output, writer)
  }

  /**
   * Cleans up any resources used to write to a Kiji table.
   *
   * @param process Current Cascading flow being run.
   * @param sinkCall Object containing the context for this source.
   */
  override def sinkCleanup(
      process: FlowProcess[Properties],
      sinkCall: SinkCall[KijiTableWriter, OutputStream]) {
    sinkCall.getContext().close()
    sinkCall.setContext(null)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case scheme: LocalKijiScheme => {
        columns == scheme.columns &&
            timestampField == scheme.timestampField &&
            timeRange == scheme.timeRange
      }
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(columns, timestampField, timeRange)
}
