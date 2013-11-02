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

package org.kiji.express.flow.framework.hfile

import java.util.concurrent.atomic.AtomicLong

import cascading.flow.FlowProcess
import cascading.scheme.NullScheme
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiAudience.Framework
import org.kiji.annotations.ApiStability
import org.kiji.annotations.ApiStability.Experimental
import org.kiji.express.EntityId
import org.kiji.express.flow.ColumnFamilyRequestOutput
import org.kiji.express.flow.ColumnRequestOutput
import org.kiji.express.flow.QualifiedColumnRequestOutput
import org.kiji.express.flow.TimeRange
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.flow.framework.KijiSourceContext
import org.kiji.express.util.Resources
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.mapreduce.HFileLoader
import org.kiji.mapreduce.framework.HFileKeyValue
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.mapreduce.impl.HFileWriterContext
import org.kiji.mapreduce.output.framework.KijiHFileOutputFormat
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.impl.ColumnNameTranslator

 /**
 * A Kiji-specific implementation of a Cascading `Scheme` which defines how to write data
 * to HFiles.
 *
 * HFileKijiScheme is responsible for converting rows from a Kiji table that are input to a
 * Cascading flow into Cascading tuples
 * (see `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to an HFile capable of being bulk loaded into HBase
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * HFileKijiScheme must be used with [[org.kiji.express.flow.framework.HFileKijiTap]],
 * since it expects the Tap to have access to a Kiji table.
 * [[org.kiji.express.flow.framework.hfile.HFileKijiSource]] handles the creation of both
 * HFileKijiScheme and KijiTap in KijiExpress.
 *
 * @param timeRange to include from the Kiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param loggingInterval to log skipped rows on. For example, if loggingInterval is 1000,
 *     then every 1000th skipped row will be logged.
 * @param columns mapping tuple field names to requests for Kiji columns.
 */
@ApiAudience.Framework
@ApiStability.Experimental
private[express] class HFileKijiScheme(
  private[express] val timeRange: TimeRange,
  private[express] val timestampField: Option[Symbol],
  private[express] val loggingInterval: Long,
  private[express] val columns: Map[String, ColumnRequestOutput])
    extends HFileKijiScheme.HFileScheme {

  import KijiScheme._
  import HFileKijiScheme._

  setSinkFields(buildSinkFields(columns, timestampField))

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkPrepare(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {
    val conf = flow.getConfigCopy()
    val uri = conf.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val kijiURI = KijiURI.newBuilder(uri).build()
    val kiji = Kiji.Factory.open(kijiURI)

    doAndRelease(kiji.openTable(kijiURI.getTable)) { table: KijiTable =>
      // Set the sink context to an opened KijiTableWriter.
      val ctx = HFileKijiSinkContext(kiji, kijiURI,
        table.getLayout, new ColumnNameTranslator(table.getLayout))
      sinkCall.setContext(ctx)
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table. This method is called once
   * for each row on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sink(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry

    val HFileKijiSinkContext(kiji, uri, layout, colTranslator) = sinkCall.getContext()
    val eidFactory = EntityIdFactory.getFactory(layout)

    outputCells(output, timestampField, columns) { key: HFileCell =>
      // Convert cell to an HFileKeyValue
      val kijiColumn = new KijiColumnName(key.col_request.family, key.col_request.qualifier);
      val hbaseColumn = colTranslator.toHBaseColumnName(kijiColumn);
      val cellSpec = layout.getCellSpec(kijiColumn)
        .setSchemaTable(kiji.getSchemaTable());
      val encoder = DefaultKijiCellEncoderFactory.get().create(cellSpec);

      val hFileKeyValue = new HFileKeyValue(
        key.entity_id.toJavaEntityId(eidFactory).getHBaseRowKey(),
        hbaseColumn.getFamily(), hbaseColumn.getQualifier(), key.timestamp,
        encoder.encode(key.datum));

      sinkCall.getOutput().collect(hFileKeyValue, NullWritable.get())
    }
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkCleanup(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    val HFileKijiSinkContext(kiji, _, _, _) = sinkCall.getContext()

    kiji.release()
    // scalastyle:off null
    sinkCall.setContext(null)
    // scalastyle:on null

  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our KijiDataRequest.
   */
  override def sinkConfInit(
    flow: FlowProcess[JobConf],
    tap: Tap[JobConf, RecordReader[_, _], OutputCollector[HFileKeyValue, NullWritable]],
    conf: JobConf) {
  }


  override def equals(other: Any): Boolean = {
    other match {
      case scheme: HFileKijiScheme => {
        columns == scheme.columns &&
          timestampField == scheme.timestampField &&
          timeRange == scheme.timeRange
      }
      case _ => false
    }
  }


  override def hashCode(): Int =
    Objects.hashCode(columns, timeRange, timestampField, loggingInterval: java.lang.Long)
}

/**
 * Private scheme that is a subclass of Cascading's NullScheme that doesn't do anything but
 * sinks data. This is used in the secondary M/R job that takes intermediate HFile Key/Values
 * from a sequence files and outputs them to the KijiHFileOutputFormat ultimately going to HFiles.
 */
private[express] final class SemiNullScheme extends HFileKijiScheme.HFileScheme {
  /**
   * Converts and writes a Cascading Tuple to a Kiji table. This method is called once
   * for each row on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sink(
    flow: FlowProcess[JobConf],
    sinkCall: SinkCall[HFileKijiSinkContext, OutputCollector[HFileKeyValue, NullWritable]]) {

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry

    val hFileKeyValue = output.getObject(0).asInstanceOf[HFileKeyValue]
    sinkCall.getOutput().collect(hFileKeyValue, NullWritable.get())
  }
}

/**
 * Context housing information necessary for the scheme to interact
 * with the Kiji table.
 */
private[express] case class HFileKijiSinkContext (
  kiji: Kiji,
  kijiUri: KijiURI,
  layout: KijiTableLayout,
  columnTranslator: ColumnNameTranslator
)

/**
 * A cell from a Kiji table containing some datum, addressed by a family, qualifier,
 * and version timestamp.
 *
 * @param family of the Kiji table cell.
 * @param qualifier of the Kiji table cell.
 * @param version  of the Kiji table cell.
 * @param datum in the Kiji table cell.
 * @tparam T is the type of the datum in the cell.
 */
private[express] case class HFileCell private[express] (
  entity_id: EntityId,
  col_request: QualifiedColumnRequestOutput,
  timestamp: Long,
  datum: AnyRef)

object HFileKijiScheme {
  type HFileScheme = NullScheme[JobConf, RecordReader[_, _],
    OutputCollector[HFileKeyValue, NullWritable], KijiSourceContext, HFileKijiSinkContext]

  private[express] def outputCells(output: TupleEntry,
                                   timestampField: Option[Symbol],
                                   columns: Map[String, ColumnRequestOutput])(
                                     cellHandler: HFileCell => Unit) {

    // Get a timestamp to write the values to, if it was specified by the user.
    val timestamp: Long = timestampField match {
      case Some(field) => output.getObject(field.name).asInstanceOf[Long]
      case None        => HConstants.LATEST_TIMESTAMP
    }

    // Get the entityId.
    val entityId: EntityId =
      output.getObject(KijiScheme.entityIdField).asInstanceOf[EntityId]

    columns.foreach(kv => {
      val (fieldName, colRequest) = kv
      val colValue = output.getObject(fieldName).asInstanceOf[AnyRef]
      val newColRequest = colRequest match {
        case cf @ ColumnFamilyRequestOutput(family, qualField, schemaId, useDefaultReader) => {
          val qualifier = output.getObject(qualField).asInstanceOf[String]
          QualifiedColumnRequestOutput(family, qualifier, schemaId, useDefaultReader)
        }
        case qc @ QualifiedColumnRequestOutput(_, _, _, _) => qc
      }
      val cell = HFileCell(entityId, newColRequest, timestamp, colValue)
      cellHandler(cell)
    })
  }
}
