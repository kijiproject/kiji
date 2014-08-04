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
package org.kiji.express.flow.framework

import scala.collection.immutable.HashMap

import cascading.scheme.SourceCall
import cascading.scheme.SinkCall
import cascading.flow.FlowProcess
import cascading.tap.Tap
import cascading.tuple.Tuple
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiURI
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.ExpressResult
import org.kiji.express.flow.ExpressColumnOutput
import org.kiji.express.flow.TimeRangeSpec
import org.kiji.express.flow.RowFilterSpec
import org.kiji.express.flow.RowRangeSpec
import org.kiji.express.flow.PagingSpec
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.express.flow.framework.serialization.KijiKryoExternalizer

/**
 * A Kiji-specific implementation of a Cascading `Scheme` for the scalding type safe API, that
 * defines how to read and write the data in a Kiji table.
 *
 * [[TypedKijiScheme]] extends trait [[org.kiji.express.flow.framework.BaseKijiScheme]], which holds
 * method implementations of [[cascading.scheme.Scheme]] that are common to both [[KijiScheme]]
 * and [[TypedKijiScheme]] for running mapreduce jobs.
 *
 * [[TypedKijiScheme]] is responsible for converting rows from a Kiji table that are input into a
 * Cascading flow into Cascading tuples
 * (see `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to a Kiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * [[TypedKijiScheme]] must be used with [[org.kiji.express.flow.framework.KijiTap]], since it
 * expects the Tap to have access to a Kiji table. [[org.kiji.express.flow.TypedKijiSource]] handles
 * the creation of both [[TypedKijiScheme]] and [[KijiTap]] in KijiExpress.
 *
 * @see[[org.kiji.express.flow.framework.BaseKijiScheme]]
 *
 * @param tableAddress The URI identifying of the target Kiji table.
 * @param timeRange The range of versions to include from the Kiji table.
 * @param icolumns A list of ColumnInputSpecs from where the data is to be read.
 * @param rowRangeSpec  The specifications for the range of the input.
 * @param rowFilterSpec The specifications for the the filters on the input.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
class TypedKijiScheme private[express] (
    private[express] val tableAddress: String,
    private[express] val timeRange: TimeRangeSpec,
    icolumns: List[ColumnInputSpec] = List(),
    private[express] val rowRangeSpec: RowRangeSpec,
    private[express] val rowFilterSpec: RowFilterSpec
) extends BaseKijiScheme {
  import TypedKijiScheme._

  private def uri: KijiURI = KijiURI.newBuilder(tableAddress).build()

  private[this] val mInputColumns = KijiKryoExternalizer(icolumns)

  def inputColumns: List[ColumnInputSpec] = mInputColumns.get

  //Create a HashSet of paged column names.
  private val mPagedColumnSet: Map[KijiColumnName, PagingSpec] =
      HashMap(
          inputColumns
              .filterNot { col: ColumnInputSpec => PagingSpec.Off.equals(col.pagingSpec)}
              .map { col: ColumnInputSpec => col.columnName -> col.pagingSpec}:_*)

  /**
   * Sets any configuration options that are required for running a MapReduce job that reads from a
   * Kiji table. This method gets called on the client machine during job setup.
   *
   * @param flow The cascading flow being built.
   * @param tap  The tap that is being used with this scheme.
   * @param conf The Job configuration to which KijiDataRequest will be added.
   */
  override def sourceConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[
          JobConf,
          RecordReader[Container[JEntityId], Container[KijiRowData]],
          OutputCollector[_, _]],
      conf: JobConf
  ): Unit = {
    // Build a data request.
    val request: KijiDataRequest = ResourceUtil.withKijiTable(uri, conf) { table =>
      BaseKijiScheme.buildRequest(table.getLayout, timeRange, inputColumns)
    }
    // Write all the required values to the job's configuration object.
    BaseKijiScheme.configureKijiRowScan(uri, conf, rowRangeSpec, rowFilterSpec)
    // Set data request.
    conf.set(
        KijiConfKeys.KIJI_INPUT_DATA_REQUEST,
        Base64.encodeBase64String(SerializationUtils.serialize(request)))
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row on the cluster.
   *
   * @param flow The current Cascading flow being run.
   * @param sourceCall The source call for the flow that contains the context for this source.
   * @return `true` if another row was read and it was converted to a tuple,
   *     `false` if there were no more rows to read.
   */
  override def source(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[
          KijiSourceContext,
          RecordReader[Container[JEntityId], Container[KijiRowData]]]
  ): Boolean = {

    // Get the current key/value pair.
    val rowContainer = sourceCall.getContext.rowContainer
    // Get the next row.
    if (sourceCall.getInput.next(null, rowContainer)) {
      val row: KijiRowData = rowContainer.getContents
      // Build a tuple from this row.
      val result: Tuple = rowToTuple(row, mPagedColumnSet)

      sourceCall.getIncomingEntry.setTuple(result)
      flow.increment(BaseKijiScheme.CounterGroupName, BaseKijiScheme.CounterSuccess, 1)
      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the RecordReader.
    }
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow The current Cascading flow being run.
   * @param sinkCall The sink call for the flow that contains the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]
  ): Unit = {
    ResourceUtil.withKijiTable(uri, flow.getConfigCopy) { table =>
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
   * @param flow The current Cascading flow being run.
   * @param sinkCall The sink call for the flow that contains the context for this source.
   */
  override def sink(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]
  ): Unit = {
    val DirectKijiSinkContext(eidFactory, writer) = sinkCall.getContext

    def writeSingleValue(singleVal:ExpressColumnOutput[_]): Unit = {
      singleVal.version match {
        case Some(timestamp) =>
          writer.put(
              singleVal.entityId.toJavaEntityId(eidFactory),
              singleVal.family,
              singleVal.qualifier,
              timestamp,
              singleVal.encode (singleVal.datum))
        case None =>
          writer.put(
              singleVal.entityId.toJavaEntityId(eidFactory),
              singleVal.family,
              singleVal.qualifier,
              HConstants.LATEST_TIMESTAMP,
              singleVal.encode (singleVal.datum))
      }
    }
    //The first object in tuple entry contains the data in the pipe.
    sinkCall.getOutgoingEntry.getObject(0) match {
      //Value being written to multiple columns.
      case nColumnOutput: Iterable[ExpressColumnOutput[_]] =>
        nColumnOutput.foreach { anyVal: ExpressColumnOutput[_] =>
          writeSingleValue(anyVal)
        }
      case _ => throw new RuntimeException("Incorrect type. " +
          "The typed sink expects a type Iterable[ExpressColumnOutput[_]]")
    }
  }
}

/**
 * Companion object for TypedKijiScheme containing utility methods.
 */
object TypedKijiScheme {
  /**
   * Converts a row of requested data from KijiTable to a cascading Tuple.
   *
   * The row of results is wrapped in a [[org.kiji.express.flow.ExpressResult]] object before adding
   * it to the cascading tuple. ExpressResult contains methods to access and retrieve the row data.
   * The wrapping of row data is done to enforce a [[ExpressResult]] type for all tuples being read
   * from [[org.kiji.express.flow.TypedKijiSource]] for the type safe api.
   *
   * @param row The row data to be converted to a tuple.
   * @param pagedColumnMap A Map containing the PagingSpecs of the columns that are requested paged.
   * @return A tuple containing the requested values from row.
   */
  private[express] def rowToTuple(
      row: KijiRowData,
      pagedColumnMap: Map[KijiColumnName, PagingSpec]
  ): Tuple = {
    val result: Tuple = new Tuple()
    result.add(ExpressResult(row, pagedColumnMap))
    result
  }
}
