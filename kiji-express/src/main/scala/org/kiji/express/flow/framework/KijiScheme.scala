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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters.asScalaIteratorConverter

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.flow.ColumnFamily
import org.kiji.express.flow.ColumnRequest
import org.kiji.express.flow.ColumnRequestOptions
import org.kiji.express.flow.QualifiedColumn
import org.kiji.express.flow.TimeRange
import org.kiji.express.util.AvroUtil
import org.kiji.express.util.ExpressGenericTable
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout

/**
 * A Kiji-specific implementation of a Cascading `Scheme`, which defines how to read and write the
 * data stored in a Kiji table.
 *
 * KijiScheme is responsible for converting rows from a Kiji table that are input to a Cascading
 * flow into Cascading tuples
 * (see `source(cascading.flow.FlowProcess, cascading.scheme.SourceCall)`) and writing output
 * data from a Cascading flow to a Kiji table
 * (see `sink(cascading.flow.FlowProcess, cascading.scheme.SinkCall)`).
 *
 * KijiScheme must be used with [[org.kiji.express.flow.framework.KijiTap]],
 * since it expects the Tap to have access to a Kiji table.  [[org.kiji.express.flow.KijiSource]]
 * handles the creation of both KijiScheme and KijiTap in KijiExpress.
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
private[express] class KijiScheme(
    private[express] val timeRange: TimeRange,
    private[express] val timestampField: Option[Symbol],
    private[express] val loggingInterval: Long,
    private[express] val columns: Map[String, ColumnRequest])
    extends Scheme[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _],
        KijiSourceContext, KijiSinkContext] {
  import KijiScheme._

  /** Keeps track of how many rows have been skipped, for logging purposes. */
  private val skippedRows: AtomicLong = new AtomicLong()

  /** Set the fields that should be in a tuple when this source is used for reading and writing. */
  setSourceFields(buildSourceFields(columns.keys))
  setSinkFields(buildSinkFields(columns, timestampField))

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our KijiDataRequest.
   */
  override def sourceConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]],
      conf: JobConf) {
    // Build a data request.
    val request: KijiDataRequest = buildRequest(timeRange, columns.values)

    // Write all the required values to the job's configuration object.
    conf.setInputFormat(classOf[KijiInputFormat])
    conf.set(
        KijiConfKeys.KIJI_INPUT_DATA_REQUEST,
        Base64.encodeBase64String(SerializationUtils.serialize(request)))
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]) {
    val tableUriProperty = flow.getStringProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI)

    // Construct an instance of ExpressGenericTable to reuse across all calls to the source method.
    val uri: KijiURI = KijiURI.newBuilder(tableUriProperty).build()
    val columnNames = columns.values.map { column => column.getColumnName() }
    val expressGenericTable = new ExpressGenericTable(uri, flow.getConfigCopy, columnNames.toSeq)
    // Set the context of the sourcecall to include the single instance of ExpressGenericTable
    sourceCall.setContext(KijiSourceContext(
        sourceCall.getInput().createValue(),
        uri,
        expressGenericTable))
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   * @return `true` if another row was read and it was converted to a tuple,
   *     `false` if there were no more rows to read.
   */
  override def source(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]): Boolean = {
    // Get the current key/value pair.
    val KijiSourceContext(value, tableUri, expressGenericTable) = sourceCall.getContext()

    // Get the first row where all the requested columns are present,
    // and use that to set the result tuple.
    // Return true as soon as a result tuple has been set,
    // or false if we reach the end of the RecordReader.

    // scalastyle:off null
    while (sourceCall.getInput().next(null, value)) {
    // scalastyle:on null
      val row: KijiRowData = value.get()
      val result: Option[Tuple] = rowToTuple(
          columns,
          getSourceFields,
          timestampField,
          row,
          tableUri,
          expressGenericTable)

      // If no fields were missing, set the result tuple and return from this method.
      result match {
        case Some(tuple) => {
          sourceCall.getIncomingEntry().setTuple(tuple)
          flow.increment(counterGroupName, counterSuccess, 1)
          return true // We set a result tuple, return true for success.
        }
        case None => {
          // Otherwise, this row was missing fields.
          // Increment the counter for rows with missing fields.
          flow.increment(counterGroupName, counterMissingField, 1)
          if (skippedRows.getAndIncrement() % loggingInterval == 0) {
            logger.warn("Row %s skipped because of missing field(s)."
                .format(row.getEntityId.toShellString))
          }
        }
      }
      // We didn't return true because this row was missing fields; continue the loop.
    }
    return false // We reached the end of the RecordReader.
  }

  /**
   * Cleans up any resources used during the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow currently being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourceCleanup(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiSourceContext, RecordReader[KijiKey, KijiValue]]) {
    val KijiSourceContext(_, _, expressGenericTable) = sourceCall.getContext()
    expressGenericTable.close()
    // scalastyle:off null
    sourceCall.setContext(null)
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
      tap: Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]],
      conf: JobConf) {
    // No-op since no configuration parameters need to be set to encode data for Kiji.
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called
   * on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkPrepare(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]) {
    // Open a table writer.
    val uriString: String = flow.getConfigCopy().get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(uriString).build()

    doAndRelease(Kiji.Factory.open(uri, flow.getConfigCopy)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(uri.getTable())) { table: KijiTable =>
        // Set the sink context to an opened KijiTableWriter.
        sinkCall.setContext(KijiSinkContext(table.openTableWriter(), table.getLayout))
      }
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
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]) {
    // Retrieve writer from the scheme's context.
    val KijiSinkContext(writer, layout) = sinkCall.getContext()

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry()
    putTuple(columns, timestampField, output, writer, layout)
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
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]) {
    // Close the writer.
    sinkCall.getContext().kijiTableWriter.close()
    // scalastyle:off null
    sinkCall.setContext(null)
    // scalastyle:on null
  }

  override def equals(other: Any): Boolean = {
    other match {
      case scheme: KijiScheme => {
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
 * Companion object for KijiScheme.
 *
 * Contains constants and helper methods for converting between Kiji rows and Cascading tuples,
 * building Kiji data requests, and some utility methods for handling Cascading fields.
 */
private[express] object KijiScheme {
  private val logger: Logger = LoggerFactory.getLogger(classOf[KijiScheme])

  /** Hadoop mapred counter group for KijiExpress. */
  private[express] val counterGroupName = "kiji-express"
  /** Counter name for the number of rows successfully read. */
  private[express] val counterSuccess = "ROWS_SUCCESSFULLY_READ"
  /** Counter name for the number of rows skipped because of missing fields. */
  private[express] val counterMissingField = "ROWS_SKIPPED_WITH_MISSING_FIELDS"

  /** Field name containing a row's [[org.kiji.schema.EntityId]]. */
  private[express] val entityIdField: String = "entityId"

  /**
   * Converts a KijiRowData to a Cascading tuple.
   *
   * If there is no data in a column in this row, the value of that field in the tuple will be the
   * replacement specified in the request for that column.
   *
   * Returns None if one of the columns didn't exist and no replacement for it was specified.
   *
   * This is used in the `source` method of KijiScheme, for reading rows from Kiji into
   * KijiExpress tuples.
   *
   * @param columns Mapping from field name to column definition.
   * @param fields Field names of desired tuple elements.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @param row to convert to a tuple.
   * @param tableUri is the URI of the Kiji table.
   * @param genericTable is an instance of ExpressGenericTable to use for reading/writing generic
   *     AvroRecords.
   * @return a tuple containing the values contained in the specified row, or None if some columns
   *     didn't exist and no replacement was specified.
   */
  private[express] def rowToTuple(
      columns: Map[String, ColumnRequest],
      fields: Fields,
      timestampField: Option[Symbol],
      row: KijiRowData,
      tableUri: KijiURI,
      genericTable: ExpressGenericTable): Option[Tuple] = {
    val result: Tuple = new Tuple()
    val iterator = fields.iterator().asScala

    // Add the row's EntityId to the tuple.
    result.add(EntityId(tableUri.toString(), row.getEntityId()))

    val expressRow = genericTable.getRow(row)

    // Get rid of the entity id and timestamp fields, then map over each field to add a column
    // to the tuple.
    iterator
        .filter { field => field.toString != entityIdField  }
        .filter { field => field.toString != timestampField.getOrElse(Symbol("")).name }
        .map { field => columns(field.toString) }
        // Build the tuple, by adding each requested value into result.
        .foreach {
            case ColumnFamily(family, _, ColumnRequestOptions(_, _, replacementOption)) => {
              if (row.containsColumn(family)) {
                result.add (KijiSlice(expressRow.iterator(family)))
              } else {
                replacementOption match {
                  // TODO convert replacement slice
                  case Some(replacement) => {
                    result.add(replacement)
                  }
                  case None =>
                    // this row cannot be converted to a tuple since this column is missing.
                    return None
                }
              }
            }
            case QualifiedColumn(
                family,
                qualifier,
                ColumnRequestOptions(_, _, replacementOption)) => {
              if (row.containsColumn(family, qualifier)) {
                result.add(KijiSlice(expressRow.iterator(family, qualifier)))
              } else {
                replacementOption match {
                  // TODO convert replacement slice
                  case Some(replacement) => {
                    result.add(replacement)
                  }
                  // this row cannot be converted to a tuple since this column is missing.
                  case None => return None
                }
              }
            }
        }

    return Some(result)
  }

  // TODO(CHOP-35): Use an output format that writes to HFiles.
  /**
   * Writes a Cascading tuple to a Kiji table.
   *
   * This is used in KijiScheme's `sink` method.
   *
   * @param columns mapping field names to column definitions.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @param output to write out to Kiji.
   * @param writer to use for writing to Kiji.
   * @param layout Kiji table layout.
   */
  private[express] def putTuple(
      columns: Map[String, ColumnRequest],
      timestampField: Option[Symbol],
      output: TupleEntry,
      writer: KijiTableWriter,
      layout: KijiTableLayout) {
    val iterator = columns.keys.iterator

    // Get the entityId.
    val entityId: EntityId = output.getObject(entityIdField).asInstanceOf[EntityId]

    // Get a timestamp to write the values to, if it was specified by the user.
    val timestamp: Long = timestampField match {
      case Some(field) => {
        output.getObject(field.name).asInstanceOf[Long]
      }
      case None => System.currentTimeMillis()
    }

    iterator
        .foreach { fieldName =>
            val value = output.getObject(fieldName.toString())
            columns(fieldName.toString()) match {
              case ColumnFamily(family, qualField, _) => {
                require(
                    qualField.isDefined,
                    "You cannot write to a map family without specifying a qualifier field.")
                writer.put(entityId.toJavaEntityId(),
                  family,
                  output.getObject(qualField.get).asInstanceOf[String],
                  timestamp,
                  AvroUtil.encodeToJava(value))
              }
              case QualifiedColumn(family, qualifier, _) => {
                writer.put(
                    entityId.toJavaEntityId(),
                    family,
                    qualifier,
                    timestamp,
                    AvroUtil.encodeToJava(value))
              }
            }
        }
  }

  /**
   * Builds a data request out of the timerange and list of column requests.
   *
   * @param timeRange of cells to retrieve.
   * @param columns to retrieve.
   * @return data request configured with timeRange and columns.
   */
  private[express] def buildRequest(
      timeRange: TimeRange,
      columns: Iterable[ColumnRequest]): KijiDataRequest = {
    def addColumn(builder: KijiDataRequestBuilder, column: ColumnRequest) {
      column match {
        case ColumnFamily(family, _, inputOptions) => {
          builder.newColumnsDef()
              .withMaxVersions(inputOptions.maxVersions)
              // scalastyle:off null
              .withFilter(inputOptions.filter.getOrElse(null))
              // scalastyle:on null
              .add(new KijiColumnName(family))
         }
         case QualifiedColumn(family, qualifier, inputOptions) => {
           builder.newColumnsDef()
               .withMaxVersions(inputOptions.maxVersions)
               // scalastyle:off null
              .withFilter(inputOptions.filter.getOrElse(null))
              // scalastyle:on null
              .add(new KijiColumnName(family, qualifier))
        }
      }
    }

    val requestBuilder: KijiDataRequestBuilder = KijiDataRequest.builder()
        .withTimeRange(timeRange.begin, timeRange.end)

    columns
        .foldLeft(requestBuilder) { (builder, column) =>
          addColumn(builder, column)
          builder
        }
        .build()
  }

  /**
   * Transforms a list of field names into a collection of fields.
   *
   * @param fieldNames is a list of field names.
   * @return a collection of fields created from the names.
   */
  private def getFieldArray(fieldNames: Iterable[String]): Fields = {
    Fields.join(fieldNames.map { new Fields(_) }.toArray: _*)
  }

  /**
   * Builds the list of tuple fields being read by a scheme. The special field name
   * "entityId" will be included to hold entity ids from the rows of Kiji tables.
   *
   * @param fieldNames is a list of field names that a scheme should read.
   * @return is a collection of fields created from the names.
   */
  private[express] def buildSourceFields(fieldNames: Iterable[String]): Fields = {
    getFieldArray(Seq(entityIdField) ++ fieldNames)
  }

  /**
   * Builds the list of tuple fields being written by a scheme. The special field name "entityId"
   * will be included to hold entity ids that values should be written to. Any fields that are
   * specified as qualifiers for a map-type column family will also be included. A timestamp field
   * can also be included, identifying a timestamp that all values will be written to.
   *
   * @param columns is the column requests for this Scheme, with the names of each of the fields
   *     that contain data to write to Kiji.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @return a collection of fields created from the parameters.
   */
  private[express] def buildSinkFields(columns: Map[String, ColumnRequest],
      timestampField: Option[Symbol]): Fields = {
    getFieldArray(Seq(entityIdField)
        ++ columns.keys
        ++ extractQualifierSelectors(columns)
        ++ timestampField.map { _.name } )
  }

  /**
   * Extracts the names of qualifier selectors from the column requests for a Scheme.
   *
   * @param columns is the column requests for a Scheme.
   * @return the names of fields that are qualifier selectors.
   */
  private[express] def extractQualifierSelectors(
      columns: Map[String, ColumnRequest]): Seq[String] = {
    columns.collect {
      case (_, ColumnFamily(_, Some(qualField), _)) => qualField
    }.toSeq
  }
}
