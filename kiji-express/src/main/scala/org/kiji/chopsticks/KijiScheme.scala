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

import java.io.InvalidClassException
import java.util.concurrent.atomic.AtomicLong
import java.util.TreeMap

import scala.collection.JavaConverters._

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificFixed
import org.apache.avro.util.Utf8
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.EntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.InvalidLayoutException
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
 * KijiScheme must be used with [[org.kiji.chopsticks.KijiTap]], since it expects the Tap to have
 * access to a Kiji table.  [[org.kiji.chopsticks.KijiSource]] handles the creation of both
 * KijiScheme and KijiTap in Chopsticks.
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
private[chopsticks] class KijiScheme(
    private val timeRange: TimeRange,
    private val timestampField: Option[Symbol],
    private val loggingInterval: Long,
    private val columns: Map[String, ColumnRequest])
    extends Scheme[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _],
        KijiSourceContext, KijiSinkContext] {
  import KijiScheme._

  // Keeps track of how many rows have been skipped, for logging purposes.
  private val skippedRows: AtomicLong = new AtomicLong()

  /** Set the fields that should be in a tuple when this source is used for reading and writing. */
  setSourceFields(buildSourceFields(columns.keys))
  setSinkFields(buildSinkFields(columns.keys, timestampField))

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
    // TODO CHOP-71 Remove hack to check for null table uri in sourcePrepare
    // get table layout
    val tableLayout = if (null != tableUriProperty) {
      val tableUri: KijiURI = KijiURI.newBuilder(tableUriProperty).build()
      doAndRelease(Kiji.Factory.open(tableUri)) { kiji: Kiji =>
        doAndRelease(kiji.openTable(tableUri.getTable())) { table: KijiTable =>
            table.getLayout()
        }
      }
    } else {
      null
    }
    val context = KijiSourceContext(
        sourceCall.getInput().createValue(),
        tableLayout)
    sourceCall.setContext(context)
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
    val KijiSourceContext(value, layout) = sourceCall.getContext()

    // Get the first row where all the requested columns are present,
    // and use that to set the result tuple.
    // Return true as soon as a result tuple has been set,
    // or false if we reach the end of the RecordReader.

    // scalastyle:off null
    while (sourceCall.getInput().next(null, value)) {
    // scalastyle:on null
      val row: KijiRowData = value.get()
      val result: Option[Tuple] = rowToTuple(columns, getSourceFields, timestampField, row)

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

    // TODO: CHOP-72 Check and see if Kiji.Factory.open should be passed the configuration object in
    //     flow.
    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
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
    putTuple(columns, getSinkFields(), timestampField, output, writer, layout)
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
private[chopsticks] object KijiScheme {
  private val logger: Logger = LoggerFactory.getLogger(classOf[KijiScheme])

  /** Hadoop mapred counter group for Chopsticks. */
  private[chopsticks] val counterGroupName = "kiji-chopsticks"
  /** Counter name for the number of rows successfully read. */
  private[chopsticks] val counterSuccess = "ROWS_SUCCESSFULLY_READ"
  /** Counter name for the number of rows skipped because of missing fields. */
  private[chopsticks] val counterMissingField = "ROWS_SKIPPED_WITH_MISSING_FIELDS"

  /** Field name containing a row's [[EntityId]]. */
  private[chopsticks] val entityIdField: String = "entityId"

  /**
   * Converts a KijiRowData to a Cascading tuple.
   *
   * If there is no data in a column in this row, the value of that field in the tuple will be the
   * replacement specified in the request for that column.
   *
   * Returns None if one of the columns didn't exist and no replacement for it was specified.
   *
   * This is used in the `source` method of KijiScheme, for reading rows from Kiji into Chopsticks
   * tuples.
   *
   * @param columns Mapping from field name to column definition.
   * @param fields Field names of desired tuple elements.
   * @param row The row data.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @return a tuple containing the values contained in the specified row, or None if some columns
   *     didn't exist and no replacement was specified.
   */
  private[chopsticks] def rowToTuple(
      columns: Map[String, ColumnRequest],
      fields: Fields,
      timestampField: Option[Symbol],
      row: KijiRowData): Option[Tuple] = {
    val result: Tuple = new Tuple()
    val iterator = fields.iterator().asScala

    // Add the row's EntityId to the tuple.
    result.add(row.getEntityId())

    // Get rid of the entity id and timestamp fields, then map over each field to add a column
    // to the tuple.
    iterator
        .filter { field => field.toString != entityIdField  }
        .filter { field => field.toString != timestampField.getOrElse(Symbol("")).name }
        .map { field => columns(field.toString) }
        // Build the tuple, by adding each requested value into result.
        .foreach {
            case colReq @ ColumnFamily(family, ColumnRequestOptions(_, _, replacement)) => {
              if (row.containsColumn(family)) {
                val familyValues: java.util.NavigableMap[String,
                    java.util.NavigableMap[java.lang.Long, AnyRef]] = row.getValues[AnyRef](family)
                val scalaFamilyValues: Map[String,
                    java.util.NavigableMap[java.lang.Long, AnyRef]] = familyValues.asScala.toMap
                result.add(
                    scalaFamilyValues.map {
                        kv: (String, java.util.NavigableMap[java.lang.Long, AnyRef]) =>
                            (kv._1, convertKijiValuesToScalaTypes(kv._2))
                    }
                )
              } else {
                replacement match {
                  // TODO convert replacement slice
                  case Some(replacementSlice) => {
                    result.add(replacementSlice)
                  }
                  case None =>
                    // this row cannot be converted to a tuple since this column is missing.
                    return None
                }
              }
            }
            case colReq @ QualifiedColumn(
                family,
                qualifier,
                ColumnRequestOptions(_, _, replacement)) => {
              if (row.containsColumn(family, qualifier)) {
                result.add(convertKijiValuesToScalaTypes(row.getValues(family, qualifier)))
              } else {
                replacement match {
                  // TODO convert replacement slice
                  case Some(replacementSlice) => {
                    result.add(replacementSlice)
                  }
                  // this row cannot be converted to a tuple since this column is missing.
                  case None => return None
                }
              }
            }
        }

    return Some(result)
  }

  /**
   * Convert a timeline of Java values that are read from a column into a corresponding Scala map.
   *
   * @param timeline is a map from Long (version) to column values for the version.
   * @return a Scala Map corresponding to the Java timeline.
   */
  private[chopsticks] def convertKijiValuesToScalaTypes (
      timeline: java.util.NavigableMap[java.lang.Long, _])
      : Map[Long, Any] = {
    timeline
        .asScala
        .map { case (timestamp, value) => (timestamp.longValue(), convertJavaType(value)) }
        .toMap
  }

  /**
   * Convert Java types (that came from Kiji columns) into corresponding Scala types for
   * usage within Chopsticks.
   *
   * @param columnValue is the value of the Kiji column.
   * @return the corresponding value converted to a Scala type.
   */
  private def convertJavaType(columnValue: Any): Any = {
    columnValue match {
      // scalastyle:off null
      case null => null
      // scalastyle:on null
      case i: java.lang.Integer => i
      case b: java.lang.Boolean => b
      case l: java.lang.Long => l
      case f: java.lang.Float => f
      case d: java.lang.Double => d
      // bytes
      case bb: java.nio.ByteBuffer => bb.array()
      // string
      case s: java.lang.CharSequence => s.toString()
      // array
      case l: java.util.List[Any] => {
        l.asScala
            .toList
            .map { elem => convertJavaType(elem) }
      }
      // map (avro maps always have keys of type CharSequence)
      // TODO CHOP-70 revisit conversion of maps between java and scala
      case m: java.util.Map[CharSequence, Any] => {
        m.asScala
            .toMap
            .map { case (key, value) => (key.toString, convertJavaType(value)) }
      }
      // fixed
      case f: SpecificFixed => f.bytes().array
      // scalastyle:off null
      // null field
      case n: java.lang.Void => null
      // scalastyle:on null
      // enum
      case e: java.lang.Enum[Any] => e
      // avro record or object
      case a: IndexedRecord => a
      // any other type we don't understand
      case _ => throw new InvalidClassException("Read an unrecognized Java type from Kiji "
          + "that could not be converted to a Scala type for use with Chopsticks: "
          + columnValue.asInstanceOf[AnyRef].getClass.toString)
    }
  }

  // TODO(CHOP-35): Use an output format that writes to HFiles.
  /**
   * Writes a Cascading tuple to a Kiji table.
   *
   * This is used in KijiScheme's `sink` method.
   *
   * @param columns mapping field names to column definitions.
   * @param fields names of incoming tuple elements.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @param output to write out to Kiji.
   * @param writer to use for writing to Kiji.
   * @param layout Kiji table layout.
   */
  private[chopsticks] def putTuple(
      columns: Map[String, ColumnRequest],
      fields: Fields,
      timestampField: Option[Symbol],
      output: TupleEntry,
      writer: KijiTableWriter,
      layout: KijiTableLayout) {
    val iterator = fields.iterator().asScala

    // Get the entityId.
    val entityId: EntityId = output.getObject(entityIdField).asInstanceOf[EntityId]
    iterator.next()

    // Get a timestamp to write the values to, if it was specified by the user.
    val timestamp: Long = timestampField match {
      case Some(field) => {
        iterator.next()
        output.getObject(field.name).asInstanceOf[Long]
      }
      case None => System.currentTimeMillis()
    }

    iterator.foreach { fieldName =>
      columns(fieldName.toString()) match {
        case ColumnFamily(family, _) => {
          // TODO CHOP-67 Design putTuple semantics for map type column families
          throw new UnsupportedOperationException("Writing to a column family without a "
              + "qualifier is not supported.")
        }
        case QualifiedColumn(family, qualifier, _) => {
          val kijiCol = new KijiColumnName(family, qualifier)
          writer.put(
              entityId,
              family,
              qualifier,
              timestamp,
              convertScalaTypes(output.getObject(
                  fieldName.toString()), layout.getSchema(kijiCol)))
        }
      }
    }
  }

  /**
   * Convert a timeline of values to be written to a column into a corresponding Java map.
   *
   * @tparam T is the type of the column value.
   * @param timeline is a map from Long (versions) to column values at that version.
   * @param columnSchema is the schema of the column to be written to.
   * @return the corresponding Java map for the Kiji table.
   */
  private[chopsticks] def convertScalaTypesToKijiValues[T](
      timeline: Map[Long, T],
      columnSchema: Schema)
      : java.util.NavigableMap[java.lang.Long, java.lang.Object] = {
    val convertedMap = timeline
        .map { case (timestamp, value) =>
          (java.lang.Long.valueOf(timestamp), convertScalaTypes(value, columnSchema))
        }
        .asJava
    new java.util.TreeMap(convertedMap)
  }

  /**
   * Convert Scala types back to Java types to write to a Kiji table.
   *
   * @param columnValue is the value written to this column.
   * @param columnSchema is the schema of the column.
   * @return the converted Java type.
   */
  private def convertScalaTypes(
      columnValue: Any,
      columnSchema: Schema): java.lang.Object = {
    columnValue match {
      // scalastyle:off null
      case null => null
      // scalastyle:on null
      case i: Int => i.asInstanceOf[java.lang.Integer]
      case b: Boolean => b.asInstanceOf[java.lang.Boolean]
      case l: Long => l.asInstanceOf[java.lang.Long]
      case f: Float => f.asInstanceOf[java.lang.Float]
      case d: Double => d.asInstanceOf[java.lang.Double]
      // this could be an avro "bytes" field or avro "fixed" field
      case bb: Array[Byte] => {
        if (columnSchema.getType == Schema.Type.BYTES) {
          java.nio.ByteBuffer.wrap(bb)
        } else if (columnSchema.getType == Schema.Type.FIXED) {
          new Fixed(columnSchema, bb)
        } else {
          throw new SchemaMismatchException("Writing an array of bytes to a column that "
            + " expects " + columnSchema.getType.getName)
        }
      }
      case s: String => s
      // this is an avro array
      case l: List[Any] => {
        l.map { elem => convertScalaTypes(elem, columnSchema.getElementType) }
            .asJava
      }
      // map
      // TODO CHOP-70 revisit conversion of maps between java and scala
      case m: Map[String, Any] => {
        val convertedMap = m.map { case (key, value) =>
          val convertedValue = convertScalaTypes(value, columnSchema.getValueType)
          (key, convertedValue)
        }
        new java.util.TreeMap[java.lang.Object, java.lang.Object](convertedMap.asJava)
      }
      // enum
      case e: java.lang.Enum[Any] => e
      // avro record or object
      case a: IndexedRecord => a
      // any other type we don't understand
      case _ => throw new InvalidClassException("Trying to write an unrecognized Scala/Java type"
          + " that cannot be converted to a Java type for writing to Kiji: "
          + columnValue.asInstanceOf[AnyRef].getClass.toString)
    }
  }

  /**
   * Builds a data request out of the timerange and list of column requests.
   *
   * @param timeRange of cells to retrieve.
   * @param columns to retrieve.
   * @return data request configured with timeRange and columns.
   */
  private[chopsticks] def buildRequest(
      timeRange: TimeRange,
      columns: Iterable[ColumnRequest]): KijiDataRequest = {
    def addColumn(builder: KijiDataRequestBuilder, column: ColumnRequest) {
      column match {
        case ColumnFamily(family, inputOptions) => {
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
   * Gets a collection of fields by joining two lists of field names and transforming each name
   * into a field.
   *
   * @param headNames is a list of field names.
   * @param tailNames is a list of field names.
   * @return a collection of fields created from the names.
   */
  private def getFieldArray(headNames: Iterable[String], tailNames: Iterable[String]): Fields = {
    Fields.join((headNames ++ tailNames).map { new Fields(_) }.toArray: _*)
  }

  /**
   * Builds the list of tuple fields being read by a scheme. The special field name
   * "entityId" will be included to hold entity ids from the rows of Kiji tables.
   *
   * @param fieldNames is a list of field names that a scheme should read.
   * @return is a collection of fields created from the names.
   */
  private[chopsticks] def buildSourceFields(fieldNames: Iterable[String]): Fields = {
    getFieldArray(Seq(entityIdField), fieldNames)
  }

  /**
   * Builds the list of tuple fields being written by a scheme. The special field name "entityId"
   * will be included to hold entity ids that values should be written to. A timestamp field can
   * also be included, identifying a timestamp that all values will be written to.
   *
   * @param fieldNames is a list of field names that a scheme should write to.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @return is a collection of fields created from the names.
   */
  private[chopsticks] def buildSinkFields(fieldNames: Iterable[String],
      timestampField: Option[Symbol]): Fields = {
    timestampField match {
      case Some(field) => getFieldArray(Seq(entityIdField, field.name), fieldNames)
      case None => getFieldArray(Seq(entityIdField), fieldNames)
    }
  }
}
