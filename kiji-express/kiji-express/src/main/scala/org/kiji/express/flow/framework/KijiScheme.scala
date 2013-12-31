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
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.ColumnFamilyInputSpec
import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.ColumnOutputSpec
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.PagingSpec
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.RowFilterSpec.NoRowFilterSpec
import org.kiji.express.flow.RowSpec
import org.kiji.express.flow.SchemaSpec
import org.kiji.express.flow.TimeRange
import org.kiji.express.flow.TransientStream
import org.kiji.express.flow.framework.serialization.KijiLocker
import org.kiji.express.flow.util.AvroUtil
import org.kiji.express.flow.util.ResourceUtil.withKijiTable
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.ColumnVersionIterator
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiBufferedWriter
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiURI
import org.kiji.schema.MapFamilyVersionIterator
import org.kiji.schema.avro.SchemaType
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.layout.ColumnReaderSpec
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.{EntityId => JEntityId}

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
 * @param tableAddress of the target Kiji table.
 * @param timeRange to include from the Kiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param icolumns mapping tuple field names to requests for Kiji columns.
 * @param ocolumns mapping tuple field names to specifications for Kiji columns to write out to.
 */
@ApiAudience.Framework
@ApiStability.Experimental
class KijiScheme(
    private[express] val tableAddress: String,
    private[express] val timeRange: TimeRange,
    private[express] val timestampField: Option[Symbol],
    icolumns: Map[String, ColumnInputSpec] = Map(),
    ocolumns: Map[String, ColumnOutputSpec] = Map(),
    private[express] val rowSpec: Option[RowSpec]
) extends Scheme[
    JobConf,
    RecordReader[Container[JEntityId], Container[KijiRowData]],
    OutputCollector[_, _],
    KijiSourceContext,
    DirectKijiSinkContext
] {
  import KijiScheme._

  /** Serialization workaround.  Do not access directly. */
  private[this] val _inputColumns = KijiLocker(icolumns)
  private[this] val _outputColumns = KijiLocker(ocolumns)

  def inputColumns: Map[String, ColumnInputSpec] = _inputColumns.get
  def outputColumns: Map[String, ColumnOutputSpec] = _outputColumns.get

  private def uri: KijiURI = KijiURI.newBuilder(tableAddress).build()

  // Including output column keys here because we might need to read back outputs during test
  // TODO (EXP-250): Ideally we should include outputColumns.keys here only during tests.
  setSourceFields(buildSourceFields(inputColumns.keys ++ outputColumns.keys))
  setSinkFields(buildSinkFields(outputColumns, timestampField))

  /**
   * Sets any configuration options that are required for running a MapReduce job that reads from a
   * Kiji table. This method gets called on the client machine during job setup.
   *
   * @param flow being built.
   * @param tap that is being used with this scheme.
   * @param conf to which we will add our KijiDataRequest.
   */
  override def sourceConfInit(
      flow: FlowProcess[JobConf],
      tap: Tap[
          JobConf,
          RecordReader[Container[JEntityId], Container[KijiRowData]],
          OutputCollector[_, _]
      ],
      conf: JobConf
  ) {
    // Build a data request.
    val request: KijiDataRequest = withKijiTable(uri, conf) { table =>
      buildRequest(table.getLayout, timeRange, inputColumns.values)
    }
    // Write all the required values to the job's configuration object.
    val concreteRowSpec = rowSpec.getOrElse(RowSpec.builder.build)
    val eidFactory = withKijiTable(uri, conf) { table =>
      EntityIdFactory.getFactory(table.getLayout())
    }
    // Set start entity id.
    concreteRowSpec.startEntityId match {
      case Some(entityId) => {
        conf.set(
            KijiConfKeys.KIJI_START_ROW_KEY,
            Base64.encodeBase64String(
                entityId.toJavaEntityId(eidFactory).getHBaseRowKey()))
      }
      case None => {}
    }
    // Set limit entity id.
    concreteRowSpec.limitEntityId match {
      case Some(entityId) => {
        conf.set(
            KijiConfKeys.KIJI_LIMIT_ROW_KEY,
            Base64.encodeBase64String(
                entityId.toJavaEntityId(eidFactory).getHBaseRowKey()))
      }
      case None => {}
    }
    // Set row filter.
    concreteRowSpec.rowFilterSpec.toKijiRowFilter match {
      case Some(kijiRowFilter) => {
        conf.set(KijiConfKeys.KIJI_ROW_FILTER, kijiRowFilter.toJson.toString)
      }
      case None => {}
    }
    // Set data request.
    conf.set(
        KijiConfKeys.KIJI_INPUT_DATA_REQUEST,
        Base64.encodeBase64String(SerializationUtils.serialize(request)))
  }

  /**
   * Sets up any resources required for the MapReduce job. This method is called on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sourceCall containing the context for this source.
   */
  override def sourcePrepare(
      flow: FlowProcess[JobConf],
      sourceCall: SourceCall[
          KijiSourceContext,
          RecordReader[Container[JEntityId], Container[KijiRowData]]
      ]
  ) {
    // Set the context used when reading data from the source.
    sourceCall.setContext(KijiSourceContext(sourceCall.getInput.createValue()))
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
      sourceCall: SourceCall[
          KijiSourceContext,
          RecordReader[Container[JEntityId], Container[KijiRowData]]
      ]
  ): Boolean = {
    // Get the current key/value pair.
    val rowContainer = sourceCall.getContext.rowContainer

    // Get the next row.
    if (sourceCall.getInput.next(null, rowContainer)) {
      val row: KijiRowData = rowContainer.getContents

      // Build a tuple from this row.
      val result: Tuple = rowToTuple(inputColumns, getSourceFields, timestampField, row)

      // If no fields were missing, set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      flow.increment(CounterGroupName, CounterSuccess, 1)

      true // We set a result tuple, return true for success.
    } else {
      false // We reached the end of the RecordReader.
    }
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
      sourceCall: SourceCall[
          KijiSourceContext,
          RecordReader[Container[JEntityId], Container[KijiRowData]]
      ]
  ) {
    sourceCall.setContext(null)
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
      tap: Tap[
          JobConf,
          RecordReader[Container[JEntityId], Container[KijiRowData]],
          OutputCollector[_, _]
      ],
      conf: JobConf
  ) {
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
      sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]) {
    val conf = flow.getConfigCopy

    withKijiTable(uri, conf) { table =>
      // Set the sink context to an opened KijiTableWriter.
      sinkCall.setContext(
        DirectKijiSinkContext(
          EntityIdFactory.getFactory(table.getLayout),
          table.getWriterFactory.openBufferedWriter()))
    }
  }

  /**
   * Converts and writes a Cascading Tuple to a Kiji table. This method is called once
   * for each row on the cluster, so it should be kept as light as possible.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sink(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]) {
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
   * Cleans up any resources used during the MapReduce job. This method is called on the cluster.
   *
   * @param flow is the current Cascading flow being run.
   * @param sinkCall containing the context for this source.
   */
  override def sinkCleanup(
      flow: FlowProcess[JobConf],
      sinkCall: SinkCall[DirectKijiSinkContext, OutputCollector[_, _]]) {
    val writer = sinkCall.getContext.writer
    writer.flush()
    writer.close()
    sinkCall.setContext(null)
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: KijiScheme => (
        inputColumns == other.inputColumns
        && outputColumns == other.outputColumns
        && timestampField == other.timestampField
        && timeRange == other.timeRange)
    case _ => false
  }

  override def hashCode(): Int = Objects.hashCode(
      inputColumns,
      outputColumns,
      timeRange,
      timestampField)
}

/**
 * Companion object for KijiScheme.
 *
 * Contains constants and helper methods for converting between Kiji rows and Cascading tuples,
 * building Kiji data requests, and some utility methods for handling Cascading fields.
 */
@ApiAudience.Framework
@ApiStability.Experimental
object KijiScheme {

  private val logger: Logger = LoggerFactory.getLogger(classOf[KijiScheme])

  /** Hadoop mapred counter group for KijiExpress. */
  private[express] val CounterGroupName = "kiji-express"
  /** Counter name for the number of rows successfully read. */
  private[express] val CounterSuccess = "ROWS_SUCCESSFULLY_READ"
  /** Field name containing a row's [[org.kiji.schema.EntityId]]. */
  val EntityIdField: String = "entityId"
  /** Default number of qualifiers to retrieve when paging in a map type family.*/
  private val qualifierPageSize: Int = 1000

  /**
   * Converts a KijiRowData to a Cascading tuple.
   *
   * If there is no data in a column in this row, the value of that field in the tuple will be an
   * empty iterable of cells.
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
   * @return a tuple containing the values contained in the specified row.
   */
  private[express] def rowToTuple(
      columns: Map[String, ColumnInputSpec],
      fields: Fields,
      timestampField: Option[Symbol],
      row: KijiRowData
  ): Tuple = {
    val result: Tuple = new Tuple()

    // Add the row's EntityId to the tuple.
    val entityId: EntityId = EntityId.fromJavaEntityId(row.getEntityId)
    result.add(entityId)

    def rowToTupleColumnFamily(cf: ColumnFamilyInputSpec) {
      if (row.containsColumn(cf.family)) {
        cf.pagingSpec match {
          case PagingSpec.Off => {
            val stream: Seq[FlowCell[_]] = row
                .iterator(cf.family)
                .asScala
                .toList
                .map { kijiCell: KijiCell[_] => FlowCell(kijiCell) }
            result.add(stream)
          }
          case PagingSpec.Cells(pageSize) => {
            def genItr(): Iterator[FlowCell[_]] = {
              new MapFamilyVersionIterator(row, cf.family, qualifierPageSize, pageSize)
                  .asScala
                  .map { entry: MapFamilyVersionIterator.Entry[_] =>
                    FlowCell(
                        cf.family,
                        entry.getQualifier,
                        entry.getTimestamp,
                        AvroUtil.avroToScala(entry.getValue))
              }
            }
            result.add(new TransientStream(genItr))
          }
        }
      } else {
        result.add(Seq())
      }
    }

    def rowToTupleQualifiedColumn(qc: QualifiedColumnInputSpec) {
      if (row.containsColumn(qc.family, qc.qualifier)) {
        qc.pagingSpec match {
          case PagingSpec.Off => {
            val stream: Seq[FlowCell[_]] = row
                .iterator(qc.family, qc.qualifier)
                .asScala
                .toList
                .map { kijiCell: KijiCell[_] => FlowCell(kijiCell) }
            result.add(stream)
          }
          case PagingSpec.Cells(pageSize) => {
            def genItr(): Iterator[FlowCell[_]] = {
              new ColumnVersionIterator(row, qc.family, qc.qualifier, pageSize)
                  .asScala
                  .map { entry: java.util.Map.Entry[java.lang.Long,_] =>
                    FlowCell(
                      qc.family,
                      qc.qualifier,
                      entry.getKey,
                      AvroUtil.avroToScala(entry.getValue)
                    )
                  }
            }
            result.add(new TransientStream(genItr))
          }
        }
      } else {
        result.add(Seq())
      }
    }

    // Get rid of the entity id and timestamp fields, then map over each field to add a column
    // to the tuple.
    fields
        .iterator()
        .asScala
        .filter { field => field.toString != EntityIdField }
        .filter { field => field.toString != timestampField.getOrElse("") }
        .map { field => columns(field.toString) }
        // Build the tuple, by adding each requested value into result.
        .foreach {
          case cf: ColumnFamilyInputSpec => rowToTupleColumnFamily(cf)
          case qc: QualifiedColumnInputSpec => rowToTupleQualifiedColumn(qc)
        }

    return result
  }

  /**
   * Builds a data request out of the timerange and list of column requests.
   *
   * @param timeRange of cells to retrieve.
   * @param columns to retrieve.
   * @return data request configured with timeRange and columns.
   */
  private[express] def buildRequest(
      layout: KijiTableLayout,
      timeRange: TimeRange,
      columns: Iterable[ColumnInputSpec]
  ): KijiDataRequest = {
    def addColumn(
        builder: KijiDataRequestBuilder,
        column: ColumnInputSpec
    ): KijiDataRequestBuilder.ColumnsDef = {
      val kijiFilter: KijiColumnFilter = column
          .filterSpec
          .toKijiColumnFilter
          .getOrElse(null)
      val columnReaderSpec: ColumnReaderSpec = {
        // Check and ensure that this column isn't a counter, protobuf, or raw bytes encoded column.
        // If it is, ignore the provided schema spec.
        val schemaType = column match {
          case QualifiedColumnInputSpec(family, qualifier, _, _, _, _) => {
            // If this fully qualified column is actually part of a map-type column family,
            // then get the schema type from the map-type column family instead. Otherwise get it
            // from the qualified column as usual.
            val columnFamily = layout
                .getFamilyMap
                .get(column.columnName.getFamily)
            if (columnFamily.isMapType) {
              columnFamily
                  .getDesc
                  .getMapSchema
                  .getType
            } else {
              columnFamily
                  .getColumnMap
                  .get(column.columnName.getQualifier)
                  .getDesc
                  .getColumnSchema
                  .getType
            }
          }
          case ColumnFamilyInputSpec(family, _, _, _, _) => {
            layout
                .getFamilyMap
                .get(column.columnName.getFamily)
                .getDesc
                .getMapSchema
                .getType
          }
        }
        schemaType match {
          case SchemaType.COUNTER => ColumnReaderSpec.counter()
          case SchemaType.PROTOBUF => ColumnReaderSpec.protobuf()
          case SchemaType.RAW_BYTES => ColumnReaderSpec.bytes()
          case _ => column.schemaSpec match {
            case SchemaSpec.DefaultReader => ColumnReaderSpec.avroDefaultReaderSchemaGeneric()
            case SchemaSpec.Writer => ColumnReaderSpec.avroWriterSchemaGeneric()
            case SchemaSpec.Generic(schema) => ColumnReaderSpec.avroReaderSchemaGeneric(schema)
            case SchemaSpec.Specific(record) => ColumnReaderSpec.avroReaderSchemaSpecific(record)
          }
        }
      }

      builder.newColumnsDef()
          .withMaxVersions(column.maxVersions)
          .withFilter(kijiFilter)
          .withPageSize(column.pagingSpec.cellsPerPage.getOrElse(0))
          .add(column.columnName, columnReaderSpec)
    }

    val requestBuilder: KijiDataRequestBuilder = KijiDataRequest.builder()
        .withTimeRange(timeRange.begin, timeRange.end)

    columns.foreach(column => addColumn(requestBuilder, column))
    requestBuilder.build()
  }

  /**
   * Transforms a list of field names into a Cascading [[cascading.tuple.Fields]] object.
   *
   * @param fieldNames is a list of field names.
   * @return a Fields object containing the names.
   */
  private def toField(fieldNames: Iterable[Comparable[_]]): Fields = {
    new Fields(fieldNames.toArray:_*)
  }

  /**
   * Builds the list of tuple fields being read by a scheme. The special field name
   * "entityId" will be included to hold entity ids from the rows of Kiji tables.
   *
   * @param fieldNames is a list of field names that a scheme should read.
   * @return is a collection of fields created from the names.
   */
  private[express] def buildSourceFields(fieldNames: Iterable[String]): Fields = {
    toField(Set(EntityIdField) ++ fieldNames)
  }

  /**
   * Builds the list of tuple fields being written by a scheme. The special field name "entityId"
   * will be included to hold entity ids that values should be written to. Any fields that are
   * specified as qualifiers for a map-type column family will also be included. A timestamp field
   * can also be included, identifying a timestamp that all values will be written to.
   *
   * @param columns is the column requests for this Scheme, with the names of each of the
   *     fields that contain data to write to Kiji.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @return a collection of fields created from the parameters.
   */
  private[express] def buildSinkFields(
      columns: Map[String, ColumnOutputSpec],
      timestampField: Option[Symbol]
  ): Fields = {
    toField(Set(EntityIdField)
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
      columns: Map[String, ColumnOutputSpec]
  ): Iterator[String] =
    columns.valuesIterator.collect {
      case x: ColumnFamilyOutputSpec => x.qualifierSelector.name
    }
}

/**
 * Container for a Kiji row data and Kiji table layout object that is required by a map reduce
 * task while reading from a Kiji table.
 *
 * @param rowContainer is the representation of a Kiji row.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[express] final case class KijiSourceContext(rowContainer: Container[KijiRowData])

/**
 * Container for the table writer and Kiji table layout required during a sink
 * operation to write the output of a map reduce task back to a Kiji table.
 * This is configured during the sink prepare operation.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
private[express] final case class DirectKijiSinkContext(
    eidFactory: EntityIdFactory,
    writer: KijiBufferedWriter)
