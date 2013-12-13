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
import org.apache.avro.Schema
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.flow.ColumnFamilyInputSpec
import org.kiji.express.flow.ColumnFamilyOutputSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.ColumnOutputSpec
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.FlowCell
import org.kiji.express.flow.PagingSpec
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.SchemaSpec
import org.kiji.express.flow.TimeRange
import org.kiji.express.flow.TransientStream
import org.kiji.express.flow.framework.serialization.KijiLocker
import org.kiji.express.flow.util.AvroUtil
import org.kiji.express.flow.util.Resources.withKijiTable
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.layout.ColumnReaderSpec
import org.kiji.schema.ColumnVersionIterator
import org.kiji.schema.{ EntityId => JEntityId }
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiCell
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiSchemaTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI
import org.kiji.schema.MapFamilyVersionIterator
import org.kiji.schema.avro.AvroSchema
import org.kiji.schema.avro.SchemaType
import org.kiji.schema.filter.KijiColumnFilter
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
 * @param tableUri of the target Kiji table.
 * @param timeRange to include from the Kiji table.
 * @param timestampField is the optional name of a field containing the timestamp that all values
 *     in a tuple should be written to.
 *     Use None if all values should be written at the current time.
 * @param inputColumns mapping tuple field names to requests for Kiji columns.
 * @param outputColumns mapping tuple field names to specifications for Kiji columns to write out
 *     to.
 */
@ApiAudience.Framework
@ApiStability.Experimental
class KijiScheme(
    tableUri: KijiURI,
    private[express] val timeRange: TimeRange,
    private[express] val timestampField: Option[Symbol],
    @transient private[express] val inputColumns: Map[String, ColumnInputSpec] = Map(),
    @transient private[express] val outputColumns: Map[String, ColumnOutputSpec] = Map()
) extends Scheme[
    JobConf,
    RecordReader[Container[JEntityId], Container[KijiRowData]],
    OutputCollector[_, _],
    KijiSourceContext,
    KijiSinkContext
] {
  import KijiScheme._

  /**
   * Address of the table to read from or write to. This is stored as a string because KijiURIs are
   * not serializable.
   */
  private val uri: String = tableUri.toString

  // ColumnInputSpec and ColumnOutputSpec objects cannot be correctly serialized via
  // java.io.Serializable.  Chiefly, Avro objects including Schema and all of the Generic types
  // are not Serializable.  By making the inputColumns and outputColumns transient and wrapping
  // them in KijiLocker objects (which handle serialization correctly),
  // we can work around this limitation.  Thus, the following two lines should be the only to
  // reference `inputColumns` and `outputColumns`, because they will be null after serialization.
  // Everything else should instead use _inputColumns.get and _outputColumns.get.
  private val _inputColumns = KijiLocker(inputColumns)
  private val _outputColumns = KijiLocker(outputColumns)

  // Including output column keys here because we might need to read back outputs during test
  // TODO (EXP-250): Ideally we should include outputColumns.keys here only during tests.
  setSourceFields(buildSourceFields(_inputColumns.get.keys ++ _outputColumns.get.keys))
  setSinkFields(buildSinkFields(_outputColumns.get, timestampField))

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
      tap: Tap[
          JobConf,
          RecordReader[Container[JEntityId], Container[KijiRowData]],
          OutputCollector[_, _]
      ],
      conf: JobConf
  ) {
    // Build a data request.
    val tableAddress: KijiURI = KijiURI.newBuilder(uri).build()
    val request: KijiDataRequest = withKijiTable(tableAddress, conf) { table =>
      buildRequest(table.getLayout, timeRange, _inputColumns.get.values)
    }

    // Write all the required values to the job's configuration object.
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
      sourceCall: SourceCall[
          KijiSourceContext,
          RecordReader[Container[JEntityId], Container[KijiRowData]]
      ]
  ) {
    val tableUriProperty = flow.getStringProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(tableUriProperty).build()

    // Set the context used when reading data from the source.
    sourceCall.setContext(KijiSourceContext(
        sourceCall.getInput.createValue(),
        uri))
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
    val KijiSourceContext(value, tableUri) = sourceCall.getContext

    // Get the next row.
    if (sourceCall.getInput.next(null, value)) {
      val row: KijiRowData = value.getContents()

      // Build a tuple from this row.
      val result: Tuple = rowToTuple(
          _inputColumns.get,
          getSourceFields,
          timestampField,
          row,
          tableUri,
          flow.getConfigCopy
      )

      // If no fields were missing, set the result tuple and return from this method.
      sourceCall.getIncomingEntry.setTuple(result)
      flow.increment(counterGroupName, counterSuccess, 1)

      // We set a result tuple, return true for success.
      return true
    } else {
      return false // We reached the end of the RecordReader.
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
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]
  ) {
    // Open a table writer.
    val uriString: String = flow.getConfigCopy.get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(uriString).build()

    val kiji: Kiji = Kiji.Factory.open(uri, flow.getConfigCopy)
    withKijiTable(kiji, uri.getTable) { table =>
      // Set the sink context to an opened KijiTableWriter.
      sinkCall.setContext(
          KijiSinkContext(table.openTableWriter(), uri, kiji, table.getLayout))
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
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]
  ) {
    // Retrieve writer from the scheme's context.
    val KijiSinkContext(writer, tableUri, kiji, layout) = sinkCall.getContext

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry
    putTuple(
        _outputColumns.get,
        tableUri,
        kiji,
        timestampField,
        output,
        writer,
        layout,
        flow.getConfigCopy)
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
      sinkCall: SinkCall[KijiSinkContext, OutputCollector[_, _]]
  ) {
    sinkCall.getContext.kiji.release()
    sinkCall.getContext.kijiTableWriter.close()
    sinkCall.setContext(null)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case scheme: KijiScheme => {
        _inputColumns.get == scheme._inputColumns.get &&
            _outputColumns.get == scheme._outputColumns.get &&
            timestampField == scheme.timestampField &&
            timeRange == scheme.timeRange
      }
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(
      _inputColumns.get,
      _outputColumns.get,
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
  type HadoopScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]

  private val logger: Logger = LoggerFactory.getLogger(classOf[KijiScheme])

  /** Hadoop mapred counter group for KijiExpress. */
  private[express] val counterGroupName = "kiji-express"
  /** Counter name for the number of rows successfully read. */
  private[express] val counterSuccess = "ROWS_SUCCESSFULLY_READ"
  /** Field name containing a row's [[org.kiji.schema.EntityId]]. */
  val entityIdField: String = "entityId"
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
   * @param tableUri is the URI of the Kiji table.
   * @param configuration identifying the cluster to use when building EntityIds.
   * @return a tuple containing the values contained in the specified row.
   */
  private[express] def rowToTuple(
      columns: Map[String, ColumnInputSpec],
      fields: Fields,
      timestampField: Option[Symbol],
      row: KijiRowData,
      tableUri: KijiURI,
      configuration: Configuration
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
        .filter { field => field.toString != entityIdField }
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
   * Writes a Cascading tuple to a Kiji table.
   *
   * This is used in KijiScheme's `sink` method.
   *
   * @param columns mapping field names to column definitions.
   * @param tableUri of the Kiji table.
   * @param kiji is the Kiji instance the table belongs to.
   * @param timestampField is the optional name of a field containing the timestamp that all values
   *     in a tuple should be written to.
   *     Use None if all values should be written at the current time.
   * @param output to write out to Kiji.
   * @param writer to use for writing to Kiji.
   * @param layout Kiji table layout.
   * @param configuration identifying the cluster to use when building EntityIds.
   */
  private[express] def putTuple(
      columns: Map[String, ColumnOutputSpec],
      tableUri: KijiURI,
      kiji: Kiji,
      timestampField: Option[Symbol],
      output: TupleEntry,
      writer: KijiTableWriter,
      layout: KijiTableLayout,
      configuration: Configuration
  ) {
    // Get the entityId.
    val entityId = output
        .getObject(entityIdField)
        .asInstanceOf[EntityId]
        .toJavaEntityId(EntityIdFactory.getFactory(layout))

    // Get a timestamp to write the values to, if it was specified by the user.
    val timestamp: Long = timestampField match {
      case Some(field) => output.getObject(field.name).asInstanceOf[Long]
      case None => HConstants.LATEST_TIMESTAMP
    }

    columns.keys.iterator
        .foreach { field =>
          val value = output.getObject(field)
          val col: ColumnOutputSpec = columns(field)

          val qualifier = col match {
            case qc: QualifiedColumnOutputSpec => qc.qualifier
            case cf: ColumnFamilyOutputSpec => output.getString(cf.qualifierSelector.name)
          }

          writer.put(entityId, col.family, qualifier, timestamp, col.encode(value))
      }
  }

  /**
   * Gets a schema from the reader schema.
   *
   * @param readerSchema to find the schema for.
   * @param schemaTable to look up IDs in.
   * @return the resolved Schema.
   */
  private[express] def resolveSchemaFromJSONOrUid(
      readerSchema: AvroSchema,
      schemaTable: KijiSchemaTable
  ): Schema = {
    Option(readerSchema.getJson) match {
      case None => schemaTable.getSchema(readerSchema.getUid)
      case Some(json) => new Schema.Parser().parse(json)
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
            if (columnFamily.isMapType()) {
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

    columns
        .foldLeft(requestBuilder) { (builder, column) =>
          addColumn(builder, column)
          builder
        }
        .build()
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
    toField(Set(entityIdField) ++ fieldNames)
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
    toField(Set(entityIdField)
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
  ): Iterator[String] = {
    columns.valuesIterator.collect {
      case x: ColumnFamilyOutputSpec => x.qualifierSelector.name
    }
  }
}
