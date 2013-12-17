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

package org.kiji.modeling.config

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.TimeRange
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.SchemaSpec
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.layout.ColumnReaderSpec

/**
 * Configuration necessary to use a Kiji table as a data source.
 *
 * @param tableUri addressing the Kiji table that this input spec will read from.
 * @param timeRange that cells must fall into to be retrieved
 * @param columnsToFields is a map from [[org.kiji.express.flow.ColumnInputSpec]] objects to
 *     Scalding field names.  This determines how data that is requested for the extract phase is
 *     mapped onto named input fields.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class KijiInputSpec(
    tableUri: String,
    timeRange: TimeRange,
    columnsToFields: Map[_ <: ColumnInputSpec, Symbol]
) extends InputSpec {

  /**
   * Create a [[org.kiji.schema.KijiDataRequest]] for this `KijiInputSpec.`
   *
   * @return a `KijiDataRequest` that will request the columns specified by this `KijiInputSpec.`
   */
  def toKijiDataRequest: KijiDataRequest = {

    /** Add another column to the `KijiDataRequest.` */
    def addColumn(
        builder: KijiDataRequestBuilder,
        column: ColumnInputSpec
    ): KijiDataRequestBuilder.ColumnsDef = {
      val readerSpec = column.schemaSpec match {
        case SchemaSpec.DefaultReader => ColumnReaderSpec.avroDefaultReaderSchemaGeneric()
        case SchemaSpec.Writer => ColumnReaderSpec.avroWriterSchemaGeneric()
        case SchemaSpec.Generic(schema) => ColumnReaderSpec.avroReaderSchemaGeneric(schema)
        case SchemaSpec.Specific(record) => ColumnReaderSpec.avroReaderSchemaSpecific(record)
        // TODO: Check for columns that contain counters/protobuf records/bytes.
      }
      builder.newColumnsDef()
          .withMaxVersions(column.maxVersions)
          .withFilter(column.filterSpec.toKijiColumnFilter.getOrElse(null))
          .withPageSize(column.pagingSpec.cellsPerPage.getOrElse(0))
          .add(column.columnName, readerSpec)
    }

    val requestBuilder: KijiDataRequestBuilder = KijiDataRequest.builder()
        .withTimeRange(timeRange.begin, timeRange.end)

    columnsToFields
        .keys
        .toList
        .foldLeft(requestBuilder) { (builder, column) =>
          addColumn(builder, column)
          builder
        }
        .build()
  }

}
