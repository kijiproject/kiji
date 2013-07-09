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

package org.kiji.express.datarequest

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.avro._
import org.kiji.express.modeling.ModelEnvironment
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest

/**
 * Encapsulates parameters for a data request.
 *
 * @param minTimeStamp to accept for cells.
 * @param maxTimeStamp to accept for cells.
 * @param columnRequests for required columns.
 */
@ApiAudience.Public
@ApiStability.Experimental
case class ExpressDataRequest(minTimeStamp: Long, maxTimeStamp: Long,
    columnRequests: Seq[ExpressColumnRequest]) {
  /**
   * Converts this Express data request to an Avro data request.
   *
   * @return an Avro data request converted from the provided Kiji data request.
   */
  private[express] def toAvro(): AvroDataRequest = {
    val columns: Seq[ColumnSpec] = columnRequests map {colRequest: ExpressColumnRequest =>
      val avroFilter = colRequest.filter.flatMap { expFil:  ExpressColumnFilter =>
        Option(ExpressColumnFilter.expressToAvroFilter(expFil))
      }
      ColumnSpec
        .newBuilder()
        .setName(colRequest.name)
        .setMaxVersions(colRequest.maxVersions)
        .setFilter(avroFilter.getOrElse(null))
        .build()
    }

    // Build an Avro data request.
    AvroDataRequest
      .newBuilder()
      .setMinTimestamp(minTimeStamp)
      .setMaxTimestamp(maxTimeStamp)
      .setColumnDefinitions(columns.asJava)
      .build()
  }

  /**
   * Converts this Express data request to a KijiDataRequest.
   *
   * @return the requested Kiji data request.
   */
  private[express] def toKijiDataRequest(): KijiDataRequest = {
    val builder = KijiDataRequest.builder().withTimeRange(minTimeStamp, maxTimeStamp)
    columnRequests.foreach  { colRequest: ExpressColumnRequest =>
      if (colRequest.filter.isDefined) {
       builder.newColumnsDef()
         .withMaxVersions(colRequest.maxVersions)
         .withFilter(colRequest.filter.get.getKijiColumnFilter)
         .add(new KijiColumnName(colRequest.name))
      } else {
        builder.newColumnsDef()
          .withMaxVersions(colRequest.maxVersions)
          .add(new KijiColumnName(colRequest.name))
      }
    }
    builder.build
  }
}

/**
 * The companion object to ExpressDataRequest, providing methods to convert from
 * Avro specifications and data requests.
 */
object ExpressDataRequest {
  /**
   * Converts an Avro data request to an Express data request.
   *
   * @param avroDataRequest to convert.
   * @return an Express data request converted from the provided Avro data request.
   */
  def apply(avroDataRequest: AvroDataRequest): ExpressDataRequest = {
    val colSpecs: Seq[ColumnSpec] = avroDataRequest.getColumnDefinitions.asScala.toSeq
    val colRequests: Seq[ExpressColumnRequest] = colSpecs.map { colSpec: ColumnSpec =>
      if (null != colSpec.getFilter) {
        new ExpressColumnRequest(colSpec.getName, colSpec.getMaxVersions,
            Some(filterFromAvro(colSpec.getFilter)))
      } else {
        new ExpressColumnRequest(colSpec.getName, colSpec.getMaxVersions, None)
      }
    }
    new ExpressDataRequest(avroDataRequest.getMinTimestamp(), avroDataRequest.getMaxTimestamp(),
        colRequests)
  }

  /**
   * Converts an Avro data request to a Kiji data request.
   *
   * @param avroDataRequest to convert.
   * @return a Kiji data request converted from the provided Avro data request.
   */
  private[express] def avroToKijiDataRequest(avroDataRequest: AvroDataRequest): KijiDataRequest = {
    val builder = KijiDataRequest.builder()
      .withTimeRange(avroDataRequest.getMinTimestamp(), avroDataRequest.getMaxTimestamp())

    avroDataRequest
      .getColumnDefinitions
      .asScala
      .foreach { columnSpec: ColumnSpec =>
        val name = new KijiColumnName(columnSpec.getName())
        val maxVersions = columnSpec.getMaxVersions()
        if (columnSpec.getFilter != null) {
          val filter = ExpressDataRequest.filterFromAvro(columnSpec.getFilter).getKijiColumnFilter()
          builder.newColumnsDef().withMaxVersions(maxVersions).withFilter(filter).add(name)
        } else {
          builder.newColumnsDef().withMaxVersions(maxVersions).add(name)
        }
    }
    builder.build()
  }

  /**
   * Returns an ExpressColumnFilter given an Avro specification.
   *
   * @param filter as specified in Avro.
   * @return the corresponding ExpressColumnFilter.
   * @throws RuntimeException if the provided filter does not match any cases.
   */
  private[express] def filterFromAvro(filter: AnyRef): ExpressColumnFilter = {
    filter match {
      case regexFilter: RegexQualifierFilterSpec => new RegexQualifierFilter(regexFilter.getRegex)
      case colRangeFilter: ColumnRangeFilterSpec => {
        new ColumnRangeFilter(colRangeFilter.getMinQualifier, colRangeFilter.getMinIncluded,
          colRangeFilter.getMaxQualifier, colRangeFilter.getMaxIncluded)
      }
      case andFilter: AndFilterSpec => {
        val filterList: List[ExpressColumnFilter] = andFilter.getAndFilters.asScala
            .toList map { filterFromAvro _ }
        new AndFilter(filterList)
      }
      case orFilter: OrFilterSpec => {
        val filterList: List[ExpressColumnFilter] = orFilter.getOrFilters.asScala
            .toList map { filterFromAvro _ }
        new OrFilter(filterList)
      }
      case _ => throw new RuntimeException("The provided Avro column filter is invalid.")
    }
  }
}

/**
 * Encapsulates parameters for a request of a particular column.
 *
 * @param name for the column. For example, "info:name".
 * @param maxVersions to be returned from the column, for a given entity.
 * @param filter to apply to this column.
 */
@ApiAudience.Public
@ApiStability.Experimental
case class ExpressColumnRequest(name: String, maxVersions: Int,
    filter: Option[ExpressColumnFilter]) {
}
