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

package org.kiji.express.modeling.config

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.avro.AvroAndFilter
import org.kiji.express.avro.AvroColumnRangeFilter
import org.kiji.express.avro.AvroRegexQualifierFilter
import org.kiji.express.avro.AvroOrFilter
import org.kiji.schema.filter.KijiColumnFilter

/**
 * An extendable trait used for column filters in Express, which correspond to
 * Kiji and HBase column filters.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
trait ExpressColumnFilter {
  /**
   * Returns a KijiColumnFilter that corresponds to the Express column filter.
   */
  def getKijiColumnFilter(): KijiColumnFilter
}

/**
 * The companion object to ExpressColumnFilter, providing factory conversion methods.
 */
object ExpressColumnFilter {
  /**
   * A utility method that allows us to recursively translate And and Or express filters
   * into the corresponding Avro representation.
   *
   * @param filter to translate from Express into Avro.
   * @return an Avro column filter converted from the provided Express column filter.
   * @throws RuntimeException if the provided filter does not match any cases.
   */
  private[express] def expressToAvroFilter(filter: ExpressColumnFilter): AnyRef = {
    filter match {
      case regexFilter: RegexQualifierFilter => {
        AvroRegexQualifierFilter.newBuilder()
          .setRegex(regexFilter.regex)
          .build()
      }
      case rangeFilter: ColumnRangeFilter => {
        AvroColumnRangeFilter.newBuilder()
          .setMinQualifier(rangeFilter.minQualifier)
          .setMinIncluded(rangeFilter.minIncluded)
          .setMaxQualifier(rangeFilter.maxQualifier)
          .setMaxIncluded(rangeFilter.maxIncluded)
          .build()
      }
      case andFilter: AndFilter => {
        val expFilterList: List[AnyRef] = andFilter.filtersList map { expressToAvroFilter _ }
        AvroAndFilter.newBuilder().setAndFilters(expFilterList.asJava).build()
      }
      case orFilter: OrFilter => {
        val filterList: List[AnyRef] = orFilter.filtersList map { expressToAvroFilter _ }
        AvroOrFilter.newBuilder().setOrFilters(filterList.asJava)
      }
      case _ => throw new RuntimeException("The provided Express column filter is invalid.")
    }
  }
}
