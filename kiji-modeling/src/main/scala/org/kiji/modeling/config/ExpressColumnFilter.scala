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
import org.kiji.schema.filter.Filters
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.KijiColumnRangeFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

/**
 * An extendable trait used for column filters in Express, which correspond to
 * Kiji and HBase column filters.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait ExpressColumnFilter {
  /** @return a KijiColumnFilter that corresponds to the Express column filter. */
  def toKijiColumnFilter: KijiColumnFilter
}

/**
 * An Express column filter which combines a list of column filters using a logical "and" operator.
 *
 * @param filters to combine with a logical "and" operation.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class AndFilter(filters: Seq[ExpressColumnFilter])
    extends ExpressColumnFilter {
  override def toKijiColumnFilter: KijiColumnFilter = {
    val schemaFilters = filters
        .map { filter: ExpressColumnFilter => filter.toKijiColumnFilter }
        .toArray

    Filters.and(schemaFilters: _*)
  }
}

/**
 * An Express column filter which combines a list of column filters using a logical "or" operator.
 *
 * @param filters to combine with a logical "or" operation.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class OrFilter(filters: Seq[ExpressColumnFilter])
    extends ExpressColumnFilter {
  override def toKijiColumnFilter: KijiColumnFilter = {
    val orParams = filters
        .map { filter: ExpressColumnFilter => filter.toKijiColumnFilter }
        .toArray

    Filters.or(orParams: _*)
  }
}

/**
 * An Express column filter based on the given minimum and maximum qualifier bounds.
 *
 * @param minimum qualifier bound.
 * @param maximum qualifier bound.
 * @param minimumIncluded determines if the lower bound is inclusive.
 * @param maximumIncluded determines if the upper bound is inclusive.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ColumnRangeFilter(
    minimum: Option[String] = None,
    maximum: Option[String] = None,
    minimumIncluded: Boolean = true,
    maximumIncluded: Boolean = false)
    extends ExpressColumnFilter {
  override def toKijiColumnFilter: KijiColumnFilter = {
    // scalastyle:off null
    new KijiColumnRangeFilter(
        minimum.getOrElse { null },
        minimumIncluded,
        maximum.getOrElse { null },
        maximumIncluded)
    // scalastyle:on null
  }
}

/**
 * An Express column filter which matches a regular expression against the full qualifier.
 *
 * @param regex to match on.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class RegexQualifierFilter(regex: String)
    extends ExpressColumnFilter {
  override def toKijiColumnFilter: KijiColumnFilter = new RegexQualifierColumnFilter(regex)
}
