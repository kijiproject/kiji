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

package org.kiji.express.flow

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.filter.Filters
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.KijiColumnRangeFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

/**
 * An extendable trait used for column filters in Express, which correspond to Kiji and HBase column
 * filters.
 *
 * Filters are implemented via HBase filters, not on the client side, so they can cut down on the
 * amount of data transferred over your network.
 *
 * These can be used in [[org.kiji.express.flow.ColumnInputSpec]].  Currently the filters provided
 * are only useful for [[org.kiji.express.flow.ColumnFamilyInputSpec]], because they are filters for
 * qualifiers when an entire column family is requested.  In the future, there may be filters
 * provided that can filter on the data returned from a fully qualified column.
 *
 * By default, no filter is applied, but you can specify your own.  Only data that pass these
 * filters will be requested and populated into the tuple.  Two column filters are currently
 * provided: [[org.kiji.express.flow.ColumnRangeFilterSpec]] and
 * [[org.kiji.express.flow.RegexQualifierFilterSpec]].  Both of these filter the data
 * returned from a ColumnFamilyInputSpec by qualifier in some way.  These filters can be composed
 * with [[org.kiji.express.flow.AndFilterSpec]] and [[org.kiji.express.flow.OrFilterSpec]].
 *
 * To specify a range of qualifiers for the cells that should be returned.
 * {{{
 *     ColumnRangeFilterSpec(
 *         minimum = “c”,
 *         maximum = “m”,
 *         minimumIncluded = true,
 *         maximumIncluded = false)
 * }}}
 *
 * A `ColumnInputSpec` with the above filter specified will return all data from all  columns with
 * qualifiers “c” and later, up to but not including “m”.  You can omit any of the parameters, for
 * instance, you can write ``ColumnRangeFilterSpec(minimum = “m”, minimumIncluded = true)` to
 * specify columns with qualifiers “m” and later.
 *
 * To specify a regex for the qualifier names that you want data from:
 * {{{
 *     RegexQualifierFilterSpec(“http://.*”)
 * }}}
 * In this example, only data from columns whose qualifier names start with “http://” are returned.
 *
 * See the Sun Java documentation for regular expressions:
 * http://docs.oracle.com/javase/6/docs/api/java/util/regex/Pattern.html
 *
 * To compose filters using `AndFilterSpec`:
 * {{{
 *     AndFilterSpec(List(mRegexFilter, mQualifierFilter))
 * }}}
 * The `AndFilterSpec` composes a list of `FilterSpec`s, returning only data from columns that
 * satisfy all the filters in the `AndFilterSpec`.
 *
 * Analogously, you can compose them with `OrFilterSpec`:
 * {{{
 *     OrFilterSpec(List(mRegexFilter, mQualifierFilter))
 * }}}
 * This returns only data from columns that satisfy at least one of the filters.
 *
 * `OrFilterSpec` and `AndFilterSpec` can themselves be composed.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait ColumnFilterSpec {
  /** @return a KijiColumnFilter that corresponds to the Express column filter. */
  private[kiji] def toKijiColumnFilter: KijiColumnFilter
}

/**
 * An Express column filter which combines a list of column filters using a logical "and" operator.
 *
 * See the scaladocs for [[org.kiji.express.flow.ColumnFilterSpec]] for information on other
 * filters.
 *
 * @param filters to combine with a logical "and" operation.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class AndFilterSpec(filters: Seq[ColumnFilterSpec])
    extends ColumnFilterSpec {
  private[kiji] override def toKijiColumnFilter: KijiColumnFilter = {
    val schemaFilters = filters
        .map { filter: ColumnFilterSpec => filter.toKijiColumnFilter }
        .toArray

    Filters.and(schemaFilters: _*)
  }
}

/**
 * An Express column filter which combines a list of column filters using a logical "or" operator.
 *
 * See the scaladocs for [[org.kiji.express.flow.ColumnFilterSpec]] for information on other
 * filters.
 *
 * @param filters to combine with a logical "or" operation.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class OrFilterSpec(filters: Seq[ColumnFilterSpec])
    extends ColumnFilterSpec {
  private[kiji] override def toKijiColumnFilter: KijiColumnFilter = {
    val orParams = filters
        .map { filter: ColumnFilterSpec => filter.toKijiColumnFilter }
        .toArray

    Filters.or(orParams: _*)
  }
}

/**
 * An Express column filter based on the given minimum and maximum qualifier bounds.
 *
 * See the scaladocs for [[org.kiji.express.flow.ColumnFilterSpec]] for information on other
 * filters.
 *
 * @param minimum qualifier bound.
 * @param maximum qualifier bound.
 * @param minimumIncluded determines if the lower bound is inclusive.
 * @param maximumIncluded determines if the upper bound is inclusive.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class ColumnRangeFilterSpec(
    minimum: Option[String] = None,
    maximum: Option[String] = None,
    minimumIncluded: Boolean = true,
    maximumIncluded: Boolean = false)
    extends ColumnFilterSpec {
  private[kiji] override def toKijiColumnFilter: KijiColumnFilter = {
    new KijiColumnRangeFilter(
        minimum.getOrElse { null },
        minimumIncluded,
        maximum.getOrElse { null },
        maximumIncluded)
  }
}

/**
 * An Express column filter which matches a regular expression against the full qualifier.
 *
 * See the scaladocs for [[org.kiji.express.flow.ColumnFilterSpec]] for information on other
 * filters.
 *
 * @param regex to match on.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class RegexQualifierFilterSpec(regex: String)
    extends ColumnFilterSpec {
  private[kiji] override def toKijiColumnFilter: KijiColumnFilter =
      new RegexQualifierColumnFilter(regex)
}
