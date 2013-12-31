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
import org.kiji.schema.filter.KijiRandomRowFilter
import org.kiji.schema.filter.KijiRowFilter

/**
 * An extensible trait used for row filters in Express, which correspond to Kiji and HBase row
 * filters.
 *
 * Filters are implemented via HBase filters, not on the client side, so they can cut down on the
 * amount of data transferred over your network.
 *
 * These can be used in [[org.kiji.express.flow.RowSpec]].
 *
 * By default, no filter is applied, but you can specify your own. Only data that pass these
 * filters will be requested and populated into the tuple.
 *
 * Filters can be composed with [[org.kiji.express.flow.RowFilterSpec.AndFilterSpec]] and
 * [[org.kiji.express.flow.RowFilterSpec.OrFilterSpec]], which are themselves filters.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait RowFilterSpec extends Serializable {
  private[kiji] def toKijiRowFilter: Option[KijiRowFilter]
}

/**
 * Companion object to provide RowFilterSpec implementations.
 */
@ApiAudience.Public
@ApiStability.Experimental
object RowFilterSpec {
  /**
   * An Express row filter which combines a list of row filters using a logical "and" operator.
   *
   * @param filters to combine with a logical "and" operation.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class AndFilterSpec(filters: Seq[RowFilterSpec])
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] = {
      val schemaFilters = filters
          .map { filter: RowFilterSpec => filter.toKijiRowFilter.get }
          .toArray
      Some(Filters.and(schemaFilters: _*))
    }
  }

  /**
   * An Express row filter which combines a list of row filters using a logical "or" operator.
   *
   * @param filters to combine with a logical "or" operation.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class OrFilterSpec(filters: Seq[RowFilterSpec])
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] = {
      val orParams = filters
          .map { filter: RowFilterSpec => filter.toKijiRowFilter.get }
          .toArray
      Some(Filters.or(orParams: _*))
    }
  }

  /**
   * An Express row filter which selects a row with the parametrized chance.
   *
   * @param chance by which to select a row.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class KijiRandomRowFilterSpec(chance: Float)
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] =
        Some(new KijiRandomRowFilter(chance))
  }

  /**
   * An Express row filter constructed directly from a KijiRowFilter.
   *
   * @param kijiRowFilter specifying the filter conditions.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class KijiRowFilterSpec(kijiRowFilter: KijiRowFilter)
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] = Some(kijiRowFilter)
  }

  /**
   * An Express row filter specifying no filter.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final object NoRowFilterSpec extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] = None
  }
}
