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

import org.kiji.schema.filter.Filters
import org.kiji.schema.filter.KijiRandomRowFilter
import org.kiji.schema.filter.KijiRowFilter

/**
 * A specification describing a row filter to use when reading data from a Kiji table.
 *
 * Filters are implemented via HBase filters and execute on a cluster so they can cut down on the
 * amount of data transferred over the network. Filters can be combined with:
 *  - [[org.kiji.express.flow.RowFilterSpec.And]]
 *  - [[org.kiji.express.flow.RowFilterSpec.Or]]
 * which are themselves filters.
 *
 * @note Defaults to [[org.kiji.express.flow.RowFilterSpec.NoFilter RowFilterSpec.NoFilter]].
 * @example RowFilterSpec usage.
 *      - [[org.kiji.express.flow.RowFilterSpec.NoFilter RowFilterSpec.NoFilter]] - Reading rows
 *        without a filter:
 *        {{{
 *          .withRowFilterSpec(RowFilterSpec.NoFilter)
 *        }}}
 *      - [[org.kiji.express.flow.RowFilterSpec.Random RowFilterSpec.Random]] - Reading rows that
 *        pass a random chance filter:
 *        {{{
 *          // Select 20% of rows.
 *          .withRowFilterSpec(RowFilterSpec.Random(0.2))
 *        }}}
 *      - [[org.kiji.express.flow.RowFilterSpec.KijiSchemaRowFilter RowFilterSpec.KijiMRRowFilter]]
 *        - Reading rows that pass an underlying Kiji MR row filter:
 *        {{{
 *          val underlyingFilter: KijiRowFilter = // ...
 *
 *          // ...
 *
 *          // Select rows using a Kiji MR row filter.
 *          .withRowFilterSpec(RowFilterSpec.KijiMRRowFilter(underlyingFilter))
 *        }}}
 *      - [[org.kiji.express.flow.RowFilterSpec.And RowFilterSpec.And]] - Reading rows that pass a
 *        list of filters:
 *        {{{
 *          val filters: Seq[RowFilterSpec] = Seq(
 *              RowFilterSpec.Random(0.2),
 *              RowFilterSpec.Random(0.2)
 *          )
 *
 *          // Select rows that pass two 20% chance random filters.
 *          .withRowFilterSpec(RowFilterSpec.And(filters))
 *        }}}
 *      - [[org.kiji.express.flow.RowFilterSpec.Or RowFilterSpec.Or]] - Reading rows that any of
 *        the filters in a list:
 *        {{{
 *          val filters: Seq[RowFilterSpec] = Seq(
 *              RowFilterSpec.Random(0.2),
 *              RowFilterSpec.Random(0.2)
 *          )
 *
 *          // Select rows pass any of the provided filters.
 *          .withRowFilterSpec(RowFilterSpec.Or(filters))
 *        }}}
 * @see [[org.kiji.express.flow.KijiInput]] for more RowFilterSpec usage information.
 */
@ApiAudience.Public
@ApiStability.Stable
sealed trait RowFilterSpec extends Serializable {
  private[kiji] def toKijiRowFilter: Option[KijiRowFilter]
}

/**
 * Provides [[org.kiji.express.flow.RowFilterSpec]] implementations.
 */
@ApiAudience.Public
@ApiStability.Stable
object RowFilterSpec {
  /**
   * Specifies that rows should be filtered out using a list of row filters combined using a logical
   * "AND" operator. Only rows that pass all of the provided filters will be read.
   *
   * @see [[org.kiji.express.flow.RowFilterSpec]] for more usage information.
   *
   * @param filters to combine with a logical "AND" operation.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class And(filters: Seq[RowFilterSpec])
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] = {
      val schemaFilters = filters
          .map { filter: RowFilterSpec => filter.toKijiRowFilter.get }
          .toArray
      Some(Filters.and(schemaFilters: _*))
    }
  }

  /**
   * Specifies that rows should be filtered out using a list of row filters combined using a logical
   * "OR" operator. Only rows that pass one or more of the provided filters will be read.
   *
   * @see [[org.kiji.express.flow.RowFilterSpec]] for more usage information.
   *
   * @param filters to combine with a logical "OR" operation.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Or(filters: Seq[RowFilterSpec])
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] = {
      val orParams = filters
          .map { filter: RowFilterSpec => filter.toKijiRowFilter.get }
          .toArray
      Some(Filters.or(orParams: _*))
    }
  }

  /**
   * Specifies that rows should be filtered out randomly with a user-provided chance. Chance
   * represents the probability that a row will be selected.
   *
   * @see [[org.kiji.express.flow.RowFilterSpec]] for more usage information.
   *
   * @param chance by which to select a row. Should be between 0.0 and 1.0.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class Random(chance: Float)
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] =
        Some(new KijiRandomRowFilter(chance))
  }

  /**
   * Specifies that rows should be filtered out using the underlying KijiRowFilter.
   *
   * @see [[org.kiji.express.flow.RowFilterSpec]] for more usage information.
   *
   * @param kijiRowFilter specifying the filter conditions.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  final case class KijiSchemaRowFilter(kijiRowFilter: KijiRowFilter)
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] = Some(kijiRowFilter)
  }

  /**
   * Specifies that no row filters should be used.
   *
   * @see [[org.kiji.express.flow.RowFilterSpec]] for more usage information.
   */
  @ApiAudience.Public
  @ApiStability.Stable
  case object NoFilter
      extends RowFilterSpec {
    private[kiji] override def toKijiRowFilter: Option[KijiRowFilter] = None
  }
}
