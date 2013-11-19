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
import org.kiji.schema.KConstants

/**
 * A trait implemented by classes that specify time ranges when reading data from Kiji tables. This
 * class is used to specify the range of cell versions to request from a column in a Kiji table.
 *
 * To specify that [[org.kiji.express.flow.All]] versions should be requested:
 * {{{
 *   val timeRange: TimeRange = All
 * }}}
 * To specify that a specific version should be requested ([[org.kiji.express.flow.At]]):
 * {{{
 *   // Gets only cells with the version `123456789`.
 *   val timeRange: TimeRange = At(123456789L)
 * }}}
 * To specify that all versions [[org.kiji.express.flow.After]] the specified version should be
 * requested:
 * {{{
 *   // Gets only cells with versions larger than `123456789`.
 *   val timeRange: TimeRange = After(123456789L)
 * }}}
 * To specify that all versions [[org.kiji.express.flow.Before]] the specified version should be
 * requested:
 * {{{
 *   // Gets only cells with versions smaller than `123456789`.
 *   val timeRange: TimeRange = Before(123456789L)
 * }}}
 * To specify that all versions [[org.kiji.express.flow.Between]] the two specified bounds should be
 * requested:
 * {{{
 *   // Gets only cells with versions between `12345678` and `123456789`.
 *   val timeRange: TimeRange = Between(12345678, 123456789)
 * }}}
 *
 * See [[org.kiji.express.flow.KijiInput]] for more information.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait TimeRange extends Serializable {
  /** Earliest version of the TimeRange, inclusive. */
  def begin: Long

  /** Latest version of the TimeRange, exclusive. */
  def end: Long
}

/**
 * Implementation of [[org.kiji.express.flow.TimeRange]] for specifying that all versions should be
 * requested.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
case object All extends TimeRange {
  override val begin: Long = KConstants.BEGINNING_OF_TIME
  override val end: Long = KConstants.END_OF_TIME
}

/**
 * Implementation of [[org.kiji.express.flow.TimeRange]] for specifying that only the provided
 * version should be requested.
 *
 * @param version to request.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class At(version: Long) extends TimeRange {
  override val begin: Long = version
  override val end: Long = version
}

/**
 * Implementation of [[org.kiji.express.flow.TimeRange]] for specifying that all versions after the
 * provided version should be requested (exclusive).
 *
 * @param begin is the earliest version that should be requested (exclusive).
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class After(override val begin: Long) extends TimeRange {
  override val end: Long = KConstants.END_OF_TIME
}

/**
 * Implementation of [[org.kiji.express.flow.TimeRange]] for specifying that all versions before the
 * provided version should be requested (inclusive).
 *
 * @param end is the latest version that should be requested (inclusive).
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class Before(override val end: Long) extends TimeRange {
  override val begin: Long = KConstants.BEGINNING_OF_TIME
}

/**
 * Implementation of [[org.kiji.express.flow.TimeRange]] for specifying that all versions between
 * the provided begin and end versions should be requested.
 *
 * @param begin is the earliest version that should be requested (inclusive).
 * @param end is the latest version that should be requested (exclusive).
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class Between(
    override val begin: Long,
    override val end: Long) extends TimeRange {
  // Ensure that the timerange bounds are sensible.
  require(begin <= end, "Invalid time range specified: (%d, %d)".format(begin, end))
}
