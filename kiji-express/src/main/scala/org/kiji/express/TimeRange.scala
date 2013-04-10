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

package org.kiji.express

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.KConstants

/**
 * TimeRange is used to specify a range of timestamps. This class is intended to be used with
 * KijiInput, to specify the range of timestamps to request from Kiji.  The range is inclusive
 * at the `begin` timestamp end exclusive at the `end` timestamp.
 *
 * There are five concrete implementations of this trait:
 * <ul>
 *   <li>All</li>
 *   <li>At(timestamp: Long)</li>
 *   <li>After(begin: Long)</li>
 *   <li>Before(end: Long)</li>
 *   <li>Between(begin: Long, end: Long)</li>
 * </ul>
 *
 * @example {{{
 *     KijiInput("kiji://.env/default/words", timeRange=Between(0L, 10L))("info:word" -> 'word)
 *     }}}
 */
@ApiAudience.Public
@ApiStability.Experimental
sealed trait TimeRange extends Serializable {
  /** Earliest timestamp of the TimeRange, inclusive. */
  def begin: Long

  /** Latest timestamp of the TimeRange, exclusive. */
  def end: Long
}

/** Companion object for TimeRange containing concrete implementations of TimeRange. */
object TimeRange {
  /**
   * Specifies that all timestamps should be requested.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case object All extends TimeRange {
    override val begin: Long = KConstants.BEGINNING_OF_TIME
    override val end: Long = KConstants.END_OF_TIME
  }

  /**
   * Specifies that only the specified timestamp should be requested.
   *
   * @param timestamp to request.
   */
  final case class At(timestamp: Long) extends TimeRange {
    override val begin: Long = timestamp
    override val end: Long = timestamp
  }

  /**
   * Specifies that all timestamps after the specified begin timestamp should be requested.
   *
   * @param begin is the earliest timestamp that should be requested.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class After(override val begin: Long) extends TimeRange {
    override val end: Long = KConstants.END_OF_TIME
  }

  /**
   * Specifies that all timestamps before the specified end timestamp should be requested.
   *
   * @param end is the latest timestamp that should be requested.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class Before(override val end: Long) extends TimeRange {
    override val begin: Long = KConstants.BEGINNING_OF_TIME
  }

  /**
   * Specifies that all timestamps between the specified begin and end timestamps should be
   * requested.
   *
   * @param begin is the earliest timestamp that should be requested.
   * @param end is the latest timestamp that should be requested.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  final case class Between(
      override val begin: Long,
      override val end: Long) extends TimeRange {
    // Ensure that the timerange bounds are sensible.
    require(begin <= end, "Invalid time range specified: (%d, %d)".format(begin, end))
  }
}
