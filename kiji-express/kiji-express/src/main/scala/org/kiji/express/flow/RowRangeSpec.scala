/**
 * (c) Copyright 2014 WibiData, Inc.
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

import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.{EntityId => JEntityId}

/**
 * A specification of the range of rows that should be read from a Kiji table.
 *
 * @note Defaults to [[org.kiji.express.flow.RowRangeSpec.All RowRangeSpec.All]].
 * @example
 *      - [[org.kiji.express.flow.RowRangeSpec.All RowRangeSpec.All]] - Reading all rows:
 *        {{{
 *          .withRowRangeSpec(RowRangeSpec.All)
 *        }}}
 *      - [[org.kiji.express.flow.RowRangeSpec.From RowRangeSpec.From]] - Reading rows after the
 *        provided start row key (inclusive):
 *        {{{
 *          .withRowRangeSpec(RowRangeSpec.From(EntityId(startRow)))
 *        }}}
 *      - [[org.kiji.express.flow.RowRangeSpec.Before RowRangeSpec.Before]] - Reading rows before
 *        the provided limit row key (exclusive):
 *        {{{
 *          .withRowRangeSpec(RowRangeSpec.Before(EntityId(limitRow)))
 *        }}}
 *      - [[org.kiji.express.flow.RowRangeSpec.Between RowRangeSpec.Between]] - Reading rows between
 *        the provided start (inclusive) and end (exclusive) row keys:
 *        {{{
 *          .withRowRangeSpec(RowRangeSpec.Between(EntityId(startRow), EntityId(limitRow)))
 *        }}}
 * @see [[org.kiji.express.flow.KijiInput]] for more RowRangeSpec usage information.
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
sealed trait RowRangeSpec {
  /**
   * The start entity id from which to scan.
   *
   * @return start entity id from which to scan.
   */
  def startEntityId: Option[EntityId]

  /**
   * The limit entity id until which to scan.
   *
   * @return limit entity id until which to scan.
   */
  def limitEntityId: Option[EntityId]

  override def toString: String = Objects.toStringHelper(classOf[RowRangeSpec])
      .add("start_entity_id", startEntityId)
      .add("limit_entity_id", limitEntityId)
      .toString
  override def hashCode: Int =
      Objects.hashCode(startEntityId, limitEntityId)
  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[RowRangeSpec]) {
      false
    } else {
      val other = obj.asInstanceOf[RowRangeSpec]
      startEntityId == other.startEntityId && limitEntityId == other.limitEntityId
    }
  }
}

/**
 * Provides [[org.kiji.express.flow.RowRangeSpec]] implementations.
 */
@ApiAudience.Public
@ApiStability.Experimental
object RowRangeSpec {
  /** Constants for default parameters. */
  val DEFAULT_START_ENTITY_ID = None
  val DEFAULT_LIMIT_ENTITY_ID = None

  /**
   * Construct a row range specification from Java entity ids.
   *
   * @param startEntityId the row to start scanning from. Use null to default from beginning.
   * @param limitEntityId the row to scanning until. Use null to default till the end.
   */
  private[express] def construct(
      startEntityId: JEntityId,
      limitEntityId: JEntityId
  ): RowRangeSpec = {
    // Construct RowSpec
    Option(startEntityId) match {
      case None => {
        Option(limitEntityId) match {
          case None => All
          case _ => Before(EntityId.fromJavaEntityId(limitEntityId))
        }
      }
      case _ => {
        Option(limitEntityId) match {
          case None => From(EntityId.fromJavaEntityId(startEntityId))
          case _ => Between(
              EntityId.fromJavaEntityId(startEntityId),
              EntityId.fromJavaEntityId(limitEntityId))
        }
      }
    }
  }

  /**
   * Specifies that all rows should be read.
   *
   * @see [[org.kiji.express.flow.RowRangeSpec]] for more usage information.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  case object All extends RowRangeSpec {
    override val startEntityId: Option[EntityId] = RowRangeSpec.DEFAULT_START_ENTITY_ID
    override val limitEntityId: Option[EntityId] = RowRangeSpec.DEFAULT_LIMIT_ENTITY_ID
  }

  /**
   * Specifies that all rows after the provided start row key should be requested (inclusive).
   *
   * @see [[org.kiji.express.flow.RowRangeSpec]] for more usage information.
   *
   * @param specifiedStartEntityId the row to start scanning from.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class From(specifiedStartEntityId: EntityId) extends RowRangeSpec {
    override val startEntityId: Option[EntityId] = Option(specifiedStartEntityId)
    require(None != startEntityId, "Specified entity id can not be null.")
    override val limitEntityId: Option[EntityId] = RowRangeSpec.DEFAULT_LIMIT_ENTITY_ID
  }

  /**
   * Specifies that all rows before the provided limit row key should be requested (exclusive).
   *
   * @see [[org.kiji.express.flow.RowRangeSpec]] for more usage information.
   *
   * @param specifiedLimitEntityId the row to scanning up to.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class Before(specifiedLimitEntityId: EntityId) extends RowRangeSpec {
    override val limitEntityId: Option[EntityId] = Option(specifiedLimitEntityId)
    require(None != limitEntityId, "Specified entity id can not be null.")
    override val startEntityId: Option[EntityId] = RowRangeSpec.DEFAULT_START_ENTITY_ID
  }

  /**
   * Specifies that all rows between the provided start (inclusive) and limit (exclusive) row keys
   * should be requested.
   *
   * @see [[org.kiji.express.flow.RowRangeSpec]] for more usage information.
   *
   * @param specifiedStartEntityId the row to start scanning from.
   * @param specifiedLimitEntityId the row to scanning up to.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class Between(
      specifiedStartEntityId: EntityId,
      specifiedLimitEntityId: EntityId
  ) extends RowRangeSpec {
    override val startEntityId: Option[EntityId] = Option(specifiedStartEntityId)
    override val limitEntityId: Option[EntityId] = Option(specifiedLimitEntityId)
    require(None != limitEntityId || None != startEntityId,
        "Specified entity id can not be null.")
  }
}
