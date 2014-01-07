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
 * A trait implemented by classes that specify row ranges when reading data from Kiji tables. This
 * class is used to specify the range of rows to request from a Kiji table.
 *
 * To specify that [[org.kiji.express.flow.RowRangeSpec.AllRows]] rows should be requested:
 * {{{
 *   val rowRangeSpec: RowRangeSpec = RowRangeSpec.AllRows
 * }}}
 * To specify that all rows starting [[org.kiji.express.flow.RowRangeSpec.FromRow]]
 * the specified row should be requested:
 * {{{
 *   val rowRangeSpec: RowRangeSpec = RowRangeSpec.FromRow(EntityId(startRow))
 * }}}
 * To specify that all rows [[org.kiji.express.flow.RowRangeSpec.UntilRow]] (exclusive)
 * the specified row should be requested:
 * {{{
 *   val rowRangeSpec: RowRangeSpec = RowRangeSpec.UntilRow(EntityId(limitRow))
 * }}}
 * To specify that all rows between [[org.kiji.express.flow.RowRangeSpec.BetweenRows]]
 * the specified start and end row keys should be requested:
 * {{{
 *   val rowRangeSpec: RowRangeSpec =
 *       RowRangeSpec.BetweenRows(EntityId(startRow), EntityId(limitRow))
 * }}}
 *
 * To see more information about reading data from a Kiji table, see
 * [[org.kiji.express.flow.KijiInput]].
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
 * Provides factory functions for creating [[org.kiji.express.flow.RowSpec]]
 * instances.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
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
          case None => AllRows
          case _ => UntilRow(EntityId.fromJavaEntityId(limitEntityId))
        }
      }
      case _ => {
        Option(limitEntityId) match {
          case None => FromRow(EntityId.fromJavaEntityId(startEntityId))
          case _ => BetweenRows(
              EntityId.fromJavaEntityId(startEntityId),
              EntityId.fromJavaEntityId(limitEntityId))
        }
      }
    }
  }

  /**
   * Implementation of [[org.kiji.express.flow.RowRangeSpec]]
   * for specifying that all rows should be scanned.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case object AllRows extends RowRangeSpec {
    override val startEntityId: Option[EntityId] = RowRangeSpec.DEFAULT_START_ENTITY_ID
    override val limitEntityId: Option[EntityId] = RowRangeSpec.DEFAULT_LIMIT_ENTITY_ID
  }

  /**
   * Implementation of [[org.kiji.express.flow.RowRangeSpec]]
   * for specifying start row key.
   *
   * @param specifiedStartEntityId the row to start scanning from.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class FromRow(specifiedStartEntityId: EntityId) extends RowRangeSpec {
    override val startEntityId: Option[EntityId] = Option(specifiedStartEntityId)
    require(None != startEntityId, "Specified entity id can not be null.")
    override val limitEntityId: Option[EntityId] = RowRangeSpec.DEFAULT_LIMIT_ENTITY_ID
  }

  /**
   * Implementation of [[org.kiji.express.flow.RowRangeSpec]]
   * for specifying limit row key (exclusive endpoint).
   *
   * @param specifiedLimitEntityId the row to scanning until.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class UntilRow(specifiedLimitEntityId: EntityId) extends RowRangeSpec {
    override val limitEntityId: Option[EntityId] = Option(specifiedLimitEntityId)
    require(None != limitEntityId, "Specified entity id can not be null.")
    override val startEntityId: Option[EntityId] = RowRangeSpec.DEFAULT_START_ENTITY_ID
  }

  /**
   * Implementation of [[org.kiji.express.flow.RowRangeSpec]]
   * for specifying start and limit row keys (limit row key is an exclusive endpoint).
   *
   * @param specifiedStartEntityId the row to start scanning from.
   * @param specifiedLimitEntityId the row to scanning until.
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  @Inheritance.Sealed
  final case class BetweenRows (
      specifiedStartEntityId: EntityId,
      specifiedLimitEntityId: EntityId
  ) extends RowRangeSpec {
    override val startEntityId: Option[EntityId] = Option(specifiedStartEntityId)
    override val limitEntityId: Option[EntityId] = Option(specifiedLimitEntityId)
    require(None != limitEntityId || None != startEntityId,
        "Specified entity id can not be null.")
  }
}
