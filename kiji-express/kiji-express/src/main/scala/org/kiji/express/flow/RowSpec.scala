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

import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.express.flow.RowFilterSpec.NoRowFilterSpec

/**
 * Parametrizes Kiji table scans with three selection criteria:
 * <ul>
 *   <li>start scanning from `startEntityId`</li>
 *   <li>scan until `limitEntityId`</li>
 *   <li>apply row filter by setting `rowFilterSpec`</li>
 * </ul>
 *
 * To see more information about reading data from a Kiji table, see
 * [[org.kiji.express.flow.KijiInput]].
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
final case class RowSpec private(
    startEntityId: Option[EntityId] = None,
    limitEntityId: Option[EntityId] = None,
    rowFilterSpec: RowFilterSpec = NoRowFilterSpec
) {
  override def toString: String = Objects.toStringHelper(classOf[RowSpec])
      .add("start_entity_id", startEntityId)
      .add("limit_entity_id", limitEntityId)
      .add("kiji_row_filter_spec", rowFilterSpec)
      .toString
  override def hashCode: Int =
    Objects.hashCode(startEntityId, limitEntityId, rowFilterSpec)

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[RowSpec]) {
      false
    } else {
      val other = obj.asInstanceOf[RowSpec]
      startEntityId == other.startEntityId &&
          limitEntityId == other.limitEntityId &&
          rowFilterSpec == other.rowFilterSpec
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
object RowSpec {
  /**
   * Construct a row specification including: starting row of scan,
   * limiting row of scan, and row filter.
   *
   * @param startEntityId the row to start scanning from. Use null to default from beginning.
   * @param limitEntityId the row to scanning until. Use null to default till the end.
   * @param rowFilterSpec the row filter specification. Use null to apply no row filter.
   */
  private[express] def construct(
      startEntityId: JEntityId,
      limitEntityId: JEntityId,
      rowFilterSpec: RowFilterSpec
  ): RowSpec = {
    // Construct RowSpec
    RowSpec(
        Option(startEntityId) match {
          case None => None
          case _ => Option(EntityId.fromJavaEntityId(startEntityId))
        },
        Option(limitEntityId) match {
          case None => None
          case _ => Option(EntityId.fromJavaEntityId(limitEntityId))
        },
        Option(rowFilterSpec) match {
          case None => NoRowFilterSpec
          case _ => rowFilterSpec
        })
  }

  /**
   * Create a new RowSpec.Builder.
   *
   * @return a new RowSpec.Builder.
   */
  def builder: Builder = Builder()

  /**
   * Create a new RowSpec.Builder as a copy of the given Builder.
   *
   * @param other Builder to copy.
   * @return a new RowSpec.Builder as a copy of the given Builder.
   */
  def builder(other: Builder): Builder = Builder(other)

  /**
   * Builder for RowSpec.
   *
   * @param constructorStartEntityId optional start entity id with which to initialize this builder.
   * @param constructorLimitEntityId optional limit entity id with which to initialize this builder.
   * @param constructorRowFilterSpec optional row filter with which to initialize this builder.
   */
  final class Builder private(
      constructorStartEntityId: Option[EntityId],
      constructorLimitEntityId: Option[EntityId],
      constructorRowFilterSpec: Option[RowFilterSpec]
  ) {
    private[this] val monitor = new AnyRef
    private var mStartEntityId: Option[EntityId] = constructorStartEntityId
    private var mLimitEntityId: Option[EntityId] = constructorLimitEntityId
    private var mRowFilterSpec: Option[RowFilterSpec] = constructorRowFilterSpec

    /**
     * Configure row spec to start scans from given start entity id.
     *
     * @param entityId to start scans from.
     * @return this builder.
     */
    def withStartEntityId(entityId: EntityId): Builder = monitor.synchronized {
      require(None == mStartEntityId, "Start entity id already set to: " + mStartEntityId.get)
      mStartEntityId = Some(entityId)
      this
    }

    /**
     * Configure row spec to scan until given limit entity id.
     *
     * @param entityId to scan until.
     * @return this builder.
     */
    def withLimitEntityId(entityId: EntityId): Builder = monitor.synchronized {
      require(None == mLimitEntityId, "Limit entity id already set to: " + mLimitEntityId.get)
      mLimitEntityId = Some(entityId)
      this
    }

    /**
     * Configure row spec to scan using given row filter.
     *
     * @param rowFilterSpec to use as the selection criteria for the scan.
     * @return this builder.
     */
    def withRowFilterSpec(rowFilterSpec: RowFilterSpec): Builder = monitor.synchronized {
      require(None == mRowFilterSpec, "Row filter already set to: " + mRowFilterSpec.get)
      mRowFilterSpec = Some(rowFilterSpec)
      this
    }

    /**
     * The start entity id from which to scan.
     *
     * @return start entity id from which to scan.
     */
    def startEntityId: Option[EntityId] = mStartEntityId

    /**
     * The limit entity id until which to scan.
     *
     * @return limit entity id until which to scan.
     */
    def limitEntityId: Option[EntityId] = mLimitEntityId

    /**
     * The row filter specification to scan with.
     *
     * @return row filter specification to scan with.
     */
    def rowFilterSpec: Option[RowFilterSpec] = mRowFilterSpec

    def build: RowSpec = new RowSpec(
        mStartEntityId,
        mLimitEntityId,
        mRowFilterSpec.getOrElse(NoRowFilterSpec)
    )

    override def toString: String = Objects.toStringHelper(classOf[Builder])
        .add("start_entity_id", mStartEntityId)
        .add("limit_entity_id", mLimitEntityId)
        .add("row_filter_spec", mRowFilterSpec)
        .toString

    override def hashCode: Int =
      Objects.hashCode(mStartEntityId, mLimitEntityId, mRowFilterSpec)

    override def equals(target: Any): Boolean = {
      if (!target.isInstanceOf[Builder]) {
        false
      } else {
        val other: Builder = target.asInstanceOf[Builder]
        startEntityId == other.startEntityId &&
            limitEntityId == other.limitEntityId &&
            rowFilterSpec == other.rowFilterSpec
      }
    }
  }

  /**
   * Companion object providing factory methods for creating new instances of
   * [[org.kiji.express.flow.RowSpec.Builder]].
   */
  @ApiAudience.Public
  @ApiStability.Experimental
  object Builder {

    /**
     * Create a new empty RowSpec.Builder.
     *
     * @return a new empty RowSpec.Builder.
     */
    private[express] def apply(): Builder = new Builder(None, None, None)

    /**
     * Create a new RowSpec.Builder as a copy of the given Builder.
     *
     * @param other Builder to copy.
     * @return a new RowSpec.Builder as a copy of the given Builder.
     */
    private[express] def apply(other: Builder): Builder = {
      new Builder(other.startEntityId, other.limitEntityId, other.rowFilterSpec)
    }
  }
}


