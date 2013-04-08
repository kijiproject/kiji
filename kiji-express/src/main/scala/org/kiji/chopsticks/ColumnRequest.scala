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

package org.kiji.chopsticks

import java.io.Serializable

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.util.KijiNameValidator

/**
 * A trait that marks case classes that hold column-level options for cell requests to Kiji.
 *
 * End-users receive instances of this trait, used to request cells from qualified columns or
 * map-type column families, using the factory methods [[org.kiji.chopsticks.DSL.Column()]] and
 * [[org.kiji.chopsticks.DSL.MapFamily()]]. They can then use these requests to obtain a
 * [[org.kiji.chopsticks.KijiSource]] that reads cells into tuples while obeying the specified
 * request options.
 *
 * If desired, end-users can add information about how to handle missing values in this column,
 * with the methods `replaceMissingWith` or `ignoreMissing`, only one of which can be called before
 * it is transformed to a ChopsticksColumn.
 *
 * If a ColumnRequest is used in the DSL without calling `replaceMissingWith` or `ignoreMissing`,
 * `ignoreMissing` is the default.
 */
@ApiAudience.Public
@ApiStability.Experimental
private[chopsticks] sealed trait ColumnRequest extends Serializable {
  /**
   * Specifies what to replace any missing values on this column with.
   *
   * @param replacementSlice Replacement specification.
   * @return this ColumnRequest with replacement configured.
   */
  def replaceMissingWith(replacementSlice: KijiSlice[_]): ColumnRequest

  /**
   * Specifies that missing values on this column mean the row should be skipped.
   * This is the default behavior in the DSL if neither this nor `replaceMissingWith` are called
   * on a ColumnRequest.
   *
   * @return this ColumnRequest with skipping behavior configured.
   */
  def ignoreMissing(): ColumnRequest
}

/**
 * A request for cells from a fully qualified column in a Kiji table.
 *
 * @param family of columns that the requested column belongs to.
 * @param qualifier of the requested column.
 * @param options that will be used to request cells from the column. If unspecified, default
 *     request options will be used.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class QualifiedColumn private[chopsticks] (
    family: String,
    qualifier: String,
    options: ColumnRequestOptions = ColumnRequestOptions())
    extends ColumnRequest {
  KijiNameValidator.validateLayoutName(family)
  KijiNameValidator.validateLayoutName(qualifier)

  override def replaceMissingWith(replacementSlice: KijiSlice[_]): ColumnRequest = {
    return new QualifiedColumn(
        family,
        qualifier,
        options.newWithReplacement(Some(replacementSlice)))
  }

  override def ignoreMissing(): ColumnRequest = {
    return new QualifiedColumn(family, qualifier, options.newWithReplacement(None))
  }
}

/**
 * A request for cells from columns in a map-type column family in a Kiji table.
 *
 * @param family (map-type) of the Kiji table whose columns are being requested.
 * @param options that will be used to request cells from the columns of the family. If
 *     unspecified, default request options are used.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class ColumnFamily private[chopsticks] (
    family: String,
    options: ColumnRequestOptions = ColumnRequestOptions())
    extends ColumnRequest {
  KijiNameValidator.validateLayoutName(family)

  override def replaceMissingWith(replacementSlice: KijiSlice[_]): ColumnRequest = {
    return new ColumnFamily(family, options.newWithReplacement(Some(replacementSlice)))
  }

  override def ignoreMissing(): ColumnRequest = {
    return new ColumnFamily(family, options.newWithReplacement(None))
  }
}

/**
 * The column-level options for cell requests to Kiji.
 *
 * @param maxVersions is the maximum number of cells (from the most recent) that will be
 *     retrieved for the column. By default only the most recent cell is retrieved.
 * @param filter that a cell must pass to be retrieved by the request. By default no filter is
 *     applied.
 */
@ApiAudience.Public
@ApiStability.Experimental
final case class ColumnRequestOptions private[chopsticks] (
    maxVersions: Int = 1,
    // Not accessible to end-users because the type is soon to be replaced by a
    // Chopsticks-specific implementation.
    private[chopsticks] val filter: Option[KijiColumnFilter] = None,
    replacementSlice: Option[KijiSlice[_]] = None)
    extends Serializable {
      def newWithReplacement(
          newReplacement: Option[KijiSlice[_]]): ColumnRequestOptions = {
        new ColumnRequestOptions(maxVersions, filter, newReplacement)
      }
}
