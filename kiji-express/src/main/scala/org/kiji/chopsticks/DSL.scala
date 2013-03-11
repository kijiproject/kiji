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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.util.ReferenceCountable

@ApiAudience.Public
@ApiStability.Unstable
object DSL {
  // TODO(CHOP-36): Support request-level options.
  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to read from.
   */
  def KijiInput(
      tableURI: String) (
        columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to read from.
   */
  def KijiInput(
      tableURI: String,
      columns: Map[org.kiji.lang.Column, Symbol]): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
    new KijiSource(tableURI, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to write to.
   */
  def KijiOutput(
      tableURI: String) (
        columns: (Symbol, String)*)
    : KijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, columnMap)
  }

  /**
   * Factory method for KijiSource.
   *
   * @param tableURI Address of the Kiji table to use.
   * @param columns Columns to write to.
   */
  def KijiOutput(
      tableURI: String,
      columns: Map[Symbol, org.kiji.lang.Column])
    : KijiSource = new KijiSource(tableURI, columns)

  /**
   * Constructs a column definition. Use inputOps and outputOps to specify input
   * and output options.
   *
   * @param name Name of the column with the format "family:qualifier".
   * @param inputOps Options for the column when reading data.
   */
  def Column(
      name: String,
      inputOps: org.kiji.lang.Column.InputOptions = InputOptions()): org.kiji.lang.Column
    = new org.kiji.lang.Column(name, inputOps)

  // TODO(CHOP-37): A simpler way to specify column filters.
  /**
   * Sets input options for a column.
   *
   * @param maxVersions Maximum number of versions of a cell to read.
   * @param filter Filter to apply to this column.
   */
  def InputOptions(
      maxVersions: Int = 1,
      filter: KijiColumnFilter = null): org.kiji.lang.Column.InputOptions
    = new org.kiji.lang.Column.InputOptions(maxVersions, filter)

  /**
   * Performs an operation with a releaseable resource by first retaining the resource and releasing
   * it upon completion of the operation.
   *
   * @param resource Retainable resource used by the operation.
   * @param fn Operation to perform.
   * @return The result of the operation.
   */
  def retainAnd[T, R](resource: ReferenceCountable[R])(fn: R => T): T = {
    try {
      fn(resource.retain())
    } finally {
      resource.release()
    }
  }

  /**
   * Performs an operation with an already retained releaseable resource by releasing it upon
   * completion of the operation.
   *
   * @param resource Retainable resource used by the operation.
   * @param fn Operation to perform.
   * @return The restult of the operation.
   */
  def doAndRelease[T, R <: ReferenceCountable[_]](resource: R)(fn: R => T): T = {
    try {
      fn(resource)
    } finally {
      resource.release()
    }
  }
}
