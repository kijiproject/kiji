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

import org.kiji.schema.filter.KijiColumnFilter

import org.kiji.lang.{ Column => JColumn }
import org.kiji.annotations.{ApiStability, ApiAudience}

@ApiAudience.Public
@ApiStability.Unstable
object DSL {
  /** Factory method for KijiSource. */
  def KijiInput(
      /** Address of the Kiji table to use. */
      tableURI: String) (
        /** Columns to read from. */
        columns: (String, Symbol)*)
    : KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, columnMap)
  }

  /** Factory method for KijiSource. */
  def KijiInput(
      /** Address of the Kiji table to use. */
      tableURI: String,
      /** Columns to read from. */
      columns: Map[JColumn, Symbol])
    : KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
    new KijiSource(tableURI, columnMap)
  }

  /** Factory method for KijiSource. */
  def KijiOutput(
      /** Address of the Kiji table to use. */
      tableURI: String) (
        /** Columns to write to. */
        columns: (Symbol, String)*)
    : KijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, columnMap)
  }

  /** Factory method for KijiSource. */
  def KijiOutput(
      /** Address of the Kiji table to use. */
      tableURI: String,
      /** Columns to write to. */
      columns: Map[Symbol, JColumn])
    : KijiSource = new KijiSource(tableURI, columns)

  /**
   * Constructs a column definition. Use inputOps and outputOps to specify input
   * and output options.
   */
  def Column(
      /** Name of the column with the format "family:qualifier". */
      name: String,
      /** Options for the column when reading data. */
      inputOps: JColumn.InputOptions = InputOptions())
    : JColumn = new JColumn(name, inputOps)

  /** Sets input options for a column. */
  def InputOptions(
      /** Maximum number of versions of a cell to read. Default is 1. */
      maxVersions: Int = 1,
      /** Filter to apply to this column. Set this to null to not use a filter. Default is null. */
      filter: KijiColumnFilter = null)
    : JColumn.InputOptions = new JColumn.InputOptions(maxVersions, filter)
}
