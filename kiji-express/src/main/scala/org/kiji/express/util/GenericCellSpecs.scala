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

package org.kiji.express.util

import java.util.HashMap
import java.util.{Map => JMap}

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.{Map => MMap}

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.GenericCellDecoderFactory
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.CellSpec
import org.kiji.schema.layout.KijiTableLayout

/**
 * A factory for mappings from columns in a Kiji table to generic cell specifications for those
 * columns. This factory can be used to easily construct generic cell specifications for a table,
 * which can then be passed to a Kiji table reader to allow for reading data generically. See
 * [[org.kiji.schema.KijiReaderFactory]] for more information on how to use these cell specs to
 * obtain a Kiji table reader that decodes data using the Avro GenericData API.
 */
@ApiAudience.Private
@ApiStability.Experimental
object GenericCellSpecs {

  /**
   * A cache for generic cell specifications for columns in a particular table layout. This cache
   * exists to prevent us from generating these cell spec maps multiple times for the same layout.
   */
  private val specMapCache = MMap[KijiTableLayout, JMap[KijiColumnName, CellSpec]]()

  /**
   * Gets a generic cell specification mapping for a Kiji table.
   *
   * @param table used to get the generic cell specification mapping.
   * @return a map from names of columns in the table to generic cell specifications for the
   *     columns.
   */
  def apply(table: KijiTable): JMap[KijiColumnName, CellSpec] = {
    apply(table.getLayout())
  }

  /**
   * Gets a generic cell specification mapping for a layout of a Kiji table.
   *
   * @param layout used to get the generic cell specification mapping.
   * @return a map from names of columns in the layout to generic cell specifications for the
   *     columns.
   */
  def apply(layout: KijiTableLayout): JMap[KijiColumnName, CellSpec] = {
    // Get the cell specification map cached for the layout, or generate a new one which is then
    // cached and returned.
    specMapCache.get(layout).getOrElse {
      val specMap = createCellSpecMap(layout)
      specMapCache.put(layout, specMap)
      specMap
    }
  }

  /**
   * Creates a mapping from columns in a table layout to generic cell specifications for those
   * columns.
   *
   * @param layout used to create the generic cell specifications.
   * @return a map from the names of columns in the layout to generic cell specifications for
   *     those columns.
   */
  private def createCellSpecMap(layout: KijiTableLayout): JMap[KijiColumnName, CellSpec] = {
    // Fold the column names in the layout into a map from column name to a generic cell
    // specification for the column.
    layout
        .getColumnNames()
        .asScala
        .foldLeft(new HashMap[KijiColumnName, CellSpec]()) { (specMap, columnName) =>
          val cellSpec = layout
              .getCellSpec(columnName)
              .setUseWriterSchema()
              .setDecoderFactory(GenericCellDecoderFactory.get())
          specMap.put(columnName, cellSpec)
          specMap
        }
  }
}
