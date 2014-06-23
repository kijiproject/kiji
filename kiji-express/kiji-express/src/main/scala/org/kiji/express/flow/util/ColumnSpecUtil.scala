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

package org.kiji.express.flow.util

import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters.asScalaSetConverter

import org.kiji.annotations.ApiStability
import org.kiji.annotations.ApiAudience
import org.kiji.express.flow.ColumnFamilyInputSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.QualifiedColumnInputSpec
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

/**
 * Provides convenience methods for aiding in building a KijiInput.
 */
@ApiAudience.Public
@ApiStability.Experimental
object ColumnSpecUtil {

  /**
   * Will look at a KijiTable's layout and construct the appropriate column specs
   * for selecting ALL of the columns in the table, including map type families.
   * @param uri The table for which to retrieve ColumnInputSpecs.
   * @return Map of ColumnInputSpecs to Fields named by the fully qualified column name.
   */
  def columnSpecsFromURI(uri: String): Map[_ <: ColumnInputSpec, Symbol] = {
    require(uri != null, "Must specify a Kiji URI")
    val kijiURI = KijiURI.newBuilder(uri).build()

    ResourceUtil.withKiji(kijiURI, new Configuration()) { kiji: Kiji =>
      try {
        val metaTable = kiji.getMetaTable()
        try {
          val layout = metaTable.getTableLayout(kijiURI.getTable())

          val columnNames = layout.getColumnNames().asScala
          columnNames.iterator.map { colName =>
            if (colName.isFullyQualified) {
              (
                  QualifiedColumnInputSpec(colName.getFamily, colName.getQualifier),
                  Symbol(colName.getFamily + colName.getQualifier.capitalize)
                  )
            } else {
              (ColumnFamilyInputSpec(colName.getFamily), Symbol(colName.getFamily))
            }
          }.toMap
        }
      }
    }
  }
}
