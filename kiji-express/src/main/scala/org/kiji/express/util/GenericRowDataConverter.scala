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

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.mutable.{Map => MMap}

import org.apache.hadoop.conf.Configuration

import org.kiji.schema.GenericCellDecoderFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.HBaseKijiRowData
import org.kiji.schema.layout.impl.CellDecoderProvider
import org.kiji.schema.layout.{KijiTableLayout, CellSpec}
import org.kiji.schema.util.ResourceUtils

/**
 * A factory for [[org.kiji.schema.KijiRowData]] that use the Avro GenericData API to decode
 * their data.
 *
 * @param uri to a Kiji instance for rows that will be converted.
 * @param conf is the hadoop configuration used when accessing Kiji.
 */
private[express] final class GenericRowDataConverter(uri: KijiURI, conf: Configuration) {

  /**
   * The Kiji instance containing the tables whose rows will be converted.
   */
  private val kiji = Kiji.Factory.open(uri, conf)

  /**
   * A schema table that will be given to decoders.
   */
  private val schemaTable = kiji.getSchemaTable()

  /**
   * A cache of decoder providers for table layouts, that provide generic decoders that decode
   * data without a reader schema.
   */
  private val decoderProviderCache = MMap[KijiTableLayout, CellDecoderProvider]()

  /**
   * Gets a row data that is a copy of the original, but configured to decode data using the Avro
   * GenericData API.
   *
   * In practice this is used to enable the use of the dynamic Avro API throughout KijiExpress.
   * The row data copy performed is shallow and thus reasonably efficient,
   * and care should be taken to maintain this.
   *
   * @param original row data to reconfigure.
   * @return a copy of the row data, configured to decode data generically.
   */
  def apply(original: KijiRowData): KijiRowData = {
    // Downcast the original data so we can access its guts.
    val hbaseRowData = original.asInstanceOf[HBaseKijiRowData]
    // Create a provider for generic data cell decoders.
    val decoderProvider = getDecoderProvider(hbaseRowData.getTable())
    // Create a new original data instance configured with the new decoder.
    new HBaseKijiRowData(hbaseRowData.getTable(), hbaseRowData.getDataRequest(),
        hbaseRowData.getEntityId(), hbaseRowData.getHBaseResult(), decoderProvider)
  }

  /**
   * Closes the resources associated with this converter.
   */
  def close() {
    // Since we are the only ones retaining this kiji, releasing it will close it,
    // which will in turn close the schema table.
    ResourceUtils.releaseOrLog(kiji)
  }

  /**
   * Retrieves a cell decoder provider for a Kiji table, that provides decoders that decode data
   * generically and without reader schema.
   *
   * @param table to retrieve the cell decoder provider for.
   * @return the cell decoder provider that decodes data generically and without reader schema.
   */
  private def getDecoderProvider(table: KijiTable): CellDecoderProvider = {
    // Retrieve a provider from the cache, or generate and cache a new one for the table.
    // Generating a new decoder provider involves creating a map of cell specifications used to
    // "overwrite" the reader schemas in the layout to null, so that no reader schema is used
    // when decoding data.
    val layout = table.getLayout()
    decoderProviderCache.get(layout).getOrElse {
      val provider = new CellDecoderProvider(layout, schemaTable,
          GenericCellDecoderFactory.get(), createCellSpecMap(layout))
      decoderProviderCache.put(layout, provider)
      provider
    }
  }

  /**
   * Creates a map from the names of columns in the layout into cell specifications without
   * reader schemas.
   *
   * @param layout to use when generation the cell spec map.
   * @return a map from columns in the layout to cell specifications without reader schema.
   */
  private def createCellSpecMap(layout: KijiTableLayout): JMap[KijiColumnName, CellSpec] = {
    // scalastyle:off null
    // Fold the columns in the layout into a map from columns to cell specifications without a
    // reader schema.
    layout
        .getColumnNames()
        .asScala
        .foldLeft(new HashMap[KijiColumnName, CellSpec]()) { (specMap, columnName) =>
          specMap.put(columnName, layout.getCellSpec(columnName).setUseWriterSchema())
          specMap
        }
    // scalastyle:on null
  }
}
