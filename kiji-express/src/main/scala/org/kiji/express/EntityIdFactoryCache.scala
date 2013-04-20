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
import org.kiji.express.Resources._
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI

/**
 * EntityIdFactoryCache performs the operation of getting the EntityIdFactory for a Kiji
 * table in a memoized way. If the required EntityIdFactory is not present in its cache, this
 * will open a connection to a Kiji table, get the factory from it and cache it for
 * subsequent calls. There will be one such cache per JVM.
 */
@ApiAudience.Private
@ApiStability.Experimental
object EntityIdFactoryCache {
  private def getFactoryFromKiji(tableUri: KijiURI): EntityIdFactory = {
    val tableLayout =
      doAndRelease(Kiji.Factory.open(tableUri)) { kiji: Kiji =>
        doAndRelease(kiji.openTable(tableUri.getTable())) { table: KijiTable =>
          table.getLayout()
        }
      }
    EntityIdFactory.getFactory(tableLayout)
  }

  private val factoryCache = Memoize(getFactoryFromKiji)

  /**
   *  Get an EntityIdFactory for the table specified.
   *
   * @param tableUriString is the string representing a [[org.kiji.schema.KijiURI]].
   * @return the EntityIdFactory to generate an EntityId for this table.
   */
  def getFactory(tableUriString: String) : EntityIdFactory = {
    val inputTableURI: KijiURI = KijiURI.newBuilder(tableUriString).build()
    getFactory(inputTableURI)
  }

  /**
   * Get an EntityIdFactory for the table specified.
   *
   * @param tableUri is the unique [[org.kiji.schema.KijiURI]] for the table.
   * @return the EntityIdFactory to generate an EntityId for this table.
   */
  def getFactory(tableUri: KijiURI): EntityIdFactory = {
    factoryCache(tableUri)
  }
}
