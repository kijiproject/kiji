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

import org.kiji.express.Resources._
import org.kiji.schema.KijiTable
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

/**
 * Unit tests for [[org.kiji.express.EntityIdFactoryCache]]
 */
class EntityIdFactoryCacheSuite extends KijiSuite {
  test("Test for caching for Row Key Format given Table Uri") {
    /** Table layout to use for tests. */
    val tableLayout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF))
    val uri: String = doAndRelease(makeTestKijiTable(tableLayout)) { table: KijiTable =>
      table.getURI().toString()
    }

    val eidFactory = EntityIdFactoryCache.getFactory(uri)

    val components = new java.util.ArrayList[Object]()
    components.add("a")
    components.add("b")
    components.add("c")
    components.add(new java.lang.Integer(1))
    components.add(new java.lang.Long(7))

    // If this works, we successfully created a Formatted EntityId
    // which means we were returned the right EID factory.
    val eid = eidFactory.getEntityId(components)
    assert(components == eid.getComponents)
  }
}
