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

import scala.collection.JavaConverters._

import org.kiji.express.Resources._
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

/**
 * Unit tests for [[org.kiji.express.EntityId]]
 */
class EntityIdSuite extends KijiSuite {
  /** Table layout to use for tests. */
  val tableLayoutFormatted = KijiTableLayout.newLayout(
      KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF))
  // Create a table to use for testing
  val tableUriFormatted: String = doAndRelease(makeTestKijiTable(tableLayoutFormatted)) {
    table: KijiTable => table.getURI.toString
  }

  val tableLayoutHashed = KijiTableLayout.newLayout(
    KijiTableLayouts.getLayout(KijiTableLayouts.HASHED_FORMATTED_RKF))
  // Create a table to use for testing
  val tableUriHashed: String = doAndRelease(makeTestKijiTable(tableLayoutHashed)) {
    table: KijiTable => table.getURI.toString
  }

  test("Create an Express EntityId from a Kiji EntityId and vice versa") {
    val eid = EntityId(tableUriFormatted)("test", "1", "2", 1, 7L)
    val entityId = eid.getJavaEntityId()
    val expected: java.util.List[java.lang.Object] = List[java.lang.Object](
        "test",
        "1",
        "2",
        new java.lang.Integer(1),
        new java.lang.Long(7))
        .asJava
    assert(expected == entityId.getComponents)

    val tableUri: KijiURI = KijiURI.newBuilder(tableUriFormatted).build()
    val recreate = EntityId(tableUri, entityId)
    assert(eid == recreate)
    assert(recreate(0) == "test")
  }

  test("Test equality between EntityIds") {
    val eid1: EntityId = EntityId(tableUriHashed)("test")
    val eid2: EntityId = EntityId(tableUriHashed)("test")
    assert(eid1 == eid2)
    // get the Java EntityId
    val jEntityId: JEntityId = eid1.getJavaEntityId()

    val tableUri: KijiURI = KijiURI.newBuilder(tableUriHashed).build()
    // this is how it would look if it were read from a table
    val tableEid = EntityId(tableUri, jEntityId)

    // ensure equals works both ways
    assert(tableEid == eid1)
    assert(eid1 == tableEid)

    val otherEid = EntityId(tableUriHashed)("other")

    assert(otherEid != eid1)
    assert(otherEid != tableEid)

    val tableEid2 = EntityId(tableUri, eid2.getJavaEntityId())
    assert(tableEid == tableEid2)
  }

  test("EntityIds from Java EntityIds are created using the right API") {
    val eidFactory: EntityIdFactory = EntityIdFactory.getFactory(tableLayoutFormatted)
    val components: java.util.List[java.lang.Object] = List[java.lang.Object](
      "test",
      "1",
      "2",
      new java.lang.Integer(1),
      new java.lang.Long(7))
      .asJava
    val jeid: JEntityId = eidFactory.getEntityId(components)
    intercept[IllegalArgumentException]{
      EntityId(tableUriFormatted)(jeid)
    }
  }
}
