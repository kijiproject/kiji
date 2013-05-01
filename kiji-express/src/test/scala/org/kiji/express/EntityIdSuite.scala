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
  val formattedEntityIdLayout = KijiTableLayout.newLayout(
      KijiTableLayouts.getLayout(KijiTableLayouts.FORMATTED_RKF))
  // Create a table to use for testing
  val formattedTableUri: String = doAndRelease(makeTestKijiTable(formattedEntityIdLayout)) {
    table: KijiTable => table.getURI.toString
  }

  val hashedEntityIdLayout = KijiTableLayout.newLayout(
    KijiTableLayouts.getLayout(KijiTableLayouts.HASHED_FORMATTED_RKF))
  // Create a table to use for testing
  val hashedTableUri: String = doAndRelease(makeTestKijiTable(hashedEntityIdLayout)) {
    table: KijiTable => table.getURI.toString
  }

  test("Create an Express EntityId from a Kiji EntityId and vice versa") {
    val eid = EntityId(formattedTableUri)("test", "1", "2", 1, 7L)
    val entityId = eid.toJavaEntityId()
    val expected: java.util.List[java.lang.Object] = List[java.lang.Object](
        "test",
        "1",
        "2",
        new java.lang.Integer(1),
        new java.lang.Long(7))
        .asJava
    assert(expected == entityId.getComponents)

    val tableUri: KijiURI = KijiURI.newBuilder(formattedTableUri).build()
    val recreate = EntityId(tableUri, entityId)
    assert(eid == recreate)
    assert(recreate(0) == "test")
  }

  test("Test equality between EntityIds") {
    val eid1: EntityId = EntityId(hashedTableUri)("test")
    val eid2: EntityId = EntityId(hashedTableUri)("test")
    assert(eid1 == eid2)
    // get the Java EntityId
    val jEntityId: JEntityId = eid1.toJavaEntityId()

    val tableUri: KijiURI = KijiURI.newBuilder(hashedTableUri).build()
    // this is how it would look if it were read from a table
    val tableEid = EntityId(tableUri, jEntityId)

    // ensure equals works both ways
    assert(tableEid == eid1)
    assert(eid1 == tableEid)

    val otherEid = EntityId(hashedTableUri)("other")

    assert(otherEid != eid1)
    assert(otherEid != tableEid)

    val tableEid2 = EntityId(tableUri, eid2.toJavaEntityId())
    assert(tableEid == tableEid2)
  }

  test("Test creating a hashed EntityId from a JEntityId and converting it back") {
    val entityId: EntityId = EntityId(hashedTableUri)("test")
    val jEntityId: JEntityId = entityId.toJavaEntityId()
    val uri: KijiURI = KijiURI.newBuilder(hashedTableUri).build()
    val reconstitutedEntityId: EntityId = EntityId(uri, jEntityId)
    val reconstitutedJEntityId: JEntityId = reconstitutedEntityId.toJavaEntityId()

    assert(entityId === reconstitutedEntityId)
    assert(jEntityId === reconstitutedJEntityId)
  }

  test("Test creating an unhashed EntityId from a JEntityId and converting it back") {
    val entityId: EntityId = EntityId(formattedTableUri)("test")
    val jEntityId: JEntityId = entityId.toJavaEntityId()
    val uri: KijiURI = KijiURI.newBuilder(formattedTableUri).build()
    val reconstitutedEntityId: EntityId = EntityId(uri, jEntityId)
    val reconstitutedJEntityId: JEntityId = reconstitutedEntityId.toJavaEntityId()

    assert(entityId === reconstitutedEntityId)
    assert(jEntityId === reconstitutedJEntityId)
  }
}
