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

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable.Buffer

import com.twitter.scalding._

import org.kiji.express.flow._
import org.kiji.express.flow.KijiJob
import org.kiji.express.util.Resources._
import org.kiji.schema.{ EntityId => JEntityId }
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

/**
 * Unit tests for [[org.kiji.express.EntityId]].
 */
class EntityIdSuite extends KijiSuite {
  /** Table layout with formatted entity IDs to use for tests. */
  val formattedEntityIdLayout = layout(KijiTableLayouts.FORMATTED_RKF)
  // Create a table to use for testing
  val formattedTableUri: KijiURI = doAndRelease(makeTestKijiTable(formattedEntityIdLayout)) {
    table: KijiTable => table.getURI
  }

  /** Table layout with hashed entity IDs to use for tests. */
  val hashedEntityIdLayout = layout(KijiTableLayouts.HASHED_FORMATTED_RKF)
  // Create a table to use for testing
  val hashedTableUri: KijiURI = doAndRelease(makeTestKijiTable(hashedEntityIdLayout)) {
    table: KijiTable => table.getURI
  }

  // ------- "Unit tests" for comparisons and creation. -------
  test("Create an Express EntityId from a Kiji EntityId and vice versa in a formatted table.") {
    val expressEid = EntityId("test", "1", "2", 1, 7L)
    val kijiEid = expressEid.toJavaEntityId(formattedTableUri)
    val expected: java.util.List[java.lang.Object] = List[java.lang.Object](
        "test",
        "1",
        "2",
        new java.lang.Integer(1),
        new java.lang.Long(7))
        .asJava

    assert(expected === kijiEid.getComponents)

    val recreate = EntityId.fromJavaEntityId(formattedTableUri, kijiEid)

    assert(expressEid === recreate)
    assert(recreate(0) === "test")
  }

  test("Test creation, comparison, and equality between hashed EntityIds from users and tables.") {
    val eid1: EntityId = EntityId("test")
    val eid2: EntityId = EntityId("test")
    val otherEid = EntityId("other")

    assert(eid1 === eid2)
    assert(eid1 != otherEid)

    // get the Java EntityId
    val jEntityId: JEntityId = eid1.toJavaEntityId(hashedTableUri)
    // this is how it would look if it were read from a table
    val tableEid: EntityId = EntityId.fromJavaEntityId(hashedTableUri, jEntityId)

    assert(tableEid.isInstanceOf[HashedEntityId])

    // get the table version of the eid2, which should be the same
    val tableEid2: EntityId =
        EntityId.fromJavaEntityId(hashedTableUri, eid2.toJavaEntityId(hashedTableUri))

    // get the table version of the otherEid, which should be different
    val tableEidOther: EntityId =
        EntityId.fromJavaEntityId(hashedTableUri, otherEid.toJavaEntityId(hashedTableUri))

    // ensure equals works both ways between user and table EntityIds
    assert(tableEid === eid1)
    assert(eid1 === tableEid)

    assert(otherEid != eid1)
    assert(otherEid != tableEid)

    // ensure the table versions were correctly generated.
    assert(tableEid === tableEid2)
    assert(tableEid2 === tableEid)
    assert(tableEid != tableEidOther)

  }

  test("Creating an EntityId from a Hashed table fails if there is more than one component.") {
    val eid: EntityId = EntityId("one", 2)
    val exception = intercept[org.kiji.schema.EntityIdException] {
      eid.toJavaEntityId(hashedTableUri)
    }
    assert(exception.getMessage.contains("Too many components"))
  }

  test("Test equality between two EntityIds.") {
    val eidComponents1: EntityId = EntityId("test", 1)
    val eidComponents2: EntityId = EntityId("test", 1)

    assert(eidComponents1 === eidComponents2)
    assert(eidComponents2 === eidComponents1)
  }
}
