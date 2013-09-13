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

package org.kiji.express.impl

import java.util.Arrays

import org.apache.hadoop.conf.Configuration

import org.kiji.express.EntityId
import org.kiji.express.util.EntityIdFactoryCache
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.KijiURI

/**
 * An EntityId that does not provide access to its components.  We keep the table URI and the
 * encoded representation, with which we can still do comparisons.
 *
 * These are never user-created.  They are constructed by KijiExpress when reading from a table with
 * row key format HASHED or with suppress-materialization enabled.
 *
 * @param tableUri for the table this EntityId is associated with.
 * @param configuration identifying the cluster to use when building EntityIds.
 * @param encoded byte array representation of this EntityId.
 */
private[express] case class HashedEntityId(
    tableUri: String,
    configuration: Configuration,
    encoded: Array[Byte])
    extends EntityId {
  /** Error message used when trying to materialize this EntityId. */
  private val materializationError: String = ("Components for this entity Id were not materialized."
      + "This may be because you have suppressed materialization or used Hashed Entity Ids")

  /** Lazily get the EntityIdFactory from the cache when necessary. */
  private[express] lazy val eidFactory: EntityIdFactory =
      EntityIdFactoryCache.getFactory(KijiURI.newBuilder(tableUri).build(), configuration)

  override def productArity: Int = sys.error(materializationError)

  override def productElement(n: Int): Any = sys.error(materializationError)

  override def toJavaEntityId(tableUri: KijiURI, configuration: Configuration): JEntityId = {
    val toJavaEntityIdError: String = (
        "This EntityId can only be used for the table %s.".format(tableUri.toString)
        + "This may be because you have suppressed materialization or used Hashed Entity Ids. ")
    require(tableUri.toString == this.tableUri, toJavaEntityIdError)
    eidFactory.getEntityIdFromHBaseRowKey(encoded)
  }

  override def toString: String = {
    "HashedEntityId(KijiTable: %s, encoded: %s)".format(tableUri, encoded.toSeq.mkString(","))
  }

  override def equals(other: Any): Boolean = {
    other match {
      case HashedEntityId(thatTableUri, _, otherEncodedVal) => {
        this.tableUri == thatTableUri && Arrays.equals(encoded, otherEncodedVal)
      }
      // Matches an EntityId with only one component.
      case that @ MaterializedEntityId(Seq(_)) => {
        // Compare it with `that` as if it belonged to the same table as this.
        val thatEncoded: Array[Byte] = that
            .toJavaEntityId(KijiURI.newBuilder(tableUri).build(), configuration)
            .getHBaseRowKey

        Arrays.equals(encoded, thatEncoded)
      }
      case _ => false
    }
  }
}
