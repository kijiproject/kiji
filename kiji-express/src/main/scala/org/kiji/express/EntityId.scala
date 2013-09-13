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

import java.lang.IllegalStateException

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.hadoop.conf.Configuration

import org.kiji.express.impl.HashedEntityId
import org.kiji.express.impl.MaterializedEntityId
import org.kiji.schema.{ EntityId => JEntityId }
import org.kiji.schema.KijiURI

/**
 * An entity id or row key that can be used to address a row in a Kiji table. This is the
 * Express representation of a [[org.kiji.schema.EntityId]].
 *
 * Users can create EntityIds either by passing in the objects that compose it.
 * For example, if a Kiji table uses formatted row keys composed of a string as
 * their first component and a long as the second, the user can create this as:
 * {{{
 * EntityId("myString", 1L)
 * }}}
 *
 * Users can retrieve the index'th element of an EntityId (0-based), as follows:
 * {{{
 * MyEntityId(index)
 * }}}
 *
 * EntityIds can either be Materialized (in the case of EntityIds from tables with formatted row
 * keys, and all user-created EntityIds), or Hashed (in the case of EntityIds from tables with
 * hashed or materialization-suppressed row keys).
 */
trait EntityId extends Product {
  /**
   * Get the Java [[org.kiji.schema.EntityId]] associated with this Scala EntityId.
   *
   * @param tableUri of the table the JavaEntityId will be associated with.
   * @param configuration identifying the cluster to use when building EntityIds.
   * @return the Java EntityId backing this EntityId.
   */
   def toJavaEntityId(tableUri: KijiURI, configuration: Configuration): JEntityId

  /**
   * Get the index'th component of the EntityId.
   *
   * @param index of the component to retrieve.
   * @return the component at index.
   */
  def apply(index: Int): Any = productElement(index)

  override def productPrefix: String = "EntityId"

  override def canEqual(that: Any): Boolean = that.isInstanceOf[EntityId]

  /**
   * When comparing two EntityIds, if both are Materialized, then their components are compared.
   * If both are Hashed, their table URIs and encoded values are compared.
   * If one is Hashed and the other is Materialized, they are compared as if the
   * MaterializedEntityId belongs to the same table as the HashedEntityId.  It is not possible to
   * compare a MaterializedEntityId with more than one component with a HashedEntityId.
   *
   * @param other object to compare this to.
   * @return whether the two objects are "equal" according to the definition in this scaladoc.
   */
  override def equals(other: Any): Boolean
}

/**
 * Companion object for EntityId. Provides factory methods for EntityIds.
 */
object EntityId {
  /**
   * Creates a KijiExpress EntityId from a Java EntityId.  This is used internally to convert
   * between kiji-schema and kiji-express, removing the need for table URIs when creating
   * materialized EntityIdComponentss.
   *
   * @param tableUri is the Java EntityId is from.
   * @param entityId is the Java EntityId to convert.
   * @param configuration identifying the cluster to use when building EntityIds.
   */
  def fromJavaEntityId(
      tableUri: KijiURI,
      entityId: JEntityId,
      configuration: Configuration): EntityId = {
    try {
      val components: Seq[AnyRef] = entityId
          .getComponents
          .asScala
          .toSeq

      MaterializedEntityId(components)
    } catch {
      // This is an exception thrown when we try to access components of an entityId which has
      // materialization suppressed. E.g. Hashed EntityIds. So we are unable to retrieve components,
      // but the behavior is legal.
      case ise: IllegalStateException => {
        HashedEntityId(
            tableUri.toString,
            configuration,
            entityId.getHBaseRowKey)
      }
    }
  }

  /**
   * Creates a new EntityId with the components specified by the user.  This only returns
   * MaterializedEntityIds.  Users cannot created HashedEntityIds.
   *
   * @param components of the EntityId to create.
   */
  def apply(components: Any*): MaterializedEntityId = {
    MaterializedEntityId(components.toSeq.map { _.asInstanceOf[AnyRef] })
  }
}
