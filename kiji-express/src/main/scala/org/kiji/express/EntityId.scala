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
import java.util.Arrays

import scala.collection.JavaConverters._

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
 * EntityId(index)
 * }}}
 */
sealed trait EntityId extends Product {
  /**
   * The table URI for the Kiji table that this EntityId is associated with.
   *
   * We need to be explicit about the table associated with this EntityId because we need to
   * eliminate ambiguity in the cases where two entityIds with the same components have different
   * row key formats. It is also required when a HashedEntityId (or one with materialization
   * suppressed) is read from a Kiji table and compared to another that was created by a user from
   * its components.
   *
   * @return URI of the table this EntityId is associated with.
   */
  def tableUri: String

  /**
   * The hbase encoded version of this EntityId.
   *
   * @return the byte array representation of this EntityId.
   */
  private[express] def encoded: Array[Byte]

  /**
   * Get the index'th component of the EntityId.
   *
   * @param index of the component to retrieve.
   * @return the component at index.
   */
  def apply(index: Int): Any = productElement(index)

  /**
   * Get the Java [[org.kiji.schema.EntityId]] associated with this Scala EntityId.
   *
   * @return the Java EntityId backing this EntityId.
   */
  private[express] def toJavaEntityId(): JEntityId

  override def productPrefix: String = "EntityId"

  override def hashCode: Int = Arrays.hashCode(encoded)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[EntityId]

  override def equals(other: Any): Boolean = {
    other match {
      case eid: EntityId => eid.canEqual(this) && Arrays.equals(encoded, eid.encoded)
      case _ => false
    }
  }
}

/**
 * An EntityId that provides access to its components.
 *
 * @param tableUri for the table this EntityId is associated with.
 * @param encoded byte array representation of this EntityId.
 * @param components that compose this EntityId.
 */
private[express] final class MaterializableEntityId(
    override val tableUri: String,
    private[express] override val encoded: Array[Byte],
    private[express] val components: Seq[Any])
    extends EntityId {
  override def productArity: Int = components.length

  override def productElement(n: Int): Any = components(n)

  private[express] override def toJavaEntityId(): JEntityId = {
    val eidFactory = EntityIdFactoryCache.getFactory(tableUri)
    val javaComponents: java.util.List[Object] = components.toList
        .map { elem => elem.asInstanceOf[AnyRef] }
        .asJava
    eidFactory.getEntityId(javaComponents)
  }
}

/**
 * An EntityId that does not provide access to its components.
 *
 * @param tableUri for the table this EntityId is associated with.
 * @param encoded byte array representation of this EntityId.
 */
private[express] final class HashedEntityId(
    override val tableUri: String,
    private[express] override val encoded: Array[Byte])
    extends EntityId {
  private val materializationError: String = ("Components for this entity Id were not materialized."
      + "This may be because you have suppressed materialization or used Hashed Entity Ids")

  override def productArity: Int = sys.error(materializationError)

  override def productElement(n: Int): Any = sys.error(materializationError)

  private[express] override def toJavaEntityId(): JEntityId = {
    val eidFactory = EntityIdFactoryCache.getFactory(tableUri)
    eidFactory.getEntityIdFromHBaseRowKey(encoded)
  }
}

/**
 * Companion object for EntityId. Provides factory methods for EntityIds.
 */
object EntityId {
  /**
   * Create an Express EntityId given a Kiji EntityId and tableUri. This method is used by the
   * framework to create EntityIds after reading rows from a Kiji table.
   *
   * @param entityId is the Java [[org.kiji.schema.EntityId]].
   * @return the Express representation of the Java EntityId.
   */
  private[express] def apply(tableUri: String, entityId: JEntityId): EntityId = {
    try {
      val components: Seq[Any] = entityId
          .getComponents
          .asScala
          .toSeq
          .map { elem => elem.asInstanceOf[AnyRef] }

      new MaterializableEntityId(
          tableUri,
          entityId.getHBaseRowKey(),
          components)
    } catch {
      // This is an exception thrown when we try to access components of an entityId which has
      // materialization suppressed. E.g. Hashed EntityIds. So we are unable to retrieve components,
      // but the behavior is legal.
      case ise: IllegalStateException => {
        new HashedEntityId(
            tableUri,
            entityId.getHBaseRowKey())
      }
    }
  }

  /**
   * Create an Express representation of EntityId from the given list of components.
   *
   * @param tableUri of the table this EntityId belongs to.
   * @param components of the EntityId.
   * @return a new EntityId addressing a row in the Kiji table specified.
   */
  def fromComponents(tableUri: String, components: Seq[Any]): EntityId = {
    val eidFactory = EntityIdFactoryCache.getFactory(tableUri)
    val javaComponents: java.util.List[Object] = components
        .map { elem => elem.asInstanceOf[AnyRef] }
        .asJava

    new MaterializableEntityId(
        tableUri,
        eidFactory.getEntityId(javaComponents).getHBaseRowKey,
        components)
  }

  /**
   * Create an Express representation of EntityId from the given list of components.
   *
   * @param tableUri of the table this EntityId belongs to.
   * @param components of the EntityId.
   * @return a new EntityId addressing a row in the Kiji table specified.
   */
  def fromComponents(tableUri: KijiURI, components: Seq[Any]): EntityId = {
    fromComponents(tableUri.toString(), components)
  }

  /**
   * Create an Express EntityId from the given components.
   *
   * @param tableUri of the table this EntityId belongs to.
   * @param components of the EntityId.
   * @return a new EntityId addressing a row in the Kiji table specified.
   */
  def apply(tableUri: String)(components: Any*): EntityId = {
    fromComponents(tableUri, components.toSeq)
  }

  /**
   * Create an Express EntityId from the given components.
   *
   * @param tableUri of the table this EntityId belongs to.
   * @param components that compose this EntityId.
   * @return a new EntityId addressing a row in the Kiji table specified.
   */
  def apply(tableUri: KijiURI)(components: Any*): EntityId = {
    fromComponents(tableUri.toString(), components.toSeq)
  }
}
