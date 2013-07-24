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

import org.kiji.express.util.EntityIdFactoryCache
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
   * @return the Java EntityId backing this EntityId.
   */
   def toJavaEntityId(tableUri: KijiURI): JEntityId

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
 * Represents the components of an EntityId in KijiExpress.  An [[org.kiji.express.EntityId]] is
 * fully defined by the table URI and its EntityIdComponents.
 *
 * @param components of an EntityId.
 */
case class MaterializedEntityId private[express](components: Seq[AnyRef]) extends EntityId {
  override def productArity: Int = components.length

  override def productElement(n: Int): Any = components(n)

  override def toJavaEntityId(tableUri: KijiURI): JEntityId = {
    val javaComponents: java.util.List[Object] =
        components.map { component: Any => component.asInstanceOf[AnyRef] }.asJava
    val eidFactory = EntityIdFactoryCache.getFactory(tableUri)
    eidFactory.getEntityId(javaComponents)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case MaterializedEntityId(otherComponents) =>{
        components.equals(otherComponents)
      }
      case otherEntityId: HashedEntityId => {
        otherEntityId.equals(MaterializedEntityId.this)
      }
      case _ => false
    }
  }
}

/**
 * An EntityId that does not provide access to its components.  We keep the table URI and the
 * encoded representation, with which we can still do comparisons.
 *
 * These are never user-created.  They are constructed by KijiExpress when reading from a table with
 * row key format HASHED or with suppress-materialization enabled.
 *
 * @param tableUri for the table this EntityId is associated with.
 * @param encoded byte array representation of this EntityId.
 */
case class HashedEntityId private[express] (tableUri: String, encoded: Array[Byte])
    extends EntityId {
  /** Error message used when trying to materialize this EntityId. */
  private val materializationError: String = ("Components for this entity Id were not materialized."
      + "This may be because you have suppressed materialization or used Hashed Entity Ids")

  /** Lazily get the EntityIdFactory from the cache when necessary. */
  private[express] lazy val eidFactory =
      EntityIdFactoryCache.getFactory(KijiURI.newBuilder(tableUri).build())

  override def productArity: Int = sys.error(materializationError)

  override def productElement(n: Int): Any = sys.error(materializationError)

  override def toJavaEntityId(tableUri: KijiURI): JEntityId = {
    val toJavaEntityIdError: String = (
            "This EntityId can only be used for the table %s.".format(tableUri.toString)
            + "This may be because you have suppressed materialization or used Hashed Entity Ids. ")
    require(tableUri.toString == this.tableUri, toJavaEntityIdError)
    eidFactory.getEntityIdFromHBaseRowKey(encoded)
  }

  override def toString(): String = {
    "HashedEntityId(KijiTable: %s, encoded: %s)".format(tableUri, encoded.toSeq.mkString(","))
  }

  override def equals(other: Any): Boolean = {
    other match {
      case otherEid: EntityId => { otherEid match {
        case HashedEntityId(thatTableUri, otherEncodedVal) => {
          return this.tableUri == thatTableUri &&
              encoded.toSeq.mkString("") == otherEncodedVal.toSeq.mkString("")
        }
        case that: MaterializedEntityId => {
          // If the other is materialized with a single component, compare it with that as if it
          // belonged to the same table as this.
          if (that.components.length == 1) {
            val thatEncoded =
                that.toJavaEntityId(KijiURI.newBuilder(tableUri).build()).getHBaseRowKey
            return encoded.toSeq.mkString("") == thatEncoded.toSeq.mkString("")
          } else {
              // An EntityId with more than one component can't be compared with a HashedEntityId.
            return false
          }
        }
      } }
      case _ => return false
    }
  }
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
   */
  def fromJavaEntityId(
      tableUri: KijiURI,
      entityId: JEntityId): EntityId = {
    try {
      val javaComponents: java.util.List[Object] = entityId.getComponents
      val components: Seq[AnyRef] = javaComponents.asScala.toSeq

      new MaterializedEntityId(components)
    } catch {
      // This is an exception thrown when we try to access components of an entityId which has
      // materialization suppressed. E.g. Hashed EntityIds. So we are unable to retrieve components,
      // but the behavior is legal.
      case ise: IllegalStateException => {
        new HashedEntityId(
            tableUri.toString,
            entityId.getHBaseRowKey())
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
    new MaterializedEntityId(components.toSeq.map { _.asInstanceOf[AnyRef] })
  }
}
