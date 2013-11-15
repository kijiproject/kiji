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

package org.kiji.express.flow

import java.lang.IllegalStateException

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.{EntityId => JEntityId}

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
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
trait EntityId extends Product with Ordered[EntityId] {
  /**
   * Get the Java [[org.kiji.schema.EntityId]] associated with this Scala EntityId.
   *
   * @param eidFactory is the EntityIdFactory used to convert the underlying components to a Java
   *     EntityId.
   * @return the Java EntityId backing this EntityId.
   */
  def toJavaEntityId(eidFactory: EntityIdFactory): JEntityId

  /**
   * Get the index'th component of the EntityId.
   *
   * @param index of the component to retrieve.
   * @return the component at index.
   */
  def apply(index: Int): Any = components(index)

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
  override def equals(other: Any): Boolean = {
    if(other.isInstanceOf[EntityId]) {
      compare(other.asInstanceOf[EntityId]) == 0
    } else {
      false
    }
  }

  /**
   * Returns the hashcode of the underlying entityId. For a materialized entity id it returns
   * the hashcode of the underlying list of components. For a hashed entityId it, it returns
   * the hashcode of the encoded byte array wrapped as a string.
   *
   * @return entityId hashcode.
   */
  override def hashCode(): Int

  /**
   * Returns the underlying components of this entityId. For a materialized entityId, this will
   * be the list of components. For a hashed entityId, this will be a singleton list containing
   * the encoded byte[].
   *
   * @return a list of the underlying components.
   */
  def components: Seq[AnyRef]

  /**
   * Returns the comparison result ( > 0, 0, < 0).
   *
   * @return the comparison result ( > 0, 0, < 0).
   */
  override def compare(rhs: EntityId): Int = {
    val zipped = this.components.zip(rhs.components)
    // Compare each element lexicographically.
    zipped.foreach {
      case (mine, theirs) => {
        try {
          val compareResult =
            if (mine.isInstanceOf[Array[Byte]] && theirs.isInstanceOf[Array[Byte]]) {
              val myArray = mine.asInstanceOf[Array[Byte]]
              val rhsArray = theirs.asInstanceOf[Array[Byte]]
              new String(myArray).compareTo(new String(rhsArray))
            } else {
              mine.asInstanceOf[Comparable[Any]].compareTo(theirs)
            }
          if (compareResult != 0) {
            // Return out of the function if these two elements are not equal.
            return compareResult
          }
          // Otherwise, continue.
        } catch {
          case e: ClassCastException =>
            throw new EntityIdFormatMismatchException(components, rhs.components)
        }
      }
    }
    // If all elements in "zipped" were equal, we compare the lengths.
    this.components.length.compare(rhs.components.length)
  }
}

/**
 * Companion object for EntityId. Provides factory methods and implementations for EntityIds.
 */
@ApiAudience.Public
@ApiStability.Experimental
object EntityId {
  /**
   * Creates a KijiExpress EntityId from a Java EntityId.  This is used internally to convert
   * between kiji-schema and kiji-express, removing the need for table URIs when creating
   * materialized EntityIdComponentss.
   *
   * @param entityId is the Java EntityId to convert.
   */
  def fromJavaEntityId(entityId: JEntityId): EntityId = {
    val hbaseKey = entityId.getHBaseRowKey()

    try {
      val components = entityId
        .getComponents
        .asScala
        .toSeq
      MaterializedEntityId(components)
    } catch {
      // This is an exception thrown when we try to access components of an entityId which has
      // materialization suppressed. E.g. Hashed EntityIds. So we are unable to retrieve components,
      // but the behavior is legal.
      case ise: IllegalStateException => {
        HashedEntityId(hbaseKey)
      }
    }
  }

  /**
   * Creates a new EntityId with the components specified by the user.
   *
   * @param components of the EntityId to create.
   *
   * @return the created entity id.
   */
  def apply(components: Any*): EntityId = {
    MaterializedEntityId(components.toSeq.map { _.asInstanceOf[AnyRef] })
  }

  /**
   * Creates a new EntityId given an array of bytes representing the raw
   * HBase rowkey.
   *
   * @param encoded is the raw hbase rowkey.
   *
   * @return the created entity id.
   */
  def apply(encoded: Array[Byte]): EntityId = {
    HashedEntityId(encoded)
  }

  /**
   * An EntityId that does not provide access to its components.  We keep the table URI and the
   * encoded representation, with which we can still do comparisons.
   *
   * These are never user-created.  They are constructed by KijiExpress when reading from a table
   * with row key format HASHED or with suppress-materialization enabled.
   *
   * @param encoded byte array representation of this EntityId.
   */
  @ApiAudience.Private
  @ApiStability.Experimental
  @Inheritance.Sealed
  private[express] case class HashedEntityId(encoded: Array[Byte])
      extends EntityId {

    /** Lazily create a string encoding of this byte array for hash code purposes. **/
    @transient
    private lazy val stringEncoding = new String(encoded)

    /** Lazily create a memoized list of components for the components method. **/
    @transient
    override lazy val components: Seq[AnyRef] = List(encoded)

    override def hashCode(): Int = {
      stringEncoding.hashCode
    }

    override def toJavaEntityId(eidFactory: EntityIdFactory): JEntityId = {
      eidFactory.getEntityIdFromHBaseRowKey(components(0).asInstanceOf[Array[Byte]])
    }
  }

  /**
   * Represents the components of an EntityId in KijiExpress.  An [[org.kiji.express.flow.EntityId]]
   * is fully defined by the table URI and its EntityIdComponents.
   *
   * @param components of an EntityId.
   */
  @ApiAudience.Private
  @ApiStability.Experimental
  @Inheritance.Sealed
  private[express] case class MaterializedEntityId(override val components: Seq[AnyRef])
      extends EntityId {

    override def toJavaEntityId(eidFactory: EntityIdFactory): JEntityId = {
      eidFactory.getEntityId(components.asJava)
    }

    override def hashCode(): Int = {
      components.hashCode
    }
  }
}
