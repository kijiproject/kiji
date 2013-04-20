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

import scala.collection.JavaConverters._

import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.KijiURI

/**
 * An entity id or row key that can be used to address a row in a Kiji table. This is the
 * Express representation of a [[org.kiji.schema.EntityId]].
 *
 * Users can create EntityIds either by passing in the objects that compose it.
 * For example, if a Kiji table uses formatted row keys composed of a string as
 * their first component and a long as the second, the user can create this as:
 * EntityId("myString", 1L)
 *
 * Users can retrieve the index'th element of an EntityId (0-based), as follows:
 * EntityId(index)
 *
 * @param tableUriString is the string representation of a [[org.kiji.schema.KijiURI]]
 *                       for the table this EntityId is associated with.
 *                       We need to be explicit about the table associated with this
 *                       EntityId because we need to eliminate ambiguity in the cases
 *                       where two entityIds with the same components have different row
 *                       key formats. It is also required when a HashedEntityId (or one
 *                       with materialization suppressed) is read from a Kiji table and
 *                       compared to another that was created by a user from its components.
 * @param hbaseEntityId is the Hbase encoding for this EntityId.
 * @param components is a variable number of objects that compose this EntityId.
 */
final case class EntityId private(
    tableUriString: String,
    private[express] val hbaseEntityId: Array[Byte],
    components: Any*){
  /**
   * Compares this entity id to another to determine if they are equal.
   *
   * @param obj is the Object to compare this EntityId with.
   * @return whether the two are equal.
   */
  override def equals(obj: Any): Boolean = {
    obj match {
      case eid: EntityId => {
        hbaseEntityId.deep == eid.hbaseEntityId.deep
      }
      case _ => false
    }
  }

  /**
   * Get the index'th component of the EntityId.
   *
   * @param index is the 0 based index of the component to retrieve.
   * @return the component at index.
   */
  def apply(index: Int): Any = {
    require(null != components, "Components for this entity Id were not materialized. This may be"
      + "because you have suppressed materialization or used Hashed Entity Ids")
    components(index)
  }

  /**
   * Get the Java EntityId associated with this Scala EntityId.
   *
   * @return the Java EntityId backing this EntityId.
   */
  private[express] def getJavaEntityId(): JEntityId = {
    val eidFactory = EntityIdFactoryCache.getFactory(KijiURI.newBuilder(tableUriString).build())
    val javaComponents: java.util.List[Object] = components.toList
      .map { elem => KijiScheme.convertScalaTypes(elem, null) }
      .asJava
    eidFactory.getEntityId(javaComponents)
  }
}

object EntityId{
  /**
   * Create an Express EntityId given a Kiji EntityId and tableUri.
   * This method is used by the framework to create EntityIds after
   * reading rows from a Kiji table.
   *
   * @param entityId is the Java [[org.kiji.schema.EntityId]].
   * @return the Express representation of the Java EntityId.
   */
  private[express] def apply(tableUri: KijiURI, entityId: JEntityId): EntityId = {
    val components = try {
      entityId.getComponents
        .asScala
        .toList
        .map { elem => KijiScheme.convertJavaTypes(elem) }
    } catch {
      // this is an exception thrown when we try to access components of an entityId
      // which has materialization suppressed. E.g. Hashed EntityIds. So we are unable
      // to retrieve components, but the behavior is legal.
      case ise: IllegalStateException => null
    }
    EntityId(tableUri.toString,
        entityId.getHBaseRowKey, components: _*)
  }

  /**
   * Create an Express representation of EntityId from the given list of components.
   *
   * @param components is a variable list of objects representing the EntityId.
   * @return an entity id addressing a row in the Kiji table specified.
   */
  def apply(tableUri: KijiURI)(components: Any*): EntityId = {
    if ((components.size == 1) && (components(0).isInstanceOf[JEntityId])) {
      throw new IllegalArgumentException("Trying to create scala EntityId using"
          + " a Java EntityId as component. You probably want to use EntityId(KijiURI, JEntityId)"
          + " for doing this.")
    }
    val jEntityIdFactory = EntityIdFactoryCache.getFactory(tableUri)
    val javaComponents: java.util.List[Object] = components.toList
        .map { elem => KijiScheme.convertScalaTypes(elem, null) }
        .asJava
    EntityId(
        tableUri.toString,
        jEntityIdFactory.getEntityId(javaComponents).getHBaseRowKey,
        components: _*)
  }

  /**
   * Create an Express EntityId from the given components.
   *
   * @param tableUriString is a string representation of a KijiURI for the table.
   * @param components is a variable list of objects representing the EntityId.
   * @return an entity id addressing a row in the Kiji table specified.
   */
  def apply(tableUriString: String)(components: Any*): EntityId = {
    val tableUri = KijiURI.newBuilder(tableUriString).build()
    EntityId(tableUri)(components: _*)
  }
}
