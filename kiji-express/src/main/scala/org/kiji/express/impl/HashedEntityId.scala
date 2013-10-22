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

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.EntityId
import org.kiji.schema.{EntityId => JEntityId}
import org.kiji.schema.EntityIdFactory

/**
 * An EntityId that does not provide access to its components.  We keep the table URI and the
 * encoded representation, with which we can still do comparisons.
 *
 * These are never user-created.  They are constructed by KijiExpress when reading from a table with
 * row key format HASHED or with suppress-materialization enabled.
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
