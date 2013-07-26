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

import scala.collection.JavaConverters._

import org.kiji.express.EntityId
import org.kiji.express.util.EntityIdFactoryCache
import org.kiji.schema.{ EntityId => JEntityId }
import org.kiji.schema.KijiURI

/**
 * Represents the components of an EntityId in KijiExpress.  An [[org.kiji.express.EntityId]] is
 * fully defined by the table URI and its EntityIdComponents.
 *
 * @param components of an EntityId.
 */
private[express] case class MaterializedEntityId (components: Seq[AnyRef]) extends EntityId {
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
