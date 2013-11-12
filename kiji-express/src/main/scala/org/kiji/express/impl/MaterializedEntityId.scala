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
import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.EntityId
import org.kiji.schema.{ EntityId => JEntityId }
import org.kiji.schema.EntityIdFactory

/**
 * Represents the components of an EntityId in KijiExpress.  An [[org.kiji.express.EntityId]] is
 * fully defined by the table URI and its EntityIdComponents.
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
