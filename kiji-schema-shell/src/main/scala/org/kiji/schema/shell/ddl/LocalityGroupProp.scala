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

package org.kiji.schema.shell.ddl

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.BloomType
import org.kiji.schema.avro.LocalityGroupDesc

import LocalityGroupPropName._
import CompressionTypeToken._

/**
 * Holds a property associated with a locality group.
 */
@ApiAudience.Private
final class LocalityGroupProp(val property: LocalityGroupPropName, val value: Any) {

  /**
   * Apply this property info to the specified LocalityGroupDesc builder. Returns
   * the input LocalityGroupDesc builder, with updated fields.
   *
   * @param group a builder for the locality group descriptor to modify.
   * @param cellSchemaContext context about the table being modified.
   * @return the updated locality group descriptor builder.
   */
  def apply(group: LocalityGroupDesc.Builder, cellSchemaContext: CellSchemaContext):
      LocalityGroupDesc.Builder = {
    property match {
      case LocalityGroupPropName.MaxVersions => { group.setMaxVersions(value.asInstanceOf[Int]) }
      case LocalityGroupPropName.InMemory => { group.setInMemory(value.asInstanceOf[Boolean]) }
      case LocalityGroupPropName.TimeToLive => { group.setTtlSeconds(value.asInstanceOf[Int]) }
      case LocalityGroupPropName.Compression => {
        group.setCompressionType(CompressionTypeToken.toCompressionType(
            value.asInstanceOf[CompressionTypeToken]))
      }
      case LocalityGroupPropName.MapFamily => {
        value.asInstanceOf[MapFamilyInfo].addToLocalityGroup(group, cellSchemaContext)
      }
      case LocalityGroupPropName.GroupFamily => {
        value.asInstanceOf[GroupFamilyInfo].addToLocalityGroup(group, cellSchemaContext)
      }
      case LocalityGroupPropName.BlockSize => {
        value.asInstanceOf[Option[Int]] match {
          case Some(size) => group.setBlockSize(size)
          case None => group.setBlockSize(null)
        }
      }
      case LocalityGroupPropName.BloomFilter => {
        group.setBloomType(value.asInstanceOf[BloomType])
      }
    }

    return group
  }
}

