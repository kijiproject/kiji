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

import scala.collection.mutable.Map

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.TableLayoutDesc
import org.kiji.schema.shell.DDLException

/** Defines how to set various named properties on tables. */
@ApiAudience.Private
trait TableProperties {

  /** Key for the max file size of the table. Uses Option[Long] value. */
  val MaxFileSize = "MaxFileSize"

  /** Key for the memstore flush size of the table. Uses Option[Long] value. */
  val MemStoreFlushSize = "MemStoreFlushSize"

  /** Key specifying the number of regions to create the table with. Uses 'Int' value. */
  val InitialRegionCount = "InitialRegionCount"


  /**
   * Returns the number of regions to create a table with, based on the table property
   * specified as an argument. If the region count is not specified in the map, returns 1.
   *
   * @param tableProperties a set of name-to-value property mappings for the table.
   * @return the value of the InitialRegionCount property, or 1 if that's not defined.
   */
  def getInitialRegionCount(tableProperties: Map[String, Object]): Int = {
    return tableProperties.getOrElse(InitialRegionCount, 1).asInstanceOf[Int]
  }

  /**
   * Applies table properties to a TableLayoutDesc builder.
   *
   * @param tableProperties the name-to-value property mappings to apply.
   * @param the table layout builder to modify.
   * @throws DDLException if there's an error like an invalid property name.
   */
  def applyTableProperties(tableProperties: Map[String, Object], table: TableLayoutDesc.Builder):
      Unit = {
    tableProperties.foreach { case (k, v) =>
      k match {
        case MaxFileSize => {
          v.asInstanceOf[Option[Long]] match {
            case Some(value) => table.setMaxFilesize(value)
            case None => table.setMaxFilesize(null)
          }
        }
        case MemStoreFlushSize => {
          v.asInstanceOf[Option[Long]] match {
            case Some(value) => table.setMemstoreFlushsize(value)
            case None => table.setMemstoreFlushsize(null)
          }
        }
        case InitialRegionCount => { /* Do nothing. Handled by the CREATE TABLE cmd itself. */ }
        case _ => throw new DDLException("Unknown table property: " + k)
      }
    }
  }
}
