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

package org.kiji.schema.shell.ddl.key

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import scala.collection.mutable.Set

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.RowKeyComponent
import org.kiji.schema.avro.RowKeyFormat2
import org.kiji.schema.shell.DDLException

/**
 * The name of the right-most row key component to include in the hash,
 * specified by a `HASH (THROUGH <i>fieldname</i>, ...)` clause.
 */
@ApiAudience.Private
final class FormattedKeyHashComponent(val name: String) extends FormattedKeyHashParam {
  override def validate(names: Set[String]): Unit = {
    if (!names.contains(name)) {
      throw new DDLException("'" + name + "' does not represent a component already defined " +
          " when encountered in the HASH(THROUGH ...) clause.")
    }
  }

  override def updateHashProperties(format: RowKeyFormat2.Builder): RowKeyFormat2.Builder = {
    // This function moves the range_scan_start_index to the offset after the specified field.
    format.getComponents().zipWithIndex.foreach { case(component, idx) =>
      if (component.getName() == name) {
        format.setRangeScanStartIndex(idx + 1)
      }
    }

    return format
  }
}
