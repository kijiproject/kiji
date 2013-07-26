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

import scala.collection.mutable.Set

import org.kiji.annotations.ApiAudience
import org.kiji.schema.avro.RowKeyFormat2
import org.kiji.schema.shell.DDLException

/**
 * The size of the hash prefix for the row key.
 */
@ApiAudience.Private
final class FormattedKeyHashSize(val size: Int) extends FormattedKeyHashParam {
  override def validate(names: Set[String]): Unit = {
    if (size < 0 || size > 16) {
      throw new DDLException("Invalid size value: " + size)
    }
  }

  override def updateHashProperties(format: RowKeyFormat2.Builder): RowKeyFormat2.Builder = {
    format.getSalt().setHashSize(size)
    return format
  }
}
