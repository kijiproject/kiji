/**
 * (c) Copyright 2012 WibiData, Inc.
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

import org.kiji.schema.avro.HashType
import org.kiji.schema.avro.RowKeyEncoding
import org.kiji.schema.avro.RowKeyFormat

import org.kiji.schema.shell.DDLException

/**
 * Specifies the RowKeyFormat and associated subtraits.
 */
class RowKeySpec(val encoding:String, val size:Int) {
  /** Maximum size tolerated. */
  val MAX_SIZE: Int = 16

  /** @throws DDLException if the encoding, etc. are not valid. */
  def validate(): Unit = {
    if (encoding == "hash") {
      if (size != MAX_SIZE) {
        throw new DDLException("Size must be equal to " + MAX_SIZE)
      }
    } else if (encoding == "raw") {
      // Nothing to do for size, etc.
    } else if (encoding == "hashprefix") {
      if (size > MAX_SIZE || size <= 0) {
        throw new DDLException("Size must be in (0, " + MAX_SIZE + "]")
      }
    } else {
      throw new DDLException("Unknown encoding: " + encoding)
    }
  }

  /** @return an Avro RowKeyFormat that represents this format in the Layout. */
  def toAvroFormat(): RowKeyFormat = {
    val keysFormat = new RowKeyFormat
    if (encoding == "hash") {
      keysFormat.setEncoding(RowKeyEncoding.HASH)
      keysFormat.setHashType(HashType.MD5) // TODO: support multiple hash types.
      keysFormat.setHashSize(size)
    } else if (encoding == "raw") {
      keysFormat.setEncoding(RowKeyEncoding.RAW)
    } else if (encoding == "hashprefix") {
      keysFormat.setEncoding(RowKeyEncoding.HASH_PREFIX)
      keysFormat.setHashType(HashType.MD5) // TODO: support multiple hash types.
      keysFormat.setHashSize(size)
    }
    return keysFormat
  }
}

/** Singleton row key specifier for raw key format. */
object RawRowKeySpec extends RowKeySpec("raw", 0)

/** Default encoding is with a 16-byte hash. */
object DefaultRowKeySpec extends RowKeySpec("hash", 16)

