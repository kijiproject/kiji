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
import org.kiji.schema.avro.CompressionType

/** Represents types of compression supported by HBase. */
@ApiAudience.Private
object CompressionTypeToken extends Enumeration {
  type CompressionTypeToken = Value
  val NONE = Value("NONE")
  val GZIP = Value("GZIP")
  val LZO = Value("LZO")
  val SNAPPY = Value("SNAPPY")

  /**
   * Return an Avro CompressionType representing the specified enum value.
   */
  def toCompressionType(c: CompressionTypeToken): CompressionType = {
    c match {
      case NONE => CompressionType.NONE
      case GZIP => CompressionType.GZ
      case LZO => CompressionType.LZO
      case SNAPPY => CompressionType.SNAPPY
    }
  }
}
