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

package org.kiji.express.util

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.FunSuite

import org.kiji.schema.avro.HashSpec
import org.kiji.express.AvroInt
import org.kiji.express.AvroEnum
import org.kiji.express.AvroRecord

class AvroUtilWithSchemaSuite extends FunSuite {
  test("Test unwrapToSpecific with an Int.") {
    val genericInt = AvroInt(1)
    val specific = AvroUtil.unwrapWithSchema(genericInt, Schema.create(Schema.Type.INT))

    assert(specific === 1)
  }

  test("Test unwrapToSpecific with a HashSpec.") {
    val genericRecord = AvroRecord(
        "hash_type" -> AvroEnum("MD5"),
        "hash_size" -> 15,
        "suppress_key_materialization" -> false,
        "unused_field" -> "hi")
    val schema = HashSpec.SCHEMA$

    val specific = AvroUtil.unwrapWithSchema(genericRecord, schema)
    assert(specific.isInstanceOf[GenericRecord])
    assert(specific.asInstanceOf[GenericRecord].get("hash_size") === 15)
    assert(specific.asInstanceOf[GenericRecord].get("suppress_key_materialization") === false)
    // Trying to get a field that's not in the record should return null.
    assert(specific.asInstanceOf[GenericRecord].get("unused_field") === null)
  }
}
