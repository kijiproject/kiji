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

package org.kiji.express

import org.scalatest.FunSuite

class AvroRecordSuite extends FunSuite {
  test("AvroRecord's apply method works for an empty record.") {
    val record = AvroRecord()

    assert(0 === record.map.size)
  }

  test("AvroRecord's apply method works for Scala primitives.") {
    val record = AvroRecord("field1" -> 1, "field2" -> "two", "field3" -> 3L, "field4" -> false)

    assert(4 === record.map.size)
    assert(1 === record("field1").asInt)
    assert("two" === record("field2").asString)
    assert(3L === record("field3").asLong)
    assert(false === record("field4").asBoolean)
  }

  test("AvroRecord's apply method works for lists and maps.") {
    val record = AvroRecord("field1" -> List(5,6,7), "field2" -> Map("a" -> 1, "b" -> 2))

    assert(5 === record("field1").asList()(0).asInt)
    assert(6 === record("field1").asList()(1).asInt)
    assert(7 === record("field1").asList()(2).asInt)

    assert(1 === record("field2").asMap()("a").asInt)
    assert(2 === record("field2").asMap()("b").asInt)
  }

  test("AvroRecord's apply method works for enums.") {
    val record = AvroRecord("field1" -> new AvroEnum("enumstring"))
    assert("enumstring" === record("field1").asEnumName)
  }

  test("AvroRecord's apply method works for nested records.") {
    val innerRecord = AvroRecord("field1" -> List(5,6,7), "field2" -> Map("a" -> 1, "b" -> 2))
    val record = AvroRecord("key1" -> innerRecord)

    assert(5 === record("key1")("field1")(0).asInt)
  }
}
