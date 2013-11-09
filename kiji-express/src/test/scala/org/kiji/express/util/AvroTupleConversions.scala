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

import org.kiji.express.KijiSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import cascading.tuple.{Tuple, TupleEntry, Fields}
import org.kiji.express.avro.NameFormats

@RunWith(classOf[JUnitRunner])
class AvroTupleConversionsSuite extends KijiSuite {

  test("Specific records get packed correctly with any variable name style.") {
    val inputs = Map(
        "snake_case_ugh" -> 13,
        "CamelCaseEvenWorse" -> 14,
        "camelPupCase" -> 15,
        "SCREAMING_SNAKE_CASE_YAH" -> 16)
    val fields = new Fields(inputs.keys.toSeq:_*)

    val converter = AvroSpecificTupleConverter[NameFormats](fields,
        implicitly[Manifest[NameFormats]])

    val tuple = new Tuple()
    inputs.values.foreach(tuple.addInteger)

    val record: NameFormats = converter.apply(new TupleEntry(fields, tuple))


    inputs.foreach { case (k, v) => assert(v === record.get(k)) }

  }

}
