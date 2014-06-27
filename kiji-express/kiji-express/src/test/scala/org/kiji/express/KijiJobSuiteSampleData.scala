/**
 * (c) Copyright 2014 WibiData, Inc.
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

import org.kiji.schema.layout.KijiTableLayout
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.util.ResourceUtil
import org.kiji.express.flow.EntityId

import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.GenericRecord

/**
 * Static values used across multiple tests.  Contains example layouts, example inputs, etc.
 */
object KijiJobSuiteSampleData {
  val avroLayout: KijiTableLayout = ResourceUtil.layout("layout/avro-types.json")

  val rawInputs: List[(Long, String)] = List(
    (1, "input 1"),
    (2, "input 2"),
    (3, "input 3"),
    (4, "input 4"),
    (5, "input 5"))

  val eids: List[EntityId] = List("row-1", "row-2", "row-3", "row-4", "row-5").map(EntityId(_))

  val genericInputs: List[GenericRecord] = {
    val builder = new GenericRecordBuilder(SimpleRecord.getClassSchema)
    rawInputs.map { case (l: Long, s: String) => builder.set("l", l).set("s", s).build }
  }

  val specificInputs: List[SimpleRecord] = {
    val builder = SimpleRecord.newBuilder()
    rawInputs.map { case (l: Long, s: String) => builder.setL(l).setS(s).build }
  }

}
