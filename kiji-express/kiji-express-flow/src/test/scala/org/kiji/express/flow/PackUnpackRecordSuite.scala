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

package org.kiji.express.flow

import cascading.pipe.Pipe
import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.TuplePacker
import com.twitter.scalding.TupleUnpacker
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.specific.SpecificRecord
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.avro.SimpleRecord
import org.kiji.express.flow.util.PipeRunner._

@RunWith(classOf[JUnitRunner])
class PackUnpackRecordSuite extends KijiSuite {

  test("Avro tuple converters should be in implicit scope of KijiJobs.") {
    class ImplicitJob(args: Args) extends KijiJob(args) {
      assert(implicitly[TupleUnpacker[GenericRecord]].getClass ===
          classOf[AvroGenericTupleUnpacker])
      assert(implicitly[TuplePacker[SpecificRecord]].getClass ===
          classOf[AvroSpecificTuplePacker[SpecificRecord]])
      assert(implicitly[TuplePacker[SimpleRecord]].getClass ===
          classOf[AvroSpecificTuplePacker[SimpleRecord]])
    }
  }

  // Import implicits to simulate REPL environment
  import org.kiji.express.repl.Implicits._

  test("Avro tuple converters should be in implicit scope of express REPL.") {
    assert(implicitly[TupleUnpacker[GenericRecord]].getClass ===
        classOf[AvroGenericTupleUnpacker])
    assert(implicitly[TuplePacker[SpecificRecord]].getClass ===
        classOf[AvroSpecificTuplePacker[SpecificRecord]])
    assert(implicitly[TuplePacker[SimpleRecord]].getClass ===
        classOf[AvroSpecificTuplePacker[SimpleRecord]])
  }

  /** Schema of SimpleRecord Avro record. */
  val schema = SimpleRecord.getClassSchema

  /** Sample inputs for testing Avro packing / unpacking in tuple form. */
  val input: List[(Long, String)] = List(
    (1, "foobar"),
    (2, "shoe"),
    (3, "random"),
    (99, "baloons"),
    (356, "sumerians"))

  /** Fields contained in the input. */
  val inputFields: Fields = ('ls, 's)

  /** Sample inputs for testing Avro packing / unpacking in Specific Record form. */
  val specificRecords: List[SimpleRecord] = input.map { t =>
    SimpleRecord.newBuilder.setL(t._1).setS(t._2).build()
  }

  /** Sample inputs for testing Avro packing / unpacking in Generic Record form. */
  val genericRecords: List[GenericRecord] = input.map { t =>
    new GenericRecordBuilder(schema).set("l", t._1).set("s", t._2).build()
  }

  /** Fields contained in record input. */
  val recordFields: Fields = 'r

  def validateSpecificSimpleRecord(outputs: Iterable[(Long, String, String, SimpleRecord)]) {
    outputs.foreach { t =>
      val (l, s, o, r) = t
      assert(specificRecords.contains(r))
      assert(r.getL === l)
      assert(r.getS === s)
      assert(r.getO === o)
    }
  }

  def validateGenericSimpleRecord(outputs: Iterable[(Long, String, String, GenericRecord)]) {
    outputs.foreach { t =>
      val (l, s, o, r) = t
      assert(genericRecords.contains(r))
      assert(r.get("l") === l)
      assert(r.get("s") === s)
      assert(r.get("o") === o)
    }
  }

  test("A KijiPipe can pack fields into a specific Avro record.") {
    val pipe = new Pipe("pack specific record")
        .map('ls -> 'l) { ls: String => ls.toLong }
        .pack[SimpleRecord](('l, 's) -> 'r)
        .map('r -> 'o) { r: SimpleRecord => r.getO }
        .project('l, 's, 'o, 'r)

    validateSpecificSimpleRecord(runPipe(pipe, inputFields, input))
  }

  test("A KijiPipe can pack fields into a generic Avro record.") {
    val pipe = new Pipe("pack generic record")
        .map('ls -> 'l) { ls: String => ls.toLong }
        .packGenericRecord(('l, 's) -> 'r)(schema)
        .map('r -> 'o) { r: GenericRecord => r.get("o") }
        .project('l, 's, 'o, 'r)

    validateGenericSimpleRecord(runPipe(pipe, inputFields, input))
  }

  test("A KijiPipe can unpack a specific Avro record into fields.") {
    val pipe = new Pipe("unpack generic record")
        .unpack[SimpleRecord]('r -> ('l, 's, 'o))
        .project('l, 's, 'o, 'r)

    validateSpecificSimpleRecord(runPipe(pipe, recordFields, specificRecords))
  }

  test("A KijiPipe can unpack a generic Avro record into fields.") {
    val pipe = new Pipe("unpack generic record")
        .unpack[GenericRecord]('r -> ('l, 's, 'o))
        .project('l, 's, 'o, 'r)

    validateGenericSimpleRecord(runPipe(pipe, recordFields, genericRecords))
  }
}
