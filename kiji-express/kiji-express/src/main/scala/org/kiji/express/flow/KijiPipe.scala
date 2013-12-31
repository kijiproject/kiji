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
import com.twitter.scalding.RichPipe
import com.twitter.scalding.TupleConversions
import com.twitter.scalding.TupleSetter
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.util.AvroGenericTupleConverter

/**
 * A class that adds Kiji-specific functionality to a Cascading pipe, allowing the user to pack
 * fields into an Avro record.
 *
 * A `KijiPipe` can be obtained by end-users during the course of authoring a Scalding flow via
 * an implicit conversion available in [[org.kiji.express.repl.Implicits]] or by constructing one
 * directly with an existing pipe.
 *
 * @param pipe enriched with extra functionality.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
class KijiPipe(private[express] val pipe: Pipe) extends TupleConversions {

  /**
   * Packs the specified fields into an Avro [[org.apache.avro.generic.GenericRecord]].  The
   * provided field names must match the fields of the generic record specified by the schema.
   *
   * @param fields is the mapping of input fields (to be packed into the
   *     [[org.apache.avro.generic.GenericRecord]]) to output field which will contain
   *     the [[org.apache.avro.generic.GenericRecord]].
   * @return a pipe containing all input fields, and an additional field containing an
   *     [[org.apache.avro.generic.GenericRecord]].
   */
  def packGenericRecord(fields: (Fields, Fields))(schema: Schema): Pipe = {
    require(fields._2.size == 1, "Cannot pack generic record to more than a single field.")
    require(schema.getType == Schema.Type.RECORD, "Cannot pack non-record Avro type.")
    new RichPipe(pipe).map(fields) { input: GenericRecord => input } (
      new AvroGenericTupleConverter(fields._1, schema), implicitly[TupleSetter[GenericRecord]])
  }

  /**
   * Packs the specified fields into an Avro [[org.apache.avro.generic.GenericRecord]] and drops
   * other fields from the flow.  The provided field names must match the fields of the
   * generic record specified by the schema.
   *
   * @param fields is the mapping of input fields (to be packed into the
   *     [[org.apache.avro.generic.GenericRecord]]) to new output field which will
   *     contain the [[org.apache.avro.generic.GenericRecord]].
   * @return a pipe containing a single field with an Avro
   *     [[org.apache.avro.generic.GenericRecord]].
   */
  def packGenericRecordTo(fields: (Fields, Fields))(schema: Schema): Pipe = {
    require(fields._2.size == 1, "Cannot pack generic record to more than a single field.")
    require(schema.getType == Schema.Type.RECORD, "Cannot pack to non-record Avro type.")
    new RichPipe(pipe).mapTo(fields) { input: GenericRecord => input } (
      new AvroGenericTupleConverter(fields._1, schema), implicitly[TupleSetter[GenericRecord]])
  }
}
