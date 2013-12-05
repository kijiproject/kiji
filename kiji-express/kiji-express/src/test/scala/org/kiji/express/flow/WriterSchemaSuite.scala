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

import scala.collection.JavaConversions

import cascading.tuple.Fields
import com.twitter.scalding.Args
import com.twitter.scalding.IterableSource
import com.twitter.scalding.Mode
import com.twitter.scalding.TupleConverter
import com.twitter.scalding.TupleSetter
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Fixed
import org.apache.hadoop.conf.Configuration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.KijiSuite
import org.kiji.express.flow.SchemaSpec.Writer
import org.kiji.express.flow.util.AvroTypesComplete
import org.kiji.schema.Kiji
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.{EntityId => SchemaEntityId}


@RunWith(classOf[JUnitRunner])
class WriterSchemaSuite extends KijiClientTest with KijiSuite {
  import WriterSchemaSuite._
  import AvroTypesComplete._

  // TODO: These non-test things can be moved to the companion object after SCHEMA-539 fix
  setupKijiTest()
  val kiji: Kiji = createTestKiji()
  val table: KijiTable = {
    kiji.createTable(AvroTypesComplete.layout.getDesc)
    kiji.openTable(AvroTypesComplete.layout.getName)
  }
  val conf: Configuration = getConf
  val uri = table.getURI.toString
  val reader: KijiTableReader = table.openTableReader()
  val writer: KijiTableWriter = table.openTableWriter()

  /**
   * Get value from HBase.
   * @param eid string of row
   * @param column column containing requested value
   * @tparam T expected type of value
   * @return the value
   */
  def getValue[T](eid: String, column: KijiColumnName): T = {
    def entityId(s: String): SchemaEntityId = table.getEntityId(s)
    val (family, qualifier) = column.getFamily -> column.getQualifier
    val get = reader.get(entityId(eid), KijiDataRequest.create(family, qualifier))
    require(get.containsColumn(family, qualifier)) // Require the cell exists for null case
    get.getMostRecentValue(family, qualifier)
  }

  /**
   * Verify that the inputs have been persisted into the Kiji column.  Checks that the types and
   * values match.
   * @param inputs to check against
   * @param column column that the inputs are stored in
   * @tparam T expected return type of value in HBase
   */
  def verify[T](inputs: Iterable[(EntityId, T)],
                column: KijiColumnName,
                verifier: (T, T) => Unit): Unit = {
    inputs.foreach { input: (EntityId, T) =>
      val (eid, value) = input
      val retrieved: T = getValue(eid.components.head.toString, column)
      verifier(value, retrieved)
    }
  }

  def valueVerifier[T](input: T, retrieved: T): Unit = {
    assert(input === retrieved)
  }

  def nullVerifier(input: Any, retrieved: Any): Unit = {
    assert(input === null)
    assert(retrieved === null)
  }

  def arrayVerifier[T](input: T, retrieved: T): Unit = {
    assert(retrieved.isInstanceOf[GenericData.Array[_]])
    val ret = JavaConversions.JListWrapper(retrieved.asInstanceOf[GenericData.Array[_]]).toSeq
    assert(input.asInstanceOf[Iterable[_]].toSeq === ret)
  }

  def fixedVerifier[T](input: T, retrieved: T): Unit = {
    assert(retrieved.isInstanceOf[Fixed])
    assert(input === retrieved.asInstanceOf[Fixed].bytes())
  }

  def enumVerifier[T](schema: Schema)(input: T, retrieved: T): Unit = {
    assert(retrieved.isInstanceOf[GenericData.EnumSymbol])
    assert(
      retrieved.asInstanceOf[GenericData.EnumSymbol] ===
        new GenericData().createEnum(input.toString, schema))
  }

  /**
   * Write provided values with express into an HBase column with options as specified in output,
   * and verify that the values have been persisted correctly.
   * @param values to test
   * @param output options to write with
   * @tparam T type of values to write
   * @return
   */
  def testWrite[T](values: Iterable[T],
                   output: ColumnOutputSpec,
                   verifier: (T, T) => Unit =  valueVerifier _) {
    val outputSource = KijiOutput(uri, Map('value -> output))
    val inputs = eids.zip(values)
    expressWrite(conf, new Fields("entityId", "value"), inputs, outputSource)
    verify(inputs, output.columnName, verifier)
  }

  test("A KijiJob can write to a counter column with a Writer schema spec.") {
    testWrite(longs, QualifiedColumnOutputSpec(family, counterColumn, Writer))
  }

  test("A KijiJob can write to a raw bytes column with a Writer schema spec.")    {
    testWrite(bytes, QualifiedColumnOutputSpec(family, rawColumn, Writer))
  }

  test("A KijiJob can write to an Avro null column with a Generic schema spec.") {
    testWrite(nulls, QualifiedColumnOutputSpec(family, nullColumn, nullSchema), nullVerifier)
  }

  test("A KijiJob can write to an Avro null column with a Writer schema spec.") {
    testWrite(nulls, QualifiedColumnOutputSpec(family, nullColumn), nullVerifier)
  }

  test("A KijiJob can write to an Avro boolean column with a Generic schema spec.") {
    testWrite(booleans, QualifiedColumnOutputSpec(family, booleanColumn, booleanSchema))
  }

  test("A KijiJob can write to an Avro boolean column with a Writer schema spec.") {
    testWrite(booleans, QualifiedColumnOutputSpec(family, booleanColumn, Writer))
  }

  test("A KijiJob can write to an Avro int column with a Generic schema spec.") {
    testWrite(ints, QualifiedColumnOutputSpec(family, intColumn, intSchema))
  }

  test("A KijiJob can write to an Avro int column with a Writer schema spec.") {
    testWrite(ints, QualifiedColumnOutputSpec(family, intColumn, Writer))
  }

  test("A KijiJob can write to an Avro long column with a Generic schema spec.") {
    testWrite(longs, QualifiedColumnOutputSpec(family, longColumn, longSchema))
  }

  test("A KijiJob can write to an Avro long column with a Writer schema spec.") {
    testWrite(longs, QualifiedColumnOutputSpec(family, longColumn, Writer))
  }

  test("A KijiJob can write ints to an Avro long column with an int schema.") {
    testWrite(ints, QualifiedColumnOutputSpec(family, longColumn, intSchema))
  }

  test("A KijiJob can write to an Avro float column with a Generic schema spec.") {
    testWrite(floats, QualifiedColumnOutputSpec(family, floatColumn, floatSchema))
  }

  test("A KijiJob can write to an Avro float column with a Writer schema spec.") {
    testWrite(floats, QualifiedColumnOutputSpec(family, floatColumn, Writer))
  }

  test("A KijiJob can write to an Avro double column with a Generic schema spec.") {
    testWrite(doubles, QualifiedColumnOutputSpec(family, doubleColumn, doubleSchema))
  }

  test("A KijiJob can write to an Avro double column with a Writer schema spec.") {
    testWrite(doubles, QualifiedColumnOutputSpec(family, doubleColumn, Writer))
  }

  test("A KijiJob can write floats to an Avro double column with a float schema.") {
    testWrite(floats, QualifiedColumnOutputSpec(family, doubleColumn, intSchema))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A KijiJob can write to an Avro bytes column with a Generic schema spec.") {
    testWrite(bytes, QualifiedColumnOutputSpec(family, bytesColumn, bytesSchema))
  }

  /** TODO: reenable when Schema-594 is fixed. */
  ignore("A KijiJob can write to an Avro bytes column with a Writer schema spec.") {
    testWrite(bytes, QualifiedColumnOutputSpec(family, bytesColumn, Writer))
  }

  test("A KijiJob can write to an Avro string column with a Generic schema spec.") {
    testWrite(strings, QualifiedColumnOutputSpec(family, stringColumn, stringSchema))
  }

  test("A KijiJob can write to an Avro string column with a Writer schema spec.") {
    testWrite(strings, QualifiedColumnOutputSpec(family, stringColumn, Writer))
  }

  test("A KijiJob can write to an Avro specific record column with a Generic schema spec.") {
    testWrite(specificRecords, QualifiedColumnOutputSpec(family, specificColumn, specificSchema))
  }

  test("A KijiJob can write to an Avro specific record column with a Writer schema spec.") {
    testWrite(specificRecords, QualifiedColumnOutputSpec(family, specificColumn, Writer))
  }

  test("A KijiJob can write to a generic record column with a Generic schema spec.") {
    testWrite(genericRecords, QualifiedColumnOutputSpec(family, genericColumn, genericSchema))
  }

  test("A KijiJob can write to a generic record column with a Writer schema spec.") {
    testWrite(genericRecords, QualifiedColumnOutputSpec(family, genericColumn, Writer))
  }

  test("A KijiJob can write to an enum column with a Generic schema spec.") {
    testWrite(enums, QualifiedColumnOutputSpec(family, enumColumn, enumSchema))
  }

  test("A KijiJob can write to an enum column with a Writer schema spec.") {
    testWrite(enums, QualifiedColumnOutputSpec(family, enumColumn, Writer))
  }

  test("A KijiJob can write a string to an enum column with a Generic schema spec.") {
    testWrite(enumStrings, QualifiedColumnOutputSpec(family, enumColumn, enumSchema),
      enumVerifier(enumSchema))
  }

  test("A KijiJob can write an avro array to an array column with a Generic schema spec.") {
    testWrite(avroArrays, QualifiedColumnOutputSpec(family, arrayColumn, arraySchema))
  }

  test("A KijiJob can write an avro array to an array column with a Writer schema spec."){
    testWrite(avroArrays, QualifiedColumnOutputSpec(family, arrayColumn, Writer))
  }

  test("A KijiJob can write an Iterable to an array column with a Generic schema spec.") {
    testWrite(arrays, QualifiedColumnOutputSpec(family, arrayColumn, arraySchema), arrayVerifier)
  }

  test("A KijiJob can write to a union column with a Generic schema spec.") {
    testWrite(unions, QualifiedColumnOutputSpec(family, unionColumn, unionSchema))
  }

  test("A KijiJob can write to a union column with a Writer schema spec.") {
    testWrite(unions, QualifiedColumnOutputSpec(family, unionColumn, Writer))
  }

  test("A KijiJob can write to a fixed column with a Generic schema spec.") {
    testWrite(fixeds, QualifiedColumnOutputSpec(family, fixedColumn, fixedSchema))
  }

  test("A KijiJob can write to a fixed column with a Writer schema spec.") {
    testWrite(fixeds, QualifiedColumnOutputSpec(family, fixedColumn, Writer))
  }

  test("A KijiJob can write a byte array to a fixed column with a Generic schema spec.") {
    testWrite(fixedByteArrays, QualifiedColumnOutputSpec(family, fixedColumn, fixedSchema),
      fixedVerifier)
  }
}

object WriterSchemaSuite {
  /**
   * Writes inputs to outputSource with Express.
   * @param fs fields contained in the input tuples.
   * @param inputs contains tuples to write to HBase with Express.
   * @param outputSource KijiSource with options for how to write values to HBase.
   * @param setter necessary for some implicit shenanigans.  Don't explicitly pass in.
   * @tparam A type of values to be written.
   */
  def expressWrite[A](conf: Configuration,
                      fs: Fields,
                      inputs: Iterable[A],
                      outputSource: KijiSource)
                     (implicit setter: TupleSetter[A]): Boolean = {
    val args = Args("--hdfs")
    Mode.mode = Mode(args, conf) // HDFS mode
    new IdentityJob(fs, inputs, outputSource, args).run
  }
}

// Must be its own top-level class for mystical serialization reasons
class IdentityJob[A](fs: Fields, inputs: Iterable[A], output: KijiSource, args: Args)
                    (implicit setter: TupleSetter[A]) extends KijiJob(args) {
  IterableSource(inputs, fs)(setter, implicitly[TupleConverter[A]]).write(output)
}
