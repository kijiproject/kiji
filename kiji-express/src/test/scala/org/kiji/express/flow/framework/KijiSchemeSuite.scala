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

package org.kiji.express.flow.framework

import cascading.tuple.Tuple
import cascading.tuple.TupleEntry

import org.kiji.express.AvroEnum
import org.kiji.express.AvroRecord
import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.flow.All
import org.kiji.express.flow.QualifiedColumn
import org.kiji.express.util.GenericCellSpecs

class KijiSchemeSuite extends KijiSuite {
  test("putTuple and rowToTuple can write and read a generic AvroRecord.") {
    // Set up the table.
    val tableLayout = layout("avro-types.json")
    val table = makeTestKijiTable(tableLayout)
    val uri = table.getURI()
    val writer = table.openTableWriter()
    val reader = table.getReaderFactory().openTableReader(GenericCellSpecs(table))

    // Set up the columns and fields.
    val columns = Map("columnSymbol" -> QualifiedColumn("family", "column3"))
    val sourceFields = KijiScheme.buildSourceFields(columns.keys)

    // Create a dummy record with an entity ID to put in the table.
    val dummyEid = EntityId("dummy")
    val record = AvroRecord(
        "hash_type" -> new AvroEnum("MD5"),
        "hash_size" -> 13,
        "suppress_key_materialization" -> false)
    val writeValue = new TupleEntry(sourceFields, new Tuple(dummyEid, record))

    // Put the tuple.
    KijiScheme.putTuple(columns,
        uri,
        None,
        writeValue,
        writer,
        tableLayout)

    // Read the tuple back.
    val rowData =
      reader.get(
          dummyEid.toJavaEntityId(uri),
          KijiScheme.buildRequest(All, columns.values))
    val readValue: Option[Tuple] = KijiScheme.rowToTuple(
        columns,
        sourceFields,
        None,
        rowData,
        uri)
    assert(readValue.isDefined)

    val readRecord = readValue.get.getObject(1).asInstanceOf[KijiSlice[_]].getFirstValue()
    assert(record === readRecord)

    reader.close()
    writer.close()
    table.release()
  }
}
