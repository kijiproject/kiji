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

package org.kiji.express.modeling.impl

import java.io.File

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.express.AvroRecord
import org.kiji.express.EntityId
import org.kiji.express.KijiSuite
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.mapreduce.kvstore.lib.{ AvroRecordKeyValueStore => JAvroRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore }
import org.kiji.mapreduce.kvstore.lib.{ KijiTableKeyValueStore => JKijiTableKeyValueStore }
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

/**
 * Tests the functionality of [[org.kiji.express.modeling.KeyValueStore]] when backed by specific
 * key-value store implementations from KijiMR.
 */
class KeyValueStoreImplSuite extends KijiSuite {
  import KeyValueStoreImplSuite._

  test("Using a KijiExpress KVStore backed by a KijiMR KijiTableKeyValueStore") {
    // Create a test Kiji instance containing a simple table.
    val simpleLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)
    val uri: KijiURI = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI()
    }
    // Create some sample data to populate the table with, and load it into the table.
    val dataToLoad = Map(EntityId(uri)("1") -> "value1",
        EntityId(uri)("2") -> "value2",
        EntityId(uri)("3") -> "value3")
    populateTestKijiTable(uri, dataToLoad)

    // Now create a KijiMR key-value store hooked to the Kiji table.
    val store: JKijiTableKeyValueStore[String] = JKijiTableKeyValueStore
        .builder()
        .withTable(uri)
        .withColumn("family", "column1")
        .withConfiguration(HBaseConfiguration.create())
        .build()

    // Now create a KijiExpress key-value store backed by the KijiMR key-value store.
    // Use the key-value store to access values and verify they are correct.
    doAndClose(KeyValueStore[String](store)) { kvstore =>
      assert("value1" === kvstore(Seq("1")))
      assert("value2" === kvstore(Seq("2")))
      assert("value3" === kvstore(Seq("3")))
    }
  }

  test("Using a KijiExpress KVStore backed by a KijiMR AvroRecordKeyValueStore") {
    val avroFilePath: Path = generateAvroKVRecordKeyValueStore()
    val conf = HBaseConfiguration.create()
    val store: JAvroRecordKeyValueStore[Int, GenericData.Record] = JAvroRecordKeyValueStore
        .builder()
        .withConfiguration(conf)
        .withInputPath(avroFilePath)
        .withKeyFieldName("key")
        .build()

    // Wrap the Java key-value store in its corresponding Scala variety.
    doAndClose(KeyValueStore[Int](store)) { kvstore =>
      assert("one" === kvstore(1)("value").asString)
      assert("two" === kvstore(2)("value").asString)

      assert("blah" === kvstore(1)("blah").asString)
      assert("blah" === kvstore(2)("blah").asString)

      assert(1 === kvstore(1)("key").asInt)
      assert(2 === kvstore(2)("key").asInt)
    }
  }

  test("Using a KijiExpress KVStore backed by a KijiMR AvroKVRecordKeyValueStore") {
    // Get the Java key-value store.
    val avroFilePath: Path = generateAvroKVRecordKeyValueStore()
    val conf = HBaseConfiguration.create()
    val store: JAvroKVRecordKeyValueStore[Int, CharSequence] = JAvroKVRecordKeyValueStore
        .builder()
        .withConfiguration(conf)
        .withInputPath(avroFilePath)
        .build()

    // Wrap the Java key-value store in its corresponding Scala variety.
    doAndClose(KeyValueStore[Int, String](store)) { kvstore =>
      assert("one" === kvstore(1))
      assert("two" === kvstore(2))
    }
  }

  test("Using a KijiExpress KVStore where keys are records") {
    // Get the Java key-value store.
    val avroFilePath: Path = generateAvroKVRecordKeyValueStoreWithRecordKey()
    val conf = HBaseConfiguration.create()
    val store: JAvroRecordKeyValueStore[AvroRecord, GenericData.Record] = JAvroRecordKeyValueStore
        .builder()
        .withConfiguration(conf)
        .withInputPath(avroFilePath)
        .withKeyFieldName("key")
        .build()

    // Wrap the Java key-value store in its corresponding Scala variety and test it.
    doAndClose(KeyValueStore[AvroRecord](store)) { kvstore =>
      assert("one" === kvstore(AvroRecord("innerkey" -> 1))("value").asString)
      assert("two" === kvstore(AvroRecord("innerkey" -> 2))("value").asString)
    }
  }
}

object KeyValueStoreImplSuite {
  /**
   * Writes an avro file of generic records with a 'key', 'blah', and 'value' field.
   *
   * @return the path to the file written.
   */
  private[express] def generateAvroKVRecordKeyValueStore(): Path = {
    // Build the schema associated with this key-value store.
    // scalastyle:off null
    val writerSchema: Schema = {
      val schema: Schema = Schema.createRecord("record", null, null, false)
      val fields: Seq[Schema.Field] = Seq(
          new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
          new Schema.Field("blah", Schema.create(Schema.Type.STRING), null, null),
          new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null))
      schema.setFields(fields.asJava)
      schema
    }
    // scalastyle:on null

    // Open a writer.
    val file: File = File.createTempFile("generic-kv", ".avro")
    val fileWriter: DataFileWriter[GenericRecord] = {
      val datumWriter = new GenericDatumWriter[GenericRecord](writerSchema)
      new DataFileWriter(datumWriter)
          .create(writerSchema, file)
    }

    doAndClose(fileWriter) { writer =>
      // Write a record.
      {
        val record: GenericData.Record = new GenericData.Record(writerSchema)
        record.put("key", 1)
        record.put("blah", "blah")
        record.put("value", "one")
        writer.append(record)
      }

      // Write another record.
      {
        val record: GenericData.Record = new GenericData.Record(writerSchema)
        record.put("key", 2)
        record.put("blah", "blah")
        record.put("value", "two")
        writer.append(record)
      }

      // Write a duplicate record with the same key field value.
      {
        val record: GenericData.Record = new GenericData.Record(writerSchema)
        record.put("key", 2)
        record.put("blah", "blah")
        record.put("value", "deux")
        writer.append(record)
      }
    }

    return new Path(file.getPath())
  }

  /**
   * Writes an avro file of generic records with a 'key', 'blah', and 'value' field, where the key
   * field is a record.
   *
   * @return the path to the file written.
   */
  private def generateAvroKVRecordKeyValueStoreWithRecordKey(): Path = {
    // Build the schema associated with this key-value store.
    // scalastyle:off null
    val keySchema: Schema =
        Schema.createRecord(
            "key_record",
            null,
            null,
            false
            )
    keySchema.setFields(
            Seq(new Schema.Field("innerkey", Schema.create(Schema.Type.INT), null, null)).asJava)
    val writerSchema: Schema = {
      val schema: Schema = Schema.createRecord("record", null, null, false)
      val fields: Seq[Schema.Field] = Seq(
          new Schema.Field(
              "key",
              keySchema,
              null,
              null),
          new Schema.Field("blah", Schema.create(Schema.Type.STRING), null, null),
          new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null))
      schema.setFields(fields.asJava)
      schema
    }
    // scalastyle:on null

    // Open a writer.
    val file: File = File.createTempFile("generic-kv", ".avro")
    val fileWriter: DataFileWriter[GenericRecord] = {
      val datumWriter = new GenericDatumWriter[GenericRecord](writerSchema)
      new DataFileWriter(datumWriter)
          .create(writerSchema, file)
    }

    doAndClose(fileWriter) { writer =>
      // Write a record.
      {
        val key1: GenericData.Record = new GenericData.Record(keySchema)
        key1.put("innerkey", 1)
        val record: GenericData.Record = new GenericData.Record(writerSchema)
        record.put("key", key1)
        record.put("blah", "blah")
        record.put("value", "one")
        writer.append(record)
      }

      // Write another record.
      {
        val key2: GenericData.Record = new GenericData.Record(keySchema)
        key2.put("innerkey", 2)
        val record: GenericData.Record = new GenericData.Record(writerSchema)
        record.put("key", key2)
        record.put("blah", "blah")
        record.put("value", "two")
        writer.append(record)
      }

      // Write a duplicate record with the same key field value.
      {
        val key2: GenericData.Record = new GenericData.Record(keySchema)
        key2.put("innerkey", 2)
        val record: GenericData.Record = new GenericData.Record(writerSchema)
        record.put("key", key2)
        record.put("blah", "blah")
        record.put("value", "deux")
        writer.append(record)
      }
    }

    return new Path(file.getPath())
  }

  /**
   * Writes test data to a Kiji table. The Kiji table should have a layout containing a column
   * named "family:column1" with a string schema.
   *
   * @param tableURI of the test Kiji table to populate.
   * @param values is a map from row entity ids to string values. The string value will be
   *   written as the latest cell in "family:column1" in the row with the associated entity id.
   */
  private def populateTestKijiTable(tableURI: KijiURI, values: Map[EntityId, String]) {
    // Open a table writer.
    val writer =
      doAndRelease(Kiji.Factory.open(tableURI)) { kiji =>
        doAndRelease(kiji.openTable(tableURI.getTable())) { table =>
          table.openTableWriter()
        }
      }

    try {
      // Write each value to the table.
      values.foreach { case(entityId, str) =>
        writer.put(
          entityId.toJavaEntityId(),
          "family",
          "column1",
          str
        )
      }
    } finally {
      writer.close()
    }
  }
}
