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

package org.kiji.modeling.impl

import scala.collection.JavaConverters.seqAsJavaListConverter

import java.io.File
import java.io.PrintWriter

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.kiji.express.EntityId
import org.kiji.express.KijiSuite
import org.kiji.modeling.KeyValueStore
import org.kiji.express.util.Resources._
import org.kiji.mapreduce.kvstore.lib.{AvroKVRecordKeyValueStore => JAvroKVRecordKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{AvroRecordKeyValueStore => JAvroRecordKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{KijiTableKeyValueStore => JKijiTableKeyValueStore}
import org.kiji.mapreduce.kvstore.lib.{TextFileKeyValueStore => JTextFileKeyValueStore}
import org.kiji.schema.EntityIdFactory
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

/**
 * Tests the functionality of [[org.kiji.modeling.KeyValueStore]] when backed by specific
 * key-value store implementations from KijiMR.
 */
@RunWith(classOf[JUnitRunner])
class KeyValueStoreImplSuite extends KijiSuite {
  import KeyValueStoreImplSuite._

  test("Using a KijiExpress KVStore backed by a KijiMR KijiTableKeyValueStore") {
    // Create a test Kiji instance containing a simple table.
    val simpleLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)
    val uri: KijiURI = doAndRelease(makeTestKijiTable(simpleLayout)) { table: KijiTable =>
      table.getURI
    }
    // Create some sample data to populate the table with, and load it into the table.
    val dataToLoad = Map[EntityId, String](EntityId("1") -> "value1",
        EntityId("2") -> "value2",
        EntityId("3") -> "value3")
    populateTestKijiTable(uri, dataToLoad)

    // Now create a KijiMR key-value store hooked to the Kiji table.
    val store: JKijiTableKeyValueStore[CharSequence] = JKijiTableKeyValueStore
        .builder()
        .withTable(uri)
        .withColumn("family", "column1")
        .withConfiguration(HBaseConfiguration.create())
        .build()

    // Now create a KijiExpress key-value store backed by the KijiMR key-value store.
    // Use the key-value store to access values and verify they are correct.
    doAndClose(KeyValueStore[CharSequence](store)) { kvstore =>
      assert("value1" === kvstore(EntityId("1")).toString)
      assert("value2" === kvstore(EntityId("2")).toString)
      assert("value3" === kvstore(EntityId("3")).toString)
    }
  }

  test("Using a KijiExpress KVStore backed by a KijiMR AvroRecordKeyValueStore") {
    val avroFilePath: Path = generateAvroKVRecordKeyValueStore()
    val conf = HBaseConfiguration.create()
    val store: JAvroRecordKeyValueStore[Int, GenericRecord] = JAvroRecordKeyValueStore
        .builder()
        .withConfiguration(conf)
        .withInputPath(avroFilePath)
        .withKeyFieldName("key")
        .build()

    // Wrap the Java key-value store in its corresponding Scala variety.
    doAndClose(KeyValueStore[Int, GenericRecord](store)) { kvstore =>
      assert("one" === kvstore(1).get("value").toString)
      assert("two" === kvstore(2).get("value").toString)

      assert("blah" === kvstore(1).get("blah").toString)
      assert("blah" === kvstore(2).get("blah").toString)

      assert(1 === kvstore(1).get("key"))
      assert(2 === kvstore(2).get("key"))
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
    doAndClose(KeyValueStore[Int, CharSequence](store)) { kvstore =>
      assert("one" === kvstore(1).toString)
      assert("two" === kvstore(2).toString)
    }
  }

  test("Using a KijiExpress KVStore backed by a KijiMR TextFileKeyValueStore") {
    // Get the Java key-value store.
    val csvFilePath: Path = generateTextFileKeyValueStore()
    val conf = HBaseConfiguration.create()
    val store: JTextFileKeyValueStore = JTextFileKeyValueStore
        .builder()
        .withConfiguration(conf)
        .withInputPath(csvFilePath)
        .withDelimiter(",")
        .build()

    doAndClose(KeyValueStore(store)) { kvstore =>
      assert("value1" === kvstore("key1"))
      assert("value2" === kvstore("key2"))
    }
  }
}

object KeyValueStoreImplSuite {
  /**
   * Writes an avro file of generic records with a 'key', 'blah', and 'value' field.
   *
   * @return the path to the file written.
   */
  private[kiji] def generateAvroKVRecordKeyValueStore(): Path = {
    // Build the schema associated with this key-value store.
    val writerSchema: Schema = {
      val schema: Schema = Schema.createRecord("record", null, null, false)
      val fields: Seq[Schema.Field] = Seq(
          new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
          new Schema.Field("blah", Schema.create(Schema.Type.STRING), null, null),
          new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null))
      schema.setFields(fields.asJava)
      schema
    }

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

    return new Path(file.getPath)
  }

  /**
   * Writes an avro file of generic records with a 'key', 'blah', and 'value' field, where the key
   * field is a record.
   *
   * @return the path to the file written.
   */
  private def generateAvroKVRecordKeyValueStoreWithRecordKey(): Path = {
    // Build the schema associated with this key-value store.
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

    return new Path(file.getPath)
  }

  /**
   * Writes a csv file with string key value pairs.
   *
   * @return the path to the file written.
   */
  private[kiji] def generateTextFileKeyValueStore(): Path = {
    val file: File = File.createTempFile("generic-csv", ".txt")
    doAndClose(new PrintWriter(file, "UTF-8")) { fileWriter =>
      fileWriter.println("key1,value1")
      fileWriter.println("key2,value2")
    }

    return new Path(file.getPath)
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
        doAndRelease(kiji.openTable(tableURI.getTable)) { table =>
          table.openTableWriter()
        }
      }

    // Layout to get the default reader schemas from.
    val layout = withKijiTable(tableURI, HBaseConfiguration.create()) { table: KijiTable =>
      table.getLayout
    }

    val eidFactory = EntityIdFactory.getFactory(layout)
    try {
      // Write each value to the table.
      values.foreach {
        case (entityId, str) =>
          writer.put(
            entityId.toJavaEntityId(eidFactory),
            "family",
            "column1",
            str)
      }
    } finally {
      writer.close()
    }
  }
}
