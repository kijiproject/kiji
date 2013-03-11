/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mapreduce.kvstore.lib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.avro.Node;

public class TestAvroRecordKeyValueStore extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestAvroRecordKeyValueStore.class);

  /** The path to an existing test avro file of specific records (Nodes). */
  public static final String NODE_AVRO_FILE = "org/kiji/mapreduce/kvstore/simple.avro";

  @Test
  public void testSpecificAvroRecordKeyValueStore() throws IOException, InterruptedException {
    final Path avroFilePath =
        new Path(getClass().getClassLoader().getResource(NODE_AVRO_FILE).toString());

    final AvroRecordKeyValueStore<CharSequence, Node> store = AvroRecordKeyValueStore
        .builder()
        .withConfiguration(getConf())
        .withInputPath(avroFilePath)
        .withReaderSchema(Node.SCHEMA$)
        .withKeyFieldName("label")
        .build();
    final KeyValueStoreReader<CharSequence, Node> reader = store.open();
    try {
      assertTrue(reader.containsKey("foo"));
      assertEquals("foo", reader.get("foo").getLabel().toString());

      assertTrue(reader.containsKey("hello"));
      assertEquals("hello", reader.get("hello").getLabel().toString());

      assertFalse(reader.containsKey("does-not-exist"));
    } finally {
      reader.close();
    }
  }

  /**
   * Returns a schema to use for generic records.
   *
   * @return a Schema to use for files containing generic records.
   */
  private Schema getGenericSchema() {
    Schema schema = Schema.createRecord("record", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("blah", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));
    return schema;
  }

  /** Writes an avro file of generic records with a 'key', 'blah', and 'value' field. */
  private Path writeGenericRecordAvroFile() throws IOException {
    // Open a writer.
    final File file = new File(getLocalTempDir(), "generic.avro");
    final Schema writerSchema = getGenericSchema();
    final DataFileWriter<GenericRecord> fileWriter =
        new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(writerSchema))
            .create(writerSchema, file);
    try {
      // Write a record.
      GenericData.Record record = new GenericData.Record(writerSchema);
      record.put("key", 1);
      record.put("blah", "blah");
      record.put("value", "one");
      fileWriter.append(record);

      // Write another record.
      record = new GenericData.Record(writerSchema);
      record.put("key", 2);
      record.put("blah", "blah");
      record.put("value", "two");
      fileWriter.append(record);

      // Write a duplicate record with the same key field value.
      record = new GenericData.Record(writerSchema);
      record.put("key", 2);
      record.put("blah", "blah");
      record.put("value", "deux");
      fileWriter.append(record);
    } finally {
      // Close it and return the path.
      fileWriter.close();
    }
    return new Path(file.getPath());
  }

  /** Writes an Avro file containing additional keys and values. */
  private Path writeSecondAvroFile() throws IOException {
    // Open a writer.
    final File file = new File(getLocalTempDir(), "generic2.avro");
    final Schema writerSchema = getGenericSchema();
    final DataFileWriter<GenericRecord> fileWriter =
        new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(writerSchema))
            .create(writerSchema, file);
    try {
      // Write a record.
      GenericData.Record record = new GenericData.Record(writerSchema);
      record.put("key", 3);
      record.put("blah", "blergh");
      record.put("value", "three");
      fileWriter.append(record);

      // Write another record that shadows a record in the first file.
      record = new GenericData.Record(writerSchema);
      record.put("key", 2);
      record.put("blah", "blah");
      record.put("value", "TWOTWO");
      fileWriter.append(record);
    } finally {
      // Close it and return the path.
      fileWriter.close();
    }
    return new Path(file.getPath());
  }

  @Test
  public void testGenericAvroRecordKeyValueStore() throws IOException, InterruptedException {
    // Only read the key and value fields (skip the 'blah' field).
    final Schema readerSchema = Schema.createRecord("record", null, null, false);
    readerSchema.setFields(Lists.newArrayList(
        new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));

    // Open the store.
    final Path avroFilePath = writeGenericRecordAvroFile();
    final AvroRecordKeyValueStore<Integer, GenericRecord> store = AvroRecordKeyValueStore
        .builder()
        .withConfiguration(getConf())
        .withInputPath(avroFilePath)
        .withReaderSchema(readerSchema)
        .withKeyFieldName("key")
        .build();
    final KeyValueStoreReader<Integer, GenericRecord> reader = store.open();
    try {
      assertTrue(reader.containsKey(1));
      assertEquals("one", reader.get(1).get("value").toString());

      assertTrue(reader.containsKey(2));
      assertEquals("The first record in the file with key 2 should be mapped as the value.",
          "two", reader.get(2).get("value").toString());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testAvroRecordKVStoreWithoutSchema() throws IOException, InterruptedException {
    // Open the store.
    final Path avroFilePath = writeGenericRecordAvroFile();
    final AvroRecordKeyValueStore<Integer, GenericRecord> store = AvroRecordKeyValueStore
        .builder()
        .withConfiguration(getConf())
        .withInputPath(avroFilePath)
        .withKeyFieldName("key")
        .build();
    final KeyValueStoreReader<Integer, GenericRecord> reader = store.open();
    try {
      assertTrue(reader.containsKey(1));
      assertEquals("one", reader.get(1).get("value").toString());

      assertTrue(reader.containsKey(2));
      assertEquals("The first record in the file with key 2 should be mapped as the value.",
          "two", reader.get(2).get("value").toString());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testMultipleInputFiles() throws IOException, InterruptedException {
    final Path avroFilePath = writeGenericRecordAvroFile();
    final Path secondPath = writeSecondAvroFile();
    final AvroRecordKeyValueStore<Integer, GenericRecord> store = AvroRecordKeyValueStore
        .builder()
        .withConfiguration(getConf())
        .withInputPath(avroFilePath)
        .withInputPath(secondPath)
        .withKeyFieldName("key")
        .build();
    final KeyValueStoreReader<Integer, GenericRecord> reader = store.open();
    try {
      assertTrue(reader.containsKey(1));
      assertEquals("one", reader.get(1).get("value").toString());

      assertTrue(reader.containsKey(3));
      assertEquals("three", reader.get(3).get("value").toString());

      assertTrue(reader.containsKey(2));
      assertEquals("The first record in the first file with key 2 should be mapped as the value.",
          "two", reader.get(2).get("value").toString());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testExpandInputDir() throws IOException, InterruptedException {
    // Test that we can specify the directory name and that will be sufficient,
    // we don't need to name both input files.
    writeGenericRecordAvroFile();
    writeSecondAvroFile();
    final Path temporaryPath = new Path("file:" + getLocalTempDir());
    final AvroRecordKeyValueStore<Integer, GenericRecord> store = AvroRecordKeyValueStore
        .builder()
        .withConfiguration(getConf())
        .withInputPath(temporaryPath)
        .withKeyFieldName("key")
        .build();
    final KeyValueStoreReader<Integer, GenericRecord> reader = store.open();
    try {
      // Check that keys from both files map to their respective values.
      // Since we're not specifying ordering on the files within the directory
      // when we add them to the store, we can't make an assertion about the
      // correct value of reader.get(2); this could be "two" or "TWOTWO" depending
      // on the filesystem.
      assertTrue(reader.containsKey(1));
      assertEquals("one", reader.get(1).get("value").toString());

      assertTrue(reader.containsKey(3));
      assertEquals("three", reader.get(3).get("value").toString());

      assertTrue(reader.containsKey(2));
    } finally {
      reader.close();
    }
  }

  @Test
  public void testExpandInputGlob() throws IOException, InterruptedException {
    // Test that we can specify the a glob of files and that these will expand
    // to all the input file names.
    writeGenericRecordAvroFile();
    writeSecondAvroFile();
    final Path glob = new Path("file:" + getLocalTempDir(), "*.avro");
    LOG.info("Using input glob: {}", glob);
    final AvroRecordKeyValueStore<Integer, GenericRecord> store = AvroRecordKeyValueStore
        .builder()
        .withConfiguration(getConf())
        .withInputPath(glob)
        .withKeyFieldName("key")
        .build();
    final KeyValueStoreReader<Integer, GenericRecord> reader = store.open();
    try {
      // Check that keys from both files map to their respective values.
      // Since we're not specifying ordering on the files within the glob
      // when we add them to the store, we can't make an assertion about the
      // correct value of reader.get(2); this could be "two" or "TWOTWO" depending
      // on the filesystem.

      assertTrue(reader.containsKey(1));
      assertEquals("one", reader.get(1).get("value").toString());

      assertTrue(reader.containsKey(3));
      assertEquals("three", reader.get(3).get("value").toString());

      assertTrue(reader.containsKey(2));
    } finally {
      reader.close();
    }
  }
}
