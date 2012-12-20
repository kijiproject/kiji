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

package org.kiji.mapreduce.kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KeyValueStoreReader;
import org.kiji.schema.avro.Node;

public class TestAvroRecordKeyValueStore {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestAvroRecordKeyValueStore.class.getName());

  /** The path to an existing test avro file of specific records (Nodes). */
  public static final String NODE_AVRO_FILE = "org/kiji/mapreduce/kvstore/simple.avro";

  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  @Test
  public void testSpecificAvroRecordKeyValueStore() throws IOException, InterruptedException {
    Path avroFilePath
        = new Path(getClass().getClassLoader().getResource(NODE_AVRO_FILE).toString());
    AvroRecordKeyValueStore<CharSequence, Node> store
        = new AvroRecordKeyValueStore<CharSequence, Node>(
            new AvroRecordKeyValueStore.Options()
            .withConfiguration(new Configuration())
            .withInputPath(avroFilePath)
            .withReaderSchema(Node.SCHEMA$)
            .withKeyFieldName("label"));
    KeyValueStoreReader<CharSequence, Node> reader = store.open();

    assertTrue(reader.containsKey("foo"));
    assertEquals("foo", reader.get("foo").getLabel().toString());

    assertTrue(reader.containsKey("hello"));
    assertEquals("hello", reader.get("hello").getLabel().toString());

    assertFalse(reader.containsKey("does-not-exist"));
  }

  /**
   * Returns a schema to use for generic records.
   *
   * @return a Schema to use for files containing generic records.
   */
  private Schema getGenericSchema() {
    Schema schema = Schema.createRecord("record", null, null, false);
    schema.setFields(
        Arrays.asList(
            new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("blah", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));
    return schema;
  }

  /** Writes an avro file of generic records with a 'key', 'blah', and 'value' field. */
  private Path writeGenericRecordAvroFile() throws IOException {
    // Open a writer.
    File file = new File(mTempDir.getRoot(), "generic.avro");
    Schema writerSchema = getGenericSchema();
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(
        new GenericDatumWriter<GenericRecord>(writerSchema)).create(writerSchema, file);

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

    // Close it and return the path.
    fileWriter.close();
    return new Path(file.getPath());
  }

  /** Writes an Avro file containing additional keys and values. */
  private Path writeSecondAvroFile() throws IOException {
    // Open a writer.
    File file = new File(mTempDir.getRoot(), "generic2.avro");
    Schema writerSchema = getGenericSchema();
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(
        new GenericDatumWriter<GenericRecord>(writerSchema)).create(writerSchema, file);

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

    // Close it and return the path.
    fileWriter.close();
    return new Path(file.getPath());
  }

  @Test
  public void testGenericAvroRecordKeyValueStore() throws IOException, InterruptedException {
    // Only read the key and value fields (skip the 'blah' field).
    Schema readerSchema = Schema.createRecord("record", null, null, false);
    readerSchema.setFields(
        Arrays.asList(
            new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));

    // Open the store.
    Path avroFilePath = writeGenericRecordAvroFile();
    AvroRecordKeyValueStore<Integer, GenericRecord> store
        = new AvroRecordKeyValueStore<Integer, GenericRecord>(
            new AvroRecordKeyValueStore.Options()
            .withConfiguration(new Configuration())
            .withInputPath(avroFilePath)
            .withReaderSchema(readerSchema)
            .withKeyFieldName("key"));
    KeyValueStoreReader<Integer, GenericRecord> reader = store.open();

    assertTrue(reader.containsKey(1));
    assertEquals("one", reader.get(1).get("value").toString());

    assertTrue(reader.containsKey(2));
    assertEquals("The first record in the file with key 2 should be mapped as the value.",
        "two", reader.get(2).get("value").toString());
  }

  @Test
  public void testAvroRecordKVStoreWithoutSchema() throws IOException, InterruptedException {
    // Open the store.
    Path avroFilePath = writeGenericRecordAvroFile();
    AvroRecordKeyValueStore<Integer, GenericRecord> store
        = new AvroRecordKeyValueStore<Integer, GenericRecord>(
            new AvroRecordKeyValueStore.Options()
            .withConfiguration(new Configuration())
            .withInputPath(avroFilePath)
            .withKeyFieldName("key"));
    KeyValueStoreReader<Integer, GenericRecord> reader = store.open();

    assertTrue(reader.containsKey(1));
    assertEquals("one", reader.get(1).get("value").toString());

    assertTrue(reader.containsKey(2));
    assertEquals("The first record in the file with key 2 should be mapped as the value.",
        "two", reader.get(2).get("value").toString());
  }

  @Test
  public void testMultipleInputFiles() throws IOException, InterruptedException {
    Path avroFilePath = writeGenericRecordAvroFile();
    Path secondPath = writeSecondAvroFile();
    AvroRecordKeyValueStore<Integer, GenericRecord> store
        = new AvroRecordKeyValueStore<Integer, GenericRecord>(
            new AvroRecordKeyValueStore.Options()
            .withConfiguration(new Configuration())
            .withInputPath(avroFilePath)
            .withInputPath(secondPath)
            .withKeyFieldName("key"));
    KeyValueStoreReader<Integer, GenericRecord> reader = store.open();

    assertTrue(reader.containsKey(1));
    assertEquals("one", reader.get(1).get("value").toString());

    assertTrue(reader.containsKey(3));
    assertEquals("three", reader.get(3).get("value").toString());

    assertTrue(reader.containsKey(2));
    assertEquals("The first record in the first file with key 2 should be mapped as the value.",
        "two", reader.get(2).get("value").toString());
  }

  @Test
  public void testExpandInputDir() throws IOException, InterruptedException {
    // Test that we can specify the directory name and that will be sufficient,
    // we don't need to name both input files.
    writeGenericRecordAvroFile();
    writeSecondAvroFile();
    Path temporaryPath = LocalFileSystem.get(new Configuration()).makeQualified(
        new Path(mTempDir.getRoot().getAbsolutePath()));
    AvroRecordKeyValueStore<Integer, GenericRecord> store
        = new AvroRecordKeyValueStore<Integer, GenericRecord>(
            new AvroRecordKeyValueStore.Options()
            .withConfiguration(new Configuration())
            .withInputPath(temporaryPath)
            .withKeyFieldName("key"));
    KeyValueStoreReader<Integer, GenericRecord> reader = store.open();

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
  }

  @Test
  public void testExpandInputGlob() throws IOException, InterruptedException {
    // Test that we can specify the a glob of files and that these will expand
    // to all the input file names.
    writeGenericRecordAvroFile();
    writeSecondAvroFile();
    Path temporaryPath = LocalFileSystem.get(new Configuration()).makeQualified(
        new Path(mTempDir.getRoot().getAbsolutePath()));
    temporaryPath = new Path(temporaryPath.toString() + "/*.avro");
    LOG.info("Using input glob: " + temporaryPath);
    AvroRecordKeyValueStore<Integer, GenericRecord> store
        = new AvroRecordKeyValueStore<Integer, GenericRecord>(
            new AvroRecordKeyValueStore.Options()
            .withConfiguration(new Configuration())
            .withInputPath(temporaryPath)
            .withKeyFieldName("key"));
    KeyValueStoreReader<Integer, GenericRecord> reader = store.open();

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
  }
}
