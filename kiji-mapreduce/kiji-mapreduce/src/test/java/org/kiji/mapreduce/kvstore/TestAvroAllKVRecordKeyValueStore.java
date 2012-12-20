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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.kiji.mapreduce.KeyValueStoreReader;

public class TestAvroAllKVRecordKeyValueStore {
  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  /** Writes an avro file of generic records with a 'key', 'blah', and 'value' field. */
  private Path writeGenericRecordAvroFile() throws IOException {
    // Open a writer.
    File file = new File(mTempDir.getRoot(), "generic-kv.avro");
    Schema writerSchema = Schema.createRecord("record", null, null, false);
    writerSchema.setFields(
        Arrays.asList(
            new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("blah", Schema.create(Schema.Type.STRING), null, null),
            new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));
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

  @Test
  public void testGenericAvroKVRecordKeyValueStore() throws IOException, InterruptedException {
    // Only read the key and value fields (skip the 'blah' field).
    Schema readerSchema = Schema.createRecord("record", null, null, false);
    readerSchema.setFields(
        Arrays.asList(
            new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
            new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));

    // Open the store.
    Path avroFilePath = writeGenericRecordAvroFile();
    AvroKVRecordKeyValueArrayStore<Integer, CharSequence> store
        = new AvroKVRecordKeyValueArrayStore<Integer, CharSequence>(
            new AvroKVRecordKeyValueArrayStore.Options()
            .withConfiguration(new Configuration())
            .withInputPath(avroFilePath)
            .withReaderSchema(readerSchema));
    KeyValueStoreReader<Integer, List<CharSequence>> reader = store.open();

    assertTrue(reader.containsKey(1));
    List<CharSequence> values = reader.get(1);
    assertEquals(1, values.size());
    assertEquals("one", values.get(0).toString());
    assertTrue(reader.containsKey(2));
    values = reader.get(2);
    assertEquals(2, values.size());
    assertEquals("two", values.get(0).toString());
    assertEquals("deux", values.get(1).toString());
  }
}
