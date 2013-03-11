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

import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.schema.KijiClientTest;

public class TestAvroKVRecordKeyValueStore extends KijiClientTest {

  /** Writes an avro file of generic records with a 'key', 'blah', and 'value' field. */
  private Path writeGenericRecordAvroFile() throws IOException {
    // Open a writer.
    final File file = new File(getLocalTempDir(), "generic-kv.avro");
    final Schema writerSchema = Schema.createRecord("record", null, null, false);
    writerSchema.setFields(Lists.newArrayList(
        new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("blah", Schema.create(Schema.Type.STRING), null, null),
        new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));

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

  @Test
  public void testGenericAvroKVRecordKeyValueStore() throws Exception {
    // Only read the key and value fields (skip the 'blah' field).
    final Schema readerSchema = Schema.createRecord("record", null, null, false);
    readerSchema.setFields(Lists.newArrayList(
        new Schema.Field("key", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("value", Schema.create(Schema.Type.STRING), null, null)));

    // Open the store.
    final Path avroFilePath = writeGenericRecordAvroFile();
    final AvroKVRecordKeyValueStore<Integer, CharSequence> store = AvroKVRecordKeyValueStore
        .builder()
        .withConfiguration(getConf())
        .withInputPath(avroFilePath)
        .withReaderSchema(readerSchema)
        .build();
    final KeyValueStoreReader<Integer, CharSequence> reader = store.open();
    try {
      assertTrue(reader.containsKey(1));
      assertEquals("one", reader.get(1).toString());

      assertTrue(reader.containsKey(2));
      assertEquals("The first record in the file with key 2 should be mapped as the value.",
          "two", reader.get(2).toString());
    } finally {
      reader.close();
    }
  }

  @Test
  public void testAvroKVRKVSWithoutSchema() throws IOException, InterruptedException {
    // Open the store.
    final Path avroFilePath = writeGenericRecordAvroFile();
    final AvroKVRecordKeyValueStore<Integer, CharSequence> store = AvroKVRecordKeyValueStore
        .builder()
        .withConfiguration(getConf())
        .withInputPath(avroFilePath)
        .build();
    final KeyValueStoreReader<Integer, CharSequence> reader = store.open();
    try {
      assertTrue(reader.containsKey(1));
      assertEquals("one", reader.get(1).toString());

      assertTrue(reader.containsKey(2));
      assertEquals("The first record in the file with key 2 should be mapped as the value.",
          "two", reader.get(2).toString());
    } finally {
      reader.close();
    }
  }
}
