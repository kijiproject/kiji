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

package org.kiji.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.hadoop.util.AvroCharSequenceComparator;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import org.kiji.mapreduce.avro.AvroMapReader;

public class TestAvroMapReader {
  @Test
  public void testMap() throws IOException {
    // Create a map.
    final Map<CharSequence, Integer> originalMap = new TreeMap<CharSequence, Integer>(
        new AvroCharSequenceComparator<CharSequence>());
    originalMap.put("foo", 42);

    assertTrue(originalMap.containsKey(new Utf8("foo")));
    assertTrue(originalMap.containsKey("foo"));

    final AvroMapReader<Integer> originalMapReader = AvroMapReader.create(originalMap);
    assertTrue(originalMapReader.containsKey(new Utf8("foo")));
    assertEquals(42, originalMapReader.get(new Utf8("foo")).intValue());
    assertTrue(originalMapReader.containsKey("foo"));
    assertEquals(42, originalMapReader.get("foo").intValue());

    // Serialize it to a stream.
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final GenericDatumWriter<Map<CharSequence, Integer>> writer
        = new GenericDatumWriter<Map<CharSequence, Integer>>(
            Schema.createMap(Schema.create(Schema.Type.INT)));
    writer.write(originalMap, EncoderFactory.get().directBinaryEncoder(outputStream, null));

    // Read from serialized stream.
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    final GenericDatumReader<Map<CharSequence, Integer>> reader
        = new GenericDatumReader<Map<CharSequence, Integer>>(
            Schema.createMap(Schema.create(Schema.Type.INT)));

    // Deserialize the map.
    final Map<CharSequence, Integer> deserializedMap
        = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));

    // Both of these *should* work, but Avro is broken, so the check for a String key "foo" fails.
    assertTrue(deserializedMap.containsKey(new Utf8("foo")));
    assertFalse("Avro has been fixed! AvroMapReader is no longer necessary.",
        deserializedMap.containsKey("foo"));

    // Use the reader.  It should work just fine with Strings or Utf8's.
    final AvroMapReader<Integer> mapReader = AvroMapReader.create(deserializedMap);
    assertTrue(mapReader.containsKey(new Utf8("foo")));
    assertEquals(42, mapReader.get(new Utf8("foo")).intValue());
    assertTrue(mapReader.containsKey("foo"));
    assertEquals(42, mapReader.get("foo").intValue());
  }

  @Test
  public void testReload() throws IOException {
    // Create an Avro "map" using serialization/deserialization.
    final Map<CharSequence, Integer> originalMap = new TreeMap<CharSequence, Integer>(
        new AvroCharSequenceComparator<CharSequence>());
    originalMap.put("to be deleted", 42);

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    final GenericDatumWriter<Map<CharSequence, Integer>> writer
        = new GenericDatumWriter<Map<CharSequence, Integer>>(
        Schema.createMap(Schema.create(Schema.Type.INT)));
    writer.write(originalMap, EncoderFactory.get().directBinaryEncoder(outputStream, null));
    final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    final GenericDatumReader<Map<CharSequence, Integer>> reader
        = new GenericDatumReader<Map<CharSequence, Integer>>(
        Schema.createMap(Schema.create(Schema.Type.INT)));
    final Map<CharSequence, Integer> deserializedMap
        = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));

    // Create a reader for it.
    final AvroMapReader<Integer> mapReader = AvroMapReader.create(deserializedMap);

    // Tests before modifying the original map.
    assertEquals(42, mapReader.get(new Utf8("to be deleted")).intValue());
    assertFalse(mapReader.containsKey(new Utf8("to be added")));

    // Tests after modifying the avro map but before calling reload.
    // The reader shouldn't yet reflect the changes to the 'wrapped' map.
    deserializedMap.put("to be added", 23);
    deserializedMap.remove(new Utf8("to be deleted"));
    assertTrue(deserializedMap.containsKey("to be added"));
    assertEquals(23, deserializedMap.get("to be added").intValue());
    assertFalse(deserializedMap.containsKey("to be deleted"));
    assertEquals(42, mapReader.get(new Utf8("to be deleted")).intValue());
    assertFalse(mapReader.containsKey(new Utf8("to be added")));

    // Tests after calling reload.
    mapReader.reload();
    assertFalse(mapReader.containsKey(new Utf8("to be deleted")));
    assertEquals(23, mapReader.get(new Utf8("to be added")).intValue());
  }
}
