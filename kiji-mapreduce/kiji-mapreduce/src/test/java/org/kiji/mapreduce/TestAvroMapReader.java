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

public class TestAvroMapReader {
  @Test
  public void testMap() throws IOException {
    // Create a map.
    Map<CharSequence, Integer> originalMap = new TreeMap<CharSequence, Integer>(
        new AvroCharSequenceComparator<CharSequence>());
    originalMap.put("foo", 42);

    assertTrue(originalMap.containsKey(new Utf8("foo")));
    assertTrue(originalMap.containsKey("foo"));

    AvroMapReader<Integer> originalMapReader = AvroMapReader.create(originalMap);
    assertTrue(originalMapReader.containsKey(new Utf8("foo")));
    assertEquals(42, originalMapReader.get(new Utf8("foo")).intValue());
    assertTrue(originalMapReader.containsKey("foo"));
    assertEquals(42, originalMapReader.get("foo").intValue());

    // Serialize it to a stream.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    GenericDatumWriter<Map<CharSequence, Integer>> writer
        = new GenericDatumWriter<Map<CharSequence, Integer>>(
            Schema.createMap(Schema.create(Schema.Type.INT)));
    writer.write(originalMap, EncoderFactory.get().directBinaryEncoder(outputStream, null));

    // Read from serialized stream.
    ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
    GenericDatumReader<Map<CharSequence, Integer>> reader
        = new GenericDatumReader<Map<CharSequence, Integer>>(
            Schema.createMap(Schema.create(Schema.Type.INT)));

    // Deserialize the map.
    Map<CharSequence, Integer> deserializedMap
        = reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));

    // Both of these *should* work, but Avro is broken, so the check for a String key "foo" fails.
    assertTrue(deserializedMap.containsKey(new Utf8("foo")));
    assertFalse("Avro has been fixed! AvroMapReader is no longer necessary.",
        deserializedMap.containsKey("foo"));

    // Use the reader.  It should work just fine with Strings or Utf8's.
    AvroMapReader<Integer> mapReader = AvroMapReader.create(deserializedMap);
    assertTrue(mapReader.containsKey(new Utf8("foo")));
    assertEquals(42, mapReader.get(new Utf8("foo")).intValue());
    assertTrue(mapReader.containsKey("foo"));
    assertEquals(42, mapReader.get("foo").intValue());
  }
}
