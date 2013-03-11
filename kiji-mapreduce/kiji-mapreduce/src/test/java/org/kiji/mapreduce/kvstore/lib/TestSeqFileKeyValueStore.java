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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.schema.KijiClientTest;

/** Test that the SeqFileKeyValueStore implementation works. */
public class TestSeqFileKeyValueStore extends KijiClientTest {

  /**
   * Write a set of (key, val) pairs to a SequenceFile.
   *
   * @return the Path object that represents the file.
   * @throws IOException if there's an error using the file system.
   */
  private Path writeSeqFile() throws IOException {
    final Path path = new Path("file:" + getLocalTempDir(), "foo.seq");
    final SequenceFile.Writer writer = SequenceFile.createWriter(
        getConf(),
        Writer.file(path),
        Writer.keyClass(Text.class),
        Writer.valueClass(IntWritable.class));
    try {
      writer.append(new Text("one"), new IntWritable(1));
      writer.append(new Text("two"), new IntWritable(2));
      writer.append(new Text("three"), new IntWritable(3));

      // Redundant key with a different value. This should have no effect on the KVStore.
      writer.append(new Text("two"), new IntWritable(42));
    } finally {
      writer.close();
    }

    return path;
  }

  @Test
  public void testSeqFileKVStore() throws Exception {
    final Path path = writeSeqFile();
    final KeyValueStore<Text, IntWritable> store = SeqFileKeyValueStore.builder()
        .withInputPath(path)
        .build();
    final KeyValueStoreReader<Text, IntWritable> reader = store.open();
    try {
      assertTrue(reader.containsKey(new Text("one")));
      assertEquals(new IntWritable(1), reader.get(new Text("one")));

      // This uses the earlier definition in the file, not the later one.
      assertTrue(reader.containsKey(new Text("two")));
      assertEquals(new IntWritable(2), reader.get(new Text("two")));

      assertTrue(reader.containsKey(new Text("three")));
      assertEquals(new IntWritable(3), reader.get(new Text("three")));
    } finally {
      reader.close();
    }
  }
}
