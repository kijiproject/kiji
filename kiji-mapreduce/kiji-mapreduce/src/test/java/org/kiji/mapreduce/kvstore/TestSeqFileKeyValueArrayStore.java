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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.kiji.mapreduce.KeyValueStoreReader;

/** Test that the SeqFileKeyValueArrayStore implementation works. */
public class TestSeqFileKeyValueArrayStore {

  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  /**
   * Write a set of (key, val) pairs to a SequenceFile.
   *
   * @return the Path object that represents the file.
   * @throws IOException if there's an error using the file system.
   */
  private Path writeSeqFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = new Path(mTempDir.getRoot() + "/foo.seq");
    SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, p,
        Text.class, IntWritable.class);
    try {
      writer.append(new Text("one"), new IntWritable(1));
      writer.append(new Text("two"), new IntWritable(2));
      writer.append(new Text("two"), new IntWritable(3));
      writer.append(new Text("two"), new IntWritable(42));
    } finally {
      writer.close();
    }

    return p;
  }

  @Test
  public void testSeqFileKVStore() throws IOException, InterruptedException {
    Path p = writeSeqFile();
    SeqFileKeyValueArrayStore<Text, IntWritable> store =
        new SeqFileKeyValueArrayStore<Text, IntWritable>(
        new SeqFileKeyValueArrayStore.Options().withInputPath(p));
    KeyValueStoreReader<Text, List<IntWritable>> reader = store.open();

    assertTrue(reader.containsKey(new Text("one")));
    assertEquals(1, reader.get(new Text("one")).size());
    assertEquals(new IntWritable(1), reader.get(new Text("one")).get(0));

    // This uses the earlier definition in the file, not the later one.
    assertTrue(reader.containsKey(new Text("two")));
    assertEquals(3, reader.get(new Text("two")).size());
    assertEquals(new IntWritable(2), reader.get(new Text("two")).get(0));
    assertEquals(new IntWritable(3), reader.get(new Text("two")).get(1));
    assertEquals(new IntWritable(42), reader.get(new Text("two")).get(2));

    reader.close();
  }

  @Test
  public void testSeqFileKVStoreCapped() throws IOException, InterruptedException {
    Path p = writeSeqFile();
    SeqFileKeyValueArrayStore<Text, IntWritable> store =
        new SeqFileKeyValueArrayStore<Text, IntWritable>(
        new SeqFileKeyValueArrayStore.Options().withInputPath(p).withMaxValues(2));
    KeyValueStoreReader<Text, List<IntWritable>> reader = store.open();

    assertTrue(reader.containsKey(new Text("one")));
    assertEquals(1, reader.get(new Text("one")).size());
    assertEquals(new IntWritable(1), reader.get(new Text("one")).get(0));

    // This uses the earlier definition in the file, not the later one.
    assertTrue(reader.containsKey(new Text("two")));
    assertEquals(2, reader.get(new Text("two")).size());
    assertEquals(new IntWritable(2), reader.get(new Text("two")).get(0));
    assertEquals(new IntWritable(3), reader.get(new Text("two")).get(1));

    reader.close();
  }
}
