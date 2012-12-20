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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.kiji.mapreduce.KeyValueStoreReader;

/** Test that the TextFileKeyValueArrayStore implementation works. */
public class TestTextFileKeyValueArrayStore {

  // Disable checkstyle for this variable.  It must be public to work with JUnit @Rule.
  // CSOFF: VisibilityModifierCheck
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();
  // CSON: VisibilityModifierCheck

  /**
   * Write a set of lines to a text file.
   *
   * @return the Path object that represents the file.
   * @throws IOException if there's an error using the file system.
   */
  private Path writeBasicTextFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = new Path(mTempDir.getRoot() + "/foo.txt");

    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(p)));
    try {
      writer.append("a\t1\n");
      writer.append("b\t2\n");
      writer.append("b\t3\n");
      writer.append("b\tX\n"); // should be ignored by c->3 mapping earlier.
    } finally {
      writer.close();
    }

    return p;
  }

  @Test
  public void testBasicTextFile() throws IOException, InterruptedException {
    Path p = writeBasicTextFile();
    TextFileKeyValueArrayStore store = new TextFileKeyValueArrayStore(
        new TextFileKeyValueArrayStore.Options().withInputPath(p));
    KeyValueStoreReader<String, List<String>> reader = store.open();

    assertTrue(reader.containsKey("a"));
    assertEquals(1, reader.get("a").size());
    assertEquals("1", reader.get("a").get(0));

    assertTrue(reader.containsKey("b"));
    assertEquals(3, reader.get("b").size());
    assertEquals("2", reader.get("b").get(0));
    assertEquals("3", reader.get("b").get(1));
    assertEquals("X", reader.get("b").get(2));

    reader.close();
  }

  @Test
  public void testBasicTextFileCapped() throws IOException, InterruptedException {
    Path p = writeBasicTextFile();
    TextFileKeyValueArrayStore store = new TextFileKeyValueArrayStore(
        new TextFileKeyValueArrayStore.Options().withInputPath(p).withMaxValues(2));
    KeyValueStoreReader<String, List<String>> reader = store.open();

    assertTrue(reader.containsKey("a"));
    assertEquals(1, reader.get("a").size());
    assertEquals("1", reader.get("a").get(0));

    assertTrue(reader.containsKey("b"));
    assertEquals(2, reader.get("b").size());
    assertEquals("2", reader.get("b").get(0));
    assertEquals("3", reader.get("b").get(1));

    reader.close();
  }
}
