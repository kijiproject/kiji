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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.kiji.mapreduce.KeyValueStoreReader;

/** Test that the TextFileKeyValueStore implementation works. */
public class TestTextFileKeyValueStore {

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
      writer.append("c\t3\n");
      writer.append("d\n");
      writer.append("\t4\n");
      writer.append("e\t\n");
      writer.append("c\tX\n"); // should be ignored by c->3 mapping earlier.
    } finally {
      writer.close();
    }

    return p;
  }

  /**
   * Write a set of lines to a text file; do not terminate the file with a newline.
   *
   * @return the Path object that represents the file.
   * @throws IOException if there's an error using the file system.
   */
  private Path writeFileWithoutNewline() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);
    Path p = new Path(mTempDir.getRoot() + "/bar.txt");

    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(p)));
    try {
      writer.append("a\t1\n");
      writer.append("\n"); // Add an empty line.
      writer.append("b\t2"); // No final newline.
    } finally {
      writer.close();
    }

    return p;
  }

  @Test
  public void testBasicTextFile() throws IOException, InterruptedException {
    Path p = writeBasicTextFile();
    TextFileKeyValueStore store = new TextFileKeyValueStore(
        new TextFileKeyValueStore.Options().withInputPath(p));
    KeyValueStoreReader<String, String> reader = store.open();

    assertTrue(reader.containsKey("a"));
    assertEquals("1", reader.get("a"));

    assertTrue(reader.containsKey("b"));
    assertEquals("2", reader.get("b"));

    assertTrue(reader.containsKey("c"));
    assertEquals("3", reader.get("c"));

    // 'd' had no following delimiter, so it maps to null.
    assertTrue(reader.containsKey("d"));
    assertNull(reader.get("d"));

    // One row started with a delim, so it is empty str -> 4.
    assertTrue(reader.containsKey(""));
    assertEquals("4", reader.get(""));

    // 'e' had a following delimiter then a newline, so it maps to empty string.
    assertTrue(reader.containsKey("e"));
    assertEquals("", reader.get("e"));

    reader.close();
  }

  @Test
  public void testFileNoEndingNewline() throws IOException, InterruptedException {
    // Tests that a file that does not end with a newline still works just fine.
    Path p = writeFileWithoutNewline();
    TextFileKeyValueStore store = new TextFileKeyValueStore(
        new TextFileKeyValueStore.Options().withInputPath(p));
    KeyValueStoreReader<String, String> reader = store.open();

    assertTrue(reader.containsKey("a"));
    assertEquals("1", reader.get("a"));

    // The blank line maps "" -> null.
    assertTrue(reader.containsKey(""));
    assertNull(reader.get(""));

    assertTrue(reader.containsKey("b"));
    assertEquals("2", reader.get("b"));
  }
}
