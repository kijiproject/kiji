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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.schema.KijiClientTest;

/** Test that the TextFileKeyValueStore implementation works. */
public class TestTextFileKeyValueStore extends KijiClientTest {

  /**
   * Write a set of lines to a text file.
   *
   * @return the Path object that represents the file.
   * @throws IOException if there's an error using the file system.
   */
  private Path writeBasicTextFile() throws IOException {
    final File file = new File(getLocalTempDir(), "foo.txt");
    final BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
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
    return new Path("file:" + file);
  }

  /**
   * Write a set of lines to a text file; do not terminate the file with a newline.
   *
   * @return the Path object that represents the file.
   * @throws IOException if there's an error using the file system.
   */
  private Path writeFileWithoutNewline() throws IOException {
    final File file = new File(getLocalTempDir(), "bar.txt");
    final BufferedWriter writer =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
    try {
      writer.append("a\t1\n");
      writer.append("\n"); // Add an empty line.
      writer.append("b\t2"); // No final newline.
    } finally {
      writer.close();
    }
    return new Path("file:" + file);
  }

  @Test
  public void testBasicTextFile() throws IOException, InterruptedException {
    final Path path = writeBasicTextFile();
    final TextFileKeyValueStore store = TextFileKeyValueStore.builder().withInputPath(path).build();
    KeyValueStoreReader<String, String> reader = store.open();
    try {
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
    } finally {
      reader.close();
    }
  }

  @Test
  public void testFileNoEndingNewline() throws IOException, InterruptedException {
    // Tests that a file that does not end with a newline still works just fine.
    final Path path = writeFileWithoutNewline();
    final TextFileKeyValueStore store = TextFileKeyValueStore.builder().withInputPath(path).build();
    final KeyValueStoreReader<String, String> reader = store.open();
    try {
      assertTrue(reader.containsKey("a"));
      assertEquals("1", reader.get("a"));

      // The blank line maps "" -> null.
      assertTrue(reader.containsKey(""));
      assertNull(reader.get(""));

      assertTrue(reader.containsKey("b"));
      assertEquals("2", reader.get("b"));
    } finally {
      reader.close();
    }
  }
}
