/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.mapreduce.input.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.input.impl.XMLInputFormat.XMLRecordReader;
import org.kiji.schema.KijiClientTest;

public class TestXMLInputFormat extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestXMLInputFormat.class);

  @Test
  public void testReadTwice() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader br = new BufferedReader(new StringReader("12345<user>"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        br,
        key,
        sb));
    assertEquals(5, key.get());
    assertEquals("<user>", sb.toString());
    assertFalse(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        br,
        new LongWritable(),
        new StringBuilder()));
  }

  @Test
  public void testNoRecord() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("There's no record in here."));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    Text record = new Text();
    bReader.mark(1000);
    assertFalse(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("", sb.toString());
    bReader.reset();
    assertFalse(reader.findRecordEnd(
        "</user>".toCharArray(),
        bReader,
        100L, // End offset
        100L, // Overrun allowance
        sb,
        record));
    assertEquals("There's no record in here.", sb.toString());
    assertEquals("", record.toString());
  }

  @Test
  public void testRegularRecord() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("<user></user>"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    Text record = new Text();
    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("<user>", sb.toString());
    assertTrue(reader.findRecordEnd(
        "</user>".toCharArray(),
        bReader,
        100L, // End offset
        100L, // Overrun allowance
        sb,
        record));
    assertEquals("<user></user>", record.toString());
  }

  @Test
  public void testRecordBeginsAfterSplit() throws java.io.IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("123456<user></user>"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    assertFalse(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        3L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("", sb.toString());
  }

  @Test
  public void testRecordStartCrossesSplit() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("<user></user>"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        3L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("<user>", sb.toString());
  }

  @Test
  public void testRecordEndsAfterSplit() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("<user></user>"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        7L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("<user>", sb.toString());
  }

  @Test
  public void testTooLongRecord() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("<user></user>"));
    StringBuilder sb = new StringBuilder();
    Text record = new Text();
    bReader.mark(1000);
    // Small overrun allowance will break before finding the end of the record.
    assertFalse(reader.findRecordEnd(
        "</user>".toCharArray(),
        bReader,
        8L, // End offset
        1L, // Overrun allowance
        sb,
        record));
    assertEquals("<user></us", sb.toString());
    assertEquals("", record.toString());

    reader = new XMLRecordReader();
    sb = new StringBuilder();
    bReader.reset();
    // Large overrun allowance will find the end of the record.
    assertTrue(reader.findRecordEnd(
        "</user>".toCharArray(),
        bReader,
        8L, // End offset
        10L, // Overrun allowance
        sb,
        record));
    assertEquals("<user></user>", sb.toString());
    assertEquals("<user></user>", record.toString());
  }

  @Test
  public void testCloseMatches() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("<use></use> <users></users>"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    Text record = new Text();
    bReader.mark(1000);
    assertFalse(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("", sb.toString());
    bReader.reset();
    assertFalse(reader.findRecordEnd(
        "</user>".toCharArray(),
        bReader,
        100L, // End offset
        100L, // Overrun allowance
        sb,
        record));
    assertEquals("<use></use> <users></users>", sb.toString());
    assertEquals("", record.toString());
  }

  @Test
  public void testCompleteRecord() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("1<user><name>Bob</name></user>"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    Text record = new Text();

    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        bReader,
        key,
        sb));

    assertTrue(reader.findRecordEnd(
        "</user>".toCharArray(),
        bReader,
        100L, // End offset
        100L, // Overrun allowance
        sb,
        record));
    assertEquals(1, key.get());
    assertEquals("<user><name>Bob</name></user>", record.toString());
  }

  @Test
  public void testWhitespace() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("<user id=\"1\">"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("<user ", sb.toString());

    reader = new XMLRecordReader();
    bReader = new BufferedReader(new StringReader("<user\nid=\"1\">"));
    key = new LongWritable();
    sb = new StringBuilder();
    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("<user\n", sb.toString());
  }

  @Test
  public void testTwoRecords() throws IOException {
    XMLRecordReader reader = new XMLRecordReader();
    BufferedReader bReader = new BufferedReader(new StringReader("<user>1</user><user>2</user>"));
    LongWritable key = new LongWritable();
    StringBuilder sb = new StringBuilder();
    Text record = new Text();

    // Find the first record.
    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        bReader,
        key,
        sb));
    assertEquals(0, key.get());
    assertEquals("<user>", sb.toString());
    assertTrue(reader.findRecordEnd(
        "</user>".toCharArray(),
        bReader,
        100L, // End offset
        100L, // Overrun allowance
        sb,
        record));
    assertEquals("<user>1</user>", record.toString());

    // Find the second record.
    sb = new StringBuilder();
    record = new Text();
    assertTrue(reader.findRecordStart(
        "<user".toCharArray(),
        0L, // Start offset
        100L, // End offset
        bReader,
        key,
        sb));
    assertEquals("<user>", sb.toString());
    assertTrue(reader.findRecordEnd(
        "</user>".toCharArray(),
        bReader,
        100L, // End offset
        100L, // Overrun allowance
        sb,
        record));
    assertEquals("<user>2</user>", record.toString());
  }

  @Test
  public void testSecondRecordOnSplitBoundary() throws IOException {

  }
}
