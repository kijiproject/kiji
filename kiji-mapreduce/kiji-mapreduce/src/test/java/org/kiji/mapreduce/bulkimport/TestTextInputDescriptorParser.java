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

package org.kiji.mapreduce.bulkimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.junit.Test;

import org.kiji.mapreduce.bulkimport.TextInputDescriptorParser.QualifierInfo;

/** Unit tests. */
public class TestTextInputDescriptorParser {
  private static final String ONE_LAYOUT =
      "qualifier:MyName\n"
      + "schema:\"string\"\n";

  private static final String BAD_SCHEMA_LAYOUT =
      "qualifier:MyName\n"
      + "schema:\"blort\"\n";

  private static final String SEVERAL_LAYOUTS =
      ONE_LAYOUT + "\n" + ONE_LAYOUT + "\n" + ONE_LAYOUT;

  private static final String SEVERAL_LAYOUTS_WITHOUT_BLANK_LINES =
      ONE_LAYOUT + ONE_LAYOUT;

  private static final String SEVERAL_LAYOUTS_AND_SKIPS_WITHOUT_BLANKS =
      ONE_LAYOUT + "skip-entry\n\n" + ONE_LAYOUT;

  private static final String LAYOUT_WITH_WHITESPACE =
      "qualifier:MyName\n"
      + "schema:\n"
      + "    \"string\"\n";

  private static final String LAYOUT_WITH_SKIPABLE_ENTRY =
      ONE_LAYOUT + "\n" + TextInputDescriptorParser.SKIP_ENTRY_TOKEN + "\n" + ONE_LAYOUT;

  private static final Schema STRING_SCHEMA = new Schema.Parser().parse("\"string\"");

  @Test
  public void parseOneLayout() throws IOException {
    InputStream in = new ByteArrayInputStream(ONE_LAYOUT.getBytes());
    TextInputDescriptorParser layoutParser = TextInputDescriptorParser.create();
    layoutParser.parse(in);

    List<QualifierInfo> info = layoutParser.getQualifierInfo();

    assertEquals(1, info.size());
    assertEquals("MyName", info.get(0).getQualifier());
    assertEquals(STRING_SCHEMA, info.get(0).getSchema());
  }

  @Test
  public void parseSeveralLayouts() throws IOException {
    InputStream in = new ByteArrayInputStream(SEVERAL_LAYOUTS.getBytes());
    TextInputDescriptorParser layoutParser = TextInputDescriptorParser.create();
    layoutParser.parse(in);

    List<QualifierInfo> info = layoutParser.getQualifierInfo();

    assertEquals(3, info.size());
    for (int i = 0; i < 3; i++) {
      assertEquals("MyName", info.get(i).getQualifier());
      assertEquals(STRING_SCHEMA, info.get(i).getSchema());
    }
  }

  @Test(expected=IOException.class)
  public void testExceptionIfNoBlankLines() throws IOException {
    InputStream in = new ByteArrayInputStream(SEVERAL_LAYOUTS_WITHOUT_BLANK_LINES.getBytes());
    TextInputDescriptorParser layoutParser = TextInputDescriptorParser.create();
    layoutParser.parse(in);
  }

  @Test(expected=IOException.class)
  public void testExceptionIfNoBlankLines2() throws IOException {
    InputStream in = new ByteArrayInputStream(SEVERAL_LAYOUTS_AND_SKIPS_WITHOUT_BLANKS.getBytes());
    TextInputDescriptorParser layoutParser = TextInputDescriptorParser.create();
    layoutParser.parse(in);
  }

  @Test(expected=RuntimeException.class)
  public void testExceptionIfIllegalSchema() throws IOException {
    InputStream in = new ByteArrayInputStream(BAD_SCHEMA_LAYOUT.getBytes());
    TextInputDescriptorParser layoutParser = TextInputDescriptorParser.create();
    layoutParser.parse(in);
  }

  @Test
  public void jsonMayContainWhitespace() throws IOException {
    InputStream in = new ByteArrayInputStream(LAYOUT_WITH_WHITESPACE.getBytes());
    TextInputDescriptorParser layoutParser = TextInputDescriptorParser.create();
    layoutParser.parse(in);
    assertEquals(STRING_SCHEMA, layoutParser.getQualifierInfo().get(0).getSchema());
  }

  @Test
  public void parseSkippable() throws IOException {
    InputStream in = new ByteArrayInputStream(LAYOUT_WITH_SKIPABLE_ENTRY.getBytes());
    TextInputDescriptorParser layoutParser = TextInputDescriptorParser.create();
    layoutParser.parse(in);

    List<QualifierInfo> info = layoutParser.getQualifierInfo();
    assertEquals(3, info.size());

    assertEquals("MyName", info.get(0).getQualifier());
    assertEquals(STRING_SCHEMA, info.get(0).getSchema());

    assertTrue(info.get(1).isEmpty());
    assertNull(info.get(1).getQualifier());
    assertNull(info.get(1).getSchema());

    assertEquals("MyName", info.get(2).getQualifier());
    assertEquals(STRING_SCHEMA, info.get(2).getSchema());
  }
}
