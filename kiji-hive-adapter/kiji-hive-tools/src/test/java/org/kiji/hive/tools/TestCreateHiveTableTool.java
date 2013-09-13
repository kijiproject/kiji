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

package org.kiji.hive.tools;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestCreateHiveTableTool {

  private static final KijiColumnName BOOLEAN_COLUMN =
      new KijiColumnName("primitive:boolean_column");
  private static final KijiColumnName INT_COLUMN = new KijiColumnName("primitive:int_column");
  private static final KijiColumnName LONG_COLUMN = new KijiColumnName("primitive:long_column");
  private static final KijiColumnName FLOAT_COLUMN = new KijiColumnName("primitive:float_column");
  private static final KijiColumnName DOUBLE_COLUMN = new KijiColumnName("primitive:double_column");
  private static final KijiColumnName BYTES_COLUMN = new KijiColumnName("primitive:bytes_column");
  private static final KijiColumnName STRING_COLUMN = new KijiColumnName("primitive:string_column");

  private static final KijiColumnName RECORD_COLUMN = new KijiColumnName("complex:record_column");
  private static final KijiColumnName ENUM_COLUMN = new KijiColumnName("complex:enum_column");
  private static final KijiColumnName ARRAY_COLUMN = new KijiColumnName("complex:array_column");
  private static final KijiColumnName MAP_COLUMN = new KijiColumnName("complex:map_column");
  private static final KijiColumnName UNION_COLUMN = new KijiColumnName("complex:union_column");
  private static final KijiColumnName FIXED_COLUMN = new KijiColumnName("complex:fixed_column");
  private static final KijiColumnName RECURSIVE_RECORD_COLUMN =
      new KijiColumnName("complex:recursive_record_column");
  private static final KijiColumnName CLASS_COLUMN = new KijiColumnName("complex:class_column");

  private static final KijiColumnName BOOLEAN_MAP_FAMILY = new KijiColumnName("boolean_map");
  private static final KijiColumnName INT_MAP_FAMILY = new KijiColumnName("int_map");
  private static final KijiColumnName LONG_MAP_FAMILY = new KijiColumnName("long_map");
  private static final KijiColumnName FLOAT_MAP_FAMILY = new KijiColumnName("float_map");
  private static final KijiColumnName DOUBLE_MAP_FAMILY = new KijiColumnName("double_map");
  private static final KijiColumnName BYTES_MAP_FAMILY = new KijiColumnName("bytes_map");
  private static final KijiColumnName STRING_MAP_FAMILY = new KijiColumnName("string_map");

  // Allow the constructor of this class to throw an exception to load the mLayout from the file
  public TestCreateHiveTableTool() throws IOException {}

  private final KijiTableLayout mLayout = KijiTableLayout.newLayout(
      KijiTableLayouts.getLayout("org/kiji/schema/layout/all-types-schema.json"));

  @Test
  public void testPrimitivesSchemas() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: BOOLEAN>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(BOOLEAN_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: INT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(INT_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BIGINT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(LONG_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: FLOAT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(FLOAT_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: DOUBLE>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(DOUBLE_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BINARY>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(BYTES_COLUMN)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(STRING_COLUMN)));
  }

  @Test
  public void testPrimitiveLayouts() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: BOOLEAN>",
        CreateHiveTableTool.getHiveType(BOOLEAN_COLUMN, mLayout));
    assertEquals("STRUCT<ts: TIMESTAMP, value: INT>",
        CreateHiveTableTool.getHiveType(INT_COLUMN, mLayout));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BIGINT>",
        CreateHiveTableTool.getHiveType(LONG_COLUMN, mLayout));
    assertEquals("STRUCT<ts: TIMESTAMP, value: FLOAT>",
        CreateHiveTableTool.getHiveType(FLOAT_COLUMN, mLayout));
    assertEquals("STRUCT<ts: TIMESTAMP, value: DOUBLE>",
        CreateHiveTableTool.getHiveType(DOUBLE_COLUMN, mLayout));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BINARY>",
        CreateHiveTableTool.getHiveType(BYTES_COLUMN, mLayout));
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.getHiveType(STRING_COLUMN, mLayout));
  }

  @Test
  public void testPrimitiveMapSchemas() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: BOOLEAN>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(BOOLEAN_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: INT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(INT_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BIGINT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(LONG_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: FLOAT>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(FLOAT_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: DOUBLE>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(DOUBLE_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: BINARY>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(BYTES_MAP_FAMILY)));
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.convertSchemaToHiveType(mLayout.getSchema(STRING_MAP_FAMILY)));
  }

  @Test
  public void testPrimitiveMaps() throws IOException {
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: BOOLEAN>>",
        CreateHiveTableTool.getHiveType(BOOLEAN_MAP_FAMILY, mLayout));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: INT>>",
        CreateHiveTableTool.getHiveType(INT_MAP_FAMILY, mLayout));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: BIGINT>>",
        CreateHiveTableTool.getHiveType(LONG_MAP_FAMILY, mLayout));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: FLOAT>>",
        CreateHiveTableTool.getHiveType(FLOAT_MAP_FAMILY, mLayout));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: DOUBLE>>",
        CreateHiveTableTool.getHiveType(DOUBLE_MAP_FAMILY, mLayout));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: BINARY>>",
        CreateHiveTableTool.getHiveType(BYTES_MAP_FAMILY, mLayout));
    assertEquals("MAP<STRING, STRUCT<ts: TIMESTAMP, value: STRING>>",
        CreateHiveTableTool.getHiveType(STRING_MAP_FAMILY, mLayout));
  }

  @Test
  public void testRecord() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRUCT<numerator: INT, denominator: INT>>",
        CreateHiveTableTool.getHiveType(RECORD_COLUMN, mLayout));
  }

  @Test
  public void testRecursiveRecord() throws IOException {
    assertEquals(
      "STRUCT<ts: TIMESTAMP, value: STRUCT<value: BIGINT, next: "
      + "STRUCT<value: BIGINT, next: STRING>>>",
      CreateHiveTableTool.getHiveType(RECURSIVE_RECORD_COLUMN, mLayout));
  }

  @Test
  public void testEnum() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.getHiveType(ENUM_COLUMN, mLayout));
  }

  @Test
  public void testArray() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: ARRAY<STRING>>",
        CreateHiveTableTool.getHiveType(ARRAY_COLUMN, mLayout));
  }

  @Test
  public void testMap() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: MAP<STRING, BIGINT>>",
        CreateHiveTableTool.getHiveType(MAP_COLUMN, mLayout));
  }

  @Test
  public void testUnion() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: STRING>",
        CreateHiveTableTool.getHiveType(UNION_COLUMN, mLayout));
  }

  @Test
  public void testFixed() throws IOException {
    assertEquals("STRUCT<ts: TIMESTAMP, value: BINARY>",
        CreateHiveTableTool.getHiveType(FIXED_COLUMN, mLayout));
  }

  @Test
  public void testClassColumn() throws IOException {
    String nodeClassHiveType =
        "STRUCT<ts: TIMESTAMP, value: STRUCT<label: STRING, weight: DOUBLE, "
          + "edges: ARRAY<STRUCT<label: STRING, weight: DOUBLE, " // begin edges1
            + "target: STRUCT<label: STRING, weight: DOUBLE, " // begin target1
              + "edges: ARRAY<STRUCT<label: STRING, weight: DOUBLE, " // begin edges2
                + "target: STRING, " // begin target2
                + "annotations: MAP<STRING, STRING>>>, " // end target2
              + "annotations: MAP<STRING, STRING>>, " // end edges2
            + "annotations: MAP<STRING, STRING>>>, " //end target1
          + "annotations: MAP<STRING, STRING>>>"; //end edges1
    assertEquals(nodeClassHiveType,
        CreateHiveTableTool.getHiveType(CLASS_COLUMN, mLayout));
  }

  @Test
  public void testGetDefaultHiveColumnName() {
    // Map type column family.
    assertEquals("family",
        CreateHiveTableTool.getDefaultHiveColumnName(new KijiColumnName("family")));
    // Fully qualified column.
    assertEquals("qualifier",
        CreateHiveTableTool.getDefaultHiveColumnName(new KijiColumnName("family", "qualifier")));
  }
}
