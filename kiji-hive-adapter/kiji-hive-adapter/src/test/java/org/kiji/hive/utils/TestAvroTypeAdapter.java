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

package org.kiji.hive.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import org.kiji.hive.utils.HiveTypes.HiveStruct;
import org.kiji.hive.utils.HiveTypes.HiveUnion;

public class TestAvroTypeAdapter {
  private static final String STRING_VALUE = "a magic string";
  private static final Integer INT_VALUE = 24601;
  private static final Double DOUBLE_VALUE = 24.56d;
  private static final Long LONG_VALUE = 31336L;

  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema INT_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);

  @Test
  public void testAvroStructTypeDecoding() throws IOException {
    // Construct the Avro schema for testing
    List<Schema.Field> mFields = Lists.newArrayList(
        new Schema.Field("a", STRING_SCHEMA, null, null),
        new Schema.Field("b", INT_SCHEMA, null, null),
        new Schema.Field("c", DOUBLE_SCHEMA, null, null)
    );
    Schema recordSchema = Schema.createRecord(mFields);

    // Construct the Hive column type
    List<String> columnNames = Lists.newArrayList("a", "b", "c");
    List<TypeInfo> columnTypes = Lists.<TypeInfo>newArrayList(
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.doubleTypeInfo);
    TypeInfo typeInfo = TypeInfoFactory.getStructTypeInfo(
        columnNames, columnTypes);

    // Construct the record
    GenericRecord testRecord = new GenericData.Record(recordSchema);
    testRecord.put("a", STRING_VALUE);
    testRecord.put("b", INT_VALUE);
    testRecord.put("c", DOUBLE_VALUE);
    Object objectifiedRecord = (Object) testRecord;

    // Convert the object into the Hive representation and make sure that the values came through
    Object hiveObj = AvroTypeAdapter.get().toHiveType(typeInfo, objectifiedRecord, recordSchema);
    assertTrue(hiveObj instanceof HiveStruct);
    HiveStruct hiveStruct = (HiveStruct) hiveObj;
    assertEquals(hiveStruct.get(0), STRING_VALUE);
    assertEquals(hiveStruct.get(1), INT_VALUE);
    assertEquals(hiveStruct.get(2), DOUBLE_VALUE);
  }

  @Test
  public void testPrimitiveUnions() throws IOException {
    // Construct the Avro schema for testing
    List<Schema> unionSchemas = Lists.newArrayList(STRING_SCHEMA, INT_SCHEMA, DOUBLE_SCHEMA);
    Schema unionSchema = Schema.createUnion(unionSchemas);

    // Construct the Hive union type
    List<TypeInfo> unionTypes = Lists.<TypeInfo>newArrayList(
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.doubleTypeInfo);
    TypeInfo unionType = TypeInfoFactory.getUnionTypeInfo(unionTypes);

    // Make sure that we can put a string into the union
    Object objectifiedString = (Object) STRING_VALUE;
    HiveUnion hiveStringUnion =
        (HiveUnion) AvroTypeAdapter.get().toHiveType(unionType, objectifiedString, unionSchema);
    assertEquals(0, hiveStringUnion.getTag());
    Object extractedHiveString = hiveStringUnion.getObject();
    assertTrue(extractedHiveString instanceof String);
    assertEquals(STRING_VALUE, extractedHiveString);

    // Make sure that we can put an Integer into the union
    Object objectifiedInteger = (Object) INT_VALUE;
    HiveUnion hiveIntUnion =
        (HiveUnion) AvroTypeAdapter.get().toHiveType(unionType, objectifiedInteger, unionSchema);
    assertEquals(1, hiveIntUnion.getTag());
    Object extractedHiveInteger = hiveIntUnion.getObject();
    assertTrue(extractedHiveInteger instanceof Integer);
    assertEquals(INT_VALUE, extractedHiveInteger);

    // Make sure that we can put a Double into the union
    Object objectifiedDouble = (Object) DOUBLE_VALUE;
    HiveUnion hiveDoubleUnion =
        (HiveUnion) AvroTypeAdapter.get().toHiveType(unionType, objectifiedDouble, unionSchema);
    assertEquals(2, hiveDoubleUnion.getTag());
    Object extractedHiveDouble = hiveDoubleUnion.getObject();
    assertTrue(extractedHiveDouble instanceof Double);
    assertEquals(DOUBLE_VALUE, extractedHiveDouble);
  }

  @Test
  public void testNotInUnionThrowsException() throws IOException {
    // Construct the Avro schema for testing
    List<Schema> unionSchemas = Lists.newArrayList(STRING_SCHEMA, INT_SCHEMA, DOUBLE_SCHEMA);
    Schema unionSchema = Schema.createUnion(unionSchemas);

    // Construct the Hive union type
    List<TypeInfo> unionTypes = Lists.<TypeInfo>newArrayList(
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.doubleTypeInfo);
    TypeInfo unionType = TypeInfoFactory.getUnionTypeInfo(unionTypes);

    // Make sure that we can't put a Long into this union since it's not there
    Object objectifiedLong = (Object) LONG_VALUE;
    try {
      HiveUnion hiveLongUnion =
          (HiveUnion) AvroTypeAdapter.get().toHiveType(unionType, objectifiedLong, unionSchema);
      fail();
    } catch (UnresolvedUnionException ue) {
      assertTrue(ue.getMessage().startsWith("Not in union"));
    }
  }

  @Test
  public void testComplexUnions() throws IOException {
    // Construct the Avro schema for testing
    List<Schema.Field> mFields = Lists.newArrayList(
        new Schema.Field("a", STRING_SCHEMA, null, null),
        new Schema.Field("b", INT_SCHEMA, null, null),
        new Schema.Field("c", DOUBLE_SCHEMA, null, null)
    );
    Schema recordSchema = Schema.createRecord("TestRecord", null, "", false);
    recordSchema.setFields(mFields);

    List<Schema> unionSchemas = Lists.newArrayList(STRING_SCHEMA, recordSchema);
    Schema unionSchema = Schema.createUnion(unionSchemas);

    // Construct the Hive union type
    List<String> columnNames = Lists.newArrayList("a", "b", "c");
    List<TypeInfo> columnTypes = Lists.<TypeInfo>newArrayList(
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.doubleTypeInfo);
    TypeInfo recordTypeInfo = TypeInfoFactory.getStructTypeInfo(
        columnNames, columnTypes);
    List<TypeInfo> unionTypes = Lists.newArrayList(TypeInfoFactory.stringTypeInfo, recordTypeInfo);
    TypeInfo unionType = TypeInfoFactory.getUnionTypeInfo(unionTypes);

    // Make sure that we can still put a string into the union
    Object objectifiedString = (Object) STRING_VALUE;
    HiveUnion hiveStringUnion =
        (HiveUnion) AvroTypeAdapter.get().toHiveType(unionType, objectifiedString, unionSchema);
    assertEquals(0, hiveStringUnion.getTag());
    Object extractedHiveString = hiveStringUnion.getObject();
    assertTrue(extractedHiveString instanceof String);
    assertEquals(STRING_VALUE, extractedHiveString);

    // Make sure that we can put the record into the union
    GenericRecord testRecord = new GenericData.Record(recordSchema);
    testRecord.put("a", STRING_VALUE);
    testRecord.put("b", INT_VALUE);
    testRecord.put("c", DOUBLE_VALUE);
    Object objectifiedRecord = (Object) testRecord;
    HiveUnion hiveRecordUnion =
        (HiveUnion) AvroTypeAdapter.get().toHiveType(unionType, objectifiedRecord, unionSchema);
    assertEquals(1, hiveRecordUnion.getTag());
    Object extractedHiveRecord = hiveRecordUnion.getObject();
    assertTrue(extractedHiveRecord instanceof HiveStruct);
    HiveStruct hiveStruct = (HiveStruct) extractedHiveRecord;
    assertEquals(hiveStruct.get(0), STRING_VALUE);
    assertEquals(hiveStruct.get(1), INT_VALUE);
    assertEquals(hiveStruct.get(2), DOUBLE_VALUE);
  }
}
