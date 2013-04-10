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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import org.kiji.hive.utils.HiveTypes.HiveStruct;
import org.kiji.schema.avro.TestRecord;

public class TestAvroTypeAdapter {
  private static final String A_VALUE = "a magic string";
  private static final Integer B_VALUE = 1;
  private static final Integer C_VALUE = 24601;

  @Test
  public void testAvroStructTypeDecoding() throws IOException {
    List<String> mColumnNames = Lists.newArrayList("a", "b", "c");
    List<TypeInfo> mColumnTypes = Lists.newArrayList(
        TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.intTypeInfo,
        TypeInfoFactory.intTypeInfo);

    TypeInfo typeInfo = TypeInfoFactory.getStructTypeInfo(
        mColumnNames, mColumnTypes);
    GenericRecord testRecord = new GenericData.Record(TestRecord.SCHEMA$);
    testRecord.put("a", A_VALUE);
    testRecord.put("b", B_VALUE);
    testRecord.put("c", C_VALUE);

    Object objectifiedRecord = (Object) testRecord;

    Object hiveObj = AvroTypeAdapter.INSTANCE.toHiveType(typeInfo, objectifiedRecord);

    assertTrue(hiveObj instanceof HiveStruct);
    HiveStruct hiveStruct = (HiveStruct) hiveObj;
    assertEquals(hiveStruct.get(0), A_VALUE);
    assertEquals(hiveStruct.get(1), B_VALUE);
    assertEquals(hiveStruct.get(2), C_VALUE);
  }
}
