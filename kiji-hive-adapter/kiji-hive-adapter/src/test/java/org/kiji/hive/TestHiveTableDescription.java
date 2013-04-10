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

package org.kiji.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.GenericCellDecoderFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.impl.HBaseKijiRowData;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

public class TestHiveTableDescription extends KijiClientTest {
  private static final Long TIMESTAMP = 1L;
  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("user", layout)
          .withRow("foo")
            .withFamily("info")
        .withQualifier("name").withValue(TIMESTAMP, "foo-val")
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() throws IOException {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mKiji);
  }

  @Test
  public void testBuilder() throws IOException {
    List<String> columnNames = Lists.newArrayList();
    List<TypeInfo> columnTypes = Lists.newArrayList();
    List<String> columnExpressions = Lists.newArrayList();
    final HiveTableDescription hiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(columnTypes)
        .withColumnExpressions(columnExpressions)
        .withSchemaTable(mKiji.getSchemaTable())
        .withTableLayout(mKiji.openTable("user").getLayout())
        .withCellDecoderFactory(GenericCellDecoderFactory.get())
        .build();
  }

  @Test
  public void testConstructDataRequestFromNoExpressions() throws IOException {
    List<String> columnNames = Lists.newArrayList();
    List<TypeInfo> columnTypes = Lists.newArrayList();
    List<String> columnExpressions = Lists.newArrayList();
    final HiveTableDescription hiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(columnTypes)
        .withColumnExpressions(columnExpressions)
        .withSchemaTable(mKiji.getSchemaTable())
        .withTableLayout(mKiji.openTable("user").getLayout())
        .withCellDecoderFactory(GenericCellDecoderFactory.get())
        .build();

    KijiDataRequest kijiDataRequest = hiveTableDescription.getDataRequest();
    assertTrue(kijiDataRequest.isEmpty());
  }

  @Test
  public void testConstructDataRequest() throws IOException {
    List<String> columnNames = Lists.newArrayList("info:name");
    List<TypeInfo> columnTypes = Lists.newArrayList();
    columnTypes.add(TypeInfoUtils.getTypeInfoFromTypeString("struct<ts:timestamp,value:string>"));
    List<String> columnExpressions = Lists.newArrayList("info:name");

    final HiveTableDescription hiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(columnTypes)
        .withColumnExpressions(columnExpressions)
        .withSchemaTable(mKiji.getSchemaTable())
        .withTableLayout(mKiji.openTable("user").getLayout())
        .withCellDecoderFactory(GenericCellDecoderFactory.get())
        .build();

    final KijiDataRequest kijiDataRequest = hiveTableDescription.getDataRequest();
    assertEquals(1, kijiDataRequest.getColumns().size());
    assertNotNull(kijiDataRequest.getColumn("info", "name"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHBaseResultDecoding() throws IOException {
    List<String> columnNames = Lists.newArrayList("info:name");
    final TypeInfo typeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString("array<struct<ts:timestamp,value:string>>");
    List<TypeInfo> columnTypes = Lists.newArrayList(typeInfo);
    List<String> columnExpressions = Lists.newArrayList("info:name");

    final HiveTableDescription hiveTableDescription = HiveTableDescription.newBuilder()
        .withColumnNames(columnNames)
        .withColumnTypes(columnTypes)
        .withColumnExpressions(columnExpressions)
        .withSchemaTable(mKiji.getSchemaTable())
        .withTableLayout(mKiji.openTable("user").getLayout())
        .withCellDecoderFactory(GenericCellDecoderFactory.get())
        .build();

    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    final KijiRowScanner scanner = mReader.getScanner(request);

    KijiRowData kijiRowData = scanner.iterator().next();
    HBaseKijiRowData hbaseRowData = (HBaseKijiRowData) kijiRowData;
    Result result = hbaseRowData.getHBaseResult();

    // array<>
    List<Object> decodedArray = (List) hiveTableDescription.createDataObject(result);
    assertEquals(1, decodedArray.size());

    // array<struct<>>
    List<Object> decodedStruct = (List) decodedArray.get(0);
    assertEquals(1, decodedStruct.size());

    // array<struct<ts:timestamp,value:string>>
    List<Object> decodedElement = (List) decodedStruct.get(0);
    assertEquals(2, decodedElement.size());

    Timestamp timestamp = (Timestamp) decodedElement.get(0);
    assertTrue(timestamp.equals(new Timestamp(TIMESTAMP)));

    String name = (String) decodedElement.get(1);
    assertEquals("foo-val", name);
  }

}
