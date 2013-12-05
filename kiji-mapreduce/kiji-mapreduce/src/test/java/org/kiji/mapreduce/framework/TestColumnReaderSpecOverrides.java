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

package org.kiji.mapreduce.framework;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.mapreduce.produce.KijiProduceJobBuilder;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.avro.EmptyRecord;
import org.kiji.schema.avro.TestRecord1;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

public class TestColumnReaderSpecOverrides extends KijiClientTest {
  private static final KijiColumnName EMPTY = new KijiColumnName("family", "empty");
  private static final KijiColumnName RECORD1 = new KijiColumnName("family", "record1");

  /** Test table, owned by this test. */
  private KijiTable mTable;

  /** Table reader, owned by this test. */
  private KijiTableReader mReader;

  @Before
  public final void setupTestProducer() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.READER_SCHEMA_TEST);

    // Build the test records.
    final EmptyRecord row1Value = EmptyRecord.newBuilder().build();
    final EmptyRecord row2Value = EmptyRecord.newBuilder().build();

    // Populate the environment.
    new InstanceBuilder(getKiji())
        .withTable(layout.getName(), layout)
            .withRow("row1")
                .withFamily(EMPTY.getFamily())
                    .withQualifier(EMPTY.getQualifier()).withValue(row1Value)
            .withRow("row2")
                .withFamily(EMPTY.getFamily())
                    .withQualifier(EMPTY.getQualifier()).withValue(row2Value)
        .build();

    // Fill local variables.
    mTable = getKiji().openTable(layout.getName());
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownTestProducer() throws Exception {
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
  }

  public static class SimpleProducer extends KijiProducer {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      final KijiDataRequestBuilder.ColumnsDef overrideColumnsDef;
      try {
        overrideColumnsDef = KijiDataRequestBuilder.ColumnsDef
            .create()
            .add(EMPTY, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class));
      } catch (InvalidLayoutException e) {
        throw new KijiIOException(e);
      }

      return KijiDataRequest
          .builder()
          .addColumns(overrideColumnsDef)
          .build();
    }

    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
      return RECORD1.getName();
    }

    /** {@inheritDoc} */
    @Override
    public void produce(KijiRowData input, ProducerContext context) throws IOException {
      // Unpack the row.
      final TestRecord1 record = input.getMostRecentValue(EMPTY.getFamily(), EMPTY.getQualifier());

      // Write out the unpacked record (with the schema mask applied).
      context.put(record);
    }
  }

  @Test
  public void testColumnReaderSpecOverrides() throws Exception {
    // Run producer.
    final KijiMapReduceJob job = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(SimpleProducer.class)
        .withInputTable(mTable.getURI())
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mTable.getURI()))
        .build();
    Assert.assertTrue(job.run());

    // Validate produced output.
    final KijiDataRequest outputRequest = KijiDataRequest
        .create(RECORD1.getFamily(), RECORD1.getQualifier());
    final KijiRowData row1 = mReader.get(mTable.getEntityId("row1"), outputRequest);
    final KijiRowData row2 = mReader.get(mTable.getEntityId("row2"), outputRequest);

    final TestRecord1 row1Expected = TestRecord1.newBuilder().build();
    final TestRecord1 row2Expected = TestRecord1.newBuilder().build();
    final TestRecord1 row1Actual =
        row1.getMostRecentValue(RECORD1.getFamily(), RECORD1.getQualifier());
    final TestRecord1 row2Actual =
        row2.getMostRecentValue(RECORD1.getFamily(), RECORD1.getQualifier());
    Assert.assertEquals(row1Expected, row1Actual);
    Assert.assertEquals(row2Expected, row2Actual);
  }
}
