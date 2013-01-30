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

package org.kiji.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.context.DirectKijiTableWriterContext;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput.RowOptions;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Runs a map-only job in-process against a fake HBase instance. */
public class TestTransform {
  private static final Logger LOG = LoggerFactory.getLogger(TestTransform.class);

  /**
   * Example mapper intended to run on the generic KijiMR test layout. This test uses the resource
   * org/kiji/mapreduce/layout/test.json
   */
  public static class ExampleMapper
      extends Mapper<EntityId, KijiRowData, HFileKeyValue, NullWritable>
      implements KijiMapper {

    private KijiTableContext mTableContext;

    /** {@inheritDoc} */
    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      Preconditions.checkState(mTableContext == null);
      mTableContext = DirectKijiTableWriterContext.create(context);
    }

    /** {@inheritDoc} */
    @Override
    protected void map(EntityId eid, KijiRowData row, Context hadoopContext)
        throws IOException, InterruptedException {
      Preconditions.checkNotNull(mTableContext);

      final String userId = Bytes.toString(eid.getKijiRowKey());
      final EntityId genEId = mTableContext.getEntityId("generated row for " + userId);
      mTableContext.put(genEId, "primitives", "string", "generated content for " + userId);
    }

    /** {@inheritDoc} */
    @Override
    protected void cleanup(Context context)
        throws IOException, InterruptedException {
      Preconditions.checkNotNull(mTableContext);
      mTableContext.close();
      mTableContext = null;
      super.cleanup(context);
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputKeyClass() {
      return HFileKeyValue.class;
    }

    /** {@inheritDoc} */
    @Override
    public Class<?> getOutputValueClass() {
      return NullWritable.class;
    }
  }

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout =
        new KijiTableLayout(KijiMRTestLayouts.getTestLayout(), null);

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("test", layout)
            .withRow("Marsellus Wallace")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Marsellus")
                    .withQualifier("last_name").withValue("Wallace")
            .withRow("Vincent Vega")
                .withFamily("info")
                    .withQualifier("first_name").withValue("Vincent")
                    .withQualifier("last_name").withValue("Vega")
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("test");
    mReader = mTable.openTableReader();
  }

  @After
  public void cleanupEnvironment() throws IOException {
    IOUtils.closeQuietly(mReader);
    IOUtils.closeQuietly(mTable);
    mKiji.release();
  }

  @Test
  public void testMapReduce() throws Exception {
    // Run the transform (map-only job):
    final KijiConfiguration kijiConf = new KijiConfiguration(mKiji.getConf(), mKiji.getURI());
    final MapReduceJob job = KijiTransformJobBuilder.create()
        .withKijiConfiguration(kijiConf)
        .withMapper(ExampleMapper.class)
        .withInput(new KijiTableMapReduceJobInput(
            (HBaseKijiTable) mTable,
            new KijiDataRequest().addColumn(new Column("info")),
            new RowOptions()))
        .withOutput(new DirectKijiTableMapReduceJobOutput(mTable))
        .build();
    assertTrue(job.run());

    // Validate the output table content:
    final KijiRowScanner scanner = mReader.getScanner(
        new KijiDataRequest().addColumn(new Column("info")));
    for (KijiRowData row : scanner) {
      final EntityId eid = row.getEntityId();
      final String userId = Bytes.toString(eid.getKijiRowKey());
      LOG.info("Row: {}", userId);
      if (!userId.startsWith("generated row for ")) {
        assertEquals(userId, String.format("%s %s",
            row.getMostRecentValue("info", "first_name"),
            row.getMostRecentValue("info", "last_name")));
      }
    }
    scanner.close();
  }
}
