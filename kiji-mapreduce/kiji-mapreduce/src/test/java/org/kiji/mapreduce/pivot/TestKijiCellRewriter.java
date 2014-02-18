/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.mapreduce.pivot;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.mapreduce.Counters;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.avro.dsl.JavaAvroDSL;
import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.avro.generated.CellRewriteSpec;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.util.InstanceBuilder;

/** Tests for the KijiCellRewriter pivot M/R job. */
public class TestKijiCellRewriter extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiCellRewriter.class);

  /** Table layout to test cell-rewrites. */
  private static final String LAYOUT_TEST1 =
      "org/kiji/mapreduce/layout/org.kiji.mapreduce.pivot.TestKijiCellRewriter.test1.json";

  /** Table layout to test chained cell-rewrites. */
  private static final String LAYOUT_TEST2 =
      "org/kiji/mapreduce/layout/org.kiji.mapreduce.pivot.TestKijiCellRewriter.test2.json";

  @Test
  public void testConvertAvro() throws Exception {
    final Schema intSchema = Schema.create(Schema.Type.INT);
    final Schema longSchema = Schema.create(Schema.Type.LONG);
    final DecodedCell<Object> original = new DecodedCell<Object>(intSchema, 1);
    Assert.assertEquals(1L, KijiCellRewriter.convertAvro(original, longSchema).getData());
  }

  @Test
  public void testSimpleRewrite() throws Exception {
    final String tableName = "test1";
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiMRTestLayouts.getLayout(LAYOUT_TEST1))
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("qualifier")
                        .withValue(1L, 1)
                        .withValue(2L, 2)
                        .withValue(3L, 3)
            .withRow("row2")
                .withFamily("family")
                    .withQualifier("qualifier")
                        .withValue(4L, 4)
                        .withValue(5L, 5)
                        .withValue(6L, 6)
        .build();

    final KijiURI tableURI;
    // Check the initial content of the table:
    {
      final KijiTable table = kiji.openTable(tableName);
      try {
        tableURI = table.getURI();
        final KijiTableReader reader = table.getReaderFactory().openTableReader();
        try {
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("family",  "qualifier", ColumnReaderSpec.avroWriterSchemaGeneric()))
              .build();
          final KijiRowData row1 = reader.get(table.getEntityId("row1"), dataRequest);
          Assert.assertEquals(new Integer(1), row1.getValue("family", "qualifier", 1L));
          Assert.assertEquals(new Integer(2), row1.getValue("family", "qualifier", 2L));
          Assert.assertEquals(new Integer(3), row1.getValue("family", "qualifier", 3L));
          final KijiRowData row2 = reader.get(table.getEntityId("row2"), dataRequest);
          Assert.assertEquals(new Integer(4), row2.getValue("family", "qualifier", 4L));
          Assert.assertEquals(new Integer(5), row2.getValue("family", "qualifier", 5L));
          Assert.assertEquals(new Integer(6), row2.getValue("family", "qualifier", 6L));
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    }

    // Run the cell-rewriter job:
    final Schema intSchema = Schema.create(Schema.Type.INT);
    final Schema longSchema = Schema.create(Schema.Type.LONG);
    final CellRewriteSpec spec = CellRewriteSpec.newBuilder()
        .setColumn("family:qualifier")
        .setRules(ImmutableMap.<String, String>builder()
            .put(intSchema.toString(), longSchema.toString())
            .build())
        .build();

    final Configuration conf = getConf();
    final JavaAvroDSL avroDSL = new JavaAvroDSL();
    conf.set(
        KijiCellRewriter.ConfKeys.spec.get(),
        avroDSL.valueToString(spec, CellRewriteSpec.getClassSchema()));

    final KijiMapReduceJob job = KijiPivotJobBuilder.create()
        .withConf(conf)
        .withPivoter(KijiCellRewriter.class)
        .withInputTable(tableURI)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(tableURI))
        .build();
    Assert.assertTrue(job.run());

    // Validate job-level expectations (Map/Reduce counters):
    final Counters counter = job.getHadoopJob().getCounters();
    Assert.assertEquals(6,
        counter.findCounter(KijiCellRewriter.Counters.CELLS_PROCESSED).getValue());
    Assert.assertEquals(6,
        counter.findCounter(KijiCellRewriter.Counters.CELLS_REWRITTEN).getValue());

    // Validate the new, rewritten content of the job:
    {
      final KijiTable table = kiji.openTable(tableName);
      try {
        final KijiTableReader reader = table.getReaderFactory().openTableReader();
        try {
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("family",  "qualifier", ColumnReaderSpec.avroWriterSchemaGeneric()))
              .build();
          final KijiRowData row1 = reader.get(table.getEntityId("row1"), dataRequest);
          Assert.assertEquals(new Long(1), row1.getValue("family", "qualifier", 1L));
          Assert.assertEquals(new Long(2), row1.getValue("family", "qualifier", 2L));
          Assert.assertEquals(new Long(3), row1.getValue("family", "qualifier", 3L));
          final KijiRowData row2 = reader.get(table.getEntityId("row2"), dataRequest);
          Assert.assertEquals(new Long(4), row2.getValue("family", "qualifier", 4L));
          Assert.assertEquals(new Long(5), row2.getValue("family", "qualifier", 5L));
          Assert.assertEquals(new Long(6), row2.getValue("family", "qualifier", 6L));
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    }
  }

  /**
   * Tests that chaining cell rewrites works as expected:
   *  - TestRecord v1 instances will be rewritten as TestRecord v2 instances.
   *  - TestRecord v2 instances will be rewritten as TestRecord v3 instances.
   *  - TestRecord v3 instances are left unmodified.
   * At the end of the process, there should be no instance of TestRecord v1 or v2 left.
   */
  @Test
  public void testChainedRewrite() throws Exception {
    final Schema intSchema = Schema.create(Schema.Type.INT);
    final Schema longSchema = Schema.create(Schema.Type.LONG);
    final Schema stringSchema = Schema.create(Schema.Type.STRING);

    final Schema recordV1 = Schema.createRecord("TestRecord", null, null, false);
    recordV1.setFields(Lists.newArrayList(
        new Field("int_field", intSchema, null, null),
        new Field("long_field", longSchema, null, null)));

    final Schema recordV2 = Schema.createRecord("TestRecord", null, null, false);
    recordV2.setFields(Lists.newArrayList(
        new Field("long_field", longSchema, null, null)));

    final Schema recordV3 = Schema.createRecord("TestRecord", null, null, false);
    recordV3.setFields(Lists.newArrayList(
        new Field("int_field", stringSchema, null, JsonNodeFactory.instance.textNode("")),
        new Field("long_field", longSchema, null, null)));

    final String tableName = "test2";
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiMRTestLayouts.getLayout(LAYOUT_TEST2))
            .withRow("row1")
                .withFamily("family")
                    .withQualifier("qualifier")
                        .withValue(1L, new GenericRecordBuilder(recordV1)
                            .set("int_field", 1234)
                            .set("long_field", 12345L)
                            .build())
                        .withValue(2L, new GenericRecordBuilder(recordV2)
                            .set("long_field", 12345L)
                            .build())
                        .withValue(3L, new GenericRecordBuilder(recordV3)
                            .set("int_field", "string")
                            .set("long_field", 12345L)
                            .build())
        .build();

    final KijiURI tableURI;
    // Check the initial content of the table:
    {
      final KijiTable table = kiji.openTable(tableName);
      try {
        tableURI = table.getURI();
        final KijiTableReader reader = table.getReaderFactory().openTableReader();
        try {
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("family",  "qualifier", ColumnReaderSpec.avroWriterSchemaGeneric()))
              .build();
          final KijiRowData row1 = reader.get(table.getEntityId("row1"), dataRequest);
          for (KijiCell<Object> cell : row1.asIterable("family", "qualifier")) {
            LOG.info("Row {} - Cell: {}", row1.getEntityId(), cell);
          }
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    }

    // Run the cell-rewriter job:
    final CellRewriteSpec spec = CellRewriteSpec.newBuilder()
        .setColumn("family:qualifier")
        .setRules(ImmutableMap.<String, String>builder()
            .put(recordV1.toString(), recordV2.toString())
            .put(recordV2.toString(), recordV3.toString())
            .build())
        .build();

    final Configuration conf = getConf();
    final JavaAvroDSL avroDSL = new JavaAvroDSL();
    conf.set(
        KijiCellRewriter.ConfKeys.spec.get(),
        avroDSL.valueToString(spec, CellRewriteSpec.getClassSchema()));

    final KijiMapReduceJob job = KijiPivotJobBuilder.create()
        .withConf(conf)
        .withPivoter(KijiCellRewriter.class)
        .withInputTable(tableURI)
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(tableURI))
        .build();
    Assert.assertTrue(job.run());

    // Validate job-level expectations (Map/Reduce counters):
    final Counters counter = job.getHadoopJob().getCounters();
    Assert.assertEquals(3,
        counter.findCounter(KijiCellRewriter.Counters.CELLS_PROCESSED).getValue());
    Assert.assertEquals(2,
        counter.findCounter(KijiCellRewriter.Counters.CELLS_REWRITTEN).getValue());

    // Validate the new, rewritten content of the job:
    {
      final KijiTable table = kiji.openTable(tableName);
      try {
        final KijiTableReader reader = table.getReaderFactory().openTableReader();
        try {
          final KijiDataRequest dataRequest = KijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("family",  "qualifier", ColumnReaderSpec.avroWriterSchemaGeneric()))
              .build();
          final KijiRowData row1 = reader.get(table.getEntityId("row1"), dataRequest);
          for (KijiCell<Object> cell : row1.asIterable("family", "qualifier")) {
            LOG.info("Row {} - Cell: {}", row1.getEntityId(), cell);
          }
          Assert.assertEquals(
              new GenericRecordBuilder(recordV3)
                  .set("int_field", "")
                  .set("long_field", 12345L)
                  .build(),
              row1.getValue("family", "qualifier", 1L));
          Assert.assertEquals(
              new GenericRecordBuilder(recordV3)
                  .set("int_field", "")
                  .set("long_field", 12345L)
                  .build(),
              row1.getValue("family", "qualifier", 2L));
          Assert.assertEquals(
              new GenericRecordBuilder(recordV3)
                  .set("int_field", "string")
                  .set("long_field", 12345L)
                  .build(),
              row1.getValue("family", "qualifier", 3L));
        } finally {
          reader.close();
        }
      } finally {
        table.release();
      }
    }
  }
}
