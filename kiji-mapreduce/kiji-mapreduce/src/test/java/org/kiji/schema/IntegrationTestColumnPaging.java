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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.AvroValueWriter;
import org.kiji.mapreduce.KijiGatherJobBuilder;
import org.kiji.mapreduce.KijiGatherer;
import org.kiji.mapreduce.MapReduceContext;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

public class IntegrationTestColumnPaging extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestColumnPaging.class);

  private Kiji mKiji;
  private KijiTable mUserTable;

  @Before
  public void setup() throws Exception {
    mKiji = Kiji.Factory.open(getKijiConfiguration());
    KijiAdmin admin = mKiji.getAdmin();
    admin.createTable("user",
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.PAGING_TEST), null),
        false);
    // TODO(SCHEMA-158): Clean up internal hbaseAdmin in kijiAdmin.
    mUserTable = mKiji.openTable("user");

    KijiTableWriter writer = mUserTable.openTableWriter();
    try {
      EntityId entityId = mUserTable.getEntityId("garrett");
      writer.put(entityId, "info", "name", "garrett");

      writer.put(entityId, "info", "location",
          new GregorianCalendar(1984, 9, 9).getTime().getTime(),
          "Bothell, WA");
      writer.put(entityId, "info", "location",
          new GregorianCalendar(2002, 9, 20).getTime().getTime(),
          "Seattle, WA");
      writer.put(entityId, "info", "location",
          new GregorianCalendar(2006, 10, 1).getTime().getTime(),
          "San Jose, CA");
      writer.put(entityId, "info", "location",
          new GregorianCalendar(2008, 9, 1).getTime().getTime(),
          "New York, NY");
      writer.put(entityId, "info", "location",
          new GregorianCalendar(2010, 10, 3).getTime().getTime(),
          "San Francisco, CA");

      writer.put(entityId, "jobs", "Papa Murphy's Pizza",
          new GregorianCalendar(1999, 10, 10).getTime().getTime(),
          "Pizza Maker");
      writer.put(entityId, "jobs", "The Catalyst Group",
          new GregorianCalendar(2004, 9, 10).getTime().getTime(),
          "Software Developer");
      writer.put(entityId, "jobs", "Google",
          new GregorianCalendar(2006, 6, 26).getTime().getTime(),
          "Software Engineer");
      writer.put(entityId, "jobs", "WibiData",
          new GregorianCalendar(2010, 10, 4).getTime().getTime(),
          "MTS");
    } finally {
      writer.close();
    }
  }

  @After
  public void teardown() throws Exception {
    IOUtils.closeQuietly(mUserTable);
    mKiji.release();
  }

  /**
   * A gatherer that outputs the locations a user has lived in.
   */
  public static class PagingGatherer extends KijiGatherer<Text, AvroValue<List<CharSequence>>>
      implements AvroValueWriter {
    @Override
    public KijiDataRequest getDataRequest() {
      return new KijiDataRequest()
          .addColumn(new KijiDataRequest.Column("info", "name"))
          .addColumn(new KijiDataRequest.Column("info", "location")
              .withMaxVersions(5)
              .withPageSize(2))
          .addColumn(new KijiDataRequest.Column("jobs")
              .withPageSize(2));
    }

    @Override
    public void gather(KijiRowData input,
        MapReduceContext<Text, AvroValue<List<CharSequence>>> context) throws IOException {
      // Read the user name.
      if (!input.containsColumn("info", "name")) {
        return;
      }
      CharSequence name = input.getMostRecentValue("info", "name");

      // Read the first page of locations (2 cells) the user has lived in.
      List<CharSequence> locations = new ArrayList<CharSequence>();
      for (CharSequence location : input.<CharSequence>getValues("info", "location").values()) {
        locations.add(location);
      }
      assertEquals(2, locations.size());

      // Read the second page of locations (2 cells).
      assertTrue(input.nextPage("info", "location"));
      for (CharSequence location : input.<CharSequence>getValues("info", "location").values()) {
        locations.add(location);
      }
      assertEquals(4, locations.size());

      // Read the last page of locations (1 cell).
      assertTrue(input.nextPage("info", "location"));
      for (CharSequence location : input.<CharSequence>getValues("info", "location").values()) {
        locations.add(location);
      }
      assertEquals(5, locations.size());

      assertFalse(input.nextPage("info", "location"));

      // Read the first page of jobs.
      List<CharSequence> jobs = new ArrayList<CharSequence>();
      for (Map.Entry<String, CharSequence> employment
               : input.<CharSequence>getMostRecentValues("jobs").entrySet()) {
        jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
      }
      assertEquals(2, jobs.size());

      // Read the second page of jobs.
      assertTrue(input.nextPage("jobs"));
      for (Map.Entry<String, CharSequence> employment
               : input.<CharSequence>getMostRecentValues("jobs").entrySet()) {
        jobs.add(employment.getValue().toString() + " @ " + employment.getKey());
      }
      assertEquals(4, jobs.size());

      assertFalse(input.nextPage("jobs"));

      context.write(new Text(name.toString() + "-locations"),
          new AvroValue<List<CharSequence>>(locations));
      context.write(new Text(name.toString() + "-jobs"),
          new AvroValue<List<CharSequence>>(jobs));
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return AvroValue.class;
    }

    @Override
    public Schema getAvroValueWriterSchema() {
      return Schema.createArray(Schema.create(Schema.Type.STRING));
    }
  }

  @Test
  public void testPagingGatherer()
      throws ClassNotFoundException, IOException, InterruptedException {
    final Path outputPath = getDfsPath("output");

    // Run the gatherer.
    final MapReduceJob job = KijiGatherJobBuilder.create()
        .withInputTable(mUserTable)
        .withGatherer(PagingGatherer.class)
        .withOutput(new AvroKeyValueMapReduceJobOutput(outputPath, 1))
        .build();
    assertTrue(job.run());

    // Verify that the output has all 5 locations for the user.

    // Set up reader schemas.
    final Schema keySchema = Schema.create(Schema.Type.STRING);
    final Schema valueSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    final Schema recordSchema = AvroKeyValue.getSchema(keySchema, valueSchema);

    // Open the avro file.
    DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(
        new FsInput(new Path(outputPath, "part-m-00000.avro"), getKijiConfiguration().getConf()),
        new GenericDatumReader<GenericRecord>(recordSchema));

    try {
      // Read the record.
      Iterator<GenericRecord> iterator = fileReader.iterator();
      assertTrue(iterator.hasNext());
      GenericRecord locationsRecord = iterator.next();
      GenericRecord jobsRecord = iterator.next();
      assertFalse(iterator.hasNext());

      // Verify the locations record.
      AvroKeyValue<CharSequence, List<CharSequence>> locationsKeyValue =
          new AvroKeyValue<CharSequence, List<CharSequence>>(locationsRecord);

      // Verify name.
      assertEquals("garrett-locations", locationsKeyValue.getKey().toString());

      // Verify locations.
      List<CharSequence> locations = locationsKeyValue.getValue();
      assertEquals(5, locations.size());
      assertEquals("San Francisco, CA", locations.get(0).toString());
      assertEquals("New York, NY", locations.get(1).toString());
      assertEquals("San Jose, CA", locations.get(2).toString());
      assertEquals("Seattle, WA", locations.get(3).toString());
      assertEquals("Bothell, WA", locations.get(4).toString());

      // Verify the jobs record.
      AvroKeyValue<CharSequence, List<CharSequence>> jobsKeyValue =
          new AvroKeyValue<CharSequence, List<CharSequence>>(jobsRecord);

      // Verify name.
      assertEquals("garrett-jobs", jobsKeyValue.getKey().toString());

      // Verify jobs.
      List<CharSequence> jobs = jobsKeyValue.getValue();
      assertEquals(4, jobs.size());
      assertEquals("Software Engineer @ Google", jobs.get(0).toString());
      assertEquals("Pizza Maker @ Papa Murphy's Pizza", jobs.get(1).toString());
      assertEquals("Software Developer @ The Catalyst Group", jobs.get(2).toString());
      assertEquals("MTS @ WibiData", jobs.get(3).toString());
    } finally {
      fileReader.close();
    }
  }
}
