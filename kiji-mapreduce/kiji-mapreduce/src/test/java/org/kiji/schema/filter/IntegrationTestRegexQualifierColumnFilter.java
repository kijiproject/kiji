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

package org.kiji.schema.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiGatherJobBuilder;
import org.kiji.mapreduce.KijiGatherer;
import org.kiji.mapreduce.MapReduceContext;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.output.SequenceFileMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

public class IntegrationTestRegexQualifierColumnFilter extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(
      IntegrationTestRegexQualifierColumnFilter.class);

  public static final String TABLE_NAME = "regex_test";

  private Kiji mKiji;
  private KijiTable mTable;

  @Before
  public void setup() throws Exception {
    mKiji = Kiji.Factory.open(getKijiConfiguration());
    final KijiTableLayout layout =
        new KijiTableLayout(KijiTableLayouts.getLayout(KijiTableLayouts.REGEX), null);
    mKiji.getAdmin().createTable(TABLE_NAME, layout, false);
    mTable = mKiji.openTable(TABLE_NAME);

    // Write some stuff to the table.
    KijiTableWriter writer = mTable.openTableWriter();
    try {
      writer.put(mTable.getEntityId("1"), "family", "apple", "cell");
      writer.put(mTable.getEntityId("1"), "family", "banana", "cell");
      writer.put(mTable.getEntityId("1"), "family", "carrot", "cell");
      writer.put(mTable.getEntityId("1"), "family", "aardvark", "cell");
    } finally {
      writer.close();
    }
  }

  @After
  public void teardown() throws Exception {
    IOUtils.closeQuietly(mTable);
    mKiji.release();
  }

  /**
   * A test gatherer that outputs all qualifiers from the 'family' family that start with
   * the letter 'a'.
   */
  public static class MyGatherer extends KijiGatherer<Text, NullWritable> {
    @Override
    public KijiDataRequest getDataRequest() {
      return new KijiDataRequest()
          .addColumn(new KijiDataRequest.Column("family")
              .withFilter(new RegexQualifierColumnFilter("a.*"))
              .withMaxVersions(10));
    }

    @Override
    public void gather(KijiRowData input, MapReduceContext<Text, NullWritable> context)
        throws IOException {
      NavigableMap<String, NavigableMap<Long, CharSequence>> qualifiers =
          input.getValues("family");
      for (String qualifier : qualifiers.keySet()) {
        context.write(new Text(qualifier), NullWritable.get());
      }
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return NullWritable.class;
    }
  }

  @Test
  public void testRegexQualifierColumnFilter()
      throws ClassNotFoundException, IOException, InterruptedException {
    // Run a gatherer over the test_table.
    Path outputPath = getDfsPath("qualifiers-that-start-with-the-letter-a");
    MapReduceJob gatherJob = KijiGatherJobBuilder.create()
        .withInputTable(mTable)
        .withGatherer(MyGatherer.class)
        .withOutput(new SequenceFileMapReduceJobOutput(outputPath, 1))
        .build();
    assertTrue(gatherJob.run());

    // Check the output file: two things should be there (apple, aardvark).
    final Configuration conf = getKijiConfiguration().getConf();
    SequenceFile.Reader reader = new SequenceFile.Reader(
        FileSystem.get(conf), new Path(outputPath, "part-m-00000"), conf);
    try {
      Text key = new Text();
      assertTrue(reader.next(key));
      assertEquals("aardvark", key.toString());
      assertTrue(reader.next(key));
      assertEquals("apple", key.toString());
      assertFalse(reader.next(key));
    } finally {
      reader.close();
    }
  }
}
