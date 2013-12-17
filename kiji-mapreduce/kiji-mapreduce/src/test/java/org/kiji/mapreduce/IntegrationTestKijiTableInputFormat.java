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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;
import scala.actors.threadpool.Arrays;

import org.kiji.mapreduce.framework.KijiTableInputFormat;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.filter.ColumnValueEqualsRowFilter;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.testutil.FooTableIntegrationTest;

/** Tests for the KijiTableInputFormat. */
public class IntegrationTestKijiTableInputFormat
    extends FooTableIntegrationTest {

  public static class TestMapper
      extends Mapper<EntityId, KijiRowData, Text, Text> {

    @Override
    public void map(EntityId entityId, KijiRowData row, Context context)
        throws IOException, InterruptedException {
      final String name = row.getMostRecentValue("info", "name").toString();
      final String email = row.getMostRecentValue("info", "email").toString();

      // Build email domain regex.
      final Pattern emailRegex = Pattern.compile(".+@(.+)");
      final Matcher emailMatcher = emailRegex.matcher(email);

      // Extract domain from email.
      assertTrue(emailMatcher.find());
      final String emailDomain = emailMatcher.group(1);

      context.write(new Text(emailDomain), new Text(name));
    }
  }

  public static class TestReducer
      extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // Combine all the names.
      final Iterator<Text> iter = values.iterator();
      final StringBuilder names = new StringBuilder(iter.next().toString());
      while (iter.hasNext()) {
        final Text name = iter.next();

        names.append(",");
        names.append(name.toString());
      }

      // Write names to context.
      final Text output = new Text(names.toString().trim());
      context.write(key, output);
    }
  }

  private Job setupJob(EntityId start, EntityId limit, KijiRowFilter filter) throws Exception {
    return setupJob("job", createOutputFile(), null, null, start, limit, filter);
  }

  public Job setupJob(
      String jobName,
      Path outputFile,
      Class<? extends Mapper> mapperClass,
      Class<? extends Reducer> reducerClass,
      EntityId startKey,
      EntityId limitKey,
      KijiRowFilter filter) throws Exception {
    final Job job = new Job(createConfiguration());
    final Configuration conf = job.getConfiguration();

    // Get settings for test.
    final KijiDataRequest request = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create().add("info", "name").add("info", "email"))
        .build();

    job.setJarByClass(IntegrationTestKijiTableInputFormat.class);

    // Setup the InputFormat.
    KijiTableInputFormat.configureJob(job, getFooTable().getURI(), request,
        startKey, limitKey, filter);
    job.setInputFormatClass(KijiTableInputFormat.class);

    // Duplicate functionality from MapReduceJobBuilder, since we are not using it here:
    final List<Path> jarFiles = Lists.newArrayList();
    final FileSystem fs = FileSystem.getLocal(conf);
    for (String cpEntry : System.getProperty("java.class.path").split(":")) {
      if (cpEntry.endsWith(".jar")) {
        jarFiles.add(fs.makeQualified(new Path(cpEntry)));
      }
    }
    DistributedCacheJars.addJarsToDistributedCache(job, jarFiles);

    // Create a test job.
    job.setJobName(jobName);

    // Setup the OutputFormat.
    TextOutputFormat.setOutputPath(job, outputFile.getParent());
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set the mapper class.
    if (null != mapperClass) {
      job.setMapperClass(mapperClass);
    }
    // Set the reducer class.
    if (null != reducerClass) {
      job.setReducerClass(reducerClass);
    }

    return job;
  }

  private Path createOutputFile() {
    return new Path(String.format("/%s-%s-%d/part-r-00000",
        getClass().getName(), mTestName.getMethodName(), System.currentTimeMillis()));
  }

  /** Test KijiTableInputFormat in a map-only job. */
  @Test
  public void testMapJob() throws Exception {
    final Path outputFile = createOutputFile();
    // Create a test job.
    final Job job = setupJob(
        "testMapJob",
        outputFile,
        TestMapper.class,
        null,  // reducer class
        null,  // start key
        null,  // limit key
        null); // filter

    // Run the job.
    assertTrue("Hadoop job failed", job.waitForCompletion(true));

    // Check to make sure output exists.
    final FileSystem fs = FileSystem.get(job.getConfiguration());
    assertTrue(fs.exists(outputFile.getParent()));

    // Verify that the output matches what's expected.
    final FSDataInputStream in = fs.open(outputFile);
    final Set<String> actual = Sets.newHashSet(IOUtils.toString(in).trim().split("\n"));
    final Set<String> expected = Sets.newHashSet(
        "usermail.example.com\tAaron Kimball",
        "gmail.com\tJohn Doe",
        "usermail.example.com\tChristophe Bisciglia",
        "usermail.example.com\tKiyan Ahmadizadeh",
        "gmail.com\tJane Doe",
        "usermail.example.com\tGarrett Wu");
    assertEquals("Result of job wasn't what was expected", expected, actual);

    // Clean up.
    fs.delete(outputFile.getParent(), true);

    IOUtils.closeQuietly(in);
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973
  }

  /** Test KijiTableInputFormat in a map-only job with start and limit keys. */
  @Test
  public void testMapJobWithStartAndLimitKeys() throws Exception {
    final Path outputFile = createOutputFile();
    // Set the same entity IDs for start and limit, and we should get just the start row
    final EntityId startEntityId = getFooTable().getEntityId("jane.doe@gmail.com");
    final byte[] endRowKey = startEntityId.getHBaseRowKey();
    final EntityId rawLimitEntityId =
        HBaseEntityId.fromHBaseRowKey(Arrays.copyOf(endRowKey, endRowKey.length + 1));

    // Create a test job.
    final Job job = setupJob(
        "testMapJobWithStartAndLimitKeys",
        outputFile,
        TestMapper.class,
        null,  // reducer class
        startEntityId,
        rawLimitEntityId,
        null); // filter

    // Run the job.
    assertTrue("Hadoop job failed", job.waitForCompletion(true));

    // Check to make sure output exists.
    final FileSystem fs = FileSystem.get(job.getConfiguration());
    assertTrue(fs.exists(outputFile.getParent()));

    // Verify that the output matches what's expected.
    final FSDataInputStream in = fs.open(outputFile);
    final Set<String> actual = Sets.newHashSet(IOUtils.toString(in).trim().split("\n"));
    final Set<String> expected = Sets.newHashSet(
        "gmail.com\tJane Doe");
    assertEquals("Result of job wasn't what was expected", expected, actual);

    // Clean up.
    fs.delete(outputFile.getParent(), true);

    IOUtils.closeQuietly(in);
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973
  }

  /** Test KijiTableInputFormat in a map-only job with a row filter. */
  @Test
  public void testMapJobWithFilter() throws Exception {
    final KijiRowFilter filter = new ColumnValueEqualsRowFilter("info", "email",
        new DecodedCell<String>(Schema.create(Schema.Type.STRING), "aaron@usermail.example.com"));
    final Path outputFile = createOutputFile();
    // Create a test job.
    final Job job = setupJob(
        "testMapJobWithFilter",
        outputFile,
        TestMapper.class,
        null, // reducer class
        null, // start key
        null, // limit key
        filter);

    // Run the job.
    assertTrue("Hadoop job failed", job.waitForCompletion(true));

    // Check to make sure output exists.
    final FileSystem fs = FileSystem.get(job.getConfiguration());
    assertTrue(fs.exists(outputFile.getParent()));

    // Verify that the output matches what's expected.
    final FSDataInputStream in = fs.open(outputFile);
    final Set<String> actual = Sets.newHashSet(IOUtils.toString(in).trim().split("\n"));
    final Set<String> expected = Sets.newHashSet("usermail.example.com\tAaron Kimball");
    assertEquals("Result of job wasn't what was expected", expected, actual);

    // Clean up.
    fs.delete(outputFile.getParent(), true);

    IOUtils.closeQuietly(in);
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973
  }

  /** Test KijiTableInputFormat in a MapReduce job. */
  @Test
  public void testMapReduceJob() throws Exception {
    final Path outputFile = createOutputFile();
    // Create a test job.
    final Job job = setupJob(
        "testMapReduceJob",
        outputFile,
        TestMapper.class,
        TestReducer.class,
        null,  // start key
        null,  // limit key
        null); // filter

    // Run the job.
    assertTrue("Hadoop job failed", job.waitForCompletion(true));

    // Check to make sure output exists.
    final FileSystem fs = FileSystem.get(job.getConfiguration());
    assertTrue(fs.exists(outputFile.getParent()));

    // Verify that the output matches what's expected.
    final FSDataInputStream in = fs.open(outputFile);
    final Set<String> output = Sets.newHashSet(IOUtils.toString(in).trim().split("\n"));
    final ImmutableMap.Builder<String, Set<String>> builder = ImmutableMap.builder();
    for (String line : output) {
      final String[] keyValue = line.split("\t");
      final String emailDomain = keyValue[0];
      final Set<String> names = Sets.newHashSet(keyValue[1].split(","));

      builder.put(emailDomain, names);
    }
    final Map<String, Set<String>> actual = builder.build();
    final Map<String, Set<String>> expected = ImmutableMap.<String, Set<String>>builder()
        .put("usermail.example.com", Sets.newHashSet(
            "Aaron Kimball", "Christophe Bisciglia", "Kiyan Ahmadizadeh", "Garrett Wu"))
        .put("gmail.com", Sets.newHashSet("John Doe", "Jane Doe"))
        .build();
    assertEquals("Result of job wasn't what was expected", expected, actual);

    // Clean up.
    fs.delete(outputFile.getParent(), true);

    IOUtils.closeQuietly(in);
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973
  }
}
