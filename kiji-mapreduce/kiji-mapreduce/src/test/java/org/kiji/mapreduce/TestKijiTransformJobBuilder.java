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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.kvstore.EmptyKeyValueStore;
import org.kiji.mapreduce.kvstore.SeqFileKeyValueStore;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.util.Resources;

public class TestKijiTransformJobBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(TestKijiTransformJobBuilder.class);

  /** A line count mapper for testing. */
  public static class MyMapper extends KijiBaseMapper<LongWritable, Text, Text, IntWritable>
      implements KeyValueStoreClient {

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      context.write(line, new IntWritable(1));
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return IntWritable.class;
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return Collections.<String, KeyValueStore<?, ?>>singletonMap("mapperMap",
          new EmptyKeyValueStore<String, Object>());
    }
  }

  /** A sum reducer for testing. */
  public static class MyReducer extends KijiBaseReducer<Text, IntWritable, Text, IntWritable>
      implements KeyValueStoreClient {
    @Override
    protected void reduce(Text line, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(line, new IntWritable(sum));
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return Text.class;
    }

    @Override
    public Class<?> getOutputValueClass() {
      return IntWritable.class;
    }

    @Override
    public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
      return Collections.<String, KeyValueStore<?, ?>>singletonMap("reducerMap",
          new EmptyKeyValueStore<String, Object>());
    }
  }

  /** WIBI-1170. Must supply a kiji configuration object. */
  @Test(expected=JobConfigurationException.class)
  public void testSupplyKijiConfig() throws Exception {
    new KijiTransformJobBuilder()
        // .withKijiConfiguration(new KijiConfiguration(new Configuration, "foo"))
        .withInput(new TextMapReduceJobInput(new Path("/path/to/my/input")))
        .withMapper(MyMapper.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("/path/to/my/output"), 16))
        .build();
  }

  @Test
  public void testBuild() throws Exception {
    LOG.info("Configuring job...");
    KijiTransformJobBuilder builder = new KijiTransformJobBuilder()
        .withKijiConfiguration(new KijiConfiguration(new Configuration(), "foo"))
        .withInput(new TextMapReduceJobInput(new Path("/path/to/my/input")))
        .withMapper(MyMapper.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("/path/to/my/output"), 16));

    LOG.info("Building job...");
    MapReduceJob job = builder.build();

    LOG.info("Verifying job configuration...");
    Job hadoopJob = job.getHadoopJob();
    assertEquals(TextInputFormat.class, hadoopJob.getInputFormatClass());
    assertEquals(MyMapper.class, hadoopJob.getMapperClass());
    assertEquals(MyReducer.class, hadoopJob.getReducerClass());
    assertEquals(16, hadoopJob.getNumReduceTasks());
    assertEquals(TextOutputFormat.class, hadoopJob.getOutputFormatClass());

    // KeyValueStore-specific checks here.
    Configuration confOut = hadoopJob.getConfiguration();
    assertEquals(2, confOut.getInt(KeyValueStore.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "0.class"));
    assertEquals("mapperMap",
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "0.name"));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "1.class"));
    assertEquals("reducerMap",
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "1.name"));
  }

  @Test
  public void testBuildWithXmlKVStores() throws Exception {
    // Test that we can override default configuration KeyValueStores from an XML file.
    LOG.info("Configuring job...");
    InputStream xmlStores = Resources.openSystemResource("org/kiji/mapreduce/test-kvstores.xml");
    KijiTransformJobBuilder builder = new KijiTransformJobBuilder()
        .withKijiConfiguration(new KijiConfiguration(new Configuration(), "foo"))
        .withInput(new TextMapReduceJobInput(new Path("/path/to/my/input")))
        .withMapper(MyMapper.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("/path/to/my/output"), 16))
        .withStoreBindings(xmlStores);
    xmlStores.close();

    LOG.info("Building job...");
    MapReduceJob job = builder.build();

    LOG.info("Verifying job configuration...");
    Job hadoopJob = job.getHadoopJob();
    assertEquals(TextInputFormat.class, hadoopJob.getInputFormatClass());
    assertEquals(MyMapper.class, hadoopJob.getMapperClass());
    assertEquals(MyReducer.class, hadoopJob.getReducerClass());
    assertEquals(16, hadoopJob.getNumReduceTasks());
    assertEquals(TextOutputFormat.class, hadoopJob.getOutputFormatClass());

    // KeyValueStore-specific checks here.
    // We override mapperMap with a SeqFileKeyValueStore.
    Configuration confOut = hadoopJob.getConfiguration();
    assertEquals(2, confOut.getInt(KeyValueStore.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(SeqFileKeyValueStore.class.getName(),
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "0.class"));
    assertEquals("mapperMap",
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "0.name"));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "1.class"));
    assertEquals("reducerMap",
        confOut.get(KeyValueStore.CONF_KEY_VALUE_BASE + "1.name"));
  }

  @Test(expected=JobConfigurationException.class)
  public void testMissingConfiguration() throws IOException {
    KijiTransformJobBuilder builder = new KijiTransformJobBuilder()
        .withInput(new TextMapReduceJobInput(new Path("/path/to/my/input")))
        .withMapper(MyMapper.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("/path/to/my/output"), 16));

    // We didn't call withConfiguration(), so this should throw an exception when we build.
    builder.build();
  }
}
