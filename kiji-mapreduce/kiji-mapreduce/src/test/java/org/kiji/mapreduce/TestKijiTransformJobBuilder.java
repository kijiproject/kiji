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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreClient;
import org.kiji.mapreduce.kvstore.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;
import org.kiji.mapreduce.kvstore.lib.EmptyKeyValueStore;
import org.kiji.mapreduce.kvstore.lib.SeqFileKeyValueStore;
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
      return RequiredStores.just("mapperMap", EmptyKeyValueStore.get());
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
      return RequiredStores.with("reducerMap", EmptyKeyValueStore.get());
    }
  }

  /** WIBI-1170. Must supply a kiji configuration object. */
  @Test(expected=JobConfigurationException.class)
  public void testSupplyKijiConfig() throws Exception {
    KijiTransformJobBuilder.create()
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
    KijiTransformJobBuilder builder = KijiTransformJobBuilder.create()
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
    assertEquals(2, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("mapperMap",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "1."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("reducerMap",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "1."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }

  @Test
  public void testBuildWithXmlKVStores() throws Exception {
    // Test that we can override default configuration KeyValueStores from an XML file.
    LOG.info("Configuring job...");
    InputStream xmlStores = Resources.openSystemResource("org/kiji/mapreduce/test-kvstores.xml");
    KijiTransformJobBuilder builder = KijiTransformJobBuilder.create()
        .withKijiConfiguration(new KijiConfiguration(new Configuration(), "foo"))
        .withInput(new TextMapReduceJobInput(new Path("/path/to/my/input")))
        .withMapper(MyMapper.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("/path/to/my/output"), 16))
        .withStoreBindings(xmlStores);
    xmlStores.close();

    // This file needs to exist before we build the job, or else
    // we can't build the job; it's referenced by a key-value store that checks
    // for its presence.
    File tmpFile = new File("/tmp/foo.seq");
    if (tmpFile.createNewFile()) {
      // We created this temp file, we're responsible for deleting it.
      tmpFile.deleteOnExit();
    }

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
    assertEquals(2, confOut.getInt(KeyValueStoreConfigSerializer.CONF_KEY_VALUE_STORE_COUNT, 0));
    assertEquals(SeqFileKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("mapperMap",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "0."
        + KeyValueStoreConfigSerializer.CONF_NAME));
    assertEquals(EmptyKeyValueStore.class.getName(),
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "1."
        + KeyValueStoreConfigSerializer.CONF_CLASS));
    assertEquals("reducerMap",
        confOut.get(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE + "1."
        + KeyValueStoreConfigSerializer.CONF_NAME));
  }

  @Test(expected=JobConfigurationException.class)
  public void testMissingConfiguration() throws IOException {
    KijiTransformJobBuilder builder = KijiTransformJobBuilder.create()
        .withInput(new TextMapReduceJobInput(new Path("/path/to/my/input")))
        .withMapper(MyMapper.class)
        .withReducer(MyReducer.class)
        .withOutput(new TextMapReduceJobOutput(new Path("/path/to/my/output"), 16));

    // We didn't call withConfiguration(), so this should throw an exception when we build.
    builder.build();
  }
}
