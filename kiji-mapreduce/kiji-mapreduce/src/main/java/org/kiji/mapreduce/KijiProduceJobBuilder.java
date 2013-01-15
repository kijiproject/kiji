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

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.map.KijiMultithreadedMapper;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.mapreduce.mapper.ProduceMapper;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.mapreduce.util.KijiProducers;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;

/** Builds jobs that run a producer over a Kiji table. */
public class KijiProduceJobBuilder extends KijiTableInputJobBuilder<KijiProduceJobBuilder> {
  /** The default number of threads per mapper to use for running producers. */
  private static final int DEFAULT_NUM_THREADS_PER_MAPPER = 1;

  /** The class of the producer to run. */
  private Class<? extends KijiProducer> mProducerClass;
  /** The number of threads per mapper to use for running producers. */
  private int mNumThreadsPerMapper;

  /** The producer instance. */
  private KijiProducer mProducer;
  /** The mapper instance. */
  private KijiMapper mMapper;
  /** The reducer instance. */
  private KijiReducer mReducer;

  /** The data request for the job's table input. */
  private KijiDataRequest mDataRequest;

  /** Constructs a builder for jobs that run a Kiji producer over a Kiji table. */
  public KijiProduceJobBuilder() {
    mProducerClass = null;
    mNumThreadsPerMapper = DEFAULT_NUM_THREADS_PER_MAPPER;

    mProducer = null;
    mMapper = null;
    mReducer = null;

    mDataRequest = null;
  }

  /**
   * Configures the job with the Kiji producer to run.
   *
   * @param producerClass The producer class.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiProduceJobBuilder withProducer(Class<? extends KijiProducer> producerClass) {
    mProducerClass = producerClass;
    return this;
  }

  /**
   * Sets the number of threads to use for running the producer in parallel.
   *
   * <p>You may use this setting to run multiple instances of your producer in parallel
   * within each map task of the job.  This may useful for increasing throughput when your
   * producer is not CPU bound.</p>
   *
   * @param numThreads The number of produce-runner threads to use per mapper.
   * @return This build instance so you may chain configuration method calls.
   */
  public KijiProduceJobBuilder withNumThreads(int numThreads) {
    if (numThreads < 1) {
      throw new IllegalArgumentException("numThreads must be positive.");
    }
    mNumThreadsPerMapper = numThreads;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    final Configuration conf = job.getConfiguration();

    // Construct the producer instance.
    if (null == mProducerClass) {
      throw new JobConfigurationException("Must specify a producer.");
    }
    mProducer = ReflectionUtils.newInstance(mProducerClass, job.getConfiguration());
    mDataRequest = mProducer.getDataRequest();

    // Serialize the producer class name into the job configuration.
    conf.setClass(KijiProducer.CONF_PRODUCER_CLASS, mProducerClass, KijiProducer.class);

    // Configure the mapper and reducer to use.
    Preconditions.checkState(getJobOutput() instanceof KijiTableMapReduceJobOutput);
    // Write to the table, but make sure the output table is the same as the input table.
    if (!getInputTable().equals(((KijiTableMapReduceJobOutput) getJobOutput()).getTable())) {
      throw new JobConfigurationException("Output table must be the same as the input table.");
    }

    // Producers should output to HFiles.
    mMapper = new ProduceMapper();
    mReducer = new IdentityReducer<Object, Object>();

    job.setJobName("Kiji produce: " + mProducerClass.getSimpleName());

    // Configure the table input job.
    super.configureJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected void configureMapper(Job job) throws IOException {
    super.configureMapper(job);

    // Configure map-parallelism if configured.
    if (mNumThreadsPerMapper > 1) {
      @SuppressWarnings("unchecked")
      Class<? extends Mapper<EntityId, KijiRowData, Object, Object>> childMapperClass
          = (Class<? extends Mapper<EntityId, KijiRowData, Object, Object>>) mMapper.getClass();
      KijiMultithreadedMapper.setMapperClass(job, childMapperClass);
      KijiMultithreadedMapper.setNumberOfThreads(job, mNumThreadsPerMapper);
      job.setMapperClass(KijiMultithreadedMapper.class);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    return mProducer.getRequiredStores();
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJob build(Job job) {
    return new KijiMapReduceJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected KijiDataRequest getDataRequest() {
    return mDataRequest;
  }

  /** {@inheritDoc} */
  @Override
  protected void validateAgainstTableLayout(KijiTableLayout tableLayout) throws IOException {
    super.validateAgainstTableLayout(tableLayout);

    // Validate the output column the producer will write to (make sure it exists).
    try {
      KijiProducers.validateOutputColumn(mProducer, tableLayout);
    } catch (InvalidLayoutException e) {
      throw new InternalKijiError(
          "Invalid table layout found while configuring a job: " + tableLayout.toString()
          + " [Error: " + e.getMessage() + "]");
    } catch (KijiProducerOutputException e) {
      throw new JobConfigurationException(
          "Producer is configured to write a column that does not exist in the input table: "
          + e.getMessage());
    }
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapper getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer getCombiner() {
    // Producers can't have combiners.
    return null;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer getReducer() {
    return mReducer;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<?> getJarClass() {
    return mProducerClass;
  }
}
