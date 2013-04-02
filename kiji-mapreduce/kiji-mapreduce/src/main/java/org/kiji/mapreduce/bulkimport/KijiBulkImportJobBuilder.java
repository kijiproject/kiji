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

package org.kiji.mapreduce.bulkimport;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.bulkimport.impl.BulkImportMapper;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.framework.MapReduceJobBuilder;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.mapreduce.output.impl.KijiTableMapReduceJobOutput;
import org.kiji.mapreduce.reducer.IdentityReducer;

/** Builds a job that runs a KijiBulkImporter to import data into a Kiji table. */
@ApiAudience.Public
public final class KijiBulkImportJobBuilder
    extends MapReduceJobBuilder<KijiBulkImportJobBuilder> {

  /** The class of the bulk importer to run. */
  @SuppressWarnings("rawtypes")
  private Class<? extends KijiBulkImporter> mBulkImporterClass;

  /** The bulk importer instance. */
  private KijiBulkImporter<?, ?> mBulkImporter;
  /** The mapper instance to run (which runs the bulk importer inside it). */
  private KijiMapper<?, ?, ?, ?> mMapper;
  /** The reducer instance to run (may be null). */
  private KijiReducer<?, ?, ?, ?> mReducer;

  /** The job input. */
  private MapReduceJobInput mJobInput;

  /** Job output must be a Kiji table. */
  private KijiTableMapReduceJobOutput mJobOutput;

  /** Constructs a builder for jobs that run a KijiBulkImporter. */
  private KijiBulkImportJobBuilder() {
    mBulkImporterClass = null;

    mBulkImporter = null;
    mMapper = null;
    mReducer = null;

    mJobInput = null;
    mJobOutput = null;
  }

  /**
   * Creates a new builder for Kiji bulk import jobs.
   *
   * @return a new Kiji bulk import job builder.
   */
  public static KijiBulkImportJobBuilder create() {
    return new KijiBulkImportJobBuilder();
  }

  /**
   * Configures the job with input.
   *
   * @param jobInput The input for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiBulkImportJobBuilder withInput(MapReduceJobInput jobInput) {
    mJobInput = jobInput;
    return this;
  }

  /**
   * Configures the bulk-importer output Kiji table.
   *
   * @param jobOutput Bulk importer must output to a Kiji table.
   * @return this builder.
   */
  public KijiBulkImportJobBuilder withOutput(KijiTableMapReduceJobOutput jobOutput) {
    mJobOutput = jobOutput;
    super.withOutput(jobOutput);
    return this;
  }

  /**
   * Configures the job output.
   *
   * @param jobOutput The output for the job.
   *     Bulk importer must output to a Kiji table.
   * @return This builder instance so you may chain configuration method calls.
   *
   * {@inheritDoc}
   */
  @Override
  public KijiBulkImportJobBuilder withOutput(MapReduceJobOutput jobOutput) {
    if (!(jobOutput instanceof KijiTableMapReduceJobOutput)) {
      throw new JobConfigurationException(String.format(
          "Invalid job output %s: expecting %s or %s",
          jobOutput.getClass().getName(),
          DirectKijiTableMapReduceJobOutput.class.getName(),
          HFileMapReduceJobOutput.class.getName()));
    }
    return withOutput((KijiTableMapReduceJobOutput) jobOutput);
  }

  /**
   * Configures the job with a bulk importer to run in the map phase.
   *
   * @param bulkImporterClass The bulk importer class to use in the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("rawtypes")
  public KijiBulkImportJobBuilder withBulkImporter(
      Class<? extends KijiBulkImporter> bulkImporterClass) {
    mBulkImporterClass = bulkImporterClass;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    final Configuration conf = job.getConfiguration();

    // Store the name of the the importer to use in the job configuration so the mapper can
    // create instances of it.
    // Construct the bulk importer instance.
    if (null == mBulkImporterClass) {
      throw new JobConfigurationException("Must specify a bulk importer.");
    }
    conf.setClass(
        KijiConfKeys.KIJI_BULK_IMPORTER_CLASS, mBulkImporterClass, KijiBulkImporter.class);

    mJobOutput.configure(job);

    // Configure the mapper and reducer. This part depends on whether we're going to write
    // to HFiles or directly to the table.
    configureJobForHFileOutput(job);

    job.setJobName("Kiji bulk import: " + mBulkImporterClass.getSimpleName());

    mBulkImporter = ReflectionUtils.newInstance(mBulkImporterClass, conf);

    // Configure the MapReduce job (requires mBulkImporter to be set properly):
    super.configureJob(job);
  }

  /**
   * Configures the job settings specific to writing HFiles.
   *
   * @param job The job to configure.
   */
  protected void configureJobForHFileOutput(Job job) {
    // Construct the mapper instance that runs the importer.
    mMapper = new BulkImportMapper<Object, Object>();

    // Don't need to do anything during the Reducer, but we need to run the reduce phase
    // so the KeyValue records output from the map phase get sorted.
    mReducer = new IdentityReducer<Object, Object>();
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapReduceJob build(Job job) {
    return KijiMapReduceJob.create(job);
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, KeyValueStore<?, ?>> getRequiredStores() throws IOException {
    return mBulkImporter.getRequiredStores();
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJobInput getJobInput() {
    return mJobInput;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiMapper<?, ?, ?, ?> getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer<?, ?, ?, ?> getCombiner() {
    // Use no combiner.
    return null;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer<?, ?, ?, ?> getReducer() {
    return mReducer;
  }

  /** {@inheritDoc} */
  @Override
  protected Class<?> getJarClass() {
    return mBulkImporterClass;
  }
}
