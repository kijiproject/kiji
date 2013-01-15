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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.mapper.BulkImportMapper;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;
import org.kiji.mapreduce.reducer.IdentityReducer;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.KijiTableLayout;

/** Builds a job that runs a KijiBulkImporter to import data into a Kiji table. */
public class KijiBulkImportJobBuilder extends KijiMapReduceJobBuilder<KijiBulkImportJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiBulkImportJobBuilder.class);

  /** The class of the bulk importer to run. */
  private Class<? extends KijiBulkImporter> mBulkImporterClass;

  /** The bulk importer instance. */
  private KijiBulkImporter<?, ?> mBulkImporter;
  /** The mapper instance to run (which runs the bulk importer inside it). */
  private KijiMapper mMapper;
  /** The reducer instance to run (may be null). */
  private KijiReducer mReducer;

  /** The job input. */
  private MapReduceJobInput mJobInput;
  /** The target kiji table for the import. */
  private HBaseKijiTable mOutputTable;

  /** Constructs a builder for jobs that run a KijiBulkImporter. */
  public KijiBulkImportJobBuilder() {
    mBulkImporterClass = null;

    mBulkImporter = null;
    mMapper = null;
    mReducer = null;

    mJobInput = null;
    mOutputTable = null;
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
   * Configures the job with a bulk importer to run in the map phase.
   *
   * @param bulkImporterClass The bulk importer class to use in the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  public KijiBulkImportJobBuilder withBulkImporter(
      Class<? extends KijiBulkImporter> bulkImporterClass) {
    mBulkImporterClass = bulkImporterClass;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Store the name of the the importer to use in the job configuration so the mapper can
    // create instances of it.
    job.getConfiguration().setClass(
        KijiBulkImporter.CONF_BULK_IMPORTER_CLASS, mBulkImporterClass, KijiBulkImporter.class);

    // Make sure the job output format is a KijiTableMapReduceJobOutput or a subclass of it.
    MapReduceJobOutput jobOutput = getJobOutput();
    if (!(jobOutput instanceof KijiTableMapReduceJobOutput)) {
      throw new JobConfigurationException(
          "Job output must be a KijiTableMapReduceJobOutput or a subclass.");
    }
    mOutputTable = HBaseKijiTable.downcast(((KijiTableMapReduceJobOutput) jobOutput).getTable());
    jobOutput.configure(job);

    // Construct the bulk importer instance.
    if (null == mBulkImporterClass) {
      throw new JobConfigurationException("Must specify a bulk importer.");
    }
    mBulkImporter = ReflectionUtils.newInstance(mBulkImporterClass, job.getConfiguration());

    // Configure the mapper and reducer. This part depends on whether we're going to write
    // to HFiles or directly to the table.
    configureJobForHFileOutput(job);

    job.setJobName("Kiji bulk import: " + mBulkImporterClass.getSimpleName());

    // Configure the MapReduce job.
    super.configureJob(job);
  }

  /**
   * Configures the job settings specific to writing HFiles.
   *
   * @param job The job to configure.
   */
  protected void configureJobForHFileOutput(Job job) {
    // Construct the mapper instance that runs the importer.
    mMapper = new BulkImportMapper<Object, Object, Object, Object>();

    // Don't need to do anything during the Reducer, but we need to run the reduce phase
    // so the KeyValue records output from the map phase get sorted.
    mReducer = new IdentityReducer<Object, Object>();
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJob build(Job job) {
    return new KijiMapReduceJob(job);
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    // We know that this job's output is on its way to a Kiji table, so any valid
    // MapReduceJobOutput that is configured on this builder must have a reference to that
    // KijiTable.  We will use the configuration object associated with that KijiTable's
    // Kiji instance.
    MapReduceJobOutput configuredJobOutput = getJobOutput();
    if (!(configuredJobOutput instanceof KijiTableMapReduceJobOutput)) {
      throw new JobConfigurationException(
          "Incompatible job output (must be a subclass of "
          + KijiTableMapReduceJobOutput.class.getName() + ".");
    }
    KijiTable targetTable = ((KijiTableMapReduceJobOutput) configuredJobOutput).getTable();
    return targetTable.getKiji().getConf();
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
  protected KijiMapper getMapper() {
    return mMapper;
  }

  /** {@inheritDoc} */
  @Override
  protected KijiReducer getCombiner() {
    // Use no combiner.
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
    return mBulkImporterClass;
  }

  /** {@inheritDoc} */
  @Override
  protected Kiji getKiji() {
    return mOutputTable.getKiji();
  }

  /** {@inheritDoc} */
  @Override
  protected KijiTableLayout getTableLayout() {
    return mOutputTable.getLayout();
  }
}
