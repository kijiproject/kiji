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

package org.kiji.mapreduce.framework;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.mapreduce.JobConfigurationException;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestException;
import org.kiji.schema.KijiDataRequestValidator;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.util.ResourceUtils;

/**
 * Base class for MapReduce jobs that use a Kiji table as input.
 *
 * @param <T> Type of the builder class.
 */
@ApiAudience.Framework
@Inheritance.Sealed
public abstract class KijiTableInputJobBuilder<T extends KijiTableInputJobBuilder<T>>
    extends MapReduceJobBuilder<T> {
  /** The table to use as input for the job. */
  private KijiURI mInputTableURI;

  /** The entity id of the start row (inclusive). */
  private EntityId mStartRow;

  /** The entity id of the limit row (exclusive). */
  private EntityId mLimitRow;

  /** A row filter that specifies rows to exclude from the scan (optional, so may be null). */
  private KijiRowFilter mRowFilter;

  /** Constructs a builder for jobs that use a Kiji table as input. */
  protected KijiTableInputJobBuilder() {
    mInputTableURI = null;
    mStartRow = null;
    mLimitRow = null;
    mRowFilter = null;
  }

  /**
   * Configures the job input table.
   *
   * @param input The job input table.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public T withJobInput(KijiTableMapReduceJobInput input) {
    mInputTableURI = input.getInputTableURI();
    if (input.getRowOptions() != null) {
      mStartRow = input.getRowOptions().getStartRow();
      mLimitRow = input.getRowOptions().getLimitRow();
      mRowFilter = input.getRowOptions().getRowFilter();
    }
    return (T) this;
  }

  /**
   * Configures the job with the Kiji table to use as input.
   *
   * @param inputTableURI The Kiji table to use as input for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public T withInputTable(KijiURI inputTableURI) {
    mInputTableURI = inputTableURI;
    return (T) this;
  }

  /**
   * Configures the job to process rows after and including an entity id.
   *
   * @param entityId The entity id of the first row input.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public T withStartRow(EntityId entityId) {
    mStartRow = entityId;
    return (T) this;
  }

  /**
   * Configures the job to process rows before an entity id.
   *
   * @param entityId The entity id of the first row to exclude from the input.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public T withLimitRow(EntityId entityId) {
    mLimitRow = entityId;
    return (T) this;
  }

  /**
   * Configures the job to exclude rows not accepted by a row filter.
   *
   * @param rowFilter A filter that specifies which rows to exclude from the input table.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public T withFilter(KijiRowFilter rowFilter) {
    mRowFilter = rowFilter;
    return (T) this;
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Configure the input, mapper, combiner, and reducer, output.
    super.configureJob(job);

    // Validate the Kiji data request against the current table layout:
    final Kiji kiji = Kiji.Factory.open(mInputTableURI);
    try {
      final KijiTable table = kiji.openTable(mInputTableURI.getTable());
      try {
        validateInputTable(table);
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJobInput getJobInput() {
    final KijiTableMapReduceJobInput.RowOptions rowOptions =
        new KijiTableMapReduceJobInput.RowOptions(mStartRow, mLimitRow, mRowFilter);
    return new KijiTableMapReduceJobInput(mInputTableURI, getDataRequest(), rowOptions);
  }

  /** @return the URI of the input table. */
  protected KijiURI getInputTableURI() {
    return mInputTableURI;
  }

  /**
   * Subclasses must override this to provide a Kiji data request for the input table.
   *
   * @return the Kiji data request to configure the input table scanner with.
   */
  protected abstract KijiDataRequest getDataRequest();

  /**
   * Validates the input table.
   *
   * Sub-classes may override this method to perform additional validation requiring an active
   * connection to the input table.
   *
   * @param table Input table.
   * @throws IOException on I/O error.
   */
  protected void validateInputTable(KijiTable table) throws IOException {
    try {
      KijiDataRequestValidator.validatorForLayout(table.getLayout()).validate(getDataRequest());
    } catch (KijiDataRequestException kdre) {
      throw new JobConfigurationException("Invalid data request: " + kdre.getMessage());
    }
  }
}
