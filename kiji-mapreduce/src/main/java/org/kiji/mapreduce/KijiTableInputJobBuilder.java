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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestException;
import org.kiji.schema.KijiDataRequestValidator;
import org.kiji.schema.KijiTable;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Builds MapReduce jobs that use a Kiji table as input.  Users should use a concrete
 * implementation such as {@link org.kiji.mapreduce.KijiGatherJobBuilder}.
 *
 * @param <T> The type of the builder class.
 */
public abstract class KijiTableInputJobBuilder<T extends KijiTableInputJobBuilder<T>>
    extends KijiMapReduceJobBuilder<T> {
  /** The table to use as input for the job. */
  private KijiTable mInputTable;
  /** The entity id of the start row (inclusive). */
  private EntityId mStartRow;
  /** The entity id of the limit row (exclusive). */
  private EntityId mLimitRow;
  /** A row filter that specifies rows to exclude from the scan (optional, so may be null). */
  private KijiRowFilter mRowFilter;

  /** Constructs a builder for jobs that use a Kiji table as input. */
  protected KijiTableInputJobBuilder() {
    mInputTable = null;
    mStartRow = null;
    mLimitRow = null;
    mRowFilter = null;
  }

  /**
   * Configures the job with the Kiji table to use as input.
   *
   * @param inputTable The Kiji table to use as input for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public T withInputTable(KijiTable inputTable) {
    mInputTable = inputTable;
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
  public Configuration getConf() {
    return mInputTable.getKiji().getConf();
  }

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Perform validation against the table layout.
    validateAgainstTableLayout(getTableLayout());

    // Configure the input, mapper, combiner, and reducer, output.
    super.configureJob(job);
  }

  /** {@inheritDoc} */
  @Override
  protected MapReduceJobInput getJobInput() {
    KijiTableMapReduceJobInput.RowOptions rowOptions
        = new KijiTableMapReduceJobInput.RowOptions(mStartRow, mLimitRow, mRowFilter);
    return new KijiTableMapReduceJobInput(
        HBaseKijiTable.downcast(mInputTable), getDataRequest(), rowOptions);
  }

  /** {@inheritDoc} */
  @Override
  protected Kiji getKiji() {
    return mInputTable.getKiji();
  }

  /** {@inheritDoc} */
  @Override
  protected KijiTableLayout getTableLayout() {
    return HBaseKijiTable.downcast(mInputTable).getLayout();
  }

  /**
   * Provides access to the input table the built job will read from.
   *
   * @return The table the built job will read from.
   */
  protected KijiTable getInputTable() {
    return mInputTable;
  }

  /**
   * Gets the object that describes what data should be read from the Kiji table for the
   * input of the MapReduce job.
   *
   * @return The kiji data request.
   */
  protected abstract KijiDataRequest getDataRequest();

  /**
   * Checks that the job can be run against the table's layout.  Throws an exception if not.
   *
   * @param tableLayout The layout of the table the job will use as input.
   * @throws IOException If the job cannot be run over the input table because the layout
   *     is incompatible.
   */
  protected void validateAgainstTableLayout(KijiTableLayout tableLayout) throws IOException {
    // Validate the data request.
    KijiDataRequest dataRequest = getDataRequest();
    KijiDataRequestValidator validator = new KijiDataRequestValidator(dataRequest);
    try {
      validator.validate(tableLayout);
    } catch (InvalidLayoutException e) {
      throw new InternalKijiError(
          "Invalid table layout found while configuring a job: " + tableLayout.toString()
          + " [Error: " + e.getMessage() + "]");
    } catch (KijiDataRequestException e) {
      throw new JobConfigurationException("Invalid data request: " + e.getMessage());
    }
  }
}
