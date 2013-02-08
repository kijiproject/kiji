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

package org.kiji.mapreduce.input;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.KijiTableInputFormat;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.tools.JobIOConfKeys;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.filter.KijiRowFilter;

/**
 * Input for a MapReduce job that uses data from a Kiji table.
 *
 * <p>The input is Kiji table column data as specified by a <code>KijiDataRequest</code>.
 * Input may be read from the entire table, or from a range of rows using a start and end
 * key.</p>
 */
@ApiAudience.Public
public final class KijiTableMapReduceJobInput extends MapReduceJobInput {
  /** URI of the input Kiji table. */
  private KijiURI mInputTableURI;

  /** Specifies which columns and versions of cells to read from the table. */
  private KijiDataRequest mDataRequest;

  /** Optional settings that specify which rows from the input table should be included. */
  private RowOptions mRowOptions;

  /**
   * Options that specify which rows from the input table should be included.
   *
   * <p>The settings here are used conjunctively with an AND operator.  In other words, a
   * row will be included if and only if it is:
   *   <ul>
   *     <li>lexicographically equal to or after the start row, <em>and</em></li>
   *     <li>lexicographically before the limit row, <em>and</em></li>
   *     <li>accepted by the row filter.</li>
   *   </ul>
   * </p>
   */
  public static class RowOptions {
    /**
     * The start of the row range to read from the table (inclusive).  Use null to include
     * the first row in the table.
     */
    private final EntityId mStartRow;

    /**
     * The end of the row range to read from the table (exclusive).  Use null to include
     * the last row in the table.
     */
    private final EntityId mLimitRow;

    /**
     * A row filter that specifies whether a row from the table should be excluded.  Use
     * null to include all rows.
     */
    private final KijiRowFilter mRowFilter;

    /** Constructs options with default settings to include all the rows of the table. */
    public RowOptions() {
      this(null, null, null);
    }

    /**
     * Creates a new <code>RowOptions</code> instance.
     *
     * @param startRow The start row (inclusive).
     * @param limitRow The limit row (exclusive).
     * @param rowFilter A row filter.
     */
    public RowOptions(EntityId startRow, EntityId limitRow, KijiRowFilter rowFilter) {
      mStartRow = startRow;
      mLimitRow = limitRow;
      mRowFilter = rowFilter;
    }

    /** @return The start row (inclusive, may be null to include the first row of the table). */
    public EntityId getStartRow() {
      return mStartRow;
    }

    /** @return The limit row (exclusive, may be null to include the last row of the table). */
    public EntityId getLimitRow() {
      return mLimitRow;
    }

    /** @return The row filter (may be null). */
    public KijiRowFilter getRowFilter() {
      return mRowFilter;
    }
  }

  /** Default constructor. */
  public KijiTableMapReduceJobInput() {
  }

  /** {@inheritDoc} */
  @Override
  public void initialize(Map<String, String> params) throws IOException {
    mInputTableURI = KijiURI.newBuilder(params.get(JobIOConfKeys.TABLE_KEY)).build();
  }

  /**
   * Constructs job input from column data in a Kiji table over a row range.
   *
   * @param inputTableURI URI of the input table.
   * @param dataRequest Specifies the columns and versions of cells to read from the table.
   * @param rowOptions Specifies optional settings for restricting the input from the
   *     table to some subset of the rows.
   */
  public KijiTableMapReduceJobInput(
      KijiURI inputTableURI, KijiDataRequest dataRequest, RowOptions rowOptions) {
    mInputTableURI = inputTableURI;
    mDataRequest = dataRequest;
    mRowOptions = rowOptions;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // Configure the input format class.
    super.configure(job);
    final Configuration conf = job.getConfiguration();

    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, mInputTableURI.toString());
    final String dataRequestB64 =
        Base64.encodeBase64String(SerializationUtils.serialize(mDataRequest));
    conf.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, dataRequestB64);

    // TODO(KIJIMR-64): Serialize the row options (filters)
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat<?, ?>> getInputFormatClass() {
    return KijiTableInputFormat.class;
  }

  /** @return the input table URI. */
  public KijiURI getInputTableURI() {
    return mInputTableURI;
  }

  /** @return the row options. */
  public RowOptions getRowOptions() {
    return mRowOptions;
  }
}
