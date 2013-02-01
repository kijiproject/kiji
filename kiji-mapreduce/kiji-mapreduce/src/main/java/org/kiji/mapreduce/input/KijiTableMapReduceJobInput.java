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

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.GenericTableMapReduceUtil;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiConfKeys;
import org.kiji.mapreduce.KijiTableInputFormat;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiTable;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.filter.KijiRowFilterApplicator;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HBaseDataRequestAdapter;
import org.kiji.schema.layout.InvalidLayoutException;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Input for a MapReduce job that uses data from a Kiji table.
 *
 * <p>The input is Kiji table column data as specified by a <code>KijiDataRequest</code>.
 * Input may be read from the entire table, or from a range of rows using a start and end
 * key.</p>
 */
@ApiAudience.Public
public final class KijiTableMapReduceJobInput extends MapReduceJobInput {
  /** The table to read the job input from. */
  private final KijiTable mInputTable;

  /** Specifies which columns and versions of cells to read from the table. */
  private final KijiDataRequest mDataRequest;

  /** Optional settings that specify which rows from the input table should be included. */
  private final RowOptions mRowOptions;

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

  /**
   * Constructs job input from column data in a Kiji table over a row range.
   *
   * @param inputTable The table to read input from.
   * @param dataRequest Specifies the columns and versions of cells to read from the table.
   * @param rowOptions Specifies optional settings for restricting the input from the
   *     table to some subset of the rows.
   */
  public KijiTableMapReduceJobInput(
      KijiTable inputTable, KijiDataRequest dataRequest, RowOptions rowOptions) {
    // TODO(WIBI-1667): Validate these arguments.
    mInputTable = inputTable;
    mDataRequest = dataRequest;
    mRowOptions = rowOptions;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // Configure the input format class.
    super.configure(job);
    final Configuration conf = job.getConfiguration();

    final String dataRequestB64 =
        Base64.encodeBase64String(SerializationUtils.serialize(mDataRequest));
    conf.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, dataRequestB64);
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, mInputTable.getURI().toString());

    // Get the name of the HBase table that stores the Kiji table data.
    final String hbaseTableName = KijiManagedHBaseTableName.getKijiTableName(
        mInputTable.getKiji().getURI().getInstance(), mInputTable.getName()).toString();

    // Create the HBase scan configured to read the appropriate input from the Kiji table.
    final Scan configuredScan = createConfiguredScan(mInputTable.getLayout());

    // Configure the table input using HBase.
    GenericTableMapReduceUtil.initTableInput(hbaseTableName, configuredScan, job);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat<?, ?>> getInputFormatClass() {
    return KijiTableInputFormat.class;
  }

  /**
   * Constructs an HBase Scan object configured to provide the appropriate data from the HBase
   * table to the MapReduce job according to the data request and row range.
   *
   * @param tableLayout The layout of the table to use as input.
   * @return An HBase Scan descriptor that reads the data from the HTable.
   * @throws IOException If there is an error.
   */
  private Scan createConfiguredScan(KijiTableLayout tableLayout) throws IOException {
    // Build the HBase Scan from the data request.
    HBaseDataRequestAdapter hbaseDataRequestAdapter = new HBaseDataRequestAdapter(mDataRequest);
    Scan scan;
    try {
      scan = hbaseDataRequestAdapter.toScan(tableLayout);
    } catch (InvalidLayoutException e) {
      throw new InternalKijiError("Encountered an invalid table layout while configuring a job");
    }
    configureScanWithRowOptions(scan);
    return scan;
  }

  /**
   * Configure the scan according to the row options.
   *
   * @param scan The HBase scan descriptor to configure.
   * @throws IOException If there is an error.
   */
  private void configureScanWithRowOptions(Scan scan) throws IOException {
    if (null != mRowOptions.getStartRow()) {
      scan.setStartRow(mRowOptions.getStartRow().getHBaseRowKey());
    }
    if (null != mRowOptions.getLimitRow()) {
      scan.setStopRow(mRowOptions.getLimitRow().getHBaseRowKey());
    }
    if (null != mRowOptions.getRowFilter()) {
      KijiTableLayout tableLayout = mInputTable.getLayout();
      KijiSchemaTable schemaTable = mInputTable.getKiji().getSchemaTable();
      new KijiRowFilterApplicator(mRowOptions.getRowFilter(), tableLayout, schemaTable)
          .applyTo(scan);
    }
  }
}
