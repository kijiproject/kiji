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
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.impl.KijiTableSplit;
import org.kiji.schema.EntityId;
import org.kiji.schema.HBaseEntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.filter.KijiRowFilter;
import org.kiji.schema.hbase.HBaseScanOptions;
import org.kiji.schema.impl.HBaseKijiRowData;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.util.ResourceUtils;

/** InputFormat for Hadoop MapReduce jobs reading from a Kiji table. */
@ApiAudience.Framework
@ApiStability.Evolving
public final class KijiTableInputFormat
    extends InputFormat<EntityId, KijiRowData>
    implements Configurable {
  /** Configuration of this input format. */
  private Configuration mConf;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public RecordReader<EntityId, KijiRowData> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new KijiTableRecordReader(mConf);
  }

  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    final Configuration conf = context.getConfiguration();
    final KijiURI inputTableURI =
        KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
    final Kiji kiji = Kiji.Factory.open(inputTableURI, conf);
    try {
      final KijiTable table = kiji.openTable(inputTableURI.getTable());
      try {
        final byte[] htableName = HBaseKijiTable.downcast(table).getHTable().getTableName();
        final List<InputSplit> splits = Lists.newArrayList();
        byte[] scanStartKey = HConstants.EMPTY_START_ROW;
        if (null != conf.get(KijiConfKeys.KIJI_START_ROW_KEY)) {
          scanStartKey = Base64.decodeBase64(conf.get(KijiConfKeys.KIJI_START_ROW_KEY));
        }
        byte[] scanLimitKey = HConstants.EMPTY_END_ROW;
        if (null != conf.get(KijiConfKeys.KIJI_LIMIT_ROW_KEY)) {
          scanLimitKey = Base64.decodeBase64(conf.get(KijiConfKeys.KIJI_LIMIT_ROW_KEY));
        }

        for (KijiRegion region : table.getRegions()) {
          final byte[] regionStartKey = region.getStartKey();
          final byte[] regionEndKey = region.getEndKey();
          // Determine if the scan start and limit key fall into the region.
          // Logic was copied from o.a.h.h.m.TableInputFormatBase
          if ((scanStartKey.length == 0 || regionEndKey.length == 0
               || Bytes.compareTo(scanStartKey, regionEndKey) < 0)
             && (scanLimitKey.length == 0
               || Bytes.compareTo(scanLimitKey, regionStartKey) > 0)) {
            byte[] splitStartKey = (scanStartKey.length == 0
              || Bytes.compareTo(regionStartKey, scanStartKey) >= 0)
              ? regionStartKey : scanStartKey;
            byte[] splitEndKey = ((scanLimitKey.length == 0
              || Bytes.compareTo(regionEndKey, scanLimitKey) <= 0)
              && regionEndKey.length > 0)
              ? regionEndKey : scanLimitKey;

            // TODO(KIJIMR-65): For now pick the first available location (ie. region server),
            // if any.
            final String location =
              region.getLocations().isEmpty() ? null : region.getLocations().iterator().next();
            final TableSplit tableSplit =
              new TableSplit(htableName, splitStartKey, splitEndKey, location);
            splits.add(new KijiTableSplit(tableSplit));
          }
        }
        return splits;

      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /**
   * Configures a Hadoop M/R job to read from a given table.
   *
   * @param job Job to configure.
   * @param tableURI URI of the table to read from.
   * @param dataRequest Data request.
   * @param startRow Minimum row key to process. May be left null to indicate
   *     that scanning should start at the beginning of the table.
   * @param endRow Maximum row key to process. May be left null to indicate that
   *     scanning should continue to the end of the table.
   * @param filter Filter to use for scanning. May be left null.
   * @throws IOException on I/O error.
   */
  public static void configureJob(
      Job job,
      KijiURI tableURI,
      KijiDataRequest dataRequest,
      EntityId startRow,
      EntityId endRow,
      KijiRowFilter filter)
      throws IOException {
    Preconditions.checkNotNull(job, "job must not be null");
    Preconditions.checkNotNull(tableURI, "tableURI must not be null");
    Preconditions.checkNotNull(dataRequest, "dataRequest must not be null");

    final Configuration conf = job.getConfiguration();

    // TODO: Check for jars config:
    // GenericTableMapReduceUtil.initTableInput(hbaseTableName, scan, job);

    // Write all the required values to the job's configuration object.
    job.setInputFormatClass(KijiTableInputFormat.class);
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(dataRequest));
    conf.set(KijiConfKeys.KIJI_INPUT_DATA_REQUEST, serializedRequest);
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableURI.toString());
    if (null != startRow) {
      conf.set(KijiConfKeys.KIJI_START_ROW_KEY,
          Base64.encodeBase64String(startRow.getHBaseRowKey()));
    }
    if (null != endRow) {
      conf.set(KijiConfKeys.KIJI_LIMIT_ROW_KEY,
          Base64.encodeBase64String(endRow.getHBaseRowKey()));
    }
    if (null != filter) {
      conf.set(KijiConfKeys.KIJI_ROW_FILTER, filter.toJson().toString());
    }
  }

  /** Hadoop record reader for Kiji table rows. */
  public static final class KijiTableRecordReader
      extends RecordReader<EntityId, KijiRowData> {

    /** Data request. */
    private final KijiDataRequest mDataRequest;

    private Kiji mKiji = null;
    private KijiTable mTable = null;
    private KijiTableReader mReader = null;
    private KijiRowScanner mScanner = null;
    private Iterator<KijiRowData> mIterator = null;

    private KijiTableSplit mSplit = null;

    private HBaseKijiRowData mCurrentRow = null;

    /**
     * Creates a new RecordReader for this input format.
     *
     * Perform the actual reads from Kiji.
     *
     * @param conf Configuration for the target Kiji.
     */
    private KijiTableRecordReader(Configuration conf) {
      // Get data request from the job configuration.
      final String dataRequestB64 = conf.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST);
      Preconditions.checkNotNull(dataRequestB64, "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (KijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      assert split instanceof KijiTableSplit;
      mSplit = (KijiTableSplit) split;

      final Configuration conf = context.getConfiguration();
      final KijiURI inputURI =
          KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();

      // When using Kiji tables as an input to MapReduce jobs, turn off block caching.
      final HBaseScanOptions hBaseScanOptions = new HBaseScanOptions();
      hBaseScanOptions.setCacheBlocks(false);

      final KijiScannerOptions scannerOptions = new KijiScannerOptions()
          .setStartRow(HBaseEntityId.fromHBaseRowKey(mSplit.getStartRow()))
          .setStopRow(HBaseEntityId.fromHBaseRowKey(mSplit.getEndRow()))
          .setHBaseScanOptions(hBaseScanOptions);
      final String filterJson = conf.get(KijiConfKeys.KIJI_ROW_FILTER);
      if (null != filterJson) {
        KijiRowFilter filter = KijiRowFilter.toFilter(filterJson);
        scannerOptions.setKijiRowFilter(filter);
      }
      mKiji = Kiji.Factory.open(inputURI, conf);
      mTable = mKiji.openTable(inputURI.getTable());
      mReader = mTable.openTableReader();
      mScanner = mReader.getScanner(mDataRequest, scannerOptions);
      mIterator = mScanner.iterator();
      mCurrentRow = null;
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getCurrentKey() throws IOException {
      return mCurrentRow.getEntityId();
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData getCurrentValue() throws IOException {
      return mCurrentRow;
    }

    /** {@inheritDoc} */
    @Override
    public float getProgress() throws IOException {
      // TODO: Implement
      return 0.0f;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException {
      if (mIterator.hasNext()) {
        mCurrentRow = (HBaseKijiRowData) mIterator.next();
        return true;
      } else {
        mCurrentRow = null;
        return false;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      ResourceUtils.closeOrLog(mScanner);
      ResourceUtils.closeOrLog(mReader);
      ResourceUtils.releaseOrLog(mTable);
      ResourceUtils.releaseOrLog(mKiji);

      mIterator = null;
      mScanner = null;
      mReader = null;
      mTable = null;
      mKiji = null;
      mSplit = null;
      mCurrentRow = null;
    }
  }
}
