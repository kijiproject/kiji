/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.lang;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.HBaseEntityId;
import org.kiji.schema.impl.HBaseKijiTable;
import org.kiji.schema.util.ResourceUtils;

/**
 * <p>An {@link InputFormat} for use with Cascading whose input source is a Kiji table. This
 * input format will scan over a subset of rows in a Kiji table, retrieving {@link KijiRowData}
 * as a result. The columns retrieved during the scan, as well as the time and row ranges used,
 * are configured through an {@link KijiDataRequest}.</p>
 *
 * <p>The input format itself is responsible for computing input splits over the Kiji table,
 * and retrieving a {@link RecordReader} for a particular split. See
 * {@link KijiTableRecordReader} for more information on how a particular input split is scanned
 * for rows. Record readers returned by this input format return key-value pairs of type {@link
 * KijiKey} and {@link KijiValue}, which are simple wrappers around
 * {@link org.kiji.schema.EntityId} and {@link KijiRowData}, respectively.
 *
 * <p>This input format uses the "old" style MapReduce API for compatibility with Cascading.</p>
 */
@ApiAudience.Framework
@ApiStability.Unstable
public final class KijiInputFormat implements InputFormat<KijiKey, KijiValue> {

  /**
   * Gets a set of input splits for a MapReduce job running over a Kiji table. One split is
   * created per region in the input Kiji table.
   *
   * @param configuration of the job using the splits. The configuration should specify the
   *     input Kiji table being used, through the configuration variable
   *     {@link KijiConfKeys#KIJI_INPUT_TABLE_URI}.
   * @param numSplits desired for the job. This framework hint is ignored by this method.
   * @return an array of input splits to be operated on in the MapReduce job.
   * @throws IOException if an I/O error occurs while communicating with HBase to determine the
   *     regions in the Kiji table.
   */
  @Override
  public InputSplit[] getSplits(JobConf configuration, int numSplits) throws IOException {
    final String uriString = Preconditions.checkNotNull(
        configuration.get(KijiConfKeys.KIJI_INPUT_TABLE_URI));
    final KijiURI inputTableURI = KijiURI.newBuilder(uriString).build();
    final Kiji kiji = Kiji.Factory.open(inputTableURI, configuration);
    try {
      final KijiTable table = kiji.openTable(inputTableURI.getTable());
      try {
        final HTableInterface htable = HBaseKijiTable.downcast(table).getHTable();

        final List<InputSplit> splits = Lists.newArrayList();
        for (KijiRegion region : table.getRegions()) {
          final byte[] startKey = region.getStartKey();
          // TODO(KIJIMR-65): For now pick the first available location (ie. region server), if any.
          final String location =
              region.getLocations().isEmpty() ? null : region.getLocations().iterator().next();
          final TableSplit tableSplit =
              new TableSplit(htable.getTableName(), startKey, region.getEndKey(), location);
          splits.add(new KijiTableSplit(tableSplit));
        }
        return splits.toArray(new InputSplit[0]);

      } finally {
        table.release();
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Gets a record reader that will scan over a subset of rows in a Kiji table.
   *
   * @param split the record reader should operate over.
   * @param configuration of the job that uses the record reader. The configuration should specify
   *     the input Kiji table through the configuration variable
   *     {@link KijiConfKeys#KIJI_INPUT_TABLE_URI} and a serialized {@link KijiDataRequest} through
   *     the configuration variable {@link KijiConfKeys#KIJI_INPUT_DATA_REQUEST}.
   * @param reporter is ignored by this method.
   * @return An {@link KijiTableRecordReader} that will scan over a subset of rows in a Kiji table.
   * @throws IOException if there is an error constructing the record reader.
   */
  @Override
  public RecordReader<KijiKey, KijiValue> getRecordReader(
      InputSplit split, JobConf configuration, Reporter reporter) throws IOException {
    // TODO: Use reporter to report progress.
    return new KijiTableRecordReader(split, configuration);
  }

  /**
   * A record reader that can scan a subset of rows in a Kiji table. This record reader is
   * configured to read from a Kiji table under certain data request parameters through a Hadoop
   * {@link Configuration} It returns key-value pairs of type {@link KijiKey} (a wrapper around
   * {@link org.kiji.schema.EntityId} and {@link KijiValue} (a wrapper around {@link KijiRowData}).
   */
  public static class KijiTableRecordReader implements RecordReader<KijiKey, KijiValue> {

    /** The data request used to read from the Kiji table. */
    private final KijiDataRequest mDataRequest;
    /** The Kiji instance containing the table being read from. */
    private Kiji mKiji = null;
    /** The Kiji table being read from. */
    private KijiTable mTable = null;
    /** A reader for the above table. */
    private KijiTableReader mReader = null;
    /** Used to scan a subset of rows from the table. */
    private KijiRowScanner mScanner = null;
    /** An interator over the rows retrieved by the scanner. */
    private Iterator<KijiRowData> mIterator = null;
    /**
     * An input split specifying a subset of rows in a table region that should be scanned over,
     * subject to data request constraints.
     */
    private KijiTableSplit mSplit = null;

    /**
     * Creates a new record reader that scans over a subset of rows from a Kiji table. The record
     * reader will scan over rows in the table specified in the provided input split,
     * subject to row limits specified in the data request serialized into the specified
     * configuration.
     *
     * @param split for the MapReduce task that will use this record reader. The split specifies a
     *     subset of rows from a Kiji table.
     * @param configuration for the MapReduce job using this record reader. The configuration
     *     should specify the input Kiji table through the configuration variable
     *     {@link KijiConfKeys#KIJI_INPUT_TABLE_URI} and a serialized {@link KijiDataRequest}
     *     through the configuration variable {@link KijiConfKeys#KIJI_INPUT_DATA_REQUEST}.
     * @throws IOException if there is a problem constructing the record reader and opening the
     *     resources it requires.
     */
    public KijiTableRecordReader(InputSplit split, Configuration configuration) throws IOException {
      // Get data request from the job configuration.
      final String dataRequestB64 = configuration.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST);
      Preconditions.checkNotNull(dataRequestB64, "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (KijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);

      // Open connections to Kiji.
      assert split instanceof KijiTableSplit;
      mSplit = (KijiTableSplit) split;

      final KijiURI inputURI =
          KijiURI.newBuilder(configuration.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
      final KijiScannerOptions scannerOptions = new KijiScannerOptions()
          .setStartRow(new HBaseEntityId(mSplit.getStartRow()))
          .setStopRow(new HBaseEntityId(mSplit.getEndRow()));
      mKiji = Kiji.Factory.open(inputURI, configuration);
      mTable = mKiji.openTable(inputURI.getTable());
      mReader = mTable.openTableReader();
      mScanner = mReader.getScanner(mDataRequest, scannerOptions);
      mIterator = mScanner.iterator();
    }

    /**
     * @return a new, empty, reusable instance of {@link KijiKey} which will hold keys read by
     *     this record reader.
     */
    @Override
    public KijiKey createKey() {
      return new KijiKey();
    }

    /**
     * @return a new, empty, reusable instance of {@link KijiValue} which will hold values read by
     *     this record reader.
     */
    @Override
    public KijiValue createValue() {
      return new KijiValue();
    }

    /**
     * @return the current position in the input split.
     */
    @Override
    public long getPos() {
      // TODO: Implement
      return 0L;
    }

    /**
     * @return the percentage (as a float between 0 and 1) of the input split that has been
     *     processed.
     */
    @Override
    public float getProgress() {
      // TODO: Implement
      return 0.0f;
    }

    /**
     * Populates the specified key and value with the next key-value pair read from the input
     * split.
     *
     * @param key instance to populate with the next key read.
     * @param value instance to popualte with the next value read.
     * @return <code>true</code> if a new key-value was read, <code>false</code> if we have reached
     *     the end of the input split.
     */
    @Override
    public boolean next(KijiKey key, KijiValue value) {
      if (mIterator.hasNext()) {
        // Read the next row and store it in the provided key/value pair.
        final KijiRowData row = mIterator.next();
        if (null != key) {
          key.set(row.getEntityId());
        }
        if (null != value) {
          value.set(row);
        }
        return true;
      } else {
        return false;
      }
    }

    /**
     * Release all resources used by this record reader.
     *
     * @throws IOException if there is an error closing the resources.
     */
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
    }
  }
}
