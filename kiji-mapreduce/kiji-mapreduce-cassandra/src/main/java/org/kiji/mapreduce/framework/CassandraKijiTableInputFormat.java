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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraKijiURI;
import org.kiji.schema.impl.cassandra.CassandraKijiScannerOptions;
import org.kiji.schema.impl.cassandra.CassandraKijiTableReader;
import org.kiji.schema.layout.ColumnReaderSpec;
import org.kiji.schema.util.ResourceUtils;

/** InputFormat for Hadoop MapReduce jobs reading from a Kiji table. */
@ApiAudience.Framework
@ApiStability.Stable
public final class CassandraKijiTableInputFormat
    extends KijiTableInputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTableInputFormat.class);

  /**
   * Number of bytes from the row-key to include when reporting progress.
   * Use 32 bits precision, ie. 4 billion row keys granularity.
   */
  private static final int PROGRESS_PRECISION_NBYTES = 4;

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
      InputSplit split,
      TaskAttemptContext context
  ) throws IOException {
    return new CassandraKijiTableRecordReader(mConf);
  }

  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    final Configuration conf = context.getConfiguration();
    final CassandraKijiURI inputTableURI =
        CassandraKijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
    final Kiji kiji = Kiji.Factory.open(inputTableURI);
    try {
      final KijiTable table = kiji.openTable(inputTableURI.getTable());
      try {
        // Create a session with a custom load-balancing policy that will ensure that we send
        // queries for system.local and system.peers to the same node.
        Cluster cluster = Cluster
            .builder()
            .addContactPoints(inputTableURI.getContactPoints()
                .toArray(new String[inputTableURI.getContactPoints().size()]))
            .withPort(inputTableURI.getContactPort())
            .withLoadBalancingPolicy(new ConsistentHostOrderPolicy())
            .build();
        Session session = cluster.connect();

        // Get a list of all of the subsplits.  A "subsplit" contains the following:
        // - A token range (corresponding to a virtual node in the C* cluster)
        // - A list of replica nodes for that token range
        final CassandraSubSplitCreator cassandraSubSplitCreator =
            new CassandraSubSplitCreator(session);
        final List<CassandraSubSplit> subsplitsFromTokens =
            cassandraSubSplitCreator.createSubSplits();
        LOG.debug(String.format("Created %d subsplits from tokens", subsplitsFromTokens.size()));

        // In this InputFormat, we allow the user to specify a desired number of InputSplits.  We
        // will likely have far more subsplits (vnodes) than desired InputSplits.  Therefore, we
        // combine subsplits (hopefully those that share the same replica nodes) until we get to our
        // desired InputSplit count.
        final CassandraSubSplitCombiner cassandraSubSplitCombiner = new CassandraSubSplitCombiner();

        // Get a list of all of the token ranges in the Cassandra cluster.
        List<InputSplit> inputSplitList = Lists.newArrayList();

        // Java is annoying here about casting a list.
        inputSplitList.addAll(cassandraSubSplitCombiner.combineSubsplits(subsplitsFromTokens));
        cluster.close();
        LOG.info(String.format("Created a total of %d InputSplits", inputSplitList.size()));
        for (InputSplit inputSplit : inputSplitList) {
          LOG.debug(inputSplit.toString());
        }
        return inputSplitList;
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(kiji);
    }
  }

  /** Hadoop record reader for Kiji table rows. */
  public static final class CassandraKijiTableRecordReader
      extends RecordReader<EntityId, KijiRowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTableRecordReader.class);

    /** Data request. */
    private final KijiDataRequest mDataRequest;

    private Kiji mKiji = null;
    private KijiTable mTable = null;
    private CassandraKijiTableReader mReader = null;
    private KijiRowScanner mScanner = null;
    private Iterator<KijiRowData> mIterator = null;
    private CassandraInputSplit mSplit = null;
    private KijiRowData mCurrentRow = null;
    private Iterator<CassandraTokenRange> mTokenRangeIterator = null;

    private long mStartPos;
    private long mStopPos;

    /**
     * Creates a new RecordReader for this input format.
     *
     * Perform the actual reads from Kiji.
     *
     * @param conf Configuration for the target Kiji.
     */
    CassandraKijiTableRecordReader(Configuration conf) {
      // Get data request from the job configuration.
      final String dataRequestB64 = conf.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST);
      Preconditions.checkNotNull(dataRequestB64, "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (KijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      initializeWithConf(split, context.getConfiguration());
    }

    /**
     * Version of `initialize` that takes a `Configuration.`  Makes testing easier.
     *
     * @param split Input split for this record reader.
     * @param conf Hadoop Configuration.
     * @throws java.io.IOException if there is a problem opening a connection to the Kiji instance.
     */
    void initializeWithConf(InputSplit split, Configuration conf) throws IOException {
      Preconditions.checkArgument(split instanceof CassandraInputSplit,
          "InputSplit is not a KijiTableSplit: %s", split);
      mSplit = (CassandraInputSplit) split;
      // Create an iterator to go through all of the token ranges.
      mTokenRangeIterator = mSplit.getTokenRangeIterator();
      assert(mTokenRangeIterator.hasNext());

      final KijiURI inputURI =
          KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();

      // TODO: Not sure if we need this...
      // Extract the ColumnReaderSpecs and build a mapping from column to the appropriate overrides.
      final ImmutableMap.Builder<KijiColumnName, ColumnReaderSpec> overridesBuilder =
          ImmutableMap.builder();
      for (KijiDataRequest.Column column : mDataRequest.getColumns()) {
        if (column.getReaderSpec() != null) {
          overridesBuilder.put(column.getColumnName(), column.getReaderSpec());
        }
      }

      mKiji = Kiji.Factory.open(inputURI);
      mTable = mKiji.openTable(inputURI.getTable());
      KijiTableReader reader = mTable.getReaderFactory().readerBuilder()
          .withColumnReaderSpecOverrides(overridesBuilder.build())
          .buildAndOpen();
      Preconditions.checkArgument(reader instanceof CassandraKijiTableReader);
      mReader = (CassandraKijiTableReader) reader;

      // TODO: Figure out progress from tokens.
      //mStartPos = bytesToPosition(mSplit.getStartRow(), PROGRESS_PRECISION_NBYTES);
      //long stopPos = bytesToPosition(mSplit.getEndRow(), PROGRESS_PRECISION_NBYTES);
      //mStopPos = (stopPos > 0) ? stopPos : (1L << (PROGRESS_PRECISION_NBYTES * 8));
      LOG.info("Progress reporting: start={} stop={}", mStartPos, mStopPos);
      queryNextTokenRange();
    }

    /**
     * Execute all of our queries over the next token range in our list of token ranges for this
     * input split.
     *
     * If were are out of token ranges, then set the current row and the current row iterator both
     * to null.
     *
     * @throws java.io.IOException if there is a problem building a scanner.
     */
    private void queryNextTokenRange() throws IOException {
      Preconditions.checkArgument(mTokenRangeIterator.hasNext());
      Preconditions.checkArgument(null == mIterator || !mIterator.hasNext());

      CassandraTokenRange nextTokenRange = mTokenRangeIterator.next();

      // Get a new scanner for this token range!
      if (null != mScanner) {
        ResourceUtils.closeOrLog(mScanner);
      }
      mScanner = mReader.getScannerWithOptions(
          mDataRequest,
          CassandraKijiScannerOptions.withTokens(
              nextTokenRange.getStartToken(),
              nextTokenRange.getEndToken()));
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

    /**
     * Converts a byte array into an integer position in the row-key space.
     *
     * @param bytes Byte array to convert to an approximate position.
     * @param nbytes Number of bytes to use (must be in the range 1..8).
     * @return the approximate position in the row-key space.
     */
    public static long bytesToPosition(final byte[] bytes, final int nbytes) {
      long position = 0;
      if (bytes != null) {
        for (int i = 0; i < nbytes; ++i) {
          final int bvalue = (i < bytes.length) ? (0xff & bytes[i]) : 0;
          position = (position << 8) + bvalue;
        }
      }
      return position;
    }

    /**
     * Computes the start position from the start row key, for progress reporting.
     *
     * @param startRowKey Start row key to compute the position of.
     * @return the start position from the start row key.
     */
    public static long getStartPos(byte[] startRowKey) {
      return bytesToPosition(startRowKey, PROGRESS_PRECISION_NBYTES);
    }

    /**
     * Computes the stop position from the stop row key, for progress reporting.
     *
     * @param stopRowKey Stop row key to compute the position of.
     * @return the stop position from the start row key.
     */
    public static long getStopPos(byte[] stopRowKey) {
      long stopPos = bytesToPosition(stopRowKey, PROGRESS_PRECISION_NBYTES);
      return (stopPos > 0) ? stopPos : (1L << (PROGRESS_PRECISION_NBYTES * 8));
    }

    /**
     * Compute the progress (between 0.0f and 1.0f) for the current row key.
     *
     * @param startPos Computed start position (using getStartPos).
     * @param stopPos Computed stop position (using getStopPos).
     * @param currentRowKey Current row to compute a progress for.
     * @return the progress indicator for the given row, start and stop positions.
     */
    public static float computeProgress(long startPos, long stopPos, byte[] currentRowKey) {
      Preconditions.checkArgument(startPos <= stopPos,
          "Invalid start/stop positions: start=%s stop=%s", startPos, stopPos);
      final long currentPos = bytesToPosition(currentRowKey, PROGRESS_PRECISION_NBYTES);
      Preconditions.checkArgument(startPos <= currentPos,
          "Invalid start/current positions: start=%s current=%s", startPos, currentPos);
      Preconditions.checkArgument(currentPos <= stopPos,
          "Invalid current/stop positions: current=%s stop=%s", currentPos, stopPos);
      if (startPos == stopPos) {
        // Row key range is too small to perceive progress: report 50% completion
        return 0.5f;
      } else {
        return (float) (((double) currentPos - startPos) / (stopPos - startPos));
      }
    }

    /** {@inheritDoc} */
    @Override
    public float getProgress() throws IOException {
      /*
      if (mCurrentRow == null) {
        return 0.0f;
      }
      final byte[] currentRowKey = mCurrentRow.getHBaseResult().getRow();
      return computeProgress(mStartPos, mStopPos, currentRowKey);
      */
      return 0.0f;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException {
      while (true) {
        if (mIterator.hasNext()) {
          mCurrentRow = mIterator.next();
          return true;
        }
        // We are out of rows in the current token range.
        Preconditions.checkArgument(!mIterator.hasNext());

        // We are also out of token ranges!
        if (!mTokenRangeIterator.hasNext()) {
          break;
        }

        // We still have more token ranges left!
        Preconditions.checkArgument(mTokenRangeIterator.hasNext());
        queryNextTokenRange();
      }

      mCurrentRow = null;
      return false;
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
