package com.wibidata.lang;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Input format for Kiji that uses the old 'mapred' api.
 */
public class KijiInputFormat
    implements InputFormat<KijiKey, KijiValue> {

  /** {@inheritDoc} */
  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    final String uriString = Preconditions.checkNotNull(
        conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI));
    final KijiURI inputTableURI = KijiURI.newBuilder(uriString).build();
    final Kiji kiji = Kiji.Factory.open(inputTableURI, conf);
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
        table.close();
      }
    } finally {
      kiji.release();
    }
  }

  /** {@inheritDoc} */
  @Override
  public RecordReader<KijiKey, KijiValue> getRecordReader(
      InputSplit split, JobConf conf, Reporter reporter) throws IOException {
    // TODO: Use reporter to report progress.
    return new KijiTableRecordReader(split, conf);
  }

  /** Hadoop record reader for Kiji table rows. */
  public static class KijiTableRecordReader
      implements RecordReader<KijiKey, KijiValue> {

    /** Data request. */
    protected final KijiDataRequest mDataRequest;

    private Kiji mKiji = null;
    private KijiTable mTable = null;
    private KijiTableReader mReader = null;
    private KijiRowScanner mScanner = null;
    private Iterator<KijiRowData> mIterator = null;

    private KijiTableSplit mSplit = null;

    /**
     * Creates a new RecordReader for this input format.
     *
     * Perform the actual reads from Kiji.
     *
     * @param conf Configuration for the target Kiji.
     */
    public KijiTableRecordReader(InputSplit split, Configuration conf) throws IOException {
      // Get data request from the job configuration.
      final String dataRequestB64 = conf.get(KijiConfKeys.KIJI_INPUT_DATA_REQUEST);
      Preconditions.checkNotNull(dataRequestB64, "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (KijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);

      // Open connections to Kiji.
      assert split instanceof KijiTableSplit;
      mSplit = (KijiTableSplit) split;

      final KijiURI inputURI =
          KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
      final KijiScannerOptions scannerOptions = new KijiScannerOptions()
          .setStartRow(new HBaseEntityId(mSplit.getStartRow()))
          .setStopRow(new HBaseEntityId(mSplit.getEndRow()));
      mKiji = Kiji.Factory.open(inputURI, conf);
      mTable = mKiji.openTable(inputURI.getTable());
      mReader = mTable.openTableReader();
      mScanner = mReader.getScanner(mDataRequest, scannerOptions);
      mIterator = mScanner.iterator();
    }

    /** {@inheritDoc} */
    @Override
    public KijiKey createKey() {
      return new KijiKey();
    }

    /** {@inheritDoc} */
    @Override
    public KijiValue createValue() {
      return new KijiValue();
    }

    /** {@inheritDoc} */
    @Override
    public long getPos() {
      // TODO: Implement
      return 0L;
    }

    /** {@inheritDoc} */
    @Override
    public float getProgress() {
      // TODO: Implement
      return 0.0f;
    }

    /** {@inheritDoc} */
    @Override
    public boolean next(KijiKey key, KijiValue value) {
      if (mIterator.hasNext()) {
        // Read the next row and store it in the provided key/value pair.
        final KijiRowData row = mIterator.next();
        key.set(row.getEntityId());
        value.set(row);

        return true;
      } else {
        return false;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      ResourceUtils.closeOrLog(mScanner);
      ResourceUtils.closeOrLog(mReader);
      ResourceUtils.closeOrLog(mTable);
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
