import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Input format for Kiji that uses the old 'mapred' api.
 */
public class KijiInputFormat
    implements InputFormat<KijiKey, KijiValue> {

  /** {@inheritDoc} */
  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    final KijiURI inputTableURI =
        KijiURI.newBuilder(conf.get(KijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
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
          splits.add(new KijiTableSplit(tableSplit, startKey));
        }
        return splits;

      } finally {
        table.close();
      }
    } finally {
      kiji.release();
    }
  }

  /** {@inheritDoc} */
  @Override
  public RecordReader<KijiKey, KijiValue> createRecordReader(
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

    private HBaseKijiRowData mCurrentRow = null;

    /**
     * Creates a new RecordReader for this input format.
     *
     * Perform the actual reads from Kiji.
     *
     * @param conf Configuration for the target Kiji.
     */
    public KijiTableRecordReader(InputSplit split, Configuration conf) {
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
      mKiji.release();
      mIterator = null;
      mScanner = null;
      mReader = null;
      mTable = null;
      mKiji = null;

      mSplit = null;
      mCurrentRow = null;
    }
  }

  // TODO: Get rid of this.
  public static class KijiKey {
    private EntityId mCurrentKey;

    public EntityId get() {
      return mCurrentKey;
    }

    public void set(EntityId key) {
      mCurrentKey = key;
    }
  }

  // TODO: Get rid of this.
  public static class KijiValue {
    private KijiRowData mCurrentValue;

    public KijiRowData get() {
      return mCurrentValue;
    }

    public void set(KijiRowData value) {
      mCurrentValue = value;
    }
  }
}
