package com.wibidata.lang;

/**
 * Must be used with KijiScheme.
 */
public class KijiTap
    extends Tap<JobConf, RecordReader, OutputCollector> {
  private final KijiURI mTableURI;
  private final KijiScheme mScheme;
  private final String mId = UUID.randomUUID().toString();

  public KijiTap(KijiURI tableURI, KijiScheme scheme) {
    mTableURI = tableURI;
    mScheme = scheme;
  }

  /** {@inheritDoc} */
  @Override
  public void sourceConfInit(FlowProcess<JobConf> process, JobConf conf) {
    conf.setInputFormat(KijiInputFormat.class);
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, mTableURI.toString());

    super.sinkConfInit(process, conf);
  }

  /** {@inheritDoc} */
  @Override
  public void sinkConfInit(FlowProcess<JobConf> process, JobConf conf) {
    // TODO: Implement
  }

  /** {@inheritDoc} */
  @Override
  public String getIdentifier() {
    return mId;
  }

  /** {@inheritDoc} */
  @Override
  public TupleEntryIterator openForRead(FlowProcess<JobConf> jobConfFlowProcess, RecordReader recordReader) throws IOException {
    return new HadoopTupleEntrySchemeIterator(jobConfFlowProcess, this, recordReader);
  }

  /** {@inheritDoc} */
  @Override
  public TupleEntryCollector openForWrite(FlowProcess<JobConf> jobConfFlowProcess, OutputCollector outputCollector) {
    // TODO: Implement.
    throw new UnsupportedOperationException("Writing is not supported with KijiTap.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean createResource(JobConf jobConf) throws IOException {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteResource(JobConf jobConf) throws IOException {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.");
  }

  /** {@inheritDoc} */
  @Override
  public boolean resourceExists(JobConf jobConf) throws IOException {
    final String tableName = mTableURI.getTable();
    final Kiji kiji = Kiji.Factory.open(mTableURI);

    return kiji.getTableNames().contains(tableName);
  }

  /** {@inheritDoc} */
  @Override
  public long getModifiedTime(JobConf jobConf) throws IOException {
    return System.currentTimeMillis(); // currently unable to find last mod time on a table
  }


  // TODO: Implement equals, hashCode.
}
