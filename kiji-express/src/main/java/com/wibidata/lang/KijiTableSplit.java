package com.wibidata.lang;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapred.InputSplit;

/**
 * Split for a Kiji table that has been extended to support hadoop's older 'mapred' api.
 */
public class KijiTableSplit
    implements InputSplit {
  private final TableSplit mSplit;

  /** The default constructor. */
  public KijiTableSplit() {
    mSplit = new TableSplit();
  }

  public KijiTableSplit(TableSplit tableSplit) {
    mSplit = tableSplit;
  }

  /** {@inheritDoc} */
  @Override
	public long getLength() {
    return mSplit.getLength();
  }

  /** {@inheritDoc} */
  @Override
	public String[] getLocations() {
    return mSplit.getLocations();
  }

  /** {@inheritDoc} */
  @Override
  public void readFields(DataInput in) throws IOException {
    mSplit.readFields(in);
  }

  /** {@inheritDoc} */
  @Override
  public void write(DataOutput out) throws IOException {
    mSplit.write(out);
  }

  public byte[] getStartRow() {
    return mSplit.getStartRow();
  }

  public byte[] getEndRow() {
    return mSplit.getEndRow();
  }
}
