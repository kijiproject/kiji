package com.wibidata.lang;

import org.kiji.schema.KijiRowData;

// TODO: Get rid of this.
@Deprecated
public class KijiValue {
  private KijiRowData mCurrentValue;

  public KijiRowData get() {
    return mCurrentValue;
  }

  public void set(KijiRowData value) {
    mCurrentValue = value;
  }
}
