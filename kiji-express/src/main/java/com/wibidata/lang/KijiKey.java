package com.wibidata.lang;

import org.kiji.schema.EntityId;

// TODO: Get rid of this.
@Deprecated
public class KijiKey {
  private EntityId mCurrentKey;

  public EntityId get() {
    return mCurrentKey;
  }

  public void set(EntityId key) {
    mCurrentKey = key;
  }
}
