package org.kiji.rest.rows;

import java.util.List;

import org.kiji.schema.EntityId;

import com.google.common.collect.Lists;

public class KijiRow {
  private String mHumanReadableEntityId;
  private String mHBaseRowKey;
  private List<KijiCell> mKijiCells;
  
  public KijiRow(EntityId pEntityId)
  {
    mHumanReadableEntityId = pEntityId.toShellString();
    mHBaseRowKey = new String(pEntityId.getHBaseRowKey());
    mKijiCells = Lists.newArrayList();
  }
  
  public void addCell(KijiCell pCell)
  {
    mKijiCells.add(pCell);
  }
  
  public String getEntityId()
  {
    return mHumanReadableEntityId;
  }
  
  public String getHBaseRowKey()
  {
    return mHBaseRowKey;
  }
  
  public List<KijiCell> getCells()
  {
    return mKijiCells;
  }
}
