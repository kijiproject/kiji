package org.kiji.rest.rows;

import org.kiji.schema.KijiColumnName;

public class KijiCell {

  private long mTimeStamp;
  private String mColumnName;
  private String mColumnQualifier;
  private Object mValue;
  
  public KijiCell(String pColName, String pColQualifier, long pTimeStamp, Object pValue)
  {
    mColumnName = pColName;
    mColumnQualifier = pColQualifier;
    mTimeStamp = pTimeStamp;
    mValue = pValue;
  }
  public KijiCell(KijiColumnName pColName, long pTimeStamp, Object pValue)
  {
    this(pColName.getFamily(),pColName.getQualifier(),pTimeStamp,pValue);
  }
  
  public long getTimestamp()
  {
    return mTimeStamp;
  }
  
  public String getColumnName()
  {
    return mColumnName;
  }
  
  public String getColumnQualifier()
  {
    return mColumnQualifier;
  }
  
  public Object getValue()
  {
    return mValue;
  }
  
}
