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

package org.kiji.rest.core;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Models what a Kiji cell looks like when returned to the client. This
 * needs to be kept in sync with the KijiRestCell because this is the bean
 * class that is used for deserializing JSON back into an object for inspection.
 *
 */
public class TestKijiRestCell {

  @JsonProperty("timestamp")
  private long mTimestamp;

  @JsonProperty("columnName")
  private String mColumnName;

  @JsonProperty("columnQualifier")
  private String mColumnQualifier;

  @JsonProperty("value")
  private Object mValue;

  /**
   * Returns the underlying cell's timestamp.
   *
   * @return the underlying cell's timestamp
   */
  public long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Returns the underlying cell's column family name.
   *
   * @return the underlying cell's column family name
   */
  public String getColumnName() {
    return mColumnName;
  }

  /**
   * Returns the underlying cell's column qualifier.
   *
   * @return the underlying cell's column qualifier
   */
  public String getColumnQualifier() {
    return mColumnQualifier;
  }

  /**
   * Returns the underlying cell's column value.
   *
   * @return the underlying cell's column value
   */
  public Object getValue() {
    return mValue;
  }
}
