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

package org.kiji.rest.representations;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.hibernate.validator.constraints.NotEmpty;

import org.kiji.schema.KijiCell;

/**
 * Models what a Kiji cell looks like when returned to the client.
 *
 */
@JsonPropertyOrder({"columnFamily", "columnQualifier"})
public class KijiRestCell {

  private Long mTimestamp;

  @NotEmpty
  private String mColumnFamily;

  private String mColumnQualifier;

  private Object mValue;

  /**
   * Constructs a KijiRestCell given a KijiCell.
   *
   * @param kijiCell the incoming cell
   */
  public KijiRestCell(KijiCell<?> kijiCell) {
    mTimestamp = kijiCell.getTimestamp();
    mColumnFamily = kijiCell.getFamily();
    mColumnQualifier = kijiCell.getQualifier();
    mValue = kijiCell.getData();
  }

  /**
   * Dummy constructor required for Jackson to (de)serialize JSON properly.
   */
  public KijiRestCell() {
  }

  /**
   * Returns the underlying cell's timestamp.
   *
   * @return the underlying cell's timestamp
   */
  public Long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Returns the underlying cell's column family.
   *
   * @return the underlying cell's column family
   */
  public String getColumnFamily() {
    return mColumnFamily;
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
