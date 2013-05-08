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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the Kiji row returned to the client. This
 * needs to be kept in sync with the KijiRestRow because this is the bean
 * class that is used for deserializing JSON back into an object for inspection.
 *
 */
public class TestKijiRestRow {

  @JsonProperty("humanReadableEntityId")
  private String mHumanReadableEntityId;

  @JsonProperty("hbaseRowKey")
  private String mHBaseRowKey;

  @JsonProperty("cells")
  private List<TestKijiRestCell> mKijiCells;

  /**
   * Adds a new TestKijiRestCell to the list of cells in the row.
   *
   * @param cell is the cell to add to the row's list of cells.
   */
  public void addCell(TestKijiRestCell cell) {
    mKijiCells.add(cell);
  }

  /**
   * Returns the human readable entity_id (i.e. a string representation of the list of
   * components).
   *
   * @return the human readable entity_id (i.e. a string representation of the list of
   *         components).
   */
  public String getEntityId() {
    return mHumanReadableEntityId;
  }

  /**
   * Returns the hex encoding (in ASCII) of this row's hbase row key.
   *
   * @return the hex encoding (in ASCII) of this row's hbase row key.
   */
  public String getHBaseRowKey() {
    return mHBaseRowKey;
  }

  /**
   * Returns the list of TestKijiRestCells contained in this row.
   *
   * @return the list of TestKijiRestCells contained in this row.
   */
  public List<TestKijiRestCell> getCells() {
    return mKijiCells;
  }
}
