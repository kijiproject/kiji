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

import com.google.common.collect.Lists;

import org.apache.commons.codec.binary.Hex;

import org.kiji.schema.EntityId;

/**
 * Represents the Kiji row returned to the client.
 *
 */
public class KijiRestRow {
  private String mHumanReadableEntityId;
  private String mHBaseRowKey;
  private List<KijiRestCell> mKijiCells;

  /**
   * Construct a new KijiRestRow object given the entity id.
   *
   * @param entityId is the entity id of the row.
   */
  public KijiRestRow(EntityId entityId) {
    mHumanReadableEntityId = entityId.toShellString();
    mHBaseRowKey = new String(Hex.encodeHex(entityId.getHBaseRowKey()));
    mKijiCells = Lists.newArrayList();
  }

  /**
   * Adds a new KijiRestCell to the list of cells in the row.
   *
   * @param cell is the cell to add to the row's list of cells.
   */
  public void addCell(KijiRestCell cell) {
    mKijiCells.add(cell);
  }

  /**
   * Returns the human readable entity_id (i.e. a string representation of the list of
   * components).
   *
   * @return the human readable entity_id (i.e. a string representation of the list of
   *     components).
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
   * Returns the list of KijiRestCells contained in this row.
   *
   * @return the list of KijiRestCells contained in this row.
   */
  public List<KijiRestCell> getCells() {
    return mKijiCells;
  }
}
