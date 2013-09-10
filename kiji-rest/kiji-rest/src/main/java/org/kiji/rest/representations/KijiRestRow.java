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

import java.util.List;
import java.util.NavigableMap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;

/**
 * Represents the Kiji row returned to the client.
 *
 */
@JsonPropertyOrder({ "entityId", "rowKey" })
public class KijiRestRow {

  @JsonProperty("entityId")
  private String mHumanReadableEntityId;

  @JsonProperty("cells")
  private NavigableMap<String, NavigableMap<String, List<KijiRestCell>>> mKijiCellMap;

  /**
   * Dummy constructor required for Jackson to (de)serialize JSON properly.
   */
  public KijiRestRow() {
  }

  /**
   * Construct a new KijiRestRow object given the entity id.
   *
   * @param entityId is the entity id of the row.
   */
  public KijiRestRow(EntityId entityId) {
    mHumanReadableEntityId = entityId.toShellString();
    mKijiCellMap = Maps.newTreeMap();
  }

  /**
   * Convenience method to add a new cell (represented by the KijiCell) to the row.
   *
   * @param cell is the cell to add.
   */
  public void addCell(KijiCell<?> cell) {
    addCell(cell.getFamily(), cell.getQualifier(), cell.getTimestamp(), cell.getData());
  }

  /**
   * Adds a new cell (specified by the family, qualifier, timestamp and value) to the current row.
   *
   * @param family is the family of the cell to add.
   * @param qualifier is the qualifier of the cell to add.
   * @param timestamp is the timestamp of the cell to add.
   * @param value is the value of the cell to add.
   */
  public void addCell(String family, String qualifier, Long timestamp, Object value) {
    NavigableMap<String, List<KijiRestCell>> familyMap = mKijiCellMap.get(family);
    if (familyMap == null) {
      familyMap = Maps.newTreeMap();
      mKijiCellMap.put(family, familyMap);
    }
    List<KijiRestCell> restCells = familyMap.get(qualifier);
    if (restCells == null) {
      restCells = Lists.newArrayList();
      familyMap.put(qualifier, restCells);
    }
    restCells.add(new KijiRestCell(timestamp, value));
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
   * Returns the map of cell data contained in this row.
   *
   * @return the map of cell data contained in this row.
   */
  public NavigableMap<String, NavigableMap<String, List<KijiRestCell>>> getCells() {
    return mKijiCellMap;
  }
}
