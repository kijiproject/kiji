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

import org.kiji.schema.KijiCell;

/**
 * Models what a Kiji cell looks like when returned to the client.
 *
 */
public class KijiRestCell {

  private KijiCell<?> mCell;

  /**
   * Constructs a KijiRestCell given a KijiCell.
   *
   * @param kijiCell the incoming cell
   */
  public KijiRestCell(KijiCell<?> kijiCell) {
    mCell = kijiCell;
  }

  /**
   * Returns the underlying cell's timestamp.
   *
   * @return the underlying cell's timestamp
   */
  public long getTimestamp() {
    return mCell.getTimestamp();
  }

  /**
   * Returns the underlying cell's column family name.
   *
   * @return the underlying cell's column family name
   */
  public String getColumnName() {
    return mCell.getFamily();
  }

  /**
   * Returns the underlying cell's column qualifier.
   *
   * @return the underlying cell's column qualifier
   */
  public String getColumnQualifier() {
    return mCell.getQualifier();
  }

  /**
   * Returns the underlying cell's column value.
   *
   * @return the underlying cell's column value
   */
  public Object getValue() {
    return mCell.getData();
  }
}
