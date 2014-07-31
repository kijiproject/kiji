/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.testing.fakehtable

import java.lang.{Long => JLong}
import java.util.NavigableMap

trait FakeTypes {
  /** Byte array shortcut. */
  type Bytes = Array[Byte]

  /** Time series in a column, ordered by decreasing time stamps. */
  type ColumnSeries = NavigableMap[JLong, Bytes]

  /** Map column qualifiers to cell series. */
  type FamilyQualifiers = NavigableMap[Bytes, ColumnSeries]

  /** Map of column family names to qualifier maps. */
  type RowFamilies = NavigableMap[Bytes, FamilyQualifiers]

  /** Map of row keys. */
  type Table = NavigableMap[Bytes, RowFamilies]
}