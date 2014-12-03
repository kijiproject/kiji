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
package org.kiji.spark.connector.rdd

import org.kiji.schema.EntityId
import org.kiji.schema.HBaseEntityId

import org.apache.spark.Partition

/**
 * A partition of Kiji data created by [[KijiRDD.getPartitions]].
 * Contains information needed by [[KijiRDD.compute]].
 *
 * @param mIndex the index of this partition.
 * @param mStartRow the start row of this partition.
 * @param mStopRow the stop row of this partition.
 */
class KijiPartition (
    private val mIndex: Int,
    private val mStartRow: Array[Byte],
    private val mStopRow: Array[Byte]
) extends Partition {

  /* Gets the row at which the partition starts, e.g. for a scanner. */
  def getStartRow: EntityId = {
    HBaseEntityId.fromHBaseRowKey(mStartRow)
  }

  /* Gets the row at which the partition ends. */
  def getStopRow: EntityId = {
    HBaseEntityId.fromHBaseRowKey(mStopRow)
  }

  override def index: Int = { mIndex }
}