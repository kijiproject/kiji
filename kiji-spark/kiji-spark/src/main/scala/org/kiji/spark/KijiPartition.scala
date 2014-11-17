package org.kiji.spark

import org.apache.spark.Partition

import org.kiji.schema.EntityId
import org.kiji.schema.HBaseEntityId

/**
 * A partition of Kiji data created by [[org.kiji.spark.KijiRDD.getPartitions]].
 * Contains information needed by [[org.kiji.spark.KijiRDD.compute]].
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
