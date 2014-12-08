package org.kiji.spark.connector.rdd.hbase

import org.apache.spark.Partition

import org.kiji.commons.scala.ScalaLogged
import org.kiji.schema.EntityId
import org.kiji.schema.HBaseEntityId

/**
 * A partition of Kiji data created by [[KijiRDD.getPartitions]].
 * Contains information needed by [[KijiRDD.compute]].
 *
 * @param mIndex the index of this partition.
 * @param mStartRow the start row of this partition.
 * @param mStopRow the stop row of this partition.
 */
class HBaseKijiPartition private (
    val mIndex: Int,
    val mStartRow: Array[Byte],
    val mStopRow: Array[Byte]
) extends Partition {

  /* Gets the row at which the partition starts, e.g. for a scanner. */
  def startLocation: EntityId = {
    HBaseEntityId.fromHBaseRowKey(mStartRow)
  }

  /* Gets the row at which the partition ends. */
  def stopLocation: EntityId = {
    HBaseEntityId.fromHBaseRowKey(mStopRow)
  }

  override def index: Int = mIndex
}

object HBaseKijiPartition extends ScalaLogged {

  /**
   *
   * @param mIndex
   * @param mStartRow
   * @param mStopRow
   * @return
   */
  def apply(
    mIndex: Int,
    mStartRow: Array[Byte],
    mStopRow: Array[Byte]
  ): HBaseKijiPartition = {
    new HBaseKijiPartition(mIndex, mStartRow, mStopRow)
  }
}