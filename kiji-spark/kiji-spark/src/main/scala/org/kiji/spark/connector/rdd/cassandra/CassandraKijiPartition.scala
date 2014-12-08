package org.kiji.spark.connector.rdd.cassandra

import org.apache.spark.Partition

import org.kiji.commons.scala.ScalaLogged

/**
 *
 * @param mIndex the index of this partition.
 * @param mStartToken
 * @param mStopToken
 */
class CassandraKijiPartition private (
    val mIndex: Int,
    val mStartToken: Long,
    val mStopToken: Long
) extends Partition {

  /* Gets the row at which the partition starts, e.g. for a scanner. */
  def startToken = mStartToken

  /* Gets the row at which the partition ends. */
  def stopToken = mStopToken

  override def index: Int = mIndex
}

object CassandraKijiPartition extends ScalaLogged {

  /**
   *
   * @param mIndex
   * @param mStartToken
   * @param mStopToken
   * @return
   */
  def apply(
    mIndex: Int,
    mStartToken: Long,
    mStopToken: Long
  ): CassandraKijiPartition = {
    new CassandraKijiPartition(mIndex, mStartToken, mStopToken)
  }
}