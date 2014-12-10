package org.kiji.spark.connector.rdd

import org.kiji.commons.scala.ScalaLogged

import scala.collection.Iterator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.spark.TaskContext
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.kiji.schema.cassandra.CassandraKijiURI
import org.kiji.schema.hbase.HBaseKijiURI
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI
import org.kiji.schema.KijiResult
import org.kiji.spark.connector.rdd.hbase.HBaseKijiRDD
import org.kiji.spark.connector.rdd.cassandra.CassandraKijiRDD
import org.kiji.spark.connector.KijiSpark

/**
 *
 */
abstract class KijiRDD[T] private[rdd] (
    @transient sc: SparkContext,
    //@transient conf: Configuration = None, TODO: add support
    //@transient credentials: Credentials = None,
    @transient kijiURI: KijiURI,
    kijiDataRequest: KijiDataRequest
) extends RDD[KijiResult[T]](sc, Nil) with ScalaLogged {

  override def compute(split: Partition, context: TaskContext): Iterator[KijiResult[T]]
  override protected def getPartitions: Array[Partition]
}

object KijiRDD {

  /**
   *
   * @param sc
   * @param conf
   * @param credentials
   * @param kijiURI
   * @param kijiDataRequest
   * @return
   */
  def apply (
      @transient sc: SparkContext,
      @transient kijiURI: KijiURI,
      kijiDataRequest: KijiDataRequest
  ): KijiRDD[_] = {
    kijiURI match {
      case cassandraKijiURI: CassandraKijiURI => CassandraKijiRDD(sc, kijiURI, kijiDataRequest)
      case _ => throw new UnsupportedOperationException(KijiSpark.UnsupportedKiji)
    }
  }

  /**
   *
   * @param sc
   * @param conf
   * @param credentials
   * @param kijiURI
   * @param kijiDataRequest
   * @return
   */
  def apply (
      @transient sc: SparkContext,
      @transient conf: Configuration,
      @transient credentials: Credentials,
      @transient kijiURI: KijiURI,
      kijiDataRequest: KijiDataRequest
  ): KijiRDD[_] = {
    kijiURI match {
      case hbaseKijiURI: HBaseKijiURI => HBaseKijiRDD(sc, conf, credentials, kijiURI, kijiDataRequest)
      case cassandraKijiURI: CassandraKijiURI => CassandraKijiRDD(sc, kijiURI, kijiDataRequest)
      case _ => throw new UnsupportedOperationException(KijiSpark.UnsupportedKiji)
    }
  }
}
