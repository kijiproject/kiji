package org.kiji.spark.connector

/**
 * Defines static global variables
 */
object KijiSpark {

  /** Error message indicating that the Kiji instance must be an HBaseKiji or a CassandraKiji. */
  val UnsupportedKiji = "KijiSpark currently only supports HBase and Cassandra Kiji instances."
  val IncorrectHBaseParams = "Error: You passed in parameters for an HBase table but specified a Cassandra table"
  val IncorrectCassandraParams = "Error: You passed in parameters for a Cassandra table but specified and HBase table"
  val HbaseKiji = "hbaseKiji"
  val CassandraKiji = "cassandraKiji"
}
