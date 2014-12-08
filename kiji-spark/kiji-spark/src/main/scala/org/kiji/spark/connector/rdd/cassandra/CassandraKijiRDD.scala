package org.kiji.spark.connector.rdd.cassandra

import scala.collection.Iterator
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import java.util.{SortedMap => JSortedMap}
import java.util.{List => JList}
import java.util.{TreeMap => JTreeMap}
import java.util.{ArrayList => JArrayList}

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext

import org.kiji.schema.KijiResult
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiDataRequest.Column
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiCell
import org.kiji.schema.impl.MaterializedKijiResult
import org.kiji.schema.impl.cassandra.CassandraKijiScannerOptions
import org.kiji.schema.impl.cassandra.CassandraKijiTableReader
import org.kiji.schema.impl.cassandra.CassandraKijiResultScanner
import org.kiji.schema.impl.cassandra.CassandraKiji
import org.kiji.schema.impl.cassandra.CassandraKijiTable
import org.kiji.spark.connector.rdd.KijiRDD
import org.kiji.spark.connector.KijiSpark

/**
 * An RDD that provides the core functionality for reading Kiji data.
 *
 * TODO Add Support for secure clusters
 *
 * Currently, KijiSpark supports only C* and HBase Kiji instances.
 *
 * @param sc The SparkContext to associate this RDD with.
 * @param kijiURI The KijiURI to identify the Kiji instance and table; must include the table name.
 * @param kijiDataRequest The KijiDataRequest for the table provided by kijiURI.
 */
class CassandraKijiRDD[T] private (
    @transient sc: SparkContext,
    @transient kijiURI: KijiURI,
    kijiDataRequest: KijiDataRequest
) extends KijiRDD[T](sc, kijiURI, kijiDataRequest) { //RDD[KijiResult[T]](sc, Nil){
  // TODO (SPARK-34) add the functionality to write Kiji data to a KijiTable from KijiSpark.
  // This would probably consist of adding a method in KijiRDD, e.g. KijiRDD#writeToKijiTable
  // following the style of spark's RDD#write

  /**
   * KijiURIs are not serializable; this string representation allows
   * the provided kijiURI to be reconstructed upon deserialization of the RDD.
   */
  private val mKijiURIString = kijiURI.toString

  override def compute(split: Partition, context: TaskContext): Iterator[KijiResult[T]] = {

    val kijiURI: KijiURI = KijiURI.newBuilder(mKijiURIString).build()
    val kiji: Kiji = downcastAndOpenKiji(kijiURI)
    val (reader, scanner) = try {
      val table: KijiTable = downcastAndOpenKijiTable(kiji, kijiURI.getTable)

      try {
        val scannerOptions: CassandraKijiScannerOptions= split match {
//          case hBasePartition: HBaseKijiPartition => {
//            val sOptions = new KijiScannerOptions
//            sOptions.setStartRow(hBasePartition.getStartLocation)
//            sOptions.setStopRow(hBasePartition.getStopLocation)
//            sOptions
//          }
          case cassandraPartition: CassandraKijiPartition =>
            CassandraKijiScannerOptions.withTokens(cassandraPartition.startToken, cassandraPartition.stopToken)

          case _ => throw new UnsupportedOperationException(KijiSpark.UnsupportedKiji)
        }

        val (reader, scanner) = table.openTableReader() match {
//          case hBaseKijiTableReader: HBaseKijiTableReader => (hBaseKijiTableReader, hBaseKijiTableReader.getKijiResultScanner(kijiDataRequest,
//            scannerOptions.asInstanceOf[KijiScannerOptions]))
          case cassandraKijiTableReader: CassandraKijiTableReader => (cassandraKijiTableReader,
            cassandraKijiTableReader.getScannerWithOptions(kijiDataRequest, scannerOptions.asInstanceOf[CassandraKijiScannerOptions]))
          case _ => throw new UnsupportedOperationException(KijiSpark.UnsupportedKiji)
        }

        (reader, scanner)

      } finally {
        table.release()
      }
    } finally {
      kiji.release()
    }

    def closeResources() {
      scanner.close()
      reader.close()
    }

    // Register an on-task-completion callback to close the input stream.
   // context.addOnCompleteCallback(() => closeResources())
    context.addTaskCompletionListener(context => closeResources())

    // Must return an iterator of MaterializedKijiResults in order to work with the serializer.
    scanner match {
      /**
      case hBaseScanner: HBaseKijiResultScanner[T] => {
        hBaseScanner
          .asScala
          .map({ result: KijiResult[T] =>
          MaterializedKijiResult.create(
            result.getEntityId,
            result.getDataRequest,
            KijiResult.Helpers.getMaterializedContents(result)
          )
        })
      }*/
      case cassandraScanner: CassandraKijiResultScanner[T] => {
        cassandraScanner
            .asScala
            .map{result: KijiResult[T] =>
            MaterializedKijiResult.create(
                result.getEntityId,
                kijiDataRequest,
                KijiResult.Helpers.getMaterializedContents(result)
            )
        }
      }
    }
  }

  /**
   * Helper Method for Cassandra. Converts KijiRowData into materialized contents
   * so that a Materialized KijiResult can be constructed.
   * @param rowData The KijiRowData to be materialized
   * @param dataRequest The dataRequest that produced the kijiRowData
   * @return The materializedContents of the KijiRowData
   */
  private def rowDataMaterialContents(rowData: KijiRowData, dataRequest: KijiDataRequest):
    JSortedMap[KijiColumnName, JList[KijiCell[T]]] = {
      val materializedResult: JSortedMap[KijiColumnName, JList[KijiCell[T]]]  =
          new JTreeMap[KijiColumnName, JList[KijiCell[T]]]
      for(column: Column <- dataRequest.getColumns().asScala) {
        if(column.isPagingEnabled)
            throw new IllegalArgumentException("Columns should not be paged when using KijiSpark")
        val cells: JList[KijiCell[T]] =
            new JArrayList(rowData.getCells[T](column.getFamily, column.getQualifier).values)
        materializedResult.put(column.getColumnName, cells)
      }
      materializedResult
    }

  override protected def getPartitions: Array[Partition] = {
    val kijiURI: KijiURI = KijiURI.newBuilder(mKijiURIString).build()
    if (null == kijiURI.getTable) {
      throw new IllegalArgumentException("KijiURI must specify a table.")
    }
    val kiji: Kiji = downcastAndOpenKiji(kijiURI)
    LOG.debug("openedKiji")
    LOG.debug("openedKiji")
    try {
      val table: KijiTable = downcastAndOpenKijiTable(kiji, kijiURI.getTable)
      table match {
        /**
        case hBaseTable: HBaseKijiTable => {
          val regions = table.getRegions
          val numRegions = regions.size()
          val result = new Array[Partition](numRegions)

          for (i <- 0 until numRegions) {
            val startKey: Array[Byte] = regions.get(i).getStartKey
            val endKey: Array[Byte] = regions.get(i).getEndKey
            result(i) = HBaseKijiPartition(i, startKey, endKey)
          }

          table.release()
          result
        } */
        case cassandraTable: CassandraKijiTable => {

          //TODO SPARK-38 Get CassandraKijiPartitions to work correctly(just return one partition for now)
          //          val partitions = getCassandraPartitions((Long.MinValue :: getTokens(getSession)) :+ Long.MaxValue)
          val partitions = new Array[Partition](1)
          partitions(0) = CassandraKijiPartition(0, Long.MinValue, Long.MaxValue)
          table.release()
          partitions
        }
      }
    } finally {
      kiji.release()
    }
  }

  /**
   * Opens and returns the Kiji instance
   *
   *
   * @param kijiURI the KijiURI specifying the instance and table.
   * @return Kiji instance specified by kijiURI.
   * @throws UnsupportedOperationException if the Kiji is not C* or HBase.
   */
  private def downcastAndOpenKiji(kijiURI: KijiURI): Kiji = {
    LOG.debug("opening kiji")
    Kiji.Factory.open(kijiURI) match {
      //case hbaseKiji: HBaseKiji => hbaseKiji
      case cassandraKiji: CassandraKiji => cassandraKiji
      case nonHBaseKiji: Kiji => {
        nonHBaseKiji.release()
        throw new UnsupportedOperationException(KijiSpark.UnsupportedKiji)
      }
    }
  }

  /**
   * Opens and returns the KijiTable.
   *
   * @param kiji the kiji instance containing the table.
   * @param tableName the name of the table to open and downcast.
   * @return the KijiTable specified by tableName.
   * @throws UnsupportedOperationException if the KijiTable is not an C* or HBase.
   */
  private def downcastAndOpenKijiTable(kiji: Kiji, tableName: String): KijiTable = {
    kiji.openTable(tableName) match {
      //case hBaseKijiTable: HBaseKijiTable => hBaseKijiTable
      case cassandraKijiTable: CassandraKijiTable => cassandraKijiTable
      case nonHBaseKijiTable: KijiTable => {
        nonHBaseKijiTable.release()
        throw new UnsupportedOperationException(KijiSpark.UnsupportedKiji)
      }
    }
  }

  //TODO SPARK-38 Attempt to make Cassandra partitions work. There are still some bugs. Attempted to copy
  //TODO the getInputSplits in CassandraMapReduce

  //  private def getTokens(session: Session): List[Long] = {
  //    def tokens(queryString: String) = {
  //      val resultSet: ResultSet = session.execute(queryString)
  //      val results: java.util.List[Row] = resultSet.all
  //      results
  //    }
  //    val localResults = tokens("SELECT tokens FROM system.local;")
  //    Preconditions.checkArgument(localResults.size == 1)
  //    val localTokens: List[Long] = localResults.get(0).getSet("tokens", classOf[String]).asScala.toList.map(_.toLong)
  //    var allTokens = localTokens
  //    val peerResults: List[Row] = tokens("SELECT rpc_address, tokens FROM system.peers;").asScala.iterator.toList
  //    for(row <- peerResults) {
  //      val tokens = row.getSet("tokens", classOf[String]).asScala
  //      val rpcAddress: InetAddress = row.getInet("rpc_address")
  //      val hostName: String = rpcAddress.getHostName
  //      Preconditions.checkArgument(!(hostName == "localhost"))
  //      allTokens = allTokens ::: tokens.toList.map(_.toLong)
  //    }
  //    allTokens
  //  }
  //
  //  private def getCassandraPartitions(tokens: List[Long]): Array[Partition] = {
  //    val sortedTokens = tokens.sorted
  //    val partitions = new Array[Partition](tokens.size)
  //    for (i <- 0 until (tokens.size - 1)) {
  //      val startToken = if (i > 0) sortedTokens(i) + 1 else sortedTokens(i)
  //      val endToken = sortedTokens(i + 1)
  //      partitions(i) = new CassandraKijiPartition(i, startToken, endToken)
  //    }
  //    partitions
  //
  //  }
  //    private def getSession(): Session = {
  //      val cluster: Cluster = Cluster
  //          .builder
  //          .addContactPoints(kijiURI.asInstanceOf[CassandraKijiURI].getContactPoints
  //          .toArray(new Array[String](kijiURI.asInstanceOf[CassandraKijiURI].getContactPoints.size)): _*)
  //          .withPort(kijiURI.asInstanceOf[CassandraKijiURI].getContactPort)
  //          .build
  //      cluster.connect
  //    }

}

/** Companion object containing static members used by the KijiRDD class. */
object CassandraKijiRDD {

  /**
   *
   * @param sc
   * @param kijiURI
   * @param kijiDataRequest
   * @return
   */
  def apply(
      @transient sc: SparkContext,
      @transient kijiURI: KijiURI,
      kijiDataRequest: KijiDataRequest
  ): CassandraKijiRDD[_] = {
    new CassandraKijiRDD(sc, kijiURI, kijiDataRequest)
  }
}
