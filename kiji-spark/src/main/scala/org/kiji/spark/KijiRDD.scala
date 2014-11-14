package org.kiji.spark

import scala.collection.Iterator
import scala.collection.JavaConverters.asScalaIteratorConverter

import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiResult
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableReader.KijiScannerOptions
import org.kiji.schema.KijiURI
import org.kiji.schema.impl.MaterializedKijiResult
import org.kiji.schema.impl.hbase.HBaseKiji
import org.kiji.schema.impl.hbase.HBaseKijiResultScanner
import org.kiji.schema.impl.hbase.HBaseKijiTable
import org.kiji.schema.impl.hbase.HBaseKijiTableReader

/**
 * An RDD that provides the core functionality for reading Kiji data.
 *
 * Currently, KijiSpark supports only HBase Kiji instances.
 *
 * @param sc The SparkContext to associate this RDD with.
 * @param kijiURI The KijiURI to identify the Kiji instance and table; must include the table name.
 * @param kijiDataRequest The KijiDataRequest for the table provided by kijiURI.
 */
class KijiRDD[T](
    @transient sc: SparkContext,
    @transient kijiURI: KijiURI,
    kijiDataRequest: KijiDataRequest
)
extends RDD[KijiResult[T]](sc, Nil){
  import org.kiji.spark.KijiRDD._

  /**
   * KijiURIs are not serializable; this string representation allows
   * the provided kijiURI to be reconstructed upon deserialization of the RDD.
   */
  private val mKijiURIString = kijiURI.toString

  override def compute(split: Partition, context: TaskContext): Iterator[KijiResult[T]] = {
    val partition = split.asInstanceOf[KijiPartition]

    val kijiURI: KijiURI = KijiURI.newBuilder(mKijiURIString).build()
    val kiji: HBaseKiji = downcastAndOpenHBaseKiji(kijiURI)

    val (reader, scanner) = try {
      val table: HBaseKijiTable = downcastAndOpenHBaseKijiTable(kiji, kijiURI.getTable)

      try {
        // Reader must be HBaseKijiTableReader in order to return a KijiResultScanner.
        val reader: HBaseKijiTableReader = table.openTableReader() match {
          case hBaseKijiTableReader: HBaseKijiTableReader => hBaseKijiTableReader
          case _ => throw new UnsupportedOperationException(HBASE_KIJI_ONLY)
        }

        val scannerOptions: KijiTableReader.KijiScannerOptions = new KijiScannerOptions
        scannerOptions.setStartRow(partition.getStartRow)
        scannerOptions.setStopRow(partition.getStopRow)
        val scanner: HBaseKijiResultScanner[T] =
          reader.getKijiResultScanner(kijiDataRequest, scannerOptions)

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
    context.addOnCompleteCallback(() => closeResources())

    // Must return an iterator of MaterializedKijiResults in order to work with the serializer.
    scanner
        .asScala
        .map({ result: KijiResult[T] =>
          MaterializedKijiResult.create(
              result.getEntityId,
              result.getDataRequest,
              KijiResult.Helpers.getMaterializedContents(result)
          )
        })
  }

  override protected def getPartitions: Array[Partition] = {
    val kijiURI: KijiURI = KijiURI.newBuilder(mKijiURIString).build()
    if (null == kijiURI.getTable) {
      throw new IllegalArgumentException("KijiURI must specify a table.")
    }
    val kiji: HBaseKiji = downcastAndOpenHBaseKiji(kijiURI)

    try {
      val table: HBaseKijiTable = downcastAndOpenHBaseKijiTable(kiji, kijiURI.getTable)
      val regions = table.getRegions
      val numRegions = regions.size()
      val result = new Array[Partition](numRegions)

      for (i <- 0 until numRegions) {
        val startKey: Array[Byte] = regions.get(i).getStartKey
        val endKey: Array[Byte] = regions.get(i).getEndKey
        result(i) = new KijiPartition(i, startKey, endKey)
      }

      table.release()
      result
    } finally {
      kiji.release()
    }
  }

  /**
   * Opens and returns the Kiji instance; throws an exception if it is not an HBaseKiji.
   *
   * @param kijiURI the KijiURI specifying the instance and table.
   * @return HBaseKiji instance specified by kijiURI.
   * @throws UnsupportedOperationException if the Kiji is not an HBaseKiji.
   */
  private def downcastAndOpenHBaseKiji(kijiURI: KijiURI): HBaseKiji = {
    Kiji.Factory.open(kijiURI) match {
      case kiji: HBaseKiji => kiji
      case nonHBaseKiji: Kiji => {
        nonHBaseKiji.release()
        throw new UnsupportedOperationException(HBASE_KIJI_ONLY)
      }
    }
  }

  /**
   * Opens and returns the KijiTable; throws an exception if it is not an HBaseKijiTable.
   *
   * @param kiji the kiji instance containing the table.
   * @param tableName the name of the table to open and downcast.
   * @return the HBaseKijiTable specified by tableName.
   * @throws UnsupportedOperationException if the KijiTable is not an HBaseKijiTable.
   */
  private def downcastAndOpenHBaseKijiTable(kiji: HBaseKiji, tableName: String): HBaseKijiTable = {
    kiji.openTable(tableName) match {
      case hBaseKijiTable: HBaseKijiTable => hBaseKijiTable
      case nonHBaseKijiTable: KijiTable => {
        nonHBaseKijiTable.release()
        throw new UnsupportedOperationException(HBASE_KIJI_ONLY)
      }
    }
  }
}

/** Companion object containing static members used by the KijiRDD class. */
object KijiRDD {

  /** Error message indicating that the Kiji instance must be an HBaseKiji. */
  val HBASE_KIJI_ONLY = "KijiSpark currently only supports HBase Kiji instances."
}
