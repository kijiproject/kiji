package org.kiji.spark

import scala.collection.JavaConverters.asScalaIteratorConverter

import org.apache.avro.util.Utf8
import org.apache.hadoop.hbase.HConstants
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.kiji.schema.{KijiDataRequestBuilder, KijiDataRequest, KijiResult, KijiURI}
import org.kiji.spark.connector.rdd.KijiRDD
import org.kiji.spark.connector.conversions._

/**
 * Example KijiSpark job that manipulates Kiji data to generate counts of track splays.
 *
 * This uses the data from the Kiji MapReduce Music Recommendation Tutorial.
 * Make sure you've followed the instructions on downloading the sample data and
 * importing it to bento-HDFS.
 *
 * http://docs.kiji.org/tutorials/music-recommendation/1.1.7/music-setup/
 * http://docs.kiji.org/tutorials/music-recommendation/1.1.7/bulk-importing/
 */
object PlayCount {
  def main(args: Array[String]): Unit = {

    // Settings that are manually defined like this take precedence
    // over the defaults defined in spark-defaults.conf
    val conf = new SparkConf()
        .setAppName("Play Count")
        // The following two required settings should also be specified in spark-defaults.conf
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "org.kiji.spark.connector.serialization.KijiSparkRegistrator")

    // The SparkContext is the main entry point for Spark functionality.
    val sc: SparkContext = new SparkContext(conf)

    // get the URI of the table
    val userProvidedURI: String = args(0)
    val kijiURI: KijiURI = KijiURI.newBuilder(userProvidedURI).build()

    // Build the DataRequest; make sure we request all versions.
    val builder: KijiDataRequestBuilder = KijiDataRequest.builder()
    builder.newColumnsDef()
        .withMaxVersions(HConstants.ALL_VERSIONS)
        .add("info", "track_plays")
    val dataRequest: KijiDataRequest = builder.build()

    val kijiRDD: KijiRDD[Utf8] = sc.kijiRDD(kijiURI, dataRequest, classOf[Utf8])

    // Track plays are stored as KijiCells at different timestamps in this column.
    // We need to collect all versions of the data in this column into one collection to work with.
    // We use flatMap on the iterator returned by KijiResult#getValues to do so.
    val allTrackPlays: RDD[Utf8] = kijiRDD.flatMap(
        result => KijiResult.Helpers.getValues(result).iterator().asScala
    )

    // Now the fun part: we map this RDD to a PairRDD (an RDD of (K,V) pairs; a tuple).
    // We map each track play to pairs of (song ID, 1) and reduce by key, adding the values.
    // Note: Calling toString is unnecessary; I just like String over org.apache.avro.util.Utf8
    val playCounts: RDD[(String, Int)] = allTrackPlays.map(
        value => (value.toString, 1)
        ).reduceByKey(_+_)

    // This RDD can be materialized with the collect action. We can print the result.
    val histogram: Array[(String, Int)] = playCounts.collect()
    histogram.foreach(println)

    // Finally, we can save the song play counts to a text file.
    // This will save to HDFS if you're using the bento box; type `hadoop fs -ls playcount`
    // Then we could, for example, use R to import the data and do more data analysis.
    playCounts.saveAsTextFile("playcount")
  }
}
