package com.wibidata.lang

import java.util.NavigableMap

import com.twitter.scalding._
import org.apache.avro.util.Utf8

import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI

/**
 * Usage:
 *   kiji jar <path/to/this/jar> com.twitter.scalding.Tool com.wibidata.lang.NewsgroupWordCount \
 *       --input kiji://.env/default/words --output ./wordcount.tsv --hdfs
 *
 * Would like usage to be: (from root of scalding dir. )
 *   scripts/scald.rb -cp $(kiji classpath) <path/to/this/script> \
 *       --input kiji://.env/default/words --output ./wordcount.tsv --hdfs
 *
 * [Currently this doesn't work because dependencies aren't packaged properly]
 */
class NewsgroupWordCount(args: Args) extends Job(args) {
  val request = {
    val builder = KijiDataRequest.builder()
    builder.addColumns(builder.newColumnsDef().add("info", "word"))
    builder.build()
  }

  val uri = KijiURI.newBuilder(args("input")).build()

  // Run the job.
  KijiSource(request, uri)
      .map('info_word -> 'token) { words: NavigableMap[Long, Utf8] =>
        // words is a map from timestamp to Utf8 string; there should only be one entry.
        val word = words.firstEntry().getValue().toString()
        word.toLowerCase() }
      .groupBy('token) { _.size }
      .write(Tsv(args("output")))
}
