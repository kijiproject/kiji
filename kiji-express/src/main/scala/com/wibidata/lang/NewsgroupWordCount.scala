package com.wibidata.lang

import java.util.HashMap
import java.util.NavigableMap

import com.twitter.scalding._
import org.apache.avro.util.Utf8

import org.kiji.schema.EntityId
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI

/**
 * Counts the words from the newsgroup table.
 *
 * Usage:
 *   kiji jar <path/to/this/jar> com.twitter.scalding.Tool com.wibidata.lang.NewsgroupWordCount \
 *       --input kiji://.env/default/words --output ./wordcount.tsv --hdfs
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

  // Demo/test of writing to a KijiSource.
  // Map from the field name to the column you want them written.
  val outputSpec = new java.util.HashMap[String, String]()
  outputSpec.put("info_doubleword", "info_doubleword")
  KijiSource(request,uri)
    .map(('entityid, 'info_word) -> 'info_doubleword) {
      tup:(EntityId, NavigableMap[Long, Utf8]) => tup match {
        case (id, words) =>
          "%s%s".format(words.firstEntry().getValue(), words.firstEntry().getValue())
      }
    }
    .write(KijiSource(request, uri, outputSpec))
}
