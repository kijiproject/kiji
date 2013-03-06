/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.chopsticks

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
 *   kiji jar <path/to/this/jar> com.twitter.scalding.Tool \
 *       org.kiji.chopsticks.NewsgroupWordCount \
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
