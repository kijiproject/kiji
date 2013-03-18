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

import org.kiji.chopsticks.DSL._
import org.kiji.schema.EntityId

/**
 * Counts the words from the newsgroup table.
 *
 * Usage:
 *   chop hdfs <path/to/this/jar> \
 *       org.kiji.chopsticks.NewsgroupWordCount \
 *       --input kiji://.env/default/words --output ./wordcount.tsv
 */
class NewsgroupWordCount(args: Args) extends Job(args) {
  val tableUri: String = args("input")
  val resultUri: String = args("output")

  def getMostRecent[T](timeline: NavigableMap[Long, T]): T = timeline.firstEntry().getValue()

  KijiInput(tableUri)("info:word" -> 'word)
      .map('word -> 'cleanword) { words: NavigableMap[Long, Utf8] =>
        getMostRecent(words)
            .toString()
            .replace("""[^A-Za-z0-9'_-]""", "")
            .toLowerCase()
      }
      .groupBy('cleanword) { _.size }
      .write(Tsv(resultUri))

  KijiInput(tableUri)("info:word" -> 'word)
      .map(('entityId, 'word) -> 'doubleword) { tuple: (EntityId, NavigableMap[Long, Utf8]) =>
        val (_, words) = tuple
        val word = getMostRecent(words).toString()
        "%s%s".format(word, word)
      }
      .write(KijiOutput(tableUri)('doubleword -> "info:doubleword"))
}
