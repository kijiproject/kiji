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

package org.kiji.express.music

import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.flow._

/**
 * Counts the number of times a song has been played by users.
 *
 * This importer expects to receive two command line arguments: `--table-uri` and `--output`.
 * The argument `--table-uri` should be set to the Kiji URI of a users table that contains
 * a column `info:track_plays` that contains a song id for each song a user has listened to.
 * The argument `--output` should be the HDFS path where a tab-delimited file listing
 * song ids and play counts should be written.
 *
 * @param args passed in from the command line.
 */
class SongPlayCounter(args: Args) extends KijiJob(args) {

  /**
   * Gets the ids of songs a user has listened to.
   *
   * @param slice from the column `info:track_plays` that records all the songs a user has
   *     listened to.
   * @return the song ids that a user has listened to.
   */
  def songsListenedTo(slice: KijiSlice[CharSequence]): Seq[String] = {
    slice.cells.map { cell => cell.datum.toString }
  }

  // This Scalding pipeline does the following.
  // 1. Reads the column "info:track_plays" from rows in a Kiji table.
  // 2. Breaks each user's track plays history into individual songs.
  // 3. Counts the number of times each song has been played.
  // 4. Writes each song id and play count to a file in HDFS.
  KijiInput(args("table-uri"),
      Map(QualifiedColumnRequestInput("info", "track_plays", maxVersions = all) -> 'playlist))
      .flatMapTo('playlist -> 'song) { songsListenedTo }
      .groupBy('song) { _.size('songCount) }
      .write(Tsv(args("output")))
}
