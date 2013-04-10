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

import scala.collection.mutable.Buffer

import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.DSL._

/**
 * A test for counting the number of times songs have been played by users.
 */
class SongPlayCounterSuite extends KijiSuite {

  // Get a Kiji to use for the test and record the Kiji URI of the users table we'll test against.
  val kiji = makeTestKiji("SongPlayCounterSuite")
  val tableURI = kiji.getURI().toString + "/users"

  // Execute the DDL shell commands in music-schema.ddl to create the tables for the music
  // tutorial, including the users table.
  executeDDLResource(kiji, "org/kiji/express/music/music-schema.ddl")

  // Create some fake track plays for three users.
  val testInput =
      (id("user-0"), slice("info:track_plays", (0L, "song-0"), (1L, "song-1"), (2L, "song-2"))) ::
      (id("user-1"), slice("info:track_plays", (0L, "song-0"), (1L, "song-0"), (2L, "song-1"))) ::
      (id("user-2"), slice("info:track_plays", (0L, "song-1"), (1L, "song-2"), (2L, "song-1"))) ::
      Nil

  /**
   * Validates the song counts produced by test jobs. For the test input used,
   * "song-0" should have a count of 3, "song-1" a count of 4, and "song-2" a count of 2.
   *
   * @param songCounts contains three tuples of song id and play count.
   */
  def validateTest(songCounts: Buffer[(String, Long)]) {
    val sortedBySong = songCounts.sortBy { case(song, count) => song }
    assert(3 == sortedBySong.size)
    (0 until 2).foreach { i =>
      assert("song-" + i == sortedBySong(i)._1)
    }
    assert(3 == sortedBySong(0)._2)
    assert(4 == sortedBySong(1)._2)
    assert(2 == sortedBySong(2)._2)
  }

  test("SongPlayCounter counts the number of times songs have been played by users (local).") {
    JobTest(new SongPlayCounter(_))
        .arg("table-uri", tableURI)
        .arg("output", "counts.tsv")
        .source(KijiInput(tableURI)(Map(Column("info:track_plays", all) -> 'playlist)), testInput)
        .sink(Tsv("counts.tsv")) { validateTest }
        .run
        .finish
  }

  test("SongPlayCounter counts the number of times songs have been played by users (Hadoop).") {
    JobTest(new SongPlayCounter(_))
        .arg("table-uri", tableURI)
        .arg("output", "counts.tsv")
        .source(KijiInput(tableURI)(Map(Column("info:track_plays", all) -> 'playlist)), testInput)
        .sink(Tsv("counts.tsv")) { validateTest }
        .runHadoop
        .finish
  }
}
