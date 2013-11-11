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

import com.twitter.scalding.JobTest

import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.flow
import org.kiji.express.flow.KijiInput
import org.kiji.express.flow.KijiOutput
import org.kiji.express.flow.QualifiedColumnRequestInput
import org.kiji.express.flow.QualifiedColumnRequestOutput
import org.kiji.express.flow.SchemaSpec
import org.kiji.express.music.avro.TopSongs

/**
 * A test for counting the number of times songs have been played by users.
 */
class TopNextSongsSuite extends KijiSuite {

  // Get a Kiji to use for the test and record the Kiji URI of the users and songs tables we'll
  // test against.
  val kiji = makeTestKiji("TopNextSongsSuite")
  val usersURI = kiji.getURI().toString + "/users"
  val songsURI = kiji.getURI().toString + "/songs"

  // Execute the DDL shell commands in music-schema.ddl to create the tables for the music
  // tutorial.
  executeDDLResource(kiji, "org/kiji/express/music/music-schema.ddl")

  // Create some fake track plays for three users.
  val testInput =
      (EntityId("user-0"),
          slice("info:track_plays", (0L, "song-0"), (1L, "song-1"), (2L, "song-2"))) ::
      (EntityId("user-1"),
          slice("info:track_plays", (0L, "song-0"), (1L, "song-0"), (2L, "song-1"))) ::
      (EntityId("user-2"),
          slice("info:track_plays", (0L, "song-1"), (1L, "song-2"), (2L, "song-1"))) ::
      Nil

  /**
   * Validates the top next songs produces for the three songs used in the test input. The counts
   * should be as follows.
   *
   * Played First     Played Second     Count
   * song-0           song-0            1
   * song-0           song-1            2
   * song-0           song-2            0
   * song-1           song-0            0
   * song-1           song-1            0
   * song-1           song-2            2
   * song-2           song-0            0
   * song-2           song-1            1
   * song-2           song-2            0
   *
   * @param topNextSongs contains three tuples for three songs, each containing a record of the
   *     top next songs played.
   */
  def validateTest(topNextSongs: Buffer[(EntityId, KijiSlice[TopSongs])]) {
    val topSongForEachSong = topNextSongs
        .map { case(eid, slice) => (eid(0).toString, slice.getFirstValue().getTopSongs) }

    topSongForEachSong.foreach {
      case ("song-0", topSongs) => {
        assert(2 === topSongs.size)
        assert("song-1" === topSongs.get(0).getSongId.toString)
        assert(2 === topSongs.get(0).getCount)
        assert("song-0" === topSongs.get(1).getSongId.toString)
        assert(1 === topSongs.get(1).getCount)
      }
      case ("song-1", topSongs) => {
        assert(1 === topSongs.size)
        assert("song-2" === topSongs.get(0).getSongId.toString)
        assert(2 === topSongs.get(0).getCount)
      }
      case ("song-2", topSongs) => {
        assert(1 === topSongs.size)
        assert("song-1" === topSongs.get(0).getSongId.toString)
        assert(1 === topSongs.get(0).getCount)
      }
    }
  }

  test("TopNextSongs computes how often one song is played after another (local).") {
    JobTest(new TopNextSongs(_))
        .arg("users-table", usersURI)
        .arg("songs-table", songsURI)
        .source(KijiInput(usersURI,
            Map(QualifiedColumnRequestInput("info", "track_plays", flow.all)
                -> 'playlist)), testInput)
        .sink(KijiOutput(songsURI, Map('top_next_songs ->
            QualifiedColumnRequestOutput(
                "info",
                "top_next_songs",
                schemaSpec = SchemaSpec.Specific(classOf[TopSongs])))))(validateTest)
        .run
        .finish
  }

  test("TopNextSongs computes how often one song is played after another (Hadoop).") {
    JobTest(new TopNextSongs(_))
        .arg("users-table", usersURI)
        .arg("songs-table", songsURI)
        .source(KijiInput(usersURI,
            Map(QualifiedColumnRequestInput("info", "track_plays", maxVersions = flow.all)
                -> 'playlist)),
            testInput)
        .sink(KijiOutput(songsURI, Map('top_next_songs ->
            QualifiedColumnRequestOutput(
                "info",
                "top_next_songs",
                schemaSpec = SchemaSpec.Specific(classOf[TopSongs]))))) { validateTest }
        .runHadoop
        .finish
  }
}
