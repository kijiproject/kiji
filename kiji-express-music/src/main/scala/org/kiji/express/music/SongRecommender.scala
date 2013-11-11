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
import org.kiji.express.music.avro._

/**
 * Generates recommendations for the next song each user might like to listen to.
 *
 * For each user, we write a recommendation for the next song into
 * the info:next_song_rec column, based on the most recent song recorded in info:track_plays.
 * We incorporate the information we generated about popular sequences of songs by joining
 * tuples in the recommendedNextSongs pipe with the tuples in the main pipe on the songId
 * and lastTrackPlayed fields.
 *
 * @param args passed to this job from the command line.
 */
class SongRecommender(args: Args) extends KijiJob(args) {
  /**
   * This method retrieves the most popular song (at index 0) in the TopNextSongs record.
   *
   * @param songs from the TopNextSongs record.
   * @return the most popular song.
   */
  def getMostPopularSong(songs: KijiSlice[TopSongs]): String = {
    songs.getFirstValue().getTopSongs.get(0).getSongId.toString
  }

  /**
   * This Scalding RichPipe does the following:
   * 1. Reads the column "info:top_next_songs" from the songs table and emits a tuple for
   *      every row.
   * 2. Retrieves the most popular song played (in the 'nextSong field) after every given
          song (in the 'songId field.)
   * 3. Emits tuples containing only the fields 'songId and 'nextSong.
   */
  val recommendedSong = KijiInput(args("songs-table"),
      Map(QualifiedColumnRequestInput("info", "top_next_songs", classOf[TopSongs])
          -> 'topNextSongs))
      .map('entityId -> 'songId) { eId: EntityId => eId(0) }
      .map('topNextSongs -> 'nextSong) { getMostPopularSong }
      .project('songId, 'nextSong)

  /**
   * This Scalding pipeline does the following:
   * 1. Reads the column "info:track_plays" from the users table.
   * 2. Retrieves the song most recently played by a user.
   * 3. Retrieve the TopNextSongs associated with the most recently played song by joining
          together the tuples emitted from the nextSongs pipe with the the 'lastTrackPlayed
          field.
   */
  KijiInput(args("users-table"),
      Map(QualifiedColumnRequestInput("info", "track_plays") -> 'trackPlays))
      .map('trackPlays -> 'lastTrackPlayed) {
           slice: KijiSlice[CharSequence] => slice.getFirstValue().toString }
      .joinWithSmaller('lastTrackPlayed -> 'songId, recommendedSong)
      .write(KijiOutput(args("users-table"),
          Map('nextSong -> QualifiedColumnRequestOutput("info", "next_song_rec"))))
}
