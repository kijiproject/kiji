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

import java.util.Random

import org.kiji.express.AvroRecord
import org.kiji.express.EntityId
import org.kiji.express.modeling.Scorer

/**
 * Recommends the song most frequently played after the song a user has most recently
 * listened to as the next track the user should listen to. This Scorer uses the tuple field
 * `trackPlay`, provided by an extractor, which should contain the latest track a user has
 * listened to. This Scorer also expects to have access to a key-value store named
 * `top_next_songs` that maps song ids to a list of songs most frequently played after the
 * song in question. The song most frequently played after the song most recently listened
 * to by the user is used as the recommendation.
 */
class SongRecommendingScorer extends Scorer {

  /**
   * Gets a score function that, given the song id of a song, accesses the `top_next_songs`
   * key-value store to determine a recommendation for the next song to listen to.
   *
   * @return the song id of the track recommended for the user.
   */
  override def scoreFn = score('trackPlay) { trackPlay: String =>
    // Retrieve the key-value store that maps song ids to a list of songs most frequently played
    // after the song in question.
    val topNextSongsKVStore = keyValueStore[EntityId, AvroRecord]("top_next_songs")
    // "Optionally" retrieve a record containing a list of songs played most frequently after the
    // most recent track a user has listened to. We say "optionally" because the key-value store
    // may not be populated with a list of top next songs for a given track. If a record of top
    // next songs is available, then a 'Some' wrapping the record is retrieved. Otherwise,
    // a 'None' is retrieved.
    val topNextSongs: Option[AvroRecord] = topNextSongsKVStore.get(EntityId(trackPlay))
    // If topNextSongs contains 'Some' AvroRecord, transform that AvroRecord into 'Some'
    //  String that contains the name of the next song to listen to. If topNextSongs is 'None',
    // then this transformation will result in 'None'.
    val nextSong: Option[String] = topNextSongs.map { songsListRecord =>
      // Access the record field "topSongs", which contains a list of song count records,
      // then get the first record in that list and extract the field "song_id" from it. This
      // will be the id of the song most frequently played after the user's most recently
      // listened to track.
      songsListRecord("top_songs").asList()(0)("song_id").asString()
    }
    // If nextSong is 'Some' recommendation, return that recommendation,
    // if it's 'None' generate a random song name and use that as the recommendation.
    nextSong.getOrElse("song-" + new Random().nextInt(50))
  }
}
