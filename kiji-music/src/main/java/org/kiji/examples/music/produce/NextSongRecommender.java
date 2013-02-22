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


package org.kiji.examples.music.produce;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.kiji.examples.music.SongCount;
import org.kiji.examples.music.TopSongs;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreClient;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.RequiredStores;
import org.kiji.mapreduce.kvstore.lib.UnconfiguredKeyValueStore;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

/**
 * Producer generating recommendations for the next songs each user might like.
 *
 * A producer operates over one Kiji row at a time, and writes out to the same row.
 *
 * In this producer, for each user, we write a recommendation for the next song into
 * the info:next_song_rec column, based on their track_plays, and using the provided
 * KeyValueStore that is a map from song to the songs played after that song, by popularity.
 * The KeyValueStore must be specified either from the command line, or must be overridden
 * when the job is configured.
 */
public class NextSongRecommender extends KijiProducer implements KeyValueStoreClient {

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // Only request the most recent version from the "info:track_plays" column.
    return KijiDataRequest.create("info", "track_plays");
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    // This is the output column of the kiji table that we write to.
    return "info:next_song_rec";
  }

    /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, ProducerContext context) throws IOException {
    // Open the key value store reader.
    KeyValueStoreReader<String, TopSongs> topNextSongsReader = null;
    try {
      topNextSongsReader = context.getStore("nextPlayed");
    } catch (InterruptedException ex) {
      // It is pointless to continue if we can't open the key value store.
      throw new RuntimeException(ex);
    }
    // Get the most recent song the user has listened to:
    String mostRecentSong = input.<CharSequence>getMostRecentValue("info", "track_plays")
        .toString(); // Avro strings get deserialized to CharSequences.
    // Read the most popular songs played after mostRecentSong, from the song table.
    TopSongs topSongs = topNextSongsReader.get(mostRecentSong);
    // Read the array of song counts stored in field "" of the KeyValueStore.
    List<SongCount> popularNextSongs = topSongs.getTopSongs();
    // Write our recommended next song to "info:next_song_rec"
    context.put(recommend(popularNextSongs));
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    // We set the default KVStore to be unconfigured; see https://jira.kiji.org/browse/KIJIMR-91
    // We will have to supply a KVStore in an .xml file from the command line
    // when running this producer.
    return RequiredStores.just("nextPlayed", UnconfiguredKeyValueStore.builder().build());
  }

  /**
   * This method uses a list of song counts to determine the next song a user should listen to.
   *
   * @param topNextSongs A list of most popular songs.
   * @return CharSequence The id of the recommended song.
   */
  private CharSequence recommend(List<SongCount> topNextSongs) {
    return topNextSongs.get(0).getSongId(); // Do the simplest possible thing.
  }
}
