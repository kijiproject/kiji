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

package org.kiji.examples.music.reduce;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.examples.music.SongCount;
import org.kiji.examples.music.TopSongs;
import org.kiji.mapreduce.AvroKeyReader;
import org.kiji.mapreduce.AvroValueReader;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.KijiTableReducer;


/**
 * This reducer writes a list of the top songs played after a song (the key) to the corresponding
 * row in song table's "info:top_next_songs" column.
 */
public class TopNextSongsReducer
    extends KijiTableReducer<AvroKey<CharSequence>, AvroValue<SongCount>>
    implements AvroKeyReader, AvroValueReader {
    private static final Logger LOG = LoggerFactory.getLogger(TopNextSongsReducer.class);

  /** An ordered set used to track the most popular songs played after the song being processed. */
  private TreeSet<SongCount> mTopNextSongs;

  /** The number of most popular next songs to keep track of for each song. */
  private final int mNumberOfTopSongs = 3;

  /** A list of SongCounts corresponding to the most popular next songs for each key/song. */
  private TopSongs mTopSongs;

  /** {@inheritDoc} */
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mTopSongs = new TopSongs();
    // This TreeSet will keep track of the "largest" SongCount objects seen so far. Two SongCount
    // objects, song1 and song2, can be compared and the object with the largest value in the field
    // count will the declared the largest object.
    mTopNextSongs = new TreeSet<SongCount>(new Comparator<SongCount>() {
      @Override
      public int compare(SongCount song1, SongCount song2) {
        if (song1.getCount().compareTo(song2.getCount()) == 0) {
          return song1.getSongId().toString().compareTo(song2.getSongId().toString());
        } else {
          return song1.getCount().compareTo(song2.getCount());
        }
      }
    });
  }

  /** {@inheritDoc} */
  @Override
  protected void reduce(AvroKey<CharSequence> key, Iterable<AvroValue<SongCount>> values,
      KijiTableContext context) throws IOException {
    // We are reusing objects, so we should make sure they are cleared for each new key.
    mTopNextSongs.clear();

    // Iterate through the song counts and track the top ${mNumberOfTopSongs} counts.
    for (AvroValue<SongCount> value : values) {
      // Remove AvroValue wrapper.
      SongCount currentSongCount = SongCount.newBuilder(value.datum()).build();

      mTopNextSongs.add(currentSongCount);
      // If we now have too many elements, remove the element with the smallest count.
      if (mTopNextSongs.size() > mNumberOfTopSongs) {
        mTopNextSongs.pollFirst();
      }
    }
    // Set the field of mTopSongs to be a list of SongCounts corresponding to the top songs played
    // next for this key/song.
    mTopSongs.setTopSongs(Lists.newArrayList(mTopNextSongs));
    // Write this to the song table.
    context.put(context.getEntityId(key.datum().toString()), "info", "top_next_songs", mTopSongs);
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueReaderSchema() throws IOException {
    return SongCount.SCHEMA$;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return Schema.create(Schema.Type.STRING);
  }
}
