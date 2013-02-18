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

package org.kiji.examples.music.gather;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;

import org.kiji.examples.music.SongBiGram;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

/**
 * This gatherer reads from the "info:track_plays" column of the user table, and for every pair of
 * songs played in a row, it emits a SongBiGram and a LongWritable(1). This will allow us to count
 * how many times the two songs have been played in that order.
 */
public class SequentialPlayCounter extends KijiGatherer<AvroKey<SongBiGram>, LongWritable>
  implements AvroKeyWriter {
  /** Only keep one LongWritable object, to reduce the chance of a garbage collection pause. */
  private static final LongWritable ONE = new LongWritable(1);
  /** Only keep one SongBiGram object, to reduce the chance of a garbage collection pause. */
  private SongBiGram mBiGram;

  /** {@inheritDoc} */
  @Override
  public void setup(GathererContext<AvroKey<SongBiGram>, LongWritable> context) throws IOException {
    mBiGram = new SongBiGram();
  }

  /** {@inheritDoc} */
  @Override
  public void gather(KijiRowData input, GathererContext<AvroKey<SongBiGram>, LongWritable> context)
      throws IOException {
    CharSequence firstSong = null;
    CharSequence nextSong = null;
    NavigableMap<Long, CharSequence> trackPlays = input.getValues("info", "track_plays");
    for (CharSequence trackId : trackPlays.values()) {
      if (null != nextSong) {
        // If this is not the first song, we need to slide our window along.
        firstSong = nextSong;
        nextSong = trackId;
        mBiGram.setFirstSongPlayed(firstSong);
        mBiGram.setSecondSongPlayed(nextSong);
        context.write(new AvroKey<SongBiGram>(mBiGram), ONE);
      } else {
        // If this is the most recent song played, set ourselves up for the next iteration.
        nextSong = trackId;
      }
    }
  }


  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // Retrieve all versions of info:track_plays:
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef()
        .withMaxVersions(HConstants.ALL_VERSIONS)
        .add("info", "track_plays");
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return LongWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    // Our class is AvroKey, note that we must also specify the schema.
    return AvroKey.class;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    // Since we are writing AvroKeys, we need to specify the schema.
    return SongBiGram.SCHEMA$;
  }

}
