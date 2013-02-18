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

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;

import org.kiji.examples.music.SongBiGram;
import org.kiji.examples.music.SongCount;
import org.kiji.mapreduce.KijiReducer;
import org.kiji.mapreduce.avro.AvroKeyReader;
import org.kiji.mapreduce.avro.AvroKeyWriter;
import org.kiji.mapreduce.avro.AvroValueWriter;

/**
 * A reducer that takes in pairs of songs that have been played sequentially and the number one.
 * It then computes the number of times those songs have been played together, and emits the id of
 * the first song as the key, and a SongCount record representing the song played after the first as
 * the value. A SongCount record has a field containing the id of the subsequent song and a field
 * for the number of time sit has been played after the initial song.
 */
public class SequentialPlayCountReducer
    extends KijiReducer<
        AvroKey<SongBiGram>, LongWritable, AvroKey<CharSequence>, AvroValue<SongCount>>
    implements AvroKeyReader, AvroKeyWriter, AvroValueWriter {

  /** {@inheritDoc} */
  @Override
  protected void reduce(AvroKey<SongBiGram> key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    // Initialize sum to zero.
    long sum = 0L;
    // Add up all the values.
    for (LongWritable value : values) {
      sum += value.get();
    }

    // Set values for this count.
    final SongBiGram songPair = key.datum();

    final SongCount nextSongCount = SongCount.newBuilder()
         .setCount(sum)
         .setSongId(songPair.getSecondSongPlayed())
         .build();
    // Write out result for this song
    context.write(
        new AvroKey<CharSequence>(songPair.getFirstSongPlayed().toString()),
        new AvroValue<SongCount>(nextSongCount));
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyReaderSchema() throws IOException {
    return SongBiGram.SCHEMA$;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return AvroKey.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return AvroValue.class;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroValueWriterSchema() throws IOException {
    return SongCount.SCHEMA$;
  }

  /** {@inheritDoc} */
  @Override
  public Schema getAvroKeyWriterSchema() throws IOException {
    // Programmatically retrieve the avro schema for a String.
    return Schema.create(Schema.Type.STRING);
  }
}
