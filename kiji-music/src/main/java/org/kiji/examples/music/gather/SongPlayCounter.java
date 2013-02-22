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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

/**
 * Gatherer to count the total number of times each song has been played.
 *
 * Reads the track plays from the user table and emits (song ID, 1) pairs for each track play.
 * This gatherer should be combined with a summing reducer (such as LongSumReducer) to count the
 * number of plays for every track.
 */
public class SongPlayCounter extends KijiGatherer<Text, LongWritable> {
  /** Only keep one Text object around to reduce the chance of a garbage collection pause.*/
  private Text mText;
  /** Only keep one LongWritable object around to reduce the chance of a garbage collection pause.*/
  private static final LongWritable ONE = new LongWritable(1);

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return LongWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(GathererContext<Text, LongWritable> context) throws IOException {
    super.setup(context); // Any time you override setup, call super.setup(context);
    mText = new Text();
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // This method is how we specify which columns in each row the gatherer operates on.
    // In this case, we need all versions of the info:track_plays column.
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef()
        .withMaxVersions(HConstants.ALL_VERSIONS) // Retrieve all versions.
        .add("info", "track_plays");
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public void gather(KijiRowData row, GathererContext<Text, LongWritable> context)
      throws IOException {
    // The gather method operates on one row at a time.  For each user, we iterate through
    // all their track plays and emit a pair of the track ID and the number 1.
    NavigableMap<Long, CharSequence> trackPlays = row.getValues("info", "track_plays");
    for (CharSequence trackId : trackPlays.values()) {
      mText.set(trackId.toString());
      context.write(mText, ONE);
      mText.clear();
    }
  }

}
