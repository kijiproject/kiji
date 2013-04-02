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

package org.kiji.examples.music;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.examples.music.gather.SequentialPlayCounter;
import org.kiji.examples.music.reduce.SequentialPlayCountReducer;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;
import org.kiji.mapreduce.kvstore.lib.AvroKVRecordKeyValueStore;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Test for SequentialPlayCounter. */
public class TestSequentialSongPlayCounter extends KijiClientTest {
   private static final Logger LOG = LoggerFactory.getLogger(TestSongPlayCounter.class);

  private KijiURI mUserTableURI;

  /** Initialize our environment. */
  @Before
  public final void setup() throws Exception {
    final KijiTableLayout userLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String userTableName = userLayout.getName();
    mUserTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(userTableName).build();


    new InstanceBuilder(getKiji())
        .withTable(userTableName, userLayout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-2")
                .withValue(3L, "song-1")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-3")
                .withValue(3L, "song-2")
                .withValue(4L, "song-1")
            .withRow("user-3").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-5")
        .build();
  }

  /** Test that our MR job computes results as expected. */
  @Test
  public void testSongPlayCounter() throws Exception {
    // Configure and run job.
    final File outputDir = new File(getLocalTempDir(), "output.sequence_file");
    final Path path = new Path("file://" + outputDir);
    final KijiMapReduceJob mrjob = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withGatherer(SequentialPlayCounter.class)
        .withReducer(SequentialPlayCountReducer.class)
        .withInputTable(mUserTableURI)
        // Note: the local map/reduce job runner does not allow more than one reducer:
        .withOutput(new AvroKeyValueMapReduceJobOutput(new Path("file://" + outputDir), 1))
        .build();
    assertTrue(mrjob.run());

    // Using a KVStoreReader here is a hack. It works in the sense it is easy to read from, but it
    // assumes that the is only one value for every key.
    AvroKVRecordKeyValueStore.Builder kvStoreBuilder = AvroKVRecordKeyValueStore.builder()
        .withInputPath(path).withConfiguration(getConf());
    final AvroKVRecordKeyValueStore outputKeyValueStore = kvStoreBuilder.build();
    KeyValueStoreReader reader = outputKeyValueStore.open();

    // Check that our results are correct.
    assertTrue(reader.containsKey("song-1"));
    SongCount song1Result = (SongCount) reader.get("song-1");
    assertEquals(2L, song1Result.getCount().longValue());
    // Avro strings are deserialized to CharSequences in Java, .toString() allows junit to correctly
    // compare the expected and actual values.

    assertEquals("song-2", song1Result.getSongId().toString());
    assertTrue(reader.containsKey("song-2"));
    SongCount song2Result = (SongCount) reader.get("song-2");
    assertEquals(1L, song2Result.getCount().longValue());
    // Avro strings are deserialized to CharSequences in Java, .toString() allows junit to correctly
    // compare the expected and actual values.
    assertEquals("song-3", song2Result.getSongId().toString());
  }
}
