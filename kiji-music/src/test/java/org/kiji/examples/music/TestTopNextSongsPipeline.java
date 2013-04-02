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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.examples.music.gather.SequentialPlayCounter;
import org.kiji.examples.music.map.IdentityMapper;
import org.kiji.examples.music.reduce.SequentialPlayCountReducer;
import org.kiji.examples.music.reduce.TopNextSongsReducer;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapReduceJobBuilder;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

/**
 * This is an example of testing multiple, chained MR jobs.
 */
public class TestTopNextSongsPipeline extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSongPlayCounter.class);

  private KijiURI mUserTableURI;
  private KijiURI mSongTableURI;
  private KijiTable mSongTable;
  private KijiTableReader mSongTableReader;

  /** Initialize our environment. */
  @Before
  public final void setup() throws Exception {
    // Create layouts and URIs to use during the test.
    final KijiTableLayout userLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String userTableName = userLayout.getName();
    mUserTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(userTableName).build();
    final KijiTableLayout songLayout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/songs.json");
    final String songTableName = songLayout.getName();
    mSongTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(songTableName).build();

    // Initialize a kiji instance with relevant tables to use during tests.
    new InstanceBuilder(getKiji())
        .withTable(userTableName, userLayout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-2")
                .withValue(3L, "song-1")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-3")
                .withValue(3L, "song-2")
                .withValue(4L, "song-1")
                .withValue(5L, "song-3")
                .withValue(6L, "song-1")
                .withValue(7L, "song-2")
                .withValue(8L, "song-1")
                .withValue(9L, "song-3")
                .withValue(10L, "song-1")
            .withRow("user-3").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-3")
                .withValue(2L, "song-1")
                .withValue(3L, "song-9")
                .withValue(4L, "song-1")
                .withValue(5L, "song-7")
                .withValue(6L, "song-1")
                .withValue(7L, "song-7")
                .withValue(8L, "song-1")
                .withValue(9L, "song-8")
                .withValue(10L, "song-1")
        .withTable(songTableName, songLayout)
        .build();
    // Open table and table reader.
    mSongTable = getKiji().openTable(songTableName);
    mSongTableReader = mSongTable.openTableReader();
  }

  /**  Close resources we open for the test. */
  @After
  public final void cleanup() {
    // Close table and table reader in the reverse order.
    ResourceUtils.closeOrLog(mSongTableReader);
    ResourceUtils.releaseOrLog(mSongTable);
  }


  /**
   * This is a unit test that executes two MapReduce jobs in local mode and checks that the outputs
   * are as expected. Notice that the only difference between the job definition here and what you
   * would use in production are the tableURIs and file paths.
   */
  @Test
  public void testTopNextSongPipeline() throws Exception {
    // Configure and run job.
    final File outputDir = new File(getLocalTempDir(), "output.sequence_file");
    final Path path = new Path("file://" + outputDir);
    // Configure first job.
    final KijiMapReduceJob mrjob1 = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withGatherer(SequentialPlayCounter.class)
        .withReducer(SequentialPlayCountReducer.class)
        .withInputTable(mUserTableURI)
        // Note: the local map/reduce job runner does not allow more than one reducer:
        .withOutput(new AvroKeyValueMapReduceJobOutput(path, 1))
        .build();
    // Configure second job.
    final MapReduceJobOutput tableOutput = new DirectKijiTableMapReduceJobOutput(mSongTableURI, 1);
    final KijiMapReduceJob mrjob2 = KijiMapReduceJobBuilder.create()
        .withConf(getConf())
        .withInput(new AvroKeyValueMapReduceJobInput(path))
        .withMapper(IdentityMapper.class)
        .withReducer(TopNextSongsReducer.class)
        .withOutput(tableOutput).build();

    // Run both jobs and confirm that they are successful.
    assertTrue(mrjob1.run());
    assertTrue(mrjob2.run());

    KijiDataRequest request = KijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .withMaxVersions(Integer.MAX_VALUE)
            .add("info", "top_next_songs"))
        .build();

    TopSongs valuesForSong1 = mSongTableReader.get(mSongTable.getEntityId("song-1"), request)
        .getMostRecentValue("info", "top_next_songs");
    assertEquals("Wrong number of most popular songs played next for song-1", 3,
        valuesForSong1.getTopSongs().size());

    TopSongs valuesForSong2 = mSongTableReader.get(mSongTable.getEntityId("song-2"), request)
        .getMostRecentValue("info", "top_next_songs");
    LOG.info("the list of song counts {}", valuesForSong2.getTopSongs().toString());
    assertEquals("Wrong number of most popular songs played next for song-2", 2,
        valuesForSong2.getTopSongs().size());

    TopSongs valuesForSong8 = mSongTableReader.get(mSongTable.getEntityId("song-8"), request)
        .getMostRecentValue("info", "top_next_songs");
    LOG.info("the list of song counts {}", valuesForSong2.getTopSongs().toString());
    assertEquals("Wrong number of most popular songs played next for song-8", 1,
        valuesForSong8.getTopSongs().size());
    assertEquals("The onyl song played aftert song-8 is song-1.", "song-1",
        valuesForSong8.getTopSongs().get(0).getSongId().toString());
  }
}
