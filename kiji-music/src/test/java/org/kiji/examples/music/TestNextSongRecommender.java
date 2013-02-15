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

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.examples.music.produce.NextSongRecommender;
import org.kiji.mapreduce.KijiProduceJobBuilder;
import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.kvstore.lib.KijiTableKeyValueStore;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/**
 * Tests our recommendation producer.
 */
public class TestNextSongRecommender extends KijiClientTest {

  private KijiURI mUserTableURI;
  private KijiURI mSongTableURI;
  private KijiTable mUserTable;
  private KijiTableReader mUserTableReader;

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

    SongCount songFour = new SongCount();
    songFour.setCount(10L);
    songFour.setSongId("song-4");
    List<SongCount> listOfSongFour = Lists.newArrayList(songFour);
    TopSongs topSongsForSong1 = new TopSongs();
    topSongsForSong1.setTopSongs(listOfSongFour);

    SongCount songFive = new SongCount();
    songFive.setCount(9L);
    songFive.setSongId("song-5");
        List<SongCount> listOfSongFive = Lists.newArrayList(songFive);
    TopSongs topSongsForSong2 = new TopSongs();
    topSongsForSong2.setTopSongs(listOfSongFive);
    // Initialize a kiji instance with relevant tables to use during tests.
    new InstanceBuilder(getKiji())
        .withTable(userTableName, userLayout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-2")
                .withValue(3L, "song-1")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(8L, "song-1")
                .withValue(9L, "song-3")
                .withValue(10L, "song-2")
        .withTable(songLayout.getName(), songLayout)
            .withRow("song-1").withFamily("info").withQualifier("top_next_songs")
                .withValue(1L, topSongsForSong1)
            .withRow("song-2").withFamily("info").withQualifier("top_next_songs")
                .withValue(1L, topSongsForSong2)
        .build();
    // Open table and table reader.
    mUserTable = getKiji().openTable(userTableName);
    mUserTableReader = mUserTable.openTableReader();
  }

  /**  Close resources we open for the test. */
  @After
  public final void cleanup() {
    // Close table and table reader in the reverse order.
    IOUtils.closeQuietly(mUserTableReader);
    IOUtils.closeQuietly(mUserTable);
  }

  @Test
  public void testProducer() throws IOException, ClassNotFoundException, InterruptedException {
     MapReduceJobOutput tableOutput = new DirectKijiTableMapReduceJobOutput(mUserTableURI, 1);
         KijiTableKeyValueStore.Builder kvStoreBuilder = KijiTableKeyValueStore.builder();
    // Our default implementation will use the default kiji instance, and a table named songs.
    kvStoreBuilder.withColumn("info", "top_next_songs").withTable(mSongTableURI);
        // Configure first job.
    final MapReduceJob mrjob = KijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(NextSongRecommender.class)
        .withInputTable(mUserTableURI)
        .withOutput(tableOutput)
        .withStore("nextPlayed", kvStoreBuilder.build())
        .build();

        // Run both jobs and confirm that they are successful.
    assertTrue(mrjob.run());

    KijiDataRequest request = KijiDataRequest.builder()
        .addColumns(KijiDataRequestBuilder.ColumnsDef.create()
            .withMaxVersions(Integer.MAX_VALUE)
            .add("info", "next_song_rec"))
        .build();

    CharSequence valueForSong1 = mUserTableReader.get(mUserTable.getEntityId("user-1"), request)
        .getMostRecentValue("info", "next_song_rec");
    assertEquals("User-1 just listened to son-1, so their next song rec should be song-4", "song-4",
      valueForSong1.toString());

  }
}
