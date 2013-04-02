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
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.examples.music.gather.SongPlayCounter;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.lib.reduce.LongSumReducer;
import org.kiji.mapreduce.output.SequenceFileMapReduceJobOutput;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.InstanceBuilder;

/** Unit-test for the SongPlayCounter gatherer. */
public class TestSongPlayCounter extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSongPlayCounter.class);

  private KijiURI mTableURI;

  /** Initialize our environment. */
  @Before
  public final void setup() throws Exception {
    final KijiTableLayout layout =
        KijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String tableName = layout.getName();
    mTableURI = KijiURI.newBuilder(getKiji().getURI()).withTableName(tableName).build();

    new InstanceBuilder(getKiji())
        .withTable(tableName, layout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-1")
                .withValue(2L, "song-2")
                .withValue(3L, "song-3")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-1")
                .withValue(2L, "song-3")
                .withValue(3L, "song-4")
                .withValue(4L, "song-1")
            .withRow("user-3").withFamily("info").withQualifier("track_plays")
                .withValue(1L, "song-5")
        .build();
  }

  /* Test our play count computes the expected results. */
  @Test
  public void testSongPlayCounter() throws Exception {
    final File outputDir = new File(getLocalTempDir(), "output.sequence_file");
    final KijiMapReduceJob mrjob = KijiGatherJobBuilder.create()
        .withConf(getConf())
        .withGatherer(SongPlayCounter.class)
        .withReducer(LongSumReducer.class)
        .withInputTable(mTableURI)
        // Note: the local map/reduce job runner does not allow more than one reducer:
        .withOutput(new SequenceFileMapReduceJobOutput(new Path("file://" + outputDir), 1))
        .build();
    assertTrue(mrjob.run());

    final Map<String, Long> counts = Maps.newTreeMap();
    readSequenceFile(new File(outputDir, "part-r-00000"), counts);
    LOG.info("Counts map: {}", counts);
    assertEquals(5, counts.size());
    assertEquals(3L, (long) counts.get("song-1"));
    assertEquals(1L, (long) counts.get("song-2"));
    assertEquals(2L, (long) counts.get("song-3"));
    assertEquals(1L, (long) counts.get("song-4"));
    assertEquals(1L, (long) counts.get("song-5"));
  }

  /**
   * Reads a sequence file of (song ID, # of song plays) into a map.
   *
   * @param path Path of the sequence file to read.
   * @param map Map to fill in with (song ID, # of song plays) entries.
   * @throws Exception on I/O error.
   */
  private void readSequenceFile(File path, Map<String, Long> map) throws Exception {
    final SequenceFile.Reader reader = new SequenceFile.Reader(
        getConf(),
        SequenceFile.Reader.file(new Path("file://" + path.toString())));
    final Text songId = new Text();
    final LongWritable nplays = new LongWritable();
    while (reader.next(songId, nplays)) {
      map.put(songId.toString(), nplays.get());
    }
    reader.close();
  }
}
