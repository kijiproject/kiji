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
package org.kiji.examples.music.bulkimport;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.examples.music.KijiMusicCounters;
import org.kiji.mapreduce.KijiTableContext;
import org.kiji.mapreduce.bulkimport.KijiBulkImporter;
import org.kiji.schema.EntityId;

/**
 * Bulk-importer to load the information about the track plays into the KijiMusic Users table.
 *
 * <p>Input files will contain JSON data representing track plays, with one song per line, as in:
 * <pre>
 * { "user_id" : "0", "play_time" : "1325725200000", "song_id" : "1" }
 * </pre>
 *
 * The bulk-importer expects a text input format:
 *   <li> input keys are the positions (in bytes) of each line in input file;
 *   <li> input values are the lines, as Text instances.
 */
public class TrackPlaysBulkImporter extends KijiBulkImporter<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(TrackPlaysBulkImporter.class);

  /** {@inheritDoc} */
  @Override
  public void produce(LongWritable filePos, Text line, KijiTableContext context)
      throws IOException {

    final JSONParser parser = new JSONParser();
    try {
      // Parse JSON:
      final JSONObject json = (JSONObject) parser.parse(line.toString());

      // Extract JSON fields:
      final String userId = json.get("user_id").toString();
      final long unixTime = Long.valueOf(json.get("play_time").toString());
      final String songId = json.get("song_id").toString();

      // Write entity to Kiji:
      final EntityId eid = context.getEntityId(userId);
      context.put(eid, "info", "track_plays", unixTime, songId);

    } catch (ParseException pe) {
      // Catch and log any malformed json records.
      context.incrementCounter(KijiMusicCounters.JSONParseFailure);
      LOG.error("Failed to parse JSON record '{}': {}", line, pe);
    }
  }
}
