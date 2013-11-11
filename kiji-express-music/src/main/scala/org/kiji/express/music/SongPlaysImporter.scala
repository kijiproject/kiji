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

package org.kiji.express.music

import scala.util.parsing.json.JSON

import com.twitter.scalding._

import org.kiji.express._
import org.kiji.express.flow._
import org.kiji.express.flow.SchemaSpec._

/**
 * Imports information about users playing tracks into a Kiji table.
 *
 * This importer expects to receive two command line arguments: `--table-uri` and `--input`.
 * The argument `--table-uri` should be set to the Kiji URI to a users table that the import
 * will target. The argument `--input` should be the HDFS path to a file containing JSON
 * records recording a song play by a user.
 *
 * See the file `song-plays.json` packaged with this tutorial for the structure of JSON
 * records imported.
 *
 * @param args passed in from the command line.
 */
class SongPlaysImporter(args: Args) extends KijiJob(args) {
  /**
   * Transforms a JSON record into a tuple whose fields correspond to the fields from the
   * JSON record.
   *
   * @param json is the record to parse into a tuple.
   * @return a Scala tuple whose fields correspond to the fields from the JSON record.
   */
  def parseJson(json: String): (String, Long, String) = {
    val playRecord = JSON.parseFull(json).get.asInstanceOf[Map[String, Any]]
    (playRecord.get("user_id").get.asInstanceOf[String],
        playRecord.get("play_time").get.asInstanceOf[String].toLong,
        playRecord.get("song_id").get.asInstanceOf[String])
  }

  // This Scalding pipeline does the following:
  // 1. Reads JSON records from a file in HDFS.
  // 2. Flattens each JSON record into a tuple with fields corresponding to information
  //    about a playing track.
  // 3. Transforms the id for each user into an entity id for the users table.
  // 4. Writes each track played by a user to the column "info:track_plays" in the user
  //    table, at the timestamp when the user listened to the song, in the row for the user.
  TextLine(args("input"))
      .map('line -> ('userId, 'playTime, 'songId)) { parseJson }
      .map('userId -> 'entityId) { userId: String => EntityId(userId) }
      .write(KijiOutput(
          args("table-uri"),
          'playTime,
          Map('songId -> QualifiedColumnRequestOutput("info", "track_plays"))))
}
