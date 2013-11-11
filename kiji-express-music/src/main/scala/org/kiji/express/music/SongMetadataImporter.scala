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
import org.kiji.express.music.avro.SongMetadata

/**
 * Imports metadata about songs into a Kiji table.
 *
 * This importer expects two command line arguments: `--table-uri` and `--input`. The
 * argument `--table-uri` should be set to the Kiji URI of a songs table that the import will
 * target. The argument `--input` should be the HDFS path to a file containing JSON records
 * of song meta data.
 *
 * See the file `song-metadata.json` packaged with this tutorial for the structure of JSON
 * records imported.
 *
 @param args passed in from the command line.
 */
class SongMetadataImporter(args: Args) extends KijiJob(args) {
  /**
   * Transforms a JSON record into a tuple whose fields correspond to the fields from the
   * JSON record.
   *
   * @param json is the record to parse into a tuple.
   * @return a Scala tuple whose fields correspond to the fields from the JSON record.
   */
  def parseJson(json: String): (String, String, String, String, String, Long, Long) = {
    val metadata = JSON.parseFull(json).get.asInstanceOf[Map[String, String]]
    (metadata.get("song_id").get,
        metadata.get("song_name").get,
        metadata.get("album_name").get,
        metadata.get("artist_name").get,
        metadata.get("genre").get,
        metadata.get("tempo").get.toLong,
        metadata.get("duration").get.toLong)
  }

  /**
   * Retrieve the Avro [[org.apache.avro.Schema]] from the `SongMetadata` record.
   * @return the Avro `Schema` of a `SongMetada` record.
   */
  def metadataSchema = SongMetadata.getClassSchema

  // This Scalding pipeline does the following:
  // 1. Reads JSON records from an input file in HDFS.
  // 2. Flattens each JSON record into a tuple with fields corresponding to the song metadata
  //    extracted from the JSON record.
  // 3. Transforms the song id for each song into an entity id for the songs table.
  // 4. Packs song name, album name, artist name, genre, tempo, and duration for the song
  //    into a generic Avro record with the SongMetadata schema.
  // 5. Writes the Avro records to the column "info:metadata" in a row for the song in a Kiji
  //    table.
  TextLine(args("input"))
      .map('line -> ('song_id, 'song_name, 'album_name, 'artist_name, 'genre, 'tempo, 'duration)) {
        parseJson
      }
      .map('song_id -> 'entityId) { songId: String => EntityId(songId) }
      .packGenericRecord(
          ('song_name, 'album_name, 'artist_name, 'genre, 'tempo, 'duration) -> 'metadata)(
              metadataSchema)
      .write(KijiOutput(
          args("table-uri"),
          Map('metadata -> QualifiedColumnRequestOutput("info", "metadata"))))
}
