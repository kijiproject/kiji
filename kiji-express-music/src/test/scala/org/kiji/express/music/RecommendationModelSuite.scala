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

import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.doAndRelease

import com.twitter.scalding.Args
import com.twitter.scalding.IterableSource

import org.kiji.express.AvroRecord
import org.kiji.express.EntityId
import org.kiji.express.KijiSuite
import org.kiji.express.flow._
import org.kiji.express.modeling.ScoreProducerJobBuilder
import org.kiji.express.modeling.config.ModelDefinition
import org.kiji.express.modeling.config.ModelEnvironment
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest

/**
 * Provides tests for the KijiExpress modeling workflow that makes song recommendations.
 * Currently, this model consists of extract+score phases that can be used to produce song
 * recommendations in batch.
 */
class RecommendationModelSuite extends KijiSuite {

  // The JSON for a model definition we will use during tests.
  val modelDefinitionJSON: String =
    """
      |{
      |  "name" : "song-recommender",
      |  "version" : "1.0.0",
      |  "scorer_phase":{
      |    "org.kiji.express.avro.AvroPhaseDefinition":{
      |      "extractor_class":"org.kiji.express.modeling.lib.FirstValueExtractor",
      |      "phase_class":"org.kiji.express.music.SongRecommendingScorer"
      |    }
      |  }
      |}
    """.stripMargin

  // The JSON for a model environment we will use during tests. This environment needs to be
  // populated with the URI for a Kiji table to run the model against (a users table) and the URI
  // for a Kiji table to use as a key-value store during scoring.
  val modelEnvironmentJSON: String =
    """
      |{
      | "protocol_version":"model_environment-0.2.0",
      | "name" : "song-recommender-env",
      | "version" : "1.0.0",
      | "score_environment":{
      |   "org.kiji.express.avro.AvroScoreEnvironment":{
      |     "input_spec":{
      |       "kiji_specification":{
      |         "org.kiji.express.avro.AvroKijiInputSpec":{
      |           "table_uri":"%s",
      |           "data_request":{
      |             "column_definitions":[{
      |               "name":"info:track_plays"
      |             } ]
      |           },
      |           "field_bindings":[{
      |             "tuple_field_name":"trackPlay",
      |             "store_field_name":"info:track_plays"
      |           }]
      |         }
      |       }
      |     },
      |     "kv_stores":[{
      |       "store_type":"KIJI_TABLE",
      |       "name":"top_next_songs",
      |       "properties":[{
      |         "name":"uri",
      |         "value":"%s"
      |       }, {
      |         "name":"column",
      |         "value":"info:top_next_songs"
      |       }]
      |     }],
      |     "output_spec":{
      |       "kiji_column_specification":{
      |         "org.kiji.express.avro.AvroKijiSingleColumnOutputSpec":{
      |           "table_uri":"%s",
      |           "output_column":"info:next_song_rec"
      |         }
      |       }
      |     }
      |   }
      | }
      |}
    """.stripMargin

  // We create a Kiji instance to use in tests, and run the schema-shell commands in music-schema
  // .ddl to create the kiji tables we'll use for tests.
  val kiji: Kiji = makeTestKiji("default")
  val usersTableURI: String = kiji.getURI.toString + "/users"
  val songsTableURI: String = kiji.getURI.toString + "/songs"
  executeDDLResource(kiji, "org/kiji/express/music/music-schema.ddl")

  // Populate the user's table with some track play information to use in tests.
  val userTableImportResult: Boolean = new KijiJob(new Args(Map())) {
    IterableSource(List(
        (EntityId("user-0"), "song-0"),
        (EntityId("user-1"), "song-1"),
        (EntityId("user-2"), "song-2")), ('entityId, 'trackPlay)
    ).write(KijiOutput(usersTableURI)(Map('trackPlay ->
        Column("info:track_plays").useDefaultReaderSchema())))
  }.run
  assert(userTableImportResult, "Failed to import track plays to user table in test setup.")

  // Write some data to the songs table, where a list of top next songs is written for some
  // sample songs. song-1 is played most frequently after song-0, song-2 the most frequently
  // after song-1, and so on.
  val songsTableImportResult: Boolean = new KijiJob(new Args(Map())) {
    IterableSource(List(
        (EntityId("song-0"),
            AvroRecord("top_songs" -> List(AvroRecord("song_id" -> "song-1", "count" -> 1L)))),
        (EntityId("song-1"),
            AvroRecord("top_songs" -> List(AvroRecord("song_id" -> "song-2", "count" -> 2L)))),
        (EntityId("song-2"),
            AvroRecord("top_songs" -> List(AvroRecord("song_id" -> "song-3", "count" -> 3L))))),
        ('entityId, 'topNextSongs)
    ).write(KijiOutput(songsTableURI)(Map('topNextSongs ->
        Column("info:top_next_songs").useDefaultReaderSchema())))
  }.run
  assert(songsTableImportResult, "Failed to import top next songs lists in test setup.")

  test("SongRecommendingScorer can be used to batch extract and score.") {
    // Build a batch extract + score job from the model and run.
    val modelDefinition = ModelDefinition.fromJson(modelDefinitionJSON)
    val modelEnvironment = ModelEnvironment
        .fromJson(modelEnvironmentJSON.format(usersTableURI, songsTableURI, usersTableURI))
    val extractScoreJob = ScoreProducerJobBuilder.buildJob(modelDefinition, modelEnvironment)
    assert(extractScoreJob.run(), "Extract+Score job failed to run.")

    // Verify the "scores" (song recommendations) produced by the model.
    doAndRelease(kiji.openTable("users")) { usersTable =>
      doAndClose(usersTable.openTableReader()) { reader =>
        def getSongRecommendation(user: String): String = {
          reader.get(usersTable.getEntityId(user), KijiDataRequest.create("info", "next_song_rec"))
              .getMostRecentValue("info", "next_song_rec")
              .toString
        }
        val user0NextSong = getSongRecommendation("user-0")
        val user1NextSong = getSongRecommendation("user-1")
        val user2NextSong = getSongRecommendation("user-2")
        assert("song-1" === user0NextSong, "Wrong song recommendation for user 0.")
        assert("song-2" === user1NextSong, "Wrong song recommendation for user 1.")
        assert("song-3" === user2NextSong, "Wrong song recommendation for user 2.")
      }
    }
  }
}
