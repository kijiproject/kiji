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

import org.kiji.express.flow.All
import org.kiji.express.flow.QualifiedColumnRequestInput
import org.kiji.express.flow.QualifiedColumnRequestOutput
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.KeyValueStoreSpec
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.KijiSingleColumnOutputSpec
import org.kiji.modeling.config.ScoreEnvironment

val license: String =
  """
    |/**
    | * (c) Copyright 2013 WibiData, Inc.
    | *
    | * See the NOTICE file distributed with this work for additional
    | * information regarding copyright ownership.
    | *
    | * Licensed under the Apache License, Version 2.0 (the "License");
    | * you may not use this file except in compliance with the License.
    | * You may obtain a copy of the License at
    | *
    | *     http://www.apache.org/licenses/LICENSE-2.0
    | *
    | * Unless required by applicable law or agreed to in writing, software
    | * distributed under the License is distributed on an "AS IS" BASIS,
    | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    | * See the License for the specific language governing permissions and
    | * limitations under the License.
    | */
  """.stripMargin

val jsonEnvironment: String =
    ModelEnvironment(
        name = "song-recommender-env",
        version = "1.0.0",
        protocolVersion = ModelEnvironment.CURRENT_MODEL_DEF_VER,
        scoreEnvironment = Some(
            ScoreEnvironment(
                inputSpec = KijiInputSpec(
                    tableUri = "kiji://.env/default/users",
                    timeRange = All,
                    columnsToFields = Map(
                        QualifiedColumnRequestInput(
                            "info",
                            "track_plays",
                            maxVersions = Int.MaxValue
                        ) -> 'trackPlay
                    )
                ),
                outputSpec = KijiSingleColumnOutputSpec(
                    tableUri = "kiji://.env/default/users",
                    outputColumn = QualifiedColumnRequestOutput(
                        family = "info",
                        qualifier = "next_song_rec"
                    )
                ),
                keyValueStoreSpecs = Seq(
                    KeyValueStoreSpec(
                        "KIJI_TABLE",
                        "top_next_songs",
                        Map("uri" -> "kiji://.env/default/songs", "column" -> "info:top_next_songs")
                    )
                )
            )
        )
    )
    .toJson

println(license)
println()
println(jsonEnvironment)
