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

package org.kiji.scoring.server

import java.io.File
import java.net.URL

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.io.Files

import org.kiji.modelrepo.tools.DeployModelRepoTool
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import org.kiji.web.KijiScoringServerCell

/**
 * Collection of random test utilities.
 */
object TestUtils {

  val ARTIFACT_NAME = "org.kiji.test.sample_model"

  /**
   * Deploys the specified artifact file to the model repository located associated
   * with the specified kiji instance. <b>Note:</b> that the name for this lifecycle
   * is fixed at "org.kiji.test.sample_lifecycle". The definition/environment files are located
   * in src/test/resources.
   *
   * @param kiji is the Kiji instance associated with this model repository.
   * @param version is the version of the lifecycle.
   * @param artifactFile is the path to the artifact file to deploy.
   */
  def deploySampleLifecycle(kiji: Kiji, artifactFile: String, version: String) {
    val deployTool = new DeployModelRepoTool
    // Deploy some bogus artifact. We don't care that it's not executable code yet.
    val qualifiedName = String.format("%s-%s",ARTIFACT_NAME,version)
    val args = List(
      qualifiedName,
      artifactFile,
      "--kiji=" + kiji.getURI().toString(),
      "--definition=src/test/resources/org/kiji/samplelifecycle/model_definition.json",
      "--environment=src/test/resources/org/kiji/samplelifecycle/model_environment.json",
      "--production-ready=true",
      "--message=Uploading Artifact")

    deployTool.toolMain(args.asJava)
  }

  /**
   * Sets up the scoring server environment by creating a configuration file based on the
   * given KijiURI. Returns a handle to the temporary directory created.
   *
   * @param repo_uri is the URI of the Kiji instance.
   *
   * @return a handle to the temporary directory created.
   */
  def setupServerEnvironment(repo_uri: KijiURI): File = {
    val tempModelDir = Files.createTempDir()
    // Create the configuration folder
    val confFolder = new File(tempModelDir, "conf")
    confFolder.mkdir()
    // Create the models folder
    new File(tempModelDir, "models/webapps").mkdirs()
    new File(tempModelDir, "models/instances").mkdirs()
    new File(tempModelDir, "models/templates").mkdirs()

    tempModelDir.deleteOnExit()

    val configMap = Map(
        "port" -> 0,
        "repo_uri" -> repo_uri.toString(),
        "repo_scan_interval" -> 2,
        "num_acceptors" -> 2)

    val mapper = new ObjectMapper()
    mapper.writeValue(new File(confFolder, "configuration.json"), configMap.asJava)

    tempModelDir
  }

  /**
   * Returns the results of querying the scoring server as a List[Map[String,Any]].
   *
   * @param httpPort is the port running the scoring server.
   * @param endPoint is the endpoint + any parameters to request.
   *
   * @return the parsed JSON response.
   */
  def scoringServerResponse(httpPort: Int, endPoint: String): KijiScoringServerCell = {
    val formattedUrl = String.format("http://localhost:%s/%s",
      httpPort: java.lang.Integer, endPoint)
    val endpointUrl = new URL(formattedUrl)
    val jsonMapper = new ObjectMapper
    jsonMapper.readValue(endpointUrl, classOf[KijiScoringServerCell])
  }
}
