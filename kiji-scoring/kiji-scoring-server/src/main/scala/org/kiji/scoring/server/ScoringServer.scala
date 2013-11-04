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

import org.eclipse.jetty.deploy.DeploymentManager
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.eclipse.jetty.server.AbstractConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.handler.ContextHandlerCollection
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerCollection
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

/**
 * Configuration parameters for a Kiji ScoringServer.
 *
 * @param port device port on which the configured ScoringServer will listen.
 * @param repo_uri is the KijiURI string of the model repository from which the configured server
 *     will read.
 * @param repo_scan_interval is the period in seconds between scans of the model repository table.
 * @param num_acceptors is the number of acceptor threads to be run by the configured server.
 */
case class ServerConfiguration(
  port: Int,
  repo_uri: String,
  repo_scan_interval: Int,
  num_acceptors: Int
)

/**
 * Scoring Server class. Provides a few wrappers around the Jetty server underneath.
 *
 * @param baseDir is the base directory in which the expected models and conf directories are
 *     expected to exist.
 * @param serverConfig is the scoring server configuration containing information such as the URI of
 *     the Kiji instance whose model repo table will back this ScoringServer.
 */
class ScoringServer(baseDir: File, serverConfig: ServerConfiguration) {

  val modelRepoURI: KijiURI = KijiURI.newBuilder(serverConfig.repo_uri).build()
  val repoInstance: Kiji = Kiji.Factory.open(modelRepoURI)
  val kijiModelRepo = KijiModelRepository.open(repoInstance)

  // Start the model lifecycle scanner thread that will scan the model repository
  // for changes.
  val lifeCycleScanner: ModelRepoScanner = new ModelRepoScanner(
    kijiModelRepo,
    serverConfig.repo_scan_interval,
    baseDir)
  val lifeCycleScannerThread: Thread = new Thread(lifeCycleScanner)
  lifeCycleScannerThread.start()

  val server: Server = new Server(serverConfig.port)

  // Increase the number of acceptor threads.
  val connector: AbstractConnector = server.getConnectors()(0).asInstanceOf[AbstractConnector]
  connector.setAcceptors(serverConfig.num_acceptors)

  val handlers: HandlerCollection = new HandlerCollection()

  val contextHandler: ContextHandlerCollection = new ContextHandlerCollection()
  val deploymentManager: DeploymentManager = new DeploymentManager()
  val overlayedProvider: OverlayedAppProvider = new OverlayedAppProvider

  overlayedProvider.setScanDir(new File(baseDir, ScoringServer.MODELS_FOLDER))
  // For now scan this directory once per second.
  overlayedProvider.setScanInterval(1)

  deploymentManager.setContexts(contextHandler)
  deploymentManager.addAppProvider(overlayedProvider)

  handlers.addHandler(contextHandler)
  handlers.addHandler(new DefaultHandler())

  server.setHandler(handlers)
  server.addBean(deploymentManager)

  /** Start the ScoringServer */
  def start() {
    server.start()
  }

  /** Stop the ScoringServer */
  def stop() {
    server.stop()
  }

  /** Release resources retained by the ScoringServer. */
  def releaseResources() {
    lifeCycleScanner.shutdown()
    kijiModelRepo.close()
  }
}

/**
 * Main entry point for the scoring server. This pulls in and combines various Jetty components
 * to boot a new web server listening for scoring requests.
 */
object ScoringServer {

  val CONF_FILE: String = "configuration.json"
  val MODELS_FOLDER: String = "models"
  val LOGS_FOLDER: String = "logs"
  val CONF_FOLDER: String = "conf"

  def main(args: Array[String]): Unit = {

    // Check that we started in the right location else bomb out
    val missingFiles: Set[String] = checkIfStartedInProperLocation()
    if (!(missingFiles.size == 0)) {
      sys.error("Missing files: %s".format(missingFiles.mkString(", ")))
    }

    // TODO what does this null do? should it be args(0)?
    val scoringServer = ScoringServer(null)

    // Gracefully shutdown the deployment thread to let it finish anything that it may be doing
    sys.ShutdownHookThread {
      scoringServer.releaseResources()
    }

    scoringServer.start()
    scoringServer.server.join()
  }

  /**
   * Constructs a Jetty Server instance configured using the conf/configuration.json.
   *
   * @param baseDir is the base directory in which models and configuration directories are expected
   *      to exist.
   * @return a constructed Jetty server.
   */
  def apply(baseDir: File): ScoringServer = {
    new ScoringServer(baseDir, getConfig(new File(baseDir, "%s/%s".format(CONF_FOLDER, CONF_FILE))))
  }

  /**
   * Checks that the server is started in the right location by ensuring the presence of a few key
   * directories under the conf, models and logs folder.
   *
   * @return whether or not the key set of folders exist or not.
   */
  def checkIfStartedInProperLocation(): Set[String] = {
    // Get the list of required files which do not exist.
    Set(
        CONF_FOLDER + "/" + CONF_FILE,
        MODELS_FOLDER + "/webapps",
        MODELS_FOLDER + "/instances",
        MODELS_FOLDER + "/templates",
        LOGS_FOLDER
    ).filter {
      (fileString: String) => !new File(fileString).exists()
    }
  }

  /**
   * Returns the ServerConfiguration object constructed from conf/configuration.json.
   *
   * @param confFile is the location of the configuration used to configure the server.
   * @return the ServerConfiguration object constructed from conf/configuration.json.
   */
  def getConfig(confFile: File): ServerConfiguration = {
    val configMapper = new ObjectMapper
    configMapper.registerModule(DefaultScalaModule)
    configMapper.readValue(confFile, classOf[ServerConfiguration])
  }
}
