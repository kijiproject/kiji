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
import java.io.PrintWriter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.FilenameUtils
import org.eclipse.jetty.overlays.OverlayedAppProvider
import org.slf4j.{Logger, LoggerFactory}
import com.google.common.io.Files
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.modelrepo.ModelLifeCycle
import org.kiji.modelrepo.ArtifactName
import org.kiji.modelrepo.uploader.MavenArtifactName
import org.kiji.express.util.Resources.doAndClose

object ModelRepoScannerState extends Enumeration {
  type State = Value
  val Runnable, Running, Stopped = Value
}

/**
 * Performs the actual deployment/undeployment of model lifecycles by scanning the model
 * repository for changes. The scanner will download any necessary artifacts from the
 * model repository and deploy the necessary files so that the application is available for
 * remote scoring.
 */
class ModelRepoScanner(
  mKijiModelRepo: KijiModelRepository,
  mScanIntervalSeconds: Int,
  mBaseDir: File
) extends Runnable {

  val LOG: Logger = LoggerFactory.getLogger(classOf[ModelRepoScanner])

  // Setup some constants that will get used when generating the various files to deploy
  // the lifecycle.
  val CONTEXT_PATH: String = "CONTEXT_PATH"
  val MODEL_NAME: String = "MODEL_NAME"
  val MODEL_GROUP: String = "MODEL_GROUP"
  val MODEL_ARTIFACT: String = "MODEL_ARTIFACT"
  val MODEL_VERSION: String = "MODEL_VERSION"
  val MODEL_REPO_URI: String = "MODEL_REPO_URI"

  // Some file constants
  val OVERLAY_FILE: String = "/org/kiji/scoring/server/instance/overlay.xml"
  val WEB_OVERLAY_FILE: String = "/org/kiji/scoring/server/instance/web-overlay.xml"
  val JETTY_TEMPLATE_FILE: String = "/org/kiji/scoring/server/template/template.xml"

  /** The webapps folder relative to the base directory of the server.server. */
  val webappsFolder: File = new File(
      mBaseDir, new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.WEBAPPS).getPath)


  /** The instances folder relative to the base directory of the server. */
  val instancesFolder: File = new File(
      mBaseDir, new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.INSTANCES).getPath)


  /** The templates folder relative to the base directory of the server. */
  val templatesFolder: File = new File(
      mBaseDir, new File(ScoringServer.MODELS_FOLDER, OverlayedAppProvider.TEMPLATES).getPath)

  /**
   *  Stores a set of deployed lifecycles (identified by group.artifact-version) mapping
   *  to the actual folder that houses this lifecycle.
   */
  private val mArtifactToInstanceDir: mutable.Map[ArtifactName, File] =
      mutable.Map[ArtifactName, File]()

  /**
   *  Stores a map of model artifact locations to their corresponding template name so that
   *  instances can be properly mapped when created against a war file that has already
   *  been previously deployed. The key is the location string from the location column
   *  in the model repo and the value is the fully qualified name of the lifecycle
   *  (group.artifact-version) to which this location is mapped to.
   */
  private val mDeployedWarFiles = mutable.Map[String, String]()

  var mState: ModelRepoScannerState.State = ModelRepoScannerState.Runnable

  initializeState()

  /**
   * Initializes the state of the internal maps from disk but going through the
   * templates and webapps folders to populate different maps. Also will clean up files/folders
   * that are not valid.
   *
   * This is called upon construction of the ModelRepoScanner and can't be called when the scanner
   * is running.
   */
  private def initializeState() {

    if(mState != ModelRepoScannerState.Runnable) {
      throw new IllegalStateException("Can not initialize state while scanner is not runnable.")
    }

    // This will load up the in memory data structures for the scoring server
    // based on information from disk. First go through the webapps and any webapp that doesn't
    // have a corresponding location file, delete.

    // Any undeploys will happen in the checkForUpdates call at the end.
    val templatesDir = templatesFolder
    val webappsDir = webappsFolder
    val instancesDir = instancesFolder

    // Validate webapps
    val (_, invalidWarFiles) = webappsDir.listFiles().partition(f => {
      val locationFile = new File(webappsDir, f.getName() + ".loc")
      locationFile.exists() && f.getName().endsWith(".war") && f.isFile()
    })

    invalidWarFiles.foreach(f => {
      delete(f)
    })

    // Validate the templates to make sure that they are pointing to a valid
    // war file.
    val (validTemplates, invalidTemplates) = templatesDir.listFiles()
      .partition(templateDir => {
        val (_, warBaseName) = parseDirectoryName(templateDir.getName())
        // For a template to be valid, it must have a name and warBaseName AND the
        // warBaseName.war must exist AND warBaseName.war.loc must also exist
        val warFile = new File(webappsDir, warBaseName.getOrElse("") + ".war")
        val locFile = new File(webappsDir, warBaseName.getOrElse("") + ".war.loc")

        !warBaseName.isEmpty && warFile.exists() && locFile.exists() && templateDir.isDirectory()
      })

    invalidTemplates.foreach(template => {
      LOG.info("Deleting invalid template directory " + template)
      delete(template)
    })

    validTemplates.foreach(template => {
      val (templateName, warBaseName) = parseDirectoryName(template.getName())
      val locFile = new File(webappsDir, warBaseName.get + ".war.loc")
      val location = getLocationInformation(locFile)
      mDeployedWarFiles.put(location, templateName)
    })

    // Loop through the instances and add them to the map.
    instancesDir.listFiles().foreach(instance => {
      //templateName=artifactFullyQualifiedName
      val (templateName, artifactName) = parseDirectoryName(instance.getName())

      // This is an inefficient lookup on validTemplates but it's a one time thing on
      // startup of the server scanner.
      if (!instance.isDirectory() || artifactName.isEmpty
        || !validTemplates.contains(templateName)) {
        LOG.info("Deleting invalid instance " + instance.getPath())
        delete(instance)
      } else {
        try {
          val parsedArtifact = new ArtifactName(artifactName.get)
          mArtifactToInstanceDir.put(parsedArtifact, instance)
        } catch {
          // Indicates an invalid ArtifactName.
          case ex: IllegalArgumentException => delete(instance)
        }
      }
    })

    val (deployed: Int, undeployed: Int) = checkForUpdates
    LOG.debug("{} new models deployed. {} old models undeployed.", deployed, undeployed)

    mState = ModelRepoScannerState.Running
  }

  /**
   * Deletes a given file or directory.
   *
   * @param file is the file/folder to delete.
   */
  private def delete(file: File) {
    if (file.isDirectory()) {
      FileUtils.deleteDirectory(file)
    } else {
      file.delete()
    }
  }

  /**
   * Parses a Jetty template/instance directory using the "=" as the delimiter. Returns
   * the two parts that comprise the directory.
   *
   * @param inputDirectory is the directory name to parse.
   * @return a 2-tuple containing the two strings on either side of the "=" operator. If there is no
   *     "=" in the string, then the second part will be None.
   */
  private def parseDirectoryName(
      inputDirectory: String
  ): (String, Option[String]) = {
    val parts: Array[String] = inputDirectory.split("=")
    if (parts.length == 1) {
      (parts(0), None)
    } else if (parts.length == 2) {
      (parts(0), Some(parts(1)))
    } else {
      throw new IllegalArgumentException(
        "Jetty template/instance directory must contain 0 or 1 '=', got: %s".format(inputDirectory))
    }
  }

  /**
   * Turns off the internal run flag to safely stop the scanning of the model repository
   * table.
   */
  def shutdown() {
    mState = ModelRepoScannerState.Stopped
  }

  override def run() {
    while (mState == ModelRepoScannerState.Running) {
      LOG.debug("Scanning model repository for changes...")
      val (deployed: Int, undeployed: Int) = checkForUpdates
      LOG.debug("{} new models deployed. {} old models undeployed.", deployed, undeployed)
      Thread.sleep(mScanIntervalSeconds * 1000L)
    }
  }

  /**
   * Checks the model repository table for updates and deploys/undeploys lifecycles
   * as necessary.
   *
   * @return a two-tuple of the number of lifecycles deployed and undeployed
   */
  def checkForUpdates: (Int, Int) = {
    val allEnabledLifecycles: Map[ArtifactName, ModelLifeCycle] = getAllEnabledLifecycles.
      foldLeft(Map[ArtifactName, ModelLifeCycle]()) {
        (accumulatorMap: Map[ArtifactName, ModelLifeCycle], lifecycle: ModelLifeCycle) =>
            accumulatorMap + (lifecycle.getArtifactName -> lifecycle)
      }

    // Split the lifecycle map into those that are already deployed and those that
    // should be undeployed (based on whether or not the currently enabled lifecycles
    // contain the deployed lifecycle.
    val (toKeep: mutable.Map[ArtifactName, File], toUndeploy: mutable.Map[ArtifactName, File]) =
        mArtifactToInstanceDir.partition {
          kv: (ArtifactName, File) => allEnabledLifecycles.contains(kv._1)
        }

    // For each lifecycle to undeploy, remove it.
    toUndeploy.foreach {
      kv: (ArtifactName, File) => {
        val (artifactName, location) = kv
        LOG.info("Undeploying lifecycle: %s from location: %s".format(
            artifactName.getFullyQualifiedName, location))
        delete(location)
      }
    }

    // Now find the set of lifecycles to add by diffing the current with the already
    // deployed and add those.
    val toDeploy: Set[ArtifactName] = allEnabledLifecycles.keySet.diff(toKeep.keySet)
    toDeploy.foreach {
      artifactName: ArtifactName => {
        val lifecycle: ModelLifeCycle = allEnabledLifecycles(artifactName)
        LOG.info("Deploying artifact: " + artifactName.getFullyQualifiedName)
        deployArtifact(lifecycle)
      }
    }

    (toDeploy.size, toUndeploy.size)
  }

  /**
   * Deploys the specified model artifact by either creating a new Jetty instance or by
   * downloading the artifact and setting up a new template/instance in Jetty.
   *
   * @param lifecycle is the specified ModelLifeCycle to deploy.
   */
  private def deployArtifact(
      lifecycle: ModelLifeCycle
  ) {
    val mavenArtifact: MavenArtifactName = new MavenArtifactName(lifecycle.getArtifactName)
    val artifact: ArtifactName = lifecycle.getArtifactName

    // Deploying requires a few things.
    // If we have deployed this artifact's war file before:
    val fullyQualifiedName: String = artifact.getFullyQualifiedName

    val contextPath = "%s/%s".format(
        "%s/%s".format(mavenArtifact.getGroupName, mavenArtifact.getArtifactName).replace('.', '/'),
        artifact.getVersion.toString)

    // Populate a map of the various placeholder values to substitute in files
    val templateParamValues: Map[String, String] = Map(
      MODEL_ARTIFACT -> mavenArtifact.getArtifactName,
      MODEL_GROUP -> mavenArtifact.getGroupName,
      MODEL_NAME -> artifact.getFullyQualifiedName,
      MODEL_VERSION -> artifact.getVersion.toString,
      MODEL_REPO_URI -> mKijiModelRepo.getURI.toString,
      CONTEXT_PATH -> contextPath)

    mDeployedWarFiles.get(lifecycle.getLocation) match {
      case Some(templateName: String) =>
          createNewInstance(lifecycle, templateName, templateParamValues)
      case None => {
        // Download the artifact to a temporary location.
        val artifactFile: File = File.createTempFile("artifact", "war")
        val finalArtifactName: String = "%s.%s".format(
            mavenArtifact.getGroupName, FilenameUtils.getName(lifecycle.getLocation))
        lifecycle.downloadArtifact(artifactFile)

        // Create a new Jetty template to map to the war file.
        // Template is (fullyQualifiedName=warFileBase)
        val templateDirName: String = "%s=%s".format(
            fullyQualifiedName, FilenameUtils.getBaseName(finalArtifactName))
        val tempTemplateDir: File = new File(Files.createTempDir(), "WEB-INF")
        tempTemplateDir.mkdirs()

        translateFile(JETTY_TEMPLATE_FILE, new File(tempTemplateDir, "template.xml"), Map())

        // Move the temporarily downloaded artifact to its final location.
        artifactFile.renameTo(new File(webappsFolder, finalArtifactName))

        // As part of the state necessary to reconstruct the scoring server on cold start, write
        // out the location of the lifecycle (which is used to determine if a war file needs to
        // actually be deployed when a lifecycle is deployed) to a text file.
        writeLocationInformation(finalArtifactName, lifecycle)

        tempTemplateDir.getParentFile.renameTo(new File(templatesFolder, templateDirName))

        // Create a new instance.
        createNewInstance(lifecycle, fullyQualifiedName, templateParamValues)

        mDeployedWarFiles.put(lifecycle.getLocation, fullyQualifiedName)
      }
    }
  }

  /**
   * Writes out the relative URI of the lifecycle (in the model repository) to a known
   * text file in the webapps folder. This is used later on server reboot to make sure that
   * currently enabled and previously deployed lifecycles are represented in the internal
   * server state. The file name will be <artifactFileName>.loc.
   *
   * @param artifactFileName is the name of the artifact file name that is being locally deployed.
   * @param lifecycle is the model lifecycle object that is associated with the artifact.
   */
  private def writeLocationInformation(
      artifactFileName: String,
      lifecycle: ModelLifeCycle
  ) = {
    doAndClose(new PrintWriter(new File(webappsFolder, artifactFileName + ".loc"), "UTF-8")) {
      pw: PrintWriter => pw.println(lifecycle.getLocation)
    }
  }

  /**
   * Returns the location information from the specified file. The input file is the name of the
   * location file that was written using the writeLocationInformation method.
   *
   * @param locationFile is the name of the location file containing the lifecycle location
   *     information.
   * @return the location information in the file.
   */
  private def getLocationInformation(
      locationFile: File
  ): String = {
    doAndClose(Source.fromFile(locationFile)) {
      inputSource: BufferedSource => inputSource.mkString.trim
    }
  }

  /**
   * Creates a new Jetty overlay instance.
   *
   * @param lifecycle is the ModelLifeCycle to deploy.
   * @param templateName is the name of the template to which this instance belongs.
   * @param bookmarkParams contains a map of parameters and values used when configuring the WEB-INF
   *     specific files. An example of a parameter includes the context name used when addressing
   *     this lifecycle via HTTP which is dynamically populated based on the fully qualified name of
   *     the ModelLifeCycle.
   */
  private def createNewInstance(
      lifecycle: ModelLifeCycle,
      templateName: String,
      bookmarkParams: Map[String, String]
  ) {
    // This will create a new instance by leveraging the template files on the classpath
    // and create the right directory. Maybe first create the directory in a temp location and
    // move to the right place.
    val artifactName: ArtifactName = lifecycle.getArtifactName

    val tempInstanceDir: File = new File(Files.createTempDir(), "WEB-INF")
    tempInstanceDir.mkdir()

    // templateName=artifactFullyQualifiedName
    val instanceDirName: String = "%s=%s".format(templateName, artifactName.getFullyQualifiedName)

    translateFile(OVERLAY_FILE, new File(tempInstanceDir, "overlay.xml"), bookmarkParams)
    translateFile(WEB_OVERLAY_FILE, new File(tempInstanceDir, "web-overlay.xml"), bookmarkParams)

    val finalInstanceDir: File = new File(instancesFolder, instanceDirName)

    tempInstanceDir.getParentFile.renameTo(finalInstanceDir)

    mArtifactToInstanceDir.put(artifactName, finalInstanceDir)
    delete(tempInstanceDir)
  }

  /**
   * Given a "template" WEB-INF .xml file on the classpath, this will produce a translated
   * version of the file replacing any "bookmark" values (i.e. "%PARAM%") with their actual values
   * which are dynamically generated based on the artifact being deployed.
   *
   * @param filePath is the path to the xml file on the classpath (bundled with this scoring server)
   * @param targetFile is the path where the file is going to be written.
   * @param bookmarkParams contains a map of parameters and values used when configuring the WEB-INF
   *     specific files. An example of a parameter includes the context name used when addressing
   *     this lifecycle via HTTP which is dynamically populated based on the fully qualified name of
   *     the ModelLifeCycle.
   */
  private def translateFile(
      filePath: String,
      targetFile: File,
      bookmarkParams: Map[String, String]
  ) {
    doAndClose(Source.fromInputStream(getClass.getResourceAsStream(filePath))) {
      fileStream: BufferedSource => {
        doAndClose(new PrintWriter(targetFile, "UTF-8")) {
          fileWriter: PrintWriter => {
            fileStream.getLines().foreach {
              line: String => {
                val newLine = {
                  if (line.matches(".*?%[A-Z_]+%.*?")) {
                    fileWriter.println(bookmarkParams.foldLeft(line) {
                      (result: String, currentParam: (String, String)) => {
                        val (key, value) = currentParam
                        result.replace("%%%s%%".format(key), value)
                      }
                    })
                  } else {
                    line
                  }
                }
                fileWriter.println(newLine)
              }
            }
          }
        }
      }
    }
  }

  /**
   * Returns all the currently enabled lifecycles from the model repository.
   *
   * @return all the currently enabled lifecycles from the model repository.
   */
  private def getAllEnabledLifecycles: Set[ModelLifeCycle] = {
    mKijiModelRepo.getModelLifeCycles(null, 1, true).asScala.toSet
  }
}
