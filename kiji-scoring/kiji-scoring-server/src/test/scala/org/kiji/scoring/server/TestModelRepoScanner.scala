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

import org.apache.commons.io.FileUtils
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.util.InstanceBuilder
import org.kiji.scoring.server.TestUtils.deploySampleLifecycle
import org.kiji.scoring.server.TestUtils.setupServerEnvironment
import org.scalatest.junit.JUnitSuite

import com.google.common.io.Files

import org.kiji.modelrepo.KijiModelRepository
import org.kiji.schema.Kiji
import org.kiji.schema.util.InstanceBuilder
import org.kiji.scoring.server.TestUtils.deploySampleLifecycle
import org.kiji.scoring.server.TestUtils.setupServerEnvironment
import org.slf4j.LoggerFactory

class TestModelRepoScanner extends JUnitSuite {

  var mFakeKiji: Kiji = null
  var mTempServerHome: File = null
  var mTempKMR: File = null
  var mModelRepo: KijiModelRepository = null

  @Before
  def setup() {
    val builder = new InstanceBuilder("default")
    mFakeKiji = builder.build()
    mTempServerHome = setupServerEnvironment(mFakeKiji.getURI)
    mTempKMR = Files.createTempDir()
    KijiModelRepository.install(mFakeKiji, mTempKMR.toURI)
    mModelRepo = KijiModelRepository.open(mFakeKiji)
  }

  @After
  def tearDown() {
    mModelRepo.close()
    mFakeKiji.release()
    FileUtils.deleteDirectory(mTempServerHome)
    FileUtils.deleteDirectory(mTempKMR)
  }

  @Test
  def testShouldDeployASingleLifecycle() {

    val bogusArtifact = new File(mTempServerHome, "conf/configuration.json").getAbsolutePath

    deploySampleLifecycle(mFakeKiji, bogusArtifact, "0.0.1")

    val scanner = new ModelRepoScanner(mModelRepo, 2, mTempServerHome)
    scanner.checkForUpdates
    val webappFile = new File(mTempServerHome,
      "models/webapps/org.kiji.test.sample_model-0.0.1.war")
    val locationFile = new File(mTempServerHome,
      "models/webapps/org.kiji.test.sample_model-0.0.1.war.loc")
    val templateDir = new File(mTempServerHome,
      "models/templates/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")
    val instanceDir = new File(mTempServerHome,
      "models/instances/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")

    assert(webappFile.exists())
    assert(locationFile.exists())
    assert(templateDir.exists())
    assert(instanceDir.exists())
  }

  @Test
  def testShouldUndeployASingleLifecycle() {
    val bogusArtifact = new File(mTempServerHome, "conf/configuration.json").getAbsolutePath

    deploySampleLifecycle(mFakeKiji, bogusArtifact, "0.0.1")

    val scanner = new ModelRepoScanner(mModelRepo, 2, mTempServerHome)
    scanner.checkForUpdates
    val instanceDir = new File(mTempServerHome,
      "models/instances/org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1")
    assert(instanceDir.exists())

    // For now undeploy will delete the instance directory
    val modelRepoTable = mFakeKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
    val writer = modelRepoTable.openTableWriter()
    val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.1")
    writer.put(eid, "model", "production_ready", false)
    writer.close()
    modelRepoTable.release()
    scanner.checkForUpdates
    assert(!instanceDir.exists())
  }

  @Test
  def testShouldLinkMultipleLifecyclesToSameArtifact() {
    val bogusArtifact = new File(mTempServerHome, "conf/configuration.json").getAbsolutePath

    deploySampleLifecycle(mFakeKiji, bogusArtifact, "0.0.1")
    deploySampleLifecycle(mFakeKiji, bogusArtifact, "0.0.2")

    // Force the location of 0.0.2 to be that of 0.0.1
    val modelRepoTable = mFakeKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
    val writer = modelRepoTable.openTableWriter()
    val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.2")

    writer.put(eid, "model", "location", "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war")
    writer.close()
    modelRepoTable.release()

    val scanner = new ModelRepoScanner(mModelRepo, 2, mTempServerHome)
    scanner.checkForUpdates

    assert(new File(mTempServerHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1").exists())

    assert(new File(mTempServerHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.2").exists())

    assert(new File(mTempServerHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.1.war").exists())
    assert(!new File(mTempServerHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.2.war").exists())

  }

  @Test
  def testShouldUndeployAnArtifactAfterMultipleLifecyclesAreDeployed() {
    val bogusArtifact = new File(mTempServerHome, "conf/configuration.json").getAbsolutePath

    deploySampleLifecycle(mFakeKiji, bogusArtifact, "0.0.1")
    deploySampleLifecycle(mFakeKiji, bogusArtifact, "0.0.2")

    // Force the location of 0.0.2 to be that of 0.0.1
    val modelRepoTable = mFakeKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
    val writer = modelRepoTable.openTableWriter()
    val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.2")

    writer.put(eid, "model", "location", "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war")

    val scanner = new ModelRepoScanner(mModelRepo, 2, mTempServerHome)
    scanner.checkForUpdates

    assert(new File(mTempServerHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1").exists())
    assert(new File(mTempServerHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.2").exists())

    assert(new File(mTempServerHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.1.war").exists())
    assert(new File(mTempServerHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.1.war.loc").exists())
    assert(!new File(mTempServerHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.2.war").exists())

    writer.put(eid, "model", "production_ready", false)
    scanner.checkForUpdates
    assert(!new File(mTempServerHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.2=org.kiji.test.sample_model-0.0.2").exists())

    writer.close()
    modelRepoTable.release()
  }

  @Test
  def testShouldResurrectStateFromDisk() {
    testShouldDeployASingleLifecycle

    val scanner = new ModelRepoScanner(mModelRepo, 2, mTempServerHome)
    val (deployed, undeployed) = scanner.checkForUpdates
    assert(deployed == 0)

    // Now let's make sure that the state is right by deploying a 0.0.2 that should rely on
    // state of 0.0.1 in the scanner to make sure that we deploy things correctly.
    val bogusArtifact = new File(mTempServerHome, "conf/configuration.json").getAbsolutePath()
    deploySampleLifecycle(mFakeKiji, bogusArtifact, "0.0.2")

    // Force the location of 0.0.2 to be that of 0.0.1
    val modelRepoTable = mFakeKiji.openTable(KijiModelRepository.MODEL_REPO_TABLE_NAME)
    val writer = modelRepoTable.openTableWriter()
    val eid = modelRepoTable.getEntityId("org.kiji.test.sample_model", "0.0.2")

    writer.put(eid, "model", "location", "org/kiji/test/sample_model/0.0.1/sample_model-0.0.1.war")
    writer.close()
    modelRepoTable.release()

    val (deployedAgain, undeployedAgain) = scanner.checkForUpdates
    assert(deployedAgain == 1)
    assert(undeployedAgain == 0)

    assert(new File(mTempServerHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.1").exists())

    assert(new File(mTempServerHome, "models/instances/"
      + "org.kiji.test.sample_model-0.0.1=org.kiji.test.sample_model-0.0.2").exists())

    assert(new File(mTempServerHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.1.war").exists())
    assert(!new File(mTempServerHome, "models/webapps/"
      + "org.kiji.test.sample_model-0.0.2.war").exists())
  }

  @Test
  def testShouldRemoveInvalidArtifacts() {
    val scanner = new ModelRepoScanner(mModelRepo, 2, mTempServerHome)

    // Create a few invalid webapps. Some directories, some files.
    val webappsFolder = scanner.webappsFolder
    assert(new File(webappsFolder, "invalid_war.war").createNewFile())
    assert(new File(webappsFolder, "invalid_war_folder").mkdir())
    assert(new File(webappsFolder, "invalid_war.txt").createNewFile())

    // Create a few invalid template folders and files
    val templatesFolder = scanner.templatesFolder
    assert(new File(templatesFolder, "invalid_template.txt").createNewFile())
    assert(new File(templatesFolder, "invalid_template_dir").mkdir())
    assert(new File(templatesFolder, "invalidname=invalidvalue").mkdir())

    // Create a few invalid instances
    val instancesFolder = scanner.instancesFolder
    assert(new File(instancesFolder, "invalid_template.txt").createNewFile())
    assert(new File(instancesFolder, "invalid_template_dir").mkdir())
    assert(new File(instancesFolder, "invalidname=invalidvalue").mkdir())

    val scanner2 = new ModelRepoScanner(mModelRepo, 2, mTempServerHome)

    // Make sure that webapps don't have any of the invalid stuff
    assert(webappsFolder.list().size == 0)
    assert(instancesFolder.list().size == 0)
    assert(templatesFolder.list().size == 0)
  }
}
