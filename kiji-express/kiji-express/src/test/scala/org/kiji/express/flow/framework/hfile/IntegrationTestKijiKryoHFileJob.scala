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

package org.kiji.express.flow.framework.hfile

import java.io.InputStream

import com.twitter.scalding.Hdfs
import com.twitter.scalding.Mode
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.avro.Schema
import org.apache.hadoop.mapred.JobConf
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.twitter.scalding.Args
import org.junit.Assert

import org.kiji.express.flow.QualifiedColumnOutputSpec
import org.kiji.express.flow.ColumnInputSpec
import org.kiji.express.flow.KijiInput
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.api.Client
import org.kiji.schema.testutil.AbstractKijiIntegrationTest
import org.kiji.schema.util.InstanceBuilder

/**
 * Integration tests with schemas specified for column requests, in an HFileKijiJob.
 */
class IntegrationTestKijiKryoHFileJob extends AbstractKijiIntegrationTest {
  private final val Log: Logger = LoggerFactory.getLogger(classOf[IntegrationTestKijiKryoHFileJob])

  // We can use the same layout as IntegrationTestSimpleFlow
  private final val TestLayout: String = "layout/org.kiji.express.flow.ITSimpleFlow.ddl"
  private final val TableName: String = "table"

  /**
   * Applies a table's DDL definition on the specified Kiji instance.
   *
   * @param resourcePath Path of the resource containing the DDL to create the table.
   * @param instanceURI URI of the Kiji instance to use.
   * @throws IOException on I/O error.
   */
  def create(resourcePath: String, instanceURI: KijiURI): Unit = {
    val client: Client = Client.newInstance(instanceURI)
    try {
      val ddl: String = readResource(resourcePath)
      Log.info("Executing DDL statement:\n{}", ddl)
      client.executeUpdate(ddl)
    } finally {
      client.close()
    }
  }

  /**
   * Loads a text resource by name.
   *
   * @param resourcePath Path of the resource to load.
   * @return the resource content, as a string.
   * @throws IOException on I/O error.
   */
  def readResource(resourcePath: String): String = {
    Log.info("Reading resource '{}'.", resourcePath)
    val istream: InputStream = getClass.getClassLoader().getResourceAsStream(resourcePath)
    try {
      val content: String = IOUtils.toString(istream)
      Log.info("Resource content is:\n{}", content)
      return content
    } finally {
      istream.close()
    }
  }

  @Test
  def testHFileOutputWithSchemasInColumnSpec(): Unit = {
    val tempHFileFolder = mTempDir.newFolder()
    FileUtils.deleteDirectory(tempHFileFolder)

    val kijiURI = getKijiURI()
    create(TestLayout, kijiURI)

    val kiji = Kiji.Factory.open(kijiURI)
    try {
      val table = kiji.openTable(TableName)
      try {
        new InstanceBuilder()
          .withTable(table)
          .withRow("row1")
          .withFamily("info")
          .withQualifier("name").withValue("name1")
          .withQualifier("email").withValue("email1")
          .withRow("row2")
          .withFamily("info")
          .withQualifier("name").withValue("name2")
          .withQualifier("email").withValue("email2")
          .build()

        class TestHFileOutputJob(args: Args) extends HFileKijiJob(args) {
          KijiInput(
            table.getURI.toString,
            Map(ColumnInputSpec("info:email", schema = Schema.create(Schema.Type.STRING))
              -> 'email))
            .debug
            .write(
            HFileKijiOutput(
              table.getURI.toString,
              tempHFileFolder.getAbsolutePath,
              Map('email ->
                QualifiedColumnOutputSpec(
                  "info:email",
                  schema = Schema.create(Schema.Type.STRING)))))
        }
        Mode.mode = Hdfs(strict = false, conf = new JobConf(getConf))
        Assert.assertTrue(
          new TestHFileOutputJob(
            Args("--hfile-output %s --output %s".format(
                table.getURI.toString,
                tempHFileFolder.getAbsolutePath))
          ).run)

      } finally {
        table.release()
      }
    } finally {
      kiji.release()
    }
  }
}
