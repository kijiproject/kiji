/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.express.flow

import java.io.InputStream
import scala.collection.JavaConverters.mapAsScalaMapConverter

import com.twitter.scalding.Args
import com.twitter.scalding.GroupBuilder
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.NullSource
import com.twitter.scalding.Stat
import org.apache.commons.io.IOUtils
import org.junit.Assert
import org.junit.Test

import org.kiji.schema.Kiji
import org.kiji.schema.KijiClientTest
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.api.Client
import org.kiji.schema.util.InstanceBuilder
import org.kiji.express.flow.framework.ExpressJobHistoryKijiTable
import org.kiji.express.avro.generated.ExpressJobHistoryEntry

class JobHistorySuite extends KijiClientTest {
  import JobHistorySuite._

  @Test
  def testSimpleFlow(): Unit = {
    val kiji: Kiji = getKiji
    createTableFromDDL(DdlPath, kiji.getURI)
    val table: KijiTable = kiji.openTable(TableName)
    try {
      new InstanceBuilder(kiji)
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

      val extendedInfo: Map[String, String] = Map(
        "testkey" -> "testvalue",
        "testkey2" -> "testvalue2"
      )
      val args = Mode.putMode(
        Local(strictSources = false),
        Args(
          List("--tableUri", table.getURI.toString, "--extendedInfo") ++ extendedInfo.map {
            kv: (String, String) => val (k, v) = kv; "%s:%s".format(k, v)
          }
        )
      )

      val job: SimpleJob = new SimpleJob(args)
      Assert.assertTrue(job.flowCounters.isEmpty)
      Assert.assertTrue(job.run)
      Assert.assertFalse(job.flowCounters.isEmpty)

      val expressJobHistoryTable: ExpressJobHistoryKijiTable =  ExpressJobHistoryKijiTable(kiji)
      try {
        val jobDetails: ExpressJobHistoryEntry = expressJobHistoryTable
          .getExpressJobDetails(job.uniqueId.get)
        Assert.assertEquals(job.uniqueId.get, jobDetails.getJobId)
        Assert.assertEquals(job.name, jobDetails.getJobName)
        val testCounters: Set[(String, String, Long)] = job.flowCounters.filter {
          triple: (String, String, Long) => {
            val (group, name, _) = triple
            group == "group" && name == "name"
          }
        }
        Assert.assertEquals(1, testCounters.size)
        Assert.assertEquals(5, testCounters.head._3)
        val recordedExtendedInfo: Map[String, String] = jobDetails.getExtendedInfo.asScala.map {
          // We know that the elements of this pair are Strings, but Avro and Scala can't seem to
          // agree, so we just toString() them.
          pair: (Any, Any) => val (k, v) = pair; (k.toString, v.toString)
        }.toMap
        Assert.assertEquals(extendedInfo, recordedExtendedInfo)
      } finally {
        expressJobHistoryTable.close()
      }
    } finally {
      table.release()
    }
  }
}

object JobHistorySuite {
  private final val DdlPath: String = "layout/org.kiji.express.flow.ITSimpleFlow.ddl"
  private final val TableName: String = "table"

  /**
   * Applies a table's DDL definition on the specified Kiji instance.
   *
   * @param resourcePath Path of the resource containing the DDL to create the table.
   * @param instanceURI URI of the Kiji instance to use.
   * @throws IOException on I/O error.
   */
  def createTableFromDDL(resourcePath: String, instanceURI: KijiURI): Unit = {
    val client: Client = Client.newInstance(instanceURI)
    try {
      val ddl: String = readResource(resourcePath)
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
    val istream: InputStream = getClass.getClassLoader.getResourceAsStream(resourcePath)
    try {
      val content: String = IOUtils.toString(istream)
      return content
    } finally {
      istream.close()
    }
  }

  class SimpleJob(args: Args) extends KijiJob(args) {
    val tableUri: String = args("tableUri")
    val stat: Stat = Stat("name", "group")

    KijiInput.builder
        .withTableURI(tableUri)
        .withColumns("info:email" -> 'email)
        .build
        .map('email -> 'email) { email: String => stat.inc; email}
        .groupAll {
          group: GroupBuilder => group.foldLeft('email -> 'size)(0) {
            (acc: Int, next: String) => {
              stat.inc; acc + 1
            }
          }
        }
        .map('size -> 'size) { email: String => stat.inc; email}
        .debug
        .write(NullSource)
  }
}
