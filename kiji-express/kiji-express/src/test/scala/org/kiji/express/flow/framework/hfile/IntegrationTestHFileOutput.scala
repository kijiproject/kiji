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

import scala.collection.JavaConverters.asScalaIteratorConverter

import com.twitter.scalding.Args
import com.twitter.scalding.IterableSource
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

import org.kiji.express.IntegrationUtil._
import org.kiji.express.flow.EntityId
import org.kiji.express.flow.KijiJob
import org.kiji.express.flow.KijiOutput
import org.kiji.express.flow.framework.KijiScheme
import org.kiji.express.flow.util.ResourceUtil._
import org.kiji.express.flow.util.{AvroTypesComplete => ATC}
import org.kiji.schema.Kiji
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiTable
import org.kiji.schema.testutil.AbstractKijiIntegrationTest

class IntegrationTestHFileOutput extends AbstractKijiIntegrationTest {
  import IntegrationTestHFileOutput._

  private var kiji: Kiji = null
  private var conf: Configuration = null

  @Before
  def setupTest(): Unit = {
    kiji = Kiji.Factory.open(getKijiURI())
    conf = getConf()
  }

  @After
  def cleanupTest(): Unit = kiji.release()

  @Test
  def testShouldBulkLoadHFiles(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    Assert.assertTrue(conf.get("mapred.output.dir").startsWith(conf.get("hadoop.tmp.dir")))
    FileSystem.get(conf).delete(new Path(hfileOutput), true)

    kiji.createTable(ATC.layout.getDesc)

    doAndRelease(kiji.openTable(ATC.name)) { table =>
      runJob(conf, classOf[HFileOutputMapOnly],
        "--hdfs", "--table-uri", table.getURI.toString, "--hfile-output", hfileOutput)

      bulkLoadHFiles(hfileOutput + "/hfiles", conf, table)

      validateInts(table)
    }
  }

  @Test
  def testShouldBulkLoadWithReducer(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    kiji.createTable(ATC.layout.getDesc)

    doAndRelease(kiji.openTable(ATC.name)) { table =>
      runJob(conf, classOf[HFileOutputWithReducer],
        "--hdfs", "--table-uri", table.getURI.toString, "--hfile-output", hfileOutput)

      bulkLoadHFiles(hfileOutput + "/hfiles", conf, table)

      validateCount(table)
    }
  }

  @Test
  def testShouldBulkLoadMultipleTables(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    val aOutput = hfileOutput + "/a"
    val bOutput = hfileOutput + "/b"

    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    val layoutA = ATC.layout.getDesc
    layoutA.setName("A")
    kiji.createTable(layoutA)

    val layoutB = ATC.layout.getDesc
    layoutB.setName("B")
    kiji.createTable(layoutB)

    doAndRelease(kiji.openTable("A")) { a: KijiTable =>
      doAndRelease(kiji.openTable("B")) { b: KijiTable =>
        runJob(conf, classOf[HFileOutputMultipleTables],
          "--hdfs",
          "--a", a.getURI.toString, "--b", b.getURI.toString,
          "--a-output", aOutput, "--b-output", bOutput)

        bulkLoadHFiles(aOutput + "/hfiles", conf, a)
        bulkLoadHFiles(bOutput + "/hfiles", conf, b)

        validateInts(a)
        validateCount(b)
      }
    }
  }

  @Test
  def testShouldBulkLoadHFileAndDirect(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    val aOutput = hfileOutput + "/a"

    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    val layoutA = ATC.layout.getDesc
    layoutA.setName("A")
    kiji.createTable(layoutA)

    val layoutB = ATC.layout.getDesc
    layoutB.setName("B")
    kiji.createTable(layoutB)

    doAndRelease(kiji.openTable("A")) { a: KijiTable =>
      doAndRelease(kiji.openTable("B")) { b: KijiTable =>
        runJob(conf, classOf[HFileOutputAndDirectOutput],
          "--hdfs",
          "--a", a.getURI.toString, "--b", b.getURI.toString,
          "--a-output", aOutput)

        bulkLoadHFiles(aOutput + "/hfiles", conf, a)

        validateInts(a)
        validateInts(b)
      }
    }
  }

  @Test
  def testShouldBulkLoadMultipleHFilesToOneTable(): Unit = {
    val hfileOutput = conf.get("mapred.output.dir")
    val aOutput = hfileOutput + "/a"
    val bOutput = hfileOutput + "/b"

    FileSystem.get(conf)delete(new Path(hfileOutput), true)

    val layout = ATC.layout.getDesc
    kiji.createTable(layout)

    doAndRelease(kiji.openTable(layout.getName)) { table: KijiTable =>
      runJob(conf, classOf[HFileOuputMultipleToSameTable],
        "--hdfs",
        "--uri", table.getURI.toString,
        "--a-output", aOutput, "--b-output", bOutput)

      bulkLoadHFiles(aOutput + "/hfiles", conf, table)
      bulkLoadHFiles(bOutput + "/hfiles", conf, table)

      validateInts(table)
      validateCount(table)
    }
  }
}

object IntegrationTestHFileOutput {
  val count: Int = 100
  val inputs: Set[(EntityId, Int)] =
    ((1 to count).map(int => EntityId(int.toString)) zip (1 to count)).toSet
  val countEid: EntityId = EntityId("count")

  def validateInts(table: KijiTable): Unit = withKijiTableReader(table) { reader =>
    val request = KijiDataRequest.create(ATC.family, ATC.intColumn)
    doAndClose(reader.getScanner(request)) { scanner =>
      val outputs = for (rowData <- scanner.iterator().asScala)
      yield (EntityId.fromJavaEntityId(rowData.getEntityId),
            rowData.getMostRecentValue(ATC.family, ATC.intColumn))
      Assert.assertEquals(inputs, outputs.toSet)
    }
  }

  def validateCount(table: KijiTable): Unit = withKijiTableReader(table) { reader =>
    val request = KijiDataRequest.create(ATC.family, ATC.longColumn)
    doAndClose(reader.getScanner(request)) { scanner =>
      val outputs = for (rowData <- scanner.iterator().asScala)
      yield (EntityId.fromJavaEntityId(rowData.getEntityId),
            rowData.getMostRecentValue(ATC.family, ATC.longColumn))
      Assert.assertEquals(List(countEid -> count), outputs.toList)
    }
  }
}

class HFileOutputMapOnly(args: Args) extends KijiJob(args) {
  import IntegrationTestHFileOutput._
  val uri = args("table-uri")
  val hfilePath = args("hfile-output")

  IterableSource(inputs, (KijiScheme.EntityIdField, 'int))
    .read
    .write(HFileKijiOutput(uri, hfilePath, 'int -> (ATC.family +":"+ ATC.intColumn)))
}

class HFileOutputWithReducer(args: Args) extends KijiJob(args) {
  import IntegrationTestHFileOutput._
  val uri = args("table-uri")
  val hfilePath = args("hfile-output")

  IterableSource(inputs, (KijiScheme.EntityIdField, 'int))
      .read
      .groupAll { _.size }
      .insert('entityId, countEid)
      .write(HFileKijiOutput(uri, hfilePath, 'size -> (ATC.family +":"+ ATC.longColumn)))
}

class HFileOutputMultipleTables(args: Args) extends KijiJob(args) {
  import IntegrationTestHFileOutput._
  val aUri = args("a")
  val bUri = args("b")
  val aOutput = args("a-output")
  val bOutput = args("b-output")

  val pipe = IterableSource(inputs, (KijiScheme.EntityIdField, 'int)).read

  pipe.write(HFileKijiOutput(aUri, aOutput, 'int -> (ATC.family +":"+ ATC.intColumn)))
  pipe
    .groupAll { _.size }
    .insert('entityId, countEid)
    .write(HFileKijiOutput(bUri, bOutput, 'size -> (ATC.family +":"+ ATC.longColumn)))
}

class HFileOutputAndDirectOutput(args: Args) extends KijiJob(args) {
  import IntegrationTestHFileOutput._
  val aUri = args("a")
  val bUri = args("b")
  val aOutput = args("a-output")

  val pipe = IterableSource(inputs, (KijiScheme.EntityIdField, 'int)).read

  pipe.write(HFileKijiOutput(aUri, aOutput, 'int -> (ATC.family +":"+ ATC.intColumn)))
  pipe.write(
      KijiOutput
        .builder
        .withTableURI(bUri)
        .withColumns('int -> (ATC.family +":"+ ATC.intColumn))
        .build)
}

class HFileOuputMultipleToSameTable(args: Args) extends KijiJob(args) {
  import IntegrationTestHFileOutput._
  val uri = args("uri")
  val aOutput = args("a-output")
  val bOutput = args("b-output")

  val pipe = IterableSource(inputs, (KijiScheme.EntityIdField, 'int)).read

  pipe.write(HFileKijiOutput(uri, aOutput, 'int -> (ATC.family +":"+ ATC.intColumn)))
  pipe
      .groupAll { _.size }
      .insert('entityId, countEid)
      .write(HFileKijiOutput(uri, bOutput, 'size -> (ATC.family +":"+ ATC.longColumn)))
}
