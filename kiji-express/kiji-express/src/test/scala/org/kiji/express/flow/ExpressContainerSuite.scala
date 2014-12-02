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

import scala.collection.mutable

import cascading.flow.FlowDef
import com.fasterxml.jackson.databind.node.ObjectNode
import com.twitter.scalding.Args
import com.twitter.scalding.JobTest
import com.twitter.scalding.Mode
import com.twitter.scalding.Tsv
import com.twitter.scalding.TypedPipe
import com.twitter.scalding.TypedTsv
import org.junit.Assert
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory


private[express] class ExpressContainerSuite {
  import ExpressContainerSuite._

  val sourceData: Seq[(String, String)] = Seq(
      ("first", "1"),
      ("second", "2")
  )

  def verifyResult(buffer: mutable.Buffer[(String, String)]): Unit = {
    Assert.assertTrue(buffer.size == sourceData.size)
    buffer.foreach { row: (String, String) =>
      Assert.assertTrue(sourceData.contains(row))
    }
  }

  @Test
  def testExampleJob(): Unit = {
    val config: String =
        s"""|{
            |  "class" : "${classOf[TestTsvIO].getCanonicalName}",
            |  "filename" : "$FILE_NAME"
            |}
        """.stripMargin

    JobTest(new ExampleJob(_))
        .arg("input", config)
        .arg("output", config)
        .source(Tsv(FILE_NAME), sourceData)
        .sink(TypedTsv[(String, String)](FILE_NAME)) { verifyResult }
        .runHadoop
  }

  @Test
  def testInvalidContainer(): Unit = {
    val config: String =
        s"""|{
            |  "class" : "${classOf[TestInvalidInterfaceIO].getCanonicalName}",
            |  "filename" : "$FILE_NAME"
            |}
        """.stripMargin

    var failed = false
    try {
      JobTest(new ExampleJob(_))
          .arg("input", config)
          .arg("output", config)
          .source(Tsv(FILE_NAME), sourceData)
          .sink(TypedTsv[(String, String)](FILE_NAME)) { verifyResult }
          .runHadoop
    } catch {
      case e: ClassCastException => {
        LOG.info(e.toString)
        failed = true
      }

    }
    Assert.assertTrue(failed)
  }

  @Test
  def testInvalidConstructor(): Unit = {
    val config: String =
        s"""|{
            |  "class" : "${classOf[TestInvalidConstructorIO].getCanonicalName}",
            |  "filename" : "$FILE_NAME"
            |}
        """.stripMargin

    var failed = false
    try {
      JobTest(new ExampleJob(_))
          .arg("input", config)
          .arg("output", config)
          .source(Tsv(FILE_NAME), sourceData)
          .sink(TypedTsv[(String, String)](FILE_NAME)) { verifyResult }
          .runHadoop
    } catch {
      case e: NoSuchMethodException => {
        LOG.info(e.toString)
        failed = true
      }
    }
    Assert.assertTrue(failed)
  }
}

private[express] object ExpressContainerSuite {
  // ----- Logging -----
  val LOG: Logger = LoggerFactory.getLogger(getClass)

  // ----- Constants -----
  val FILE_NAME: String = "dummy.tsv"

  // ----- An example job that uses the TestIO interface -----
  class ExampleJob(args: Args) extends KijiJob(args) {
    val dummyInput = TestIO("input").readDummyInput
    val dummyOutput = TestIO("output").writeDummyOutput

    val pipe: TypedPipe[(String, String)] = dummyInput.thenDo(dummyOutput)
  }
}

/**
 * Interface to preprocess the inputs and outputs of an express job.
 */
private[express] trait TestIO {
  def readDummyInput: TypedPipe[(String, String)] = ???
  def writeDummyOutput: TypedPipe[(String, String)] => TypedPipe[(String, String)] = ???
}

/**
 * Factory to construct TestIO objects from a configuration passed through an Args object in a
 * KijiJob.
 */
private[express] object TestIO extends ExpressContainerFactory[TestIO]

/**
 * A valid implementation of the TestIO interface.
 */
private[express] class TestTsvIO(conf: ObjectNode, md: Mode, fd: FlowDef)
    extends ExpressContainer(conf, md, fd) with TestIO {

  override def readDummyInput: TypedPipe[(String, String)] = {
    val filename: String = conf.findValue("filename").asText()

    Tsv(filename, ('thing1, 'thing2)).read
        .toTypedPipe[(String, String)]('thing1, 'thing2)
  }

  override def writeDummyOutput: TypedPipe[(String, String)] => TypedPipe[(String, String)] = {
    val filename: String = conf.findValue("filename").asText()

    pipe: TypedPipe[(String, String)] =>
      pipe.write(TypedTsv[(String, String)](filename))
      pipe
  }
}

/**
 * An ExpressContainer implementation that does not extend the TestIO interface. This means that
 * the TestIO container factory should fail when it attempts to construct it.
 */
private[express] class TestInvalidInterfaceIO(conf: ObjectNode, md: Mode, fd: FlowDef)
    extends ExpressContainer(conf, md, fd) // Does not extend TestIO.

/**
 * An ExpressContainer that does not have a constructor that is usable by the TestIO container
 * factory. This means that the factory should fail if it attempts to construct the object.
 */
private[express] class TestInvalidConstructorIO(
    conf: ObjectNode,
    md: Mode,
    fd: FlowDef,
    label: String // Contains an extra parameter in the constructor.
) extends ExpressContainer(conf, md, fd) with TestIO
