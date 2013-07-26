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

package org.kiji.express.tool

import java.io.File
import java.io.PrintWriter
import java.net.URL
import java.net.URLClassLoader

import scala.collection.mutable.Buffer

import com.google.common.io.Files
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.JobTest
import com.twitter.scalding.Tsv
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.express.EntityId
import org.kiji.express.KijiSlice
import org.kiji.express.KijiSuite
import org.kiji.express.flow._
import org.kiji.express.impl.MaterializedEntityId
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiURI
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts

trait TestScriptRunner { self: ScriptRunner =>
  /**
   * Provides access to ScriptRunner's protected getJob method.
   *
   * @param args from the command line used to build a job.
   * @return a job constructed with the parameters passed in from the command line.
   */
  def buildJob(args: Args): Job = getJob(args)
}

class ScriptRunnerSuite extends KijiSuite {
  /** Table layout to use for tests. */
  val layout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)

  // Create test Kiji table.
  val uri: KijiURI = doAndRelease(makeTestKijiTable(layout)) { table: KijiTable =>
    table.getURI()
  }

  /** Input tuples to use for word count tests. */
  val wordCountInput: List[(MaterializedEntityId, KijiSlice[String])] = List(
      ( EntityId("row01"), slice("family:column1", (10L, "hello")) ),
      ( EntityId("row02"), slice("family:column1", (10L, "hello")) ),
      ( EntityId("row03"), slice("family:column1", (10L, "world")) ),
      ( EntityId("row04"), slice("family:column1", (10L, "hello")) ))

  /**
   * Validates output from [[com.twitter.scalding.examples.WordCountJob]].
   *
   * @param outputBuffer containing data that output buffer has in it after the job has been run.
   */
  def validateWordCount(outputBuffer: Buffer[(String, Int)]) {
    val outMap = outputBuffer.toMap

    // Validate that the output is as expected.
    assert(3 === outMap("hello"))
    assert(1 === outMap("world"))
  }

  /** Test script to run. */
  val scriptString: String =
"""
KijiInput("%s")("family:column1" -> 'word)
    // Sanitize the word.
    .map('word -> 'cleanword) { words: KijiSlice[String] =>
      words
          .getFirstValue()
          .toLowerCase()
    }
    // Count the occurrences of each word.
    .groupBy('cleanword) { occurences => occurences.size }
    // Write the result to a file.
    .write(Tsv("outputFile"))
"""

  /** The above script string written to a file. */
  val script: File = {
    val temp = File.createTempFile("test-script", ".express")
    val printWriter = new PrintWriter(temp)
    printWriter.println(scriptString.format(uri))
    printWriter.close()
    temp
  }

  test("An script can be compiled and run as a local job.") {
    val runner = new ScriptRunner with TestScriptRunner
    runner.setConf(new Configuration(false))

    def jobc(args: Args): Job = runner.buildJob(Args(script.getPath() + " --local"))

    // Build test job.
    JobTest(jobc _)
        .source(KijiInput(uri.toString)("family:column1" -> 'word), wordCountInput)
        .sink(Tsv("outputFile"))(validateWordCount)
        // Run the test job.
        .run
        .finish
  }

  test("An script can be compiled and run as a hadoop job.") {
    val runner = new ScriptRunner with TestScriptRunner
    runner.setConf(HBaseConfiguration.create())

    def jobc(args: Args): Job = runner.buildJob(Args(script.getPath() + " --hdfs"))

    // Build test job.
    addToClasspath(runner.compileFolder)
    JobTest(jobc _)
        .source(KijiInput(uri.toString)("family:column1" -> 'word), wordCountInput)
        .sink(Tsv("outputFile"))(validateWordCount)
        .runHadoop
        .finish
  }

  test("ScriptRunner's Job builder builds a job with correctly set configuration keys.") {
    val runner = new ScriptRunner with TestScriptRunner
    val script: File = File.createTempFile("test-script", ".express")
    val args = Seq(script.getPath(), "--hdfs")

    runner.setConf(HBaseConfiguration.create())
    runner.buildJob(Args(args))
    runner
        .getConf()
        .get("tmpjars")
        .split(",")
        .foreach { jar => assert(jar != "") }
  }

  test("ScriptRunner's Job builder fails if an invalid script file is specified.") {
    val runner = new ScriptRunner with TestScriptRunner
    val directory: File = Files.createTempDir()

    {
      val thrown = intercept[IllegalArgumentException] {
        runner.buildJob(Args(directory.getPath()))
      }
      assert("requirement failed: %s is not a file".format(directory.getPath())
          === thrown.getMessage())
    }
    {
      val thrown = intercept[IllegalArgumentException] {
        runner.buildJob(Args("non-existent"))
      }
      assert("requirement failed: %s does not exist".format("non-existent")
          === thrown.getMessage())
    }
  }

  /*
   * Ugly hack to add the path where the script is compiled to the current classpath.
   * This is because it seems that the ScriptRunner compiles its classes to a
   * separate ClassLoader and hence Cascading/Hadoop can't get a handle on the Job
   * to run. Ideally the JobTest would construct a Hadoop configuration by parsing
   * the args via GenericOptionsParser but that would require a Scalding patch.
   */
  def addToClasspath(path:File) {
    val method = classOf[URLClassLoader].getDeclaredMethod("addURL", (classOf[URL]));
    method.setAccessible(true);
    method.invoke(ClassLoader.getSystemClassLoader(), (path.toURI().toURL()));
  }
}
