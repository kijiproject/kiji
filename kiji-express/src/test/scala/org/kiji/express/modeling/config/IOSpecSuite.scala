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

package org.kiji.express.modeling.config

import java.io.File

import com.twitter.scalding.Hdfs
import com.twitter.scalding.Source
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.scalatest.FunSuite

import org.kiji.express.modeling.Trainer
import org.kiji.express.modeling.framework.ModelExecutor

class IOSpecSuite extends FunSuite {
  val textSourceLocation: String = "src/test/resources/sources/TextSource"
  val textSourceOutput: String = "src/test/resources/sources/TextOutput/"
  test("A Text source can be read/written from a model phase") {
    val modelDef: ModelDefinition = ModelDefinition(
        name = "test-text-source",
        version = "1.0",
        trainerClass = Some(classOf[IOSpecSuite.TrainWordCounter]))
    val modelEnv = ModelEnvironment(
        name = "myname",
        version = "1.0.0",
        prepareEnvironment = None,
        trainEnvironment = Some(TrainEnvironment(
            inputSpec = Map("textinput" -> TextSourceSpec(textSourceLocation)),
            outputSpec = Map("textoutput" -> TextSourceSpec(textSourceOutput)),
            keyValueStoreSpecs = Seq()
        )),
        scoreEnvironment = None)
    // Hack to set the mode correctly. Scalding sets the mode in JobTest
    // which creates a problem for running the prepare/train phases, which run
    // their own jobs. This makes the test below run in HadoopTest mode instead
    // of Hadoop mode whenever it is run after another test that uses JobTest.
    // Remove this after the bug in Scalding is fixed.
    com.twitter.scalding.Mode.mode = Hdfs(false, HBaseConfiguration.create())

    ModelExecutor(modelDef, modelEnv).runTrainer()
    val lines = scala.io.Source.fromFile(textSourceOutput + "/part-00000").mkString
    assert(lines.split("""\s+""").deep == Array("kiji", "3").deep)
    FileUtils.deleteDirectory(new File(textSourceOutput))
  }
}

object IOSpecSuite {
  class TrainWordCounter extends Trainer {
    class WordCountJob(input: Map[String, Source], output: Map[String,
      Source]) extends TrainerJob {
      input("textinput")
          .flatMap('line -> 'word) { line : String => line.split("""\s+""") }
          .groupBy('word) { _.size }
          .write(output("textoutput"))
    }

    override def train(input: Map[String, Source], output: Map[String, Source]): Boolean = {
      new WordCountJob(input, output).run
      true
    }
  }
}
