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

package org.kiji.modeling.tool

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.common.flags.Flag
import org.kiji.common.flags.FlagParser
import org.kiji.modeling.ScoreProducerJobBuilder


/**
 * Provides the ability to run batch Extract-Score jobs via the command line. This tool expects
 * two flags: model-def and model-env. The value of the flag "model-def" is expected to be the
 * path in the local filesystem to the JSON file that specifies the model definition. The value of
 * the flag "model-env" is expected to be the path in the local filesystem to the JSON file that
 * specifies the model environment.
 *
 */
@ApiAudience.Private
@ApiStability.Experimental
@Inheritance.Sealed
final class ScoreJobTool extends Configured with Tool {
  @Flag(name="model-def", usage="Path to a file containing a Model Definition JSON description. ")
  val mModelDefPath: String = ""

  @Flag(name="model-env", usage="Path to a file containing a Model Environment JSON description. ")
  val mModelEnvPath: String = ""

  /** Validates that the parsed paths are not empty. */
  def validateFlags() {
    require(!mModelDefPath.isEmpty, "Specify the Model Definition to use with" +
        " --model-def=/path/to/model-def.json")
    require(!mModelEnvPath.isEmpty, "Specify the Model Environment to use with" +
        " --model-env=/path/to/model-env.json")
  }

  override def run(args: Array[String]): Int = {
    // scalastyle:off null
    val nonFlagArgs = FlagParser.init(this, args)
    if (nonFlagArgs == null) {
      ScoreJobTool.LOGGER.info("Problems parsing command line flags.")
      return 1
    }
    // scalastyle:on null
    validateFlags()
    ScoreJobTool.LOGGER.info("Building Extract-Score batch job.")
    val produceJob = ScoreProducerJobBuilder.buildJob(modelDefPath = mModelDefPath,
        environmentPath = mModelEnvPath, config = getConf())
    ScoreJobTool.LOGGER.info("Running Extract-Score batch job.")
    produceJob.run() match {
      case true => 0
      case false => 1
    }
  }
}

/*
 * The companion object to ScoreJobTool that only contains a main method.
 */
object ScoreJobTool {
  val LOGGER: Logger = LoggerFactory.getLogger(ScoreJobTool.getClass)
  /**
   * The entry point into the tool.
   *
   * @param args from the command line.
   * @return a return code that signals the success of the specified job.
   */
  def main(args: Array[String]) {
    ToolRunner.run(HBaseConfiguration.create(), new ScoreJobTool, args)
  }
}
