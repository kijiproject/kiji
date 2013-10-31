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

import com.twitter.scalding.Args
import com.twitter.scalding.Mode
import com.twitter.scalding.Tool

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.util.ToolRunner

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.modeling.config.ExpressColumnRequest
import org.kiji.modeling.config.ExpressDataRequest
import org.kiji.modeling.config.FieldBinding
import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.TextSourceSpec
import org.kiji.modeling.config.TrainEnvironment
import org.kiji.modeling.framework.ModelExecutor
import org.kiji.modeling.lib.LMTrainer

@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final class LMTool extends Tool {
  // parse arguments, create trainer and run it.
  override def run(args: Array[String]): Int = {
    // TODO EXP-204, pass the mode argument to the model executor
    val (mode: Mode, jobArgs: Args) = parseModeArgs(args)
    Mode.mode = mode

    val datasetTableURI: String = jobArgs("dataset")
    val paramsFilePath: String = jobArgs("parameters")
    val attributesColumn: String = jobArgs("attribute-column")
    val targetColumn: String = jobArgs("target-column")
    val outputPath: String = jobArgs("output")

    val modelDefinition: ModelDefinition = ModelDefinition(
      name = "lmtool",
      version = "1.0",
      trainerClass = Some(classOf[LMTrainer]))

    val request: ExpressDataRequest = new ExpressDataRequest(0, Long.MaxValue,
        Seq(new ExpressColumnRequest(attributesColumn, 1, None),
            new ExpressColumnRequest(targetColumn, 1, None))
    )

    val modelEnvironment: ModelEnvironment = ModelEnvironment(
        name = "lmtool",
        version = "1.0",
        prepareEnvironment = None,
        trainEnvironment = Some(TrainEnvironment(
            inputSpec = Map(
                "dataset" -> KijiInputSpec(
                    datasetTableURI,
                    dataRequest = request,
                    fieldBindings = Seq(
                        FieldBinding(tupleFieldName = "attributes",
                            storeFieldName = attributesColumn),
                        FieldBinding(tupleFieldName = "target",
                            storeFieldName = targetColumn))),
                "parameters" -> TextSourceSpec(
                    path = paramsFilePath
                )
            ),
            outputSpec = Map(
                "parameters" -> TextSourceSpec(
                    path = outputPath
                )
            ),
            keyValueStoreSpecs = Seq()
      )),
      scoreEnvironment = None
    )

    val modelExecutor = ModelExecutor(modelDefinition, modelEnvironment, jobArgs)
    modelExecutor.runTrainer() match {
      case true => 0
      case false => 1
    }
  }
}

/*
 * The companion object to LMTool that only contains a main method.
 */
object LMTool {
  /**
   * The entry point into the tool.
   *
   * @param args from the command line.
   * @return a return code that signals the success of the specified job.
   */
  def main(args: Array[String]) {
    ToolRunner.run(HBaseConfiguration.create(), new LMTool, args)
  }
}
