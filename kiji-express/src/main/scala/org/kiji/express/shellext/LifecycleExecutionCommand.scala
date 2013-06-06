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

package org.kiji.express.shellext

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.express.modeling.ExtractScoreJobBuilder
import org.kiji.express.modeling.ModelDefinition
import org.kiji.express.modeling.ModelEnvironment
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.DDLCommand
import org.apache.hadoop.hbase.HBaseConfiguration

/**
 * A DDL shell command capable of running a set of modeling lifecycle phases. An instance of this
 * command should be obtained by parsing a statement in the DDL language extension defined by
 * [[org.kiji.express.shellext.ModelingParserPlugin]].
 *
 * @param lifecyclePhases is a list of modeling lifecycle phases that should be executed.
 * @param modelDefConfigureVia specifies how to load a model definition.
 * @param modelEnvConfigureVia specifies how to load a model environment.
 * @param env that the schema shell command should run in.
 */
@ApiAudience.Private
@ApiStability.Experimental
private[express] final class LifecycleExecutionCommand (
    val lifecyclePhases: List[LifeCyclePhase],
    val modelDefConfigureVia: ConfigureVia,
    val modelEnvConfigureVia: ConfigureVia,
    val jobsConfiguration: JobsConfiguration,
    val env: Environment) extends DDLCommand {

  /**
   * Loads a model definition. Currently, only loading from files is supported.
   *
   * @param configureVia specifies how to load the model definition.
   * @return the model definition loaded.
   */
  private def loadModelDefinition(configureVia: ConfigureVia): ModelDefinition = {
    configureVia match {
      case ConfigureViaFile(filePath) => ModelDefinition.fromJsonFile(filePath)
    }
  }

  /**
   * Loads a model environment. Currenly, only loading from files is supported.
   *
   * @param configureVia specifies how to load the model environment.
   * @return the model environment loaded.
   */
  private def loadModelEnvironment(configureVia: ConfigureVia): ModelEnvironment = {
    configureVia match {
      case ConfigureViaFile(filePath) => ModelEnvironment.fromJsonFile(filePath)
    }
  }

  /**
   * Gets a configuration for Hadoop jobs including the library jars and properties passed to
   * this command.
   *
   * @return the Hadoop configuration for jobs launched while executing lifecycle phases.
   */
  private[express] def hadoopConfiguration = {
    val JobsConfiguration(libjarsList, propertiesMap) = jobsConfiguration
    // Add tmpjars to the config.
    val jarConfig = libjarsList.foldLeft(HBaseConfiguration.create()) { (config, libjar) =>
      config.set("tmpjars", config.get("tmpjars", "") + "," + libjar)
      config
    }

    // Add properties to the config.
    propertiesMap.foldLeft(jarConfig) { (config, property) =>
      config.set(property._1, property._2)
      config
    }
  }

  /**
   * Executes the series of modeling lifecycle phases this command was initialized with.
   *
   * @return an environment for subsequent schema shell commands to run in.
   */
  override def exec(): Environment = {
    try {
      echo("Building a Hadoop job to perform batch extract and score.")
      val modelDefinition = loadModelDefinition(modelDefConfigureVia)
      val modelEnvironment = loadModelEnvironment(modelEnvConfigureVia)
      val extractScoreJob = ExtractScoreJobBuilder.buildJob(modelDefinition, modelEnvironment,
          hadoopConfiguration)
      echo("Running batch extract and score.")
      if (!extractScoreJob.run()) {
        echo("Job failed!")
        throw new DDLException("Batch extract and score failed.")
      } else {
        echo("Job succeeded!")
      }
    } catch {
      case e: Exception => {
        echo("Failed to run batch extract and score.")
        throw new DDLException("Exception encountered while attempting to run "
            + "batch extract and score." + e.getMessage)
      }
    }
    env
  }
}

