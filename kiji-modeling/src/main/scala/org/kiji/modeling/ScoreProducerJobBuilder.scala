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

package org.kiji.modeling

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.modeling.config.KijiInputSpec
import org.kiji.modeling.config.ModelDefinition
import org.kiji.modeling.config.ModelEnvironment
import org.kiji.modeling.config.ScoreEnvironment
import org.kiji.modeling.config.ValidationException
import org.kiji.modeling.framework.ScoreProducer
import org.kiji.mapreduce.KijiMapReduceJob
import org.kiji.mapreduce.output.MapReduceJobOutputs
import org.kiji.mapreduce.produce.KijiProduceJobBuilder
import org.kiji.schema.KijiURI

/**
 * Used to build jobs for running the score phase of a model in batch over an entire
 * input table.
 */
object ScoreProducerJobBuilder {
  /**
   * Builds a job for running the score phase of a model in batch over an entire input table.
   *
   * @param model containing the desired score phase.
   * @param environment to run against.
   * @param conf to use with the job, defaults to creating a new HBaseConfiguration.
   * @return a MapReduce job.
   */
  def buildJob(model: ModelDefinition, environment: ModelEnvironment,
      conf: Configuration = HBaseConfiguration.create()): KijiMapReduceJob = {
    // Serialize the model configuration objects.
    conf.set(ScoreProducer.modelDefinitionConfKey, model.toJson)
    conf.set(ScoreProducer.modelEnvironmentConfKey, environment.toJson)

    val scoreModel: Option[Class[_]] = model.scorerClass
    val scoreEnv: Option[ScoreEnvironment] = environment.scoreEnvironment
    if ((scoreModel == None) || (scoreEnv == None)) {
      val error = "A model definition and model environment must include" +
          "the score phase in order to build a ScoreProducer."
      throw new ValidationException(error)
    }

    val uriString = environment
        .scoreEnvironment
        .get
        .inputSpec
        .asInstanceOf[KijiInputSpec]
        .tableUri
    val uri = KijiURI.newBuilder(uriString).build()

    // Build the produce job.
    KijiProduceJobBuilder.create()
        .withConf(conf)
        .withInputTable(uri)
        .withProducer(classOf[ScoreProducer])
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(uri))
        .build()
  }

  /**
   * Builds a job for running the score phase of a model in batch over an entire input table.
   *
   * @param modelDefPath to file containing the desired model definition file.
   * @param environmentPath to file containing the desired model environment file.
   * @param config to use with the job, defaults to creating a new HBaseConfiguration.
   * @return a MapReduce job.
   */
  def buildJob(modelDefPath: String, environmentPath: String,
      config: Configuration): KijiMapReduceJob = {
    buildJob(
        model = ModelDefinition.fromJsonFile(modelDefPath),
        environment = ModelEnvironment.fromJsonFile(environmentPath),
        conf = config)
  }
}
