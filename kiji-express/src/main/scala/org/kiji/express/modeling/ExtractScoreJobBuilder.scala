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

package org.kiji.express.modeling

import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.mapreduce.KijiMapReduceJob
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput
import org.kiji.mapreduce.produce.KijiProduceJobBuilder
import org.kiji.schema.KijiURI

/**
 * Used to build jobs for running the extract and score phases of a model in batch over an entire
 * input table.
 */
object ExtractScoreJobBuilder {
  /**
   * Builds a job for running the extract and score phases of a model in batch over an entire input
   * table.
   *
   * @param model containing the desired extract and score phases.
   * @param environment to run against.
   * @return a MapReduce job.
   */
  def buildJob(model: ModelDefinition, environment: ModelEnvironment): KijiMapReduceJob = {
    val uri = KijiURI.newBuilder(environment.modelTableUri).build()

    // Serialize the model configuration objects.
    val conf = HBaseConfiguration.create()
    conf.set(ExtractScoreProducer.modelDefinitionConfKey, model.toJson())
    conf.set(ExtractScoreProducer.modelEnvironmentConfKey, environment.toJson())

    // Build the produce job.
    KijiProduceJobBuilder.create()
        .withConf(conf)
        .withInputTable(uri)
        .withProducer(classOf[ExtractScoreProducer])
        .withOutput(new DirectKijiTableMapReduceJobOutput(uri))
        .build()
  }

  /**
   * Builds a job for running the extract and score phases of a model in batch over an entire input
   * table.
   *
   * @param modelFile containing the desired extract and score phases.
   * @param environmentFile to run against.
   * @return a MapReduce job.
   */
  def buildJob(modelFile: String, environmentFile: String): KijiMapReduceJob = {
    buildJob(
        model = ModelDefinition.fromJsonFile(modelFile),
        environment = ModelEnvironment.fromJsonFile(environmentFile))
  }
}
