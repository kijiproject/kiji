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

package org.kiji.express.flow

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

import java.util.Properties

import cascading.flow.Flow
import cascading.flow.hadoop.util.HadoopUtil
import cascading.pipe.Checkpoint
import cascading.pipe.Pipe
import cascading.pipe.assembly.AggregateBy
import cascading.tap.Tap
import cascading.tuple.collect.SpillableProps
import com.twitter.chill.config.ConfiguredInstantiator
import com.twitter.chill.config.ScalaMapConfig
import com.twitter.scalding.Args
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.security.UserGroupInformation

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.framework.KijiTap
import org.kiji.express.flow.framework.LocalKijiTap
import org.kiji.express.flow.framework.hfile.HFileFlowStepStrategy
import org.kiji.express.flow.framework.hfile.HFileKijiTap
import org.kiji.express.flow.framework.serialization.KijiKryoInstantiator
import org.kiji.express.flow.util.AvroTupleConversions
import org.kiji.express.flow.util.PipeConversions

/**
 * KijiJob is KijiExpress's extension of Scalding's `Job`, and users should extend it when writing
 * their own jobs in KijiExpress.  It provides extra conversions that Express needs for KijiPipes.
 *
 * @param args to the job. These get parsed in from the command line by Scalding.  Within your own
 *     KijiJob, `args("input")` will evaluate to "SomeFile.txt" if your command line contained the
 *     argument `--input SomeFile.txt`
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Extensible
class KijiJob(args: Args)
    extends Job(args)
    with PipeConversions
    with AvroTupleConversions {
  override def buildFlow: Flow[_] = {
    val taps: List[Tap[_, _, _]] = (
        flowDef.getSources.values.asScala.toList
            ++ flowDef.getSinks.values.asScala.toList
            ++ flowDef.getCheckpoints.values.asScala.toList)

    // Retrieve the configuration
    val conf: Configuration = HBaseConfiguration.create()
    mode match {
      case Hdfs(_, configuration) => {
        HBaseConfiguration.merge(conf, configuration)

        // Obtain any necessary tokens for the current user if security is enabled.
        if (User.isHBaseSecurityEnabled(conf)) {
          val user = UserGroupInformation.getCurrentUser
          if (user.getTokens == null || user.getTokens.isEmpty) {
            TokenUtil.obtainAndCacheToken(conf, user)
          }
        }
      }
      case HadoopTest(configuration, _) => {
        HBaseConfiguration.merge(conf, configuration)
      }
      case _ =>
    }

    // Validate that the Kiji parts of the sources (tables, columns) are valid and exist.
    taps.foreach {
      case tap: KijiTap => tap.validate(conf)
      case tap: HFileKijiTap => tap.validate(conf)
      case tap: LocalKijiTap => {
        val properties: Properties = new Properties()
        properties.putAll(HadoopUtil.createProperties(conf))
        tap.validate(properties)
      }
      case _ => // No Kiji parts to verify.
    }

    // Handle HFile writes.
    checkpointHFileSink()
    val flow = super.buildFlow
    // Here we set the strategy to change the sink steps since we are dumping to HFiles.
    flow.setFlowStepStrategy(HFileFlowStepStrategy)
    flow
  }

  /**
   * Modifies the flowDef to include an explicit checkpoint when writing HFiles, if necessary.
   * Checkpoints are necessary when the final stage of the job writing to an HFile tap includes a
   * reducer, i.e., if it is not a map-only stage.
   */
  private def checkpointHFileSink(): Unit = {
    val sinks: java.util.Map[String, Tap[_, _, _]] = flowDef.getSinks
    val tails: java.util.List[Pipe] = flowDef.getTails

    val hfileSinks = sinks.asScala.collect { case (name, _: HFileKijiTap) => name }.toSet

    if (!hfileSinks.isEmpty) {
      val tailsMap = flowDef.getTails.asScala.map((p: Pipe) => p.getName -> p).toMap
      val flow: Flow[JobConf] = super.buildFlow.asInstanceOf[Flow[JobConf]]

      for {
        flowStep <- flow.getFlowSteps.asScala
        sink <- flowStep.getSinks.asScala
        name <- flowStep.getSinkName(sink).asScala if hfileSinks(name)
      } {
        if (flowStep.getConfig.getNumReduceTasks > 0) {
          // insert checkpoint to force writing the tuples to a temp file inHDFS. The subsequent
          // reading of the checkpoint and tuples flowing into the HFileTap will be map-only and
          // hence allows the IdentityReducer + TotalOrderPartitioner to properly sink the values to
          // HFiles
          val tail = tailsMap(name)
          tails.remove(tail)
          flowDef.addTail(new Pipe(name, new Checkpoint(tail.getPrevious.head)))
        }
      }
    }
  }

  override def config: Map[AnyRef, AnyRef] = {
    val baseConfig: Map[AnyRef, AnyRef] = super.config

    // We configure as is done in Scalding's Job, but then append to mapred.child.java.opts to
    // disable schema validation. This system property is only useful for KijiSchema v1.1. In newer
    // versions of KijiSchema, this property has no effect.
    val disableValidation = " -Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED"
    val oldJavaOptions = baseConfig.get("mapred.child.java.opts").getOrElse("")

    // These are ignored if set in mode.config
    val lowPriorityDefaults = Map(
        SpillableProps.LIST_THRESHOLD -> defaultSpillThreshold.toString,
        SpillableProps.MAP_THRESHOLD -> defaultSpillThreshold.toString,
        AggregateBy.AGGREGATE_BY_THRESHOLD -> defaultSpillThreshold.toString
    )
    // Set up the keys for chill
    val chillConf = ScalaMapConfig(lowPriorityDefaults)
    ConfiguredInstantiator.setReflect(chillConf, classOf[KijiKryoInstantiator])

    // Append all the new keys.
    baseConfig
        .++(chillConf.toMap)
        .+("mapred.child.java.opts" -> (oldJavaOptions + disableValidation))
  }
}
