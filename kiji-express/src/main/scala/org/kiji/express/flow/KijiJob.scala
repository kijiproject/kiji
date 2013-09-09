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
import java.util.Properties

import cascading.flow.hadoop.util.HadoopUtil
import cascading.tap.Tap
import com.twitter.scalding._
import com.twitter.scalding.Hdfs
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf

import org.kiji.express.flow.framework.KijiTap
import org.kiji.express.flow.framework.LocalKijiTap
import org.kiji.express.util.PipeConversions

/**
 * KijiJob is KijiExpress's extension of Scalding's `Job`, and users should extend it when writing
 * their own jobs in KijiExpress.  It provides extra conversions that Express needs for KijiPipes.
 *
 * @param args to the job. These get parsed in from the command line by Scalding.  Within your own
 *     KijiJob, `args("input")` will evaluate to "SomeFile.txt" if your command line contained the
 *     argument `--input SomeFile.txt`
 */
class KijiJob(args: Args = Args(Nil)) extends Job(args) with PipeConversions {
  override def validateSources(mode: Mode): Unit = {
    val taps: List[Tap[_, _, _]] =
        flowDef.getSources.values.asScala.toList ++
        flowDef.getSinks.values.asScala.toList

    // Retrieve the configuration
    var conf: Configuration = HBaseConfiguration.create()
    implicitly[Mode] match {
      case Hdfs(_, configuration) => {
        conf = configuration
      }
      case HadoopTest(configuration, _) => {
        conf = configuration
      }
      case _ =>
    }

    // Validate that the Kiji parts of the sources (tables, columns) are valid and exist.
    taps.foreach { tap =>
      tap match {
        case kijiTap: KijiTap => kijiTap.validate(new JobConf(conf))
        case localKijiTap: LocalKijiTap => {
          val properties: Properties = new Properties()
          properties.putAll(HadoopUtil.createProperties(conf))
          localKijiTap.validate(properties)
        }
        case _ => // No Kiji parts to verify.
      }
    }

    // Call any validation that scalding's Job class does.
    super.validateSources(mode)
  }

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
    // We configure as is done in Scalding's Job, but then append to mapred.child.java.opts to
    // disable schema validation.
    val baseConfig = super.config(mode)
    val baseJavaOpts = baseConfig.get("mapred.child.java.opts").getOrElse("").toString()
    val newJavaOpts = (baseJavaOpts
        + " -Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED")
    baseConfig ++ Map("mapred.child.java.opts" -> newJavaOpts)
  }
}
