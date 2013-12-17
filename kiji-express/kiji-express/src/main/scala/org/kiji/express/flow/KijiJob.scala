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
import com.twitter.scalding.Args
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Job
import com.twitter.scalding.Mode
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
@ApiStability.Experimental
@Inheritance.Extensible
class KijiJob(args: Args = Args(Nil))
    extends Job(args)
    with PipeConversions
    with AvroTupleConversions {
  override def validateSources(mode: Mode): Unit = {
    val taps: List[Tap[_, _, _]] =
        flowDef.getSources.values.asScala.toList ++
        flowDef.getSinks.values.asScala.toList

    // Retrieve the configuration
    var conf: Configuration = HBaseConfiguration.create()
    implicitly[Mode] match {
      case Hdfs(_, configuration) => {
        HBaseConfiguration.merge(conf, configuration)

        // Obtain any necessary tokens for the current user if security is enabled.
        if (User.isHBaseSecurityEnabled(conf)) {
          val user = UserGroupInformation.getCurrentUser
          TokenUtil.obtainAndCacheToken(conf, user)
        }
      }
      case HadoopTest(configuration, _) => {
        conf = configuration
      }
      case _ =>
    }

    // Validate that the Kiji parts of the sources (tables, columns) are valid and exist.
    taps.foreach {
      case kijiTap: KijiTap => kijiTap.validate(new JobConf(conf))
      case localKijiTap: LocalKijiTap => {
        val properties: Properties = new Properties()
        properties.putAll(HadoopUtil.createProperties(conf))
        localKijiTap.validate(properties)
      }
      case _ => // No Kiji parts to verify.
    }

    // Call any validation that scalding's Job class does.
    super.validateSources(mode)
  }

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
    val baseConfig = super.config(mode)

    // We configure as is done in Scalding's Job, but then append to mapred.child.java.opts to
    // disable schema validation. This system property is only useful for KijiSchema v1.1. In newer
    // versions of KijiSchema, this property has no effect.
    val disableValidation = " -Dorg.kiji.schema.impl.AvroCellEncoder.SCHEMA_VALIDATION=DISABLED"
    val oldJavaOptions = baseConfig
        .get("mapred.child.java.opts")
        .getOrElse("")

    // Add support for our Kryo Avro serializers (see org.kiji.express.flow.framework.KryoKiji).
    val oldSerializations = baseConfig("io.serializations").toString
    require(oldSerializations.contains("com.twitter.scalding.serialization.KryoHadoop"))
    val newSerializations = oldSerializations.replaceFirst(
        "com.twitter.scalding.serialization.KryoHadoop",
        "org.kiji.express.flow.framework.serialization.KryoKiji")

    // Append all the new keys.
    baseConfig +
        ("mapred.child.java.opts" -> (oldJavaOptions + disableValidation)) +
        ("io.serializations" -> newSerializations)
  }
}
