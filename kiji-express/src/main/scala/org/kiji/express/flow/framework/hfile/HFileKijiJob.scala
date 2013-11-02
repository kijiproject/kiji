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

package org.kiji.express.flow.framework.hfile

import com.twitter.scalding._
import com.twitter.scalding.Hdfs
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.fs.Path
import cascading.tap.hadoop.Hfs
import cascading.util.Util
import org.kiji.mapreduce.framework.HFileKeyValue
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.FileSystem
import cascading.flow.Flow
import scala.transient
import cascading.flow.Flow
import cascading.util.Util
import java.util.Properties
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import cascading.flow.Flow
import cascading.flow.FlowListener
import cascading.tap.hadoop.Hfs
import cascading.util.Util
import com.twitter.scalding.HadoopMode
import com.twitter.scalding.Hdfs
import com.twitter.scalding._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf

import org.kiji.express.flow.KijiJob
import org.kiji.express.flow.framework.LocalKijiTap
import org.kiji.express.util.PipeConversions
import org.kiji.mapreduce.framework.HFileKeyValue

/**
 * HFileKijiJob is an extension of KijiJob and users should extend it when writing their own
 * their own jobs in KijiExpress whose output will eventually be bulk loaded into HBase.
 *
 * @param args to the job. These get parsed in from the command line by Scalding.
 *
 *     NOTE: To properly work with dumping to HFiles, the argument --hFileOutput must be provided
 *     which specifies the location where the HFiles will be written upon job completion. Also
 *     required is the --output flag which is the Kiji table to use to obtain layout information
 *     to properly format the HFiles for bulk loading.
 */
class HFileKijiJob(args: Args) extends KijiJob(args) {

  val HFILE_OUTPUT_ARG = "hFileOutput"

  // Force the check to ensure that a value has been provided for the hFileOutput
  args(HFILE_OUTPUT_ARG)
  args("output")

  @transient
  lazy private val jobConf = implicitly[Mode] match {
    case Hdfs(_, configuration) => {
      configuration
    }
    case HadoopTest(configuration, _) => {
      configuration
    }
    case _ => new JobConf()
  }

  @transient
  lazy val uniqTempFolder = makeTemporaryPathDirString("HFileDumper")

  val tempPath = new Path(Hfs.getTempPath(jobConf.asInstanceOf[JobConf]), uniqTempFolder).toString

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
    val baseConfig = super.config(mode)
    baseConfig ++ Map(HFileKijiOutput.TEMP_HFILE_OUTPUT_KEY -> tempPath.toString())
  }

  override def buildFlow(implicit mode: Mode): Flow[_] = {
    val flow = super.buildFlow
    // Here we set the strategy to change the sink steps since we are dumping to HFiles.
    flow.setFlowStepStrategy(new HFileFlowStepStrategy)
    flow
  }

  override def next: Option[Job] = {
    val fs = FileSystem.get(jobConf)
    if(fs.exists(new Path(tempPath))) {
      val newArgs = args + ("input" -> Some(tempPath))
      val job = new HFileMapJob(newArgs)
      Some(job)
    } else {
      None
    }
  }

  // Borrowed from Hfs#makeTemporaryPathDirString
  private def makeTemporaryPathDirString(name: String) = {
    // _ is treated as a hidden file, so wipe them out
    val name2 = name.replaceAll("^[_\\W\\s]+", "")

    val name3 = if (name2.isEmpty()) {
      "temp-path"
    } else {
      name2
    }

    name3.replaceAll("[\\W\\s]+", "_") + Util.createUniqueID()
  }
}

/**
 * Private job implementation that executes the conversion of the intermediary HFile key-value
 * sequence files to the final HFiles. This is done only if the first job had a Cascading
 * configured reducer.
 */
private final class HFileMapJob(args: Args) extends HFileKijiJob(args) {

  override def next: Option[Job] = {
      val conf = implicitly[Mode] match {
      case Hdfs(_, configuration) => {
        configuration
      }
      case HadoopTest(configuration, _) => {
        configuration
      }
      case _ => new JobConf()
    }
      val fs = FileSystem.get(conf)
      val input = args("input")
      fs.delete(new Path(input), true)
      None
    }

  WritableSequenceFile[HFileKeyValue, NullWritable](args("input"), ('keyValue, 'bogus))
    .write(new HFileSource(args("output"),args("hFileOutput")))
}
