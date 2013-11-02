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

import java.net.URI
import java.util.List

import cascading.flow.Flow
import cascading.flow.FlowStep
import cascading.flow.FlowStepStrategy
import cascading.tap.hadoop.Hfs
import cascading.tap.hadoop.util.TempHfs
import cascading.tuple.Tuple
import cascading.util.Util
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapred.lib.IdentityReducer
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner
import org.apache.hadoop.mapreduce.JobContext

import org.kiji.mapreduce.framework.HFileKeyValue
import org.kiji.mapreduce.framework.HFileKeyValue.FastComparator
import org.kiji.mapreduce.output.HFileMapReduceJobOutput
import org.kiji.mapreduce.output.framework.KijiHFileOutputFormat
import org.kiji.schema.KijiURI
import org.kiji.schema.mapreduce.KijiConfKeys

/**
 * An implementation of a Cascading FlowStepStrategy used to alter the properties
 * of the flow step corresponding to the sink to support writing directly to HFiles. This
 * will only operate on FlowSteps where the sink's outputFormat is the KijiHFileOutputFormat.
 *
 * There are two situations that can happen when writing to HFiles:
 * <ol>
 *  <li> The Cascading sink step is a map-only flow (with no reducer). In this case, the Identity
 *  reducer will be forced to be used and the correct partitioner configured so that the
 *  tuples will be sinked to HFiles. </li>
 *  <li> The sink step is part of an flow with a reducer in which case the output will be routed
 *  to a temporary sequence file that a secondary M/R job will use to correct sort and store
 *  the data into HFiles for bulk loading </li>
 * </ol>
 *
 */
class HFileFlowStepStrategy extends FlowStepStrategy[JobConf] {

  override def apply(flow: Flow[JobConf],
                     predecessorSteps: List[FlowStep[JobConf]],
                     flowStep: FlowStep[JobConf]) {
    val conf = flowStep.getConfig()
    val outputFormat = conf.get("elephantbird.class.for.DeprecatedOutputFormatWrapper")
    val numReducers = conf.getNumReduceTasks()
    val hfOutputFormat = classOf[KijiHFileOutputFormat].getName()

    if (outputFormat != null && outputFormat == hfOutputFormat) {
      // If this is part of a map-only job, then we configure a reducer with the correct
      // partitioner.
      if (numReducers == 0) {
        conf.setPartitionerClass(classOf[TotalOrderPartitioner[HFileKeyValue, NullWritable]])
        conf.setReducerClass(classOf[IdentityReducer[HFileKeyValue, NullWritable]])

        conf.setMapOutputKeyClass(classOf[HFileKeyValue])
        conf.setMapOutputValueClass(classOf[NullWritable])
        conf.setOutputKeyComparatorClass(classOf[FastComparator])

        val outputURI = conf.get(KijiConfKeys.OUTPUT_KIJI_TABLE_URI)
        val kijiURI = KijiURI.newBuilder(outputURI).build()
        val splits = HFileMapReduceJobOutput.makeTableKeySplit(kijiURI, 0, conf)
        conf.setNumReduceTasks(splits.size())

        // Write the file that the TotalOrderPartitioner reads to determine where to partition
        // records.
        var partitionFilePath =
          new Path(conf.getWorkingDirectory(), TotalOrderPartitioner.DEFAULT_PATH)

        val fs = partitionFilePath.getFileSystem(conf)
        partitionFilePath = fs.makeQualified(partitionFilePath)
        HFileMapReduceJobOutput.writePartitionFile(conf, partitionFilePath, splits)
        val cacheUri =
          new URI(partitionFilePath.toString() + "#" + TotalOrderPartitioner.DEFAULT_PATH)
        DistributedCache.addCacheFile(cacheUri, conf)
        DistributedCache.createSymlink(conf)
      } else {
        // We use the temporary path that was configured by the job to dump the HFileKVs
        // for the second M/R job to process and then properly produce HFiles.
        val newOutputPath = new Path(conf.get(HFileKijiOutput.TEMP_HFILE_OUTPUT_KEY))
        conf.setOutputKeyClass(classOf[HFileKeyValue])
        conf.setOutputValueClass(classOf[NullWritable])

        conf.setOutputFormat(classOf[SequenceFileOutputFormat[HFileKeyValue, NullWritable]])
        FileOutputFormat.setOutputPath(conf, newOutputPath)
      }
    }
  }
}
