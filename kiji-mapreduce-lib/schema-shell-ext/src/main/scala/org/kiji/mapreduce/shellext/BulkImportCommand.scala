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

package org.kiji.mapreduce.shellext

import java.io.IOException

import scala.collection.mutable.Map

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

import org.kiji.mapreduce.MapReduceJob
import org.kiji.mapreduce.MapReduceJobInput
import org.kiji.mapreduce.MapReduceJobOutput
import org.kiji.mapreduce.tools.framework.MapReduceJobInputFactory
import org.kiji.mapreduce.bulkimport.KijiBulkImporter
import org.kiji.mapreduce.bulkimport.KijiBulkImportJobBuilder
import org.kiji.schema.KijiURI
import org.kiji.schema.shell.DDLException
import org.kiji.schema.shell.Environment
import org.kiji.schema.shell.ddl.DDLCommand

/**
 * A complete specification of a bulk import job to run.
 *
 * @param env the environment in which the command executes.
 * @param className the name of the bulk importer class to execute.
 * @param fileUri the HDFS URI of the source data for the import.
 * @param format the string identifying the file format to use (for KijiMR's input spec).
 * @param tableName the name of the destination table.
 * @param via the properties controlling what directory this loads hfiles through, or
 *    whether it uses direct puts.
 * @param fieldMapping information that specifies how to map fields of the input data
 *    to columns in the destination table.
 * @param properties a collection of (key, value) pairs that should be used in the
 *    Configuration object submitted with the MapReduce job.
 */
class BulkImportCommand(
    val env: Environment,
    val className: String,
    val fileUri: String,
    val format: String,
    val tableName: String,
    val via: LoadVia,
    val fieldMapping: Option[FieldMapping],
    val properties: Map[String, String]) extends DDLCommand {

  /**
   * Construct a job input for the bulk import job.
   *
   * @return the MapReduceJobInput for the bulk import job.
   */
  private def makeJobInput(): MapReduceJobInput = {
    return MapReduceJobInputFactory.create()
        .fromSpaceSeparatedMap("format=" + format + " file=" + fileUri)
  }

  /**
   * Construct a job output for the bulk import job.
   *
   * @return the MapReduceJobOutput for the bulk import job.
   */
  private def makeJobOutput(): MapReduceJobOutput = {
    return via.outputToTable(KijiURI.newBuilder(env.instanceURI).withTableName(tableName).build())
  }

  /**
   * Creates a Configuration object to use for the MapReduce job running the bulk import.
   *
   * <p>This will include the necessary Hadoop and HBase resources. If <tt>fieldMapping</tt>
   * is not None, then this will create a field mapping JSON file and put it in HDFS; the
   * location of this file will be specified in the output Configuration, as will any
   * header mappings.</p>
   *
   * <p>Any mappings specified in <tt>properties</tt> will be included in this Configuration;
   * they may override any aspects of fieldMapping you would require, as they are applied
   * second.</p>
   *
   * @return a new Configuration object for running the bulk import job.
   */
  private def makeConf(): Configuration = {
    val conf: Configuration = HBaseConfiguration.create()

    if (!fieldMapping.isEmpty) {
      // Parse the field mapping descriptor to json, and point to it in this conf.
      fieldMapping.get.configureJson(conf, fileUri, tableName)
    }

    properties.foreach({ case (k, v) =>
      conf.set(k, v)
    })

    return conf
  }

  /** {@inheritDoc} */
  override def exec(): Environment = {

    val conf: Configuration = makeConf()
    val mrJob: MapReduceJob = KijiBulkImportJobBuilder.create()
        .withConf(conf)
        .withInput(makeJobInput())
        .withOutput(makeJobOutput())
        .withBulkImporter(Class.forName(className).asInstanceOf[Class[KijiBulkImporter[_,_]]])
        .build()

    echo("Running bulk import job to table [" + tableName + "] with class [" + className + "]")
    try {
      val success: Boolean = mrJob.run()
      if (!success) {
        echo ("Job failed!")
        throw new DDLException("Bulk import job failed")
      } else {
        // If a bulk import job must be followed by a bulk load, do that here.
        echo("Bulk import job succeeded.")
        echo("Running HBase load process...")
        via.runBulkLoad(conf,
            KijiURI.newBuilder(env.instanceURI).withTableName(tableName).build())
      }
    } catch { case ioe: IOException =>
      throw new DDLException("Bulk import job failed: " + ioe.getMessage())
    } finally {
      // Remove the JSON file we generated on our way out.
      if (!fieldMapping.isEmpty) {
        val fs: FileSystem = FileSystem.get(conf)
        echo("Removing field mapping file from HDFS...")
        fs.delete(fieldMapping.get.jsonFilename.makeQualified(fs), false)
      }
    }

    echo("Import complete.")
    return env
  }
}
