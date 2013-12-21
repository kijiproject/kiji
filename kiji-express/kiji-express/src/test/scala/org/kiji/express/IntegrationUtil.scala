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

package org.kiji.express

import com.twitter.scalding.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.kiji.express.flow.ExpressTool
import org.kiji.mapreduce.HFileLoader
import org.kiji.schema.KijiTable

/**
 * Provides helper functions for writing integration tests against scalding.
 */
object IntegrationUtil {

  /**
   * Run a job with the given arguments.
   * @param jobClass class of job
   * @param args to supply to job
   */
  def runJob(jobClass: Class[_ <: Job], args: String*): Unit =
    ExpressTool.main(jobClass.getName +: args.toArray)

  /**
   * Bulk load HFiles from the given path into the table specified with the uri and conf.
   *
   * @param path to HFiles
   * @param table to bulk-load into
   */
  def bulkLoadHFiles(path: String, conf: Configuration, table: KijiTable): Unit =
    HFileLoader.create(conf).load(new Path(path), table)
}
