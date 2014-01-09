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

import com.twitter.scalding.RichXHandler
import com.twitter.scalding.Tool
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.util.ToolRunner

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability

/**
 * The main-method entry point for running an Express job. Functionally the same as Scalding's
 * [[com.twitter.scalding.Tool.main]], but uses HBaseConfiguration to create the configuration,
 * so properties in any `hbase-site.xml` on the classpath will be included.
 */
@ApiAudience.Public
@ApiStability.Stable
object ExpressTool {
  def main(args: Array[String]) {
    try {
      ToolRunner.run(HBaseConfiguration.create(),  new Tool, args)
    } catch {
      case t: Throwable => {
        //create the exception URL link in GitHub wiki
        val gitHubLink = RichXHandler.createXUrl(t)
        val extraInfo = (if(RichXHandler().handlers.exists(h => h(t))) {
          RichXHandler.mapping(t.getClass) + "\n"
        }
        else {
          ""
        }) +
            "If you know what exactly caused this error, please consider contributing to" +
            " GitHub via following link.\n" + gitHubLink

        //re-throw the exception with extra info
        throw new Throwable(extraInfo, t)
      }
    }
  }
}
