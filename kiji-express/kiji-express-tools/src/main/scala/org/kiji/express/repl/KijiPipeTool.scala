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

package org.kiji.express.repl

import cascading.flow.Flow
import cascading.pipe.Pipe
import com.twitter.scalding.Args
import com.twitter.scalding.Job
import com.twitter.scalding.Mode

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance
import org.kiji.express.flow.KijiJob

/**
 * Adds extra repl-specific functionality to KijiPipe, allowing the user to build
 * and run a scalding job and automatically making a copy of the repl available to
 * the job server.
 *
 * @param pipe enriched with extra functionality.
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
class KijiPipeTool(private[express] val pipe: Pipe) {
  /**
   * Gets a job that can be used to run the data pipeline.
   *
   * @param args that should be used to construct the job.
   * @return a job that can be used to run the data pipeline.
   */
  private[express] def getJob(args: Args): Job = new KijiJob(args) {
    // The job's constructor should evaluate to the pipe to run.
    pipe

    /**
     *  The flow definition used by this job, which should be the same as that used by the user
     *  when creating their pipe.
     */
    override implicit val flowDef = Implicits.flowDef

    /**
     * Obtains a configuration used when running the job.
     *
     * This overridden method uses the same configuration as a standard Scalding job,
     * but adds options specific to KijiExpress, including adding a jar containing compiled REPL
     * code to the distributed cache if the REPL is running.
     *
     * @return the configuration that should be used to run the job.
     */
    override def config: Map[AnyRef, AnyRef] = {
      // Use the configuration from Scalding Job as our base.
      val configuration = super.config

      /** Appends a comma to the end of a string. */
      def appendComma(str: Any): String = str.toString + ","

      // If the REPL is running, we should add tmpjars passed in from the command line,
      // and a jar of REPL code, to the distributed cache of jobs run through the REPL.
      val replCodeJar = ExpressShell.createReplCodeJar()
      val tmpJarsConfig =
        if (replCodeJar.isDefined) {
          Map("tmpjars" -> {
            // Use tmpjars already in the configuration.
            configuration
              .get("tmpjars")
              .map(appendComma)
              .getOrElse("") +
              // And tmpjars passed to ExpressShell from the command line when started.
              ExpressShell.tmpjars
                .map(appendComma)
                .getOrElse("") +
              // And a jar of code compiled by the REPL.
              "file://" + replCodeJar.get.getAbsolutePath
          })
        } else {
          // No need to add the tmpjars to the configuration
          Map[String, String]()
        }

      val userClassPathFirstConfig = Map("mapreduce.task.classpath.user.precedence" -> "true")

      configuration ++ tmpJarsConfig ++ userClassPathFirstConfig
    }

    /**
     * Builds a flow from the flow definition used when creating the pipeline run by this job.
     *
     * This overridden method operates the same as that of the super class,
     * but clears the implicit flow definition defined in [[org.kiji.express.repl.Implicits]]
     * after the flow has been built from the flow definition. This allows additional pipelines
     * to be constructed and run after the pipeline encapsulated by this job.
     *
     * @return the flow created from the flow definition.
     */
    override def buildFlow: Flow[_] = {
      val flow = super.buildFlow
      Implicits.resetFlowDef()
      flow
    }
  }

  /**
   * Runs this pipe as a Scalding job.
   */
  def run() {
    // Register a new mode for each job.
    val jobArgs = Mode.putMode(Implicits.mode, Args(Nil))
    getJob(jobArgs).run

    // Clear the REPL state after running a job.
    Implicits.resetFlowDef()
  }
}
