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

package org.kiji.modeling.impl

import com.twitter.scalding.Args

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.annotations.Inheritance


/**
 * Provides access to command-line arguments used by the modeling SPI workflow phases (like Extract
 * and Score). After a concrete instance of this trait has been constructed, its arguments should be
 * added using the method `addArgs`.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Inheritance.Extensible
trait CommandLineArgs {
  /** Private field backing the command-line args accessor. */
  private var _args: Args = new Args(Map())

  /**
   * Contains the command-line arguments used for this model lifecycle phase. This may contain no
   * arguments if this phase was not executed from the command-line.
   *
   * @return the command-line arguments used for this model lifecycle phase.
   */
  def args: Args = _args

  /**
   * Adds another set of command-line arguments to the current set of arguments.
   *
   * @param otherArgs to add to the current set of arguments.
   */
  private[kiji] def addArgs(otherArgs: Args) {
    otherArgs
        .m
        .iterator
        .foreach { _args += _ }
  }

  /**
   * Adds another set of command-line arguments to the current set of arguments.
   *
   * @param cliString containing command-line arguments to add.
   */
  private[kiji] def addArgs(cliString: String) { addArgs(Args(cliString)) }
}
