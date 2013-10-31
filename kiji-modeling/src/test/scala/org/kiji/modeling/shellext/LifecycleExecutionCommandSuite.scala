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

package org.kiji.modeling.shellext

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LifecycleExecutionCommandSuite
    extends ShellExtSuite {
  test("A hadoop configuration can be obtained with key/value pairs set.") {
    // scalastyle:off null
    // Build a new lifecycle command.
    val command = new LifecycleExecutionCommand(
        lifecyclePhases = null,
        modelDefConfigureVia = null,
        modelEnvConfigureVia = null,
        jobsConfiguration = JobsConfiguration(Nil, Map("key1" -> "value1", "key2" -> "value2")),
        env = null)
    // scalastyle:on null
    val conf = command.hadoopConfiguration

    // Validate the command.
    assert("value1" === conf.get("key1"))
    assert("value2" === conf.get("key2"))
  }

  test("A hadoop configuration can be obtained with tmpjars set.") {
    // scalastyle:off null
    // Build a new lifecycle command.
    val command = new LifecycleExecutionCommand(
        lifecyclePhases = null,
        modelDefConfigureVia = null,
        modelEnvConfigureVia = null,
        jobsConfiguration = JobsConfiguration(List("path1.jar", "path2.jar"), Map()),
        env = null)
    // scalastyle:on null
    val conf = command.hadoopConfiguration
    val libjars = conf.getStrings("tmpjars")

    // Validate the command.
    assert(libjars(0).startsWith("file:///"))
    assert(libjars(1).startsWith("file:///"))
    assert(libjars(0).endsWith("path1.jar"))
    assert(libjars(1).endsWith("path2.jar"))
  }
}
