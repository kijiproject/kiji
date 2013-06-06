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

package org.kiji.express.shellext

class LifecycleExecutionCommandSuite extends ShellExtSuite {

  test("A hadoop configuration can be obtained with key/value pairs set.") {
    val command = new LifecycleExecutionCommand(null, null, null,
        JobsConfiguration(Nil, Map("key1" -> "value1", "key2" -> "value2")), null)
    val conf = command.hadoopConfiguration
    assert("value1" === conf.get("key1"))
    assert("value2" === conf.get("key2"))
  }

  test("A hadoop configuration can be obtained with tmpjars set.") {
    val command = new LifecycleExecutionCommand(null, null, null,
      JobsConfiguration(List("file:///path1.jar", "file:///path2.jar"), Map()), null)
    val conf = command.hadoopConfiguration
    val libjars = conf.getStrings("tmpjars")
    assert("file:///path1.jar" === libjars(0))
    assert("file:///path2.jar" === libjars(1))
  }
}
