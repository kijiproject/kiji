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

package org.kiji.schema.shell.util

final class PlusFourChild

/**
 * A program to be run in a child process.
 * This parses its argument as an integer, adds four to the value,
 * and returns this sum as its exit status code.
 */
object PlusFourChild {
  def main(argv: Array[String]) {
    val input: Int = argv(0).toInt
    val sum: Int = input + 4

    System.out.println("Input is: " + input)
    System.out.println("Output is: " + sum)

    // Return the sum as an exit status.
    // Note that if this were to run in the same process as unit tests,
    // a non-zero exit status would cause maven-surefire-plugin to fail.
    // The unit tests can only succeed if this is run a forked JVM.
    System.exit(sum)
  }
}
